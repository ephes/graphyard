//! Shared helpers of the query tests: fixtures, a fake InfluxDB that replays
//! the generator's scripted responses, a capturing logger and comparisons.

#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Write as _;
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use graphyard_core::MetricSample;
use graphyard_core::pycompat::py_isoformat_utc;
use graphyard_core::settings::Settings;
use graphyard_influx::{QueryClient, QueryError, QueryRange};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

pub fn load(text: &str) -> Value {
    serde_json::from_str(text).expect("fixture is valid JSON")
}

pub fn list<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value[key]
        .as_array()
        .unwrap_or_else(|| panic!("fixture field {key:?} is not a list"))
}

pub fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key]
        .as_str()
        .unwrap_or_else(|| panic!("fixture field {key:?} is not a string in {value}"))
}

/// The value of a fixture setting as environment text (numbers keep their
/// token, so huge integers survive).
fn env_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

// --------------------------------------------------------------------------
// Logging
// --------------------------------------------------------------------------

thread_local! {
    static RECORDS: RefCell<Option<Vec<(String, String)>>> = const { RefCell::new(None) };
}

struct CapturingLogger;

impl log::Log for CapturingLogger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        if !record.target().starts_with("graphyard_influx") {
            return;
        }
        let level = match record.level() {
            log::Level::Error => "ERROR",
            log::Level::Warn => "WARNING",
            log::Level::Info => "INFO",
            _ => return,
        };
        RECORDS.with(|records| {
            if let Some(records) = records.borrow_mut().as_mut() {
                records.push((level.to_owned(), record.args().to_string()));
            }
        });
    }

    fn flush(&self) {}
}

/// Start capturing the log lines this thread emits (the tests run on a
/// current-thread runtime, so the query code logs on the test thread).
pub fn capture_logs() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        log::set_logger(&CapturingLogger).expect("logger installs once");
        log::set_max_level(log::LevelFilter::Info);
    });
    RECORDS.with(|records| *records.borrow_mut() = Some(Vec::new()));
}

/// The log lines captured since [`capture_logs`].
pub fn take_logs() -> Vec<(String, String)> {
    RECORDS.with(|records| records.borrow_mut().take().unwrap_or_default())
}

/// The case's log lines as Rust writes them: `{server}` filled in, `\n` and
/// `\r` in interpolated values escaped (D-10; the Python lines here carry
/// them only inside values).
pub fn expected_logs(case: &Value, server: &str) -> Vec<(String, String)> {
    list(case, "logs")
        .iter()
        .map(|line| {
            let line = line.as_array().expect("log line pair");
            (
                line[0].as_str().expect("level").to_owned(),
                line[1]
                    .as_str()
                    .expect("message")
                    .replace("{server}", server)
                    .replace('\n', "\\n")
                    .replace('\r', "\\r"),
            )
        })
        .collect()
}

// --------------------------------------------------------------------------
// Fake InfluxDB
// --------------------------------------------------------------------------

/// A request as the fake server recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Recorded {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

const RECORDED_HEADERS: [&str; 5] = [
    "accept",
    "accept-encoding",
    "authorization",
    "content-type",
    "user-agent",
];

pub struct FakeInflux {
    pub port: u16,
    pub requests: Arc<Mutex<Vec<Recorded>>>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeInflux {
    pub async fn start(scripts: &[Value]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("address").port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let scripts = scripts.to_vec();
        let task = tokio::spawn(async move {
            for script in scripts {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                answer(stream, &script, &recorded).await;
            }
        });
        Self {
            port,
            requests,
            task,
        }
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn server(&self) -> String {
        format!("127.0.0.1:{}", self.port)
    }

    pub fn recorded(&self) -> Vec<Recorded> {
        self.requests.lock().expect("requests").clone()
    }
}

impl Drop for FakeInflux {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A port nothing listens on.
pub fn closed_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("address").port()
}

fn script_body(script: &Value) -> Vec<u8> {
    if let Some(encoded) = script.get("body_b64").and_then(Value::as_str) {
        return decode_base64(encoded);
    }
    script
        .get("body")
        .and_then(Value::as_str)
        .unwrap_or("")
        .as_bytes()
        .to_vec()
}

fn decode_base64(text: &str) -> Vec<u8> {
    graphyard_core::pycompat::py_b64decode(text).expect("fixture base64")
}

async fn answer(mut stream: TcpStream, script: &Value, recorded: &Mutex<Vec<Recorded>>) {
    let mut data = Vec::new();
    let mut buffer = [0u8; 65536];
    let head_end = loop {
        if let Some(at) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            break at;
        }
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => return,
            Ok(n) => data.extend_from_slice(&buffer[..n]),
        }
    };
    let head = String::from_utf8_lossy(&data[..head_end]).into_owned();
    let mut body = data[head_end + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_owned();
    let mut parts = request_line.splitn(3, ' ');
    let method = parts.next().unwrap_or_default().to_owned();
    let target = parts.next().unwrap_or_default().to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(": "))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.to_owned()))
        .collect();
    let length = headers
        .iter()
        .find(|(k, _)| k == "content-length")
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);
    while body.len() < length {
        match stream.read(&mut buffer).await {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&buffer[..n]),
        }
    }
    recorded.lock().expect("requests").push(Recorded {
        method,
        target,
        headers: headers
            .into_iter()
            .filter(|(k, _)| RECORDED_HEADERS.contains(&k.as_str()))
            .collect(),
        body: String::from_utf8(body).expect("request body is UTF-8"),
    });
    if script.get("action").and_then(Value::as_str) == Some("close") {
        return;
    }
    let seconds =
        |key: &str| Duration::from_secs_f64(script.get(key).and_then(Value::as_f64).unwrap_or(0.0));
    tokio::time::sleep(seconds("header_delay")).await;
    let mut payload = script_body(script);
    let mut headers: Vec<(String, String)> = script
        .get("headers")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|pair| {
                    (
                        pair[0].as_str().expect("name").to_owned(),
                        pair[1].as_str().expect("value").to_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    if script.get("gzip").and_then(Value::as_bool) == Some(true) {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&payload).expect("gzip");
        payload = encoder.finish().expect("gzip");
        headers.push(("Content-Encoding".to_owned(), "gzip".to_owned()));
    }
    let chunks: Vec<Vec<u8>> = if let Some(cut) = script.get("split").and_then(Value::as_u64) {
        let cut = usize::try_from(cut).expect("split").min(payload.len());
        vec![payload[..cut].to_vec(), payload[cut..].to_vec()]
    } else if script.get("drip").and_then(Value::as_bool) == Some(true) {
        payload.iter().map(|byte| vec![*byte]).collect()
    } else {
        vec![payload.clone()]
    };
    let length = script
        .get("content_length")
        .and_then(Value::as_u64)
        .map_or(payload.len(), |n| usize::try_from(n).expect("length"));
    headers.push(("Content-Length".to_owned(), length.to_string()));
    headers.push(("Connection".to_owned(), "close".to_owned()));
    let mut head = format!(
        "HTTP/1.1 {} {}\r\n",
        script["status"].as_u64().expect("status"),
        text(script, "reason")
    );
    for (name, value) in &headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    if stream.write_all(head.as_bytes()).await.is_err() {
        return;
    }
    for (index, chunk) in chunks.iter().enumerate() {
        if index > 0 {
            tokio::time::sleep(seconds("chunk_delay")).await;
        }
        if stream.write_all(chunk).await.is_err() {
            return;
        }
        let _ = stream.flush().await;
    }
    let _ = stream.shutdown().await;
}

// --------------------------------------------------------------------------
// Running a fixture case
// --------------------------------------------------------------------------

/// The client for a case: the generator's settings with `INFLUX_URL`
/// pointing at the fake server (or as the case spells it).
pub fn client_for(case: &Value, server: &FakeInflux) -> QueryClient {
    let mut env: HashMap<String, String> = case["settings"]
        .as_object()
        .expect("settings")
        .iter()
        .map(|(k, v)| (k.clone(), env_text(v)))
        .collect();
    let url = if case.get("refused").and_then(Value::as_bool) == Some(true) {
        format!("http://127.0.0.1:{}", closed_port())
    } else if let Some(url) = case.get("url").and_then(Value::as_str) {
        url.replace("{server}", &server.server())
    } else {
        server.url()
    };
    env.insert("INFLUX_URL".to_owned(), url);
    let settings = Settings::from_lookup(|name| env.get(name).cloned()).expect("settings");
    let mut client = QueryClient::new(&settings);
    if let Some(patch) = case.get("patch").and_then(Value::as_object) {
        let total = patch
            .get("SQL_QUERY_TOTAL_TIMEOUT_SECONDS")
            .and_then(Value::as_f64)
            .map_or(
                graphyard_influx::SQL_QUERY_TOTAL_TIMEOUT,
                Duration::from_secs_f64,
            );
        let cap = patch
            .get("SQL_QUERY_MAX_BODY_BYTES")
            .and_then(Value::as_u64)
            .map_or(graphyard_influx::SQL_QUERY_MAX_BODY_BYTES, |n| {
                usize::try_from(n).expect("cap")
            });
        client = client.with_sql_limits(total, cap);
    }
    client
}

/// The `query_range` call of the generator's end-to-end cases.
pub fn e2e_range(case: &Value) -> QueryRange {
    let call = case.get("call").cloned().unwrap_or(Value::Null);
    let string = |key: &str| call.get(key).and_then(Value::as_str).map(str::to_owned);
    let start = chrono::DateTime::parse_from_rfc3339("2026-10-21T11:00:00Z")
        .expect("start")
        .to_utc();
    let stop = chrono::DateTime::parse_from_rfc3339("2026-10-21T12:00:00.5Z")
        .expect("stop")
        .to_utc();
    let mut range = QueryRange::new(
        string("metric").unwrap_or_else(|| "host.disk_usage".to_owned()),
        start,
        stop,
    );
    range.host = string("host");
    range.service = string("service");
    range.subject_type = string("subject_type");
    range.subject_id = string("subject_id");
    if let Some(tags) = call.get("tags").and_then(Value::as_object) {
        for (key, value) in tags {
            range
                .tags
                .insert(key.clone(), value.as_str().expect("tag").to_owned());
        }
    }
    range
}

fn sample_json(sample: &MetricSample) -> Value {
    let tags: serde_json::Map<String, Value> = sample
        .tags
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    serde_json::json!({
        "ts": py_isoformat_utc(&sample.ts),
        "value": sample.value,
        "host": sample.host,
        "metric": sample.metric,
        "service": sample.service,
        "subject_type": sample.subject_type,
        "subject_id": sample.subject_id,
        "source_system": sample.source_system,
        "source_instance": sample.source_instance,
        "source_entity_id": sample.source_entity_id,
        "collector_service": sample.collector_service,
        "collector_host": sample.collector_host,
        "tags": Value::Object(tags),
    })
}

/// Compare one sample field by field (numbers as floats, tags in order).
fn assert_sample(name: &str, actual: &MetricSample, expected: &Value) {
    let actual = sample_json(actual);
    for (key, expected_value) in expected.as_object().expect("sample") {
        let actual_value = &actual[key];
        if key == "value" {
            let expected = expected_value.as_f64().expect("value");
            let actual = actual_value.as_f64().expect("value");
            assert!(
                expected.to_bits() == actual.to_bits(),
                "{name}: value {actual} != {expected}"
            );
        } else if key == "tags" {
            let pairs = |value: &Value| -> Vec<(String, String)> {
                value
                    .as_object()
                    .expect("tags")
                    .iter()
                    .map(|(k, v)| (k.clone(), v.as_str().expect("tag").to_owned()))
                    .collect()
            };
            assert_eq!(pairs(actual_value), pairs(expected_value), "{name}: tags");
        } else {
            assert_eq!(actual_value, expected_value, "{name}: {key}");
        }
    }
}

/// Compare a query result with the case's `samples` or `error`.
pub fn assert_result(
    case: &Value,
    actual: &Result<Vec<MetricSample>, QueryError>,
    server: &str,
    expected_error: Option<(&str, &str)>,
) {
    let name = text(case, "name");
    if let Some((class, message)) = expected_error {
        let err = actual.as_ref().expect_err(name);
        assert_eq!(err.python_class(), class, "{name}");
        assert_eq!(err.to_string(), message, "{name}");
        return;
    }
    match case.get("error") {
        Some(error) => {
            let err = actual
                .as_ref()
                .err()
                .unwrap_or_else(|| panic!("{name}: expected {error}, got {actual:?}"));
            assert_eq!(err.python_class(), text(error, "class"), "{name}: {err}");
            if error.get("p2").is_none() {
                assert_eq!(
                    err.to_string(),
                    text(error, "message").replace("{server}", server),
                    "{name}"
                );
            }
        }
        None => {
            let samples = actual
                .as_ref()
                .unwrap_or_else(|err| panic!("{name}: {} {err}", err.python_class()));
            let expected = list(case, "samples");
            assert_eq!(samples.len(), expected.len(), "{name}: {samples:?}");
            for (sample, expected) in samples.iter().zip(expected) {
                assert_sample(name, sample, expected);
            }
        }
    }
}

/// Compare the recorded requests (headers as a set: Rust's header order is
/// not Python's, P2).
pub fn assert_requests(case: &Value, actual: &[Recorded]) {
    let name = text(case, "name");
    let expected: Vec<Recorded> = list(case, "requests")
        .iter()
        .map(|request| {
            let mut headers: Vec<(String, String)> = list(request, "headers")
                .iter()
                .map(|pair| {
                    (
                        pair[0].as_str().expect("name").to_owned(),
                        pair[1].as_str().expect("value").to_owned(),
                    )
                })
                .collect();
            headers.sort();
            Recorded {
                method: text(request, "method").to_owned(),
                target: text(request, "target").to_owned(),
                headers,
                body: text(request, "body").to_owned(),
            }
        })
        .collect();
    let actual: Vec<Recorded> = actual
        .iter()
        .map(|request| {
            let mut request = request.clone();
            request.headers.sort();
            request
        })
        .collect();
    assert_eq!(actual, expected, "{name}");
}
