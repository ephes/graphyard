//! `InfluxWriter::write_points` against `fixtures/line_protocol.json`: every
//! `writes` case of the Python client replayed against a fake InfluxDB
//! (request line, query, headers, body, result, log records), plus the line
//! protocol read back with InfluxData's parser.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, Once};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use graphyard_core::model::{MetricPoint, Tags};
use graphyard_core::pycompat::py_float;
use graphyard_core::settings::Settings;
use graphyard_influx::InfluxWriter;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use influxdb_line_protocol::{FieldValue, parse_lines};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};

const FIXTURE: &str = include_str!("../../../fixtures/line_protocol.json");

fn fixture() -> Value {
    serde_json::from_str(FIXTURE).expect("fixture is JSON")
}

fn s<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key]
        .as_str()
        .unwrap_or_else(|| panic!("{key} is not a string in {value}"))
}

// ------------------------------------------------------------ log capture

thread_local! {
    static RECORDS: RefCell<Vec<(String, String)>> = const { RefCell::new(Vec::new()) };
}

/// Records `graphyard_influx` log records of the current thread
/// (`#[tokio::test]` runs the future on the test's thread).
struct Capture;

impl log::Log for Capture {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target().starts_with("graphyard_influx")
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            let level = match record.level() {
                log::Level::Error => "ERROR",
                log::Level::Warn => "WARNING",
                _ => "INFO",
            };
            RECORDS.with(|records| {
                records
                    .borrow_mut()
                    .push((level.to_owned(), record.args().to_string()));
            });
        }
    }

    fn flush(&self) {}
}

static CAPTURE: Capture = Capture;
static INIT: Once = Once::new();

fn take_records() -> Vec<(String, String)> {
    INIT.call_once(|| {
        log::set_logger(&CAPTURE).expect("one logger per test binary");
        log::set_max_level(log::LevelFilter::Trace);
    });
    RECORDS.with(|records| std::mem::take(&mut *records.borrow_mut()))
}

// ------------------------------------------------------------ fake servers

#[derive(Clone, Debug)]
struct Recorded {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: String,
}

#[derive(Clone, Default)]
struct Respond {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Respond {
    fn of(value: &Value) -> Self {
        Self {
            status: value
                .get("status")
                .and_then(Value::as_u64)
                .map_or(204, |status| u16::try_from(status).expect("status")),
            headers: value.get("headers").and_then(Value::as_array).map_or_else(
                Vec::new,
                |pairs| {
                    pairs
                        .iter()
                        .map(|pair| (s_at(pair, 0), s_at(pair, 1)))
                        .collect()
                },
            ),
            body: value
                .get("body")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        }
    }
}

fn s_at(pair: &Value, index: usize) -> String {
    pair[index].as_str().expect("pair item").to_owned()
}

/// The fake InfluxDB: records every request, answers with `respond`.
struct Recorder {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<Recorded>>>,
}

async fn recorder(respond: Respond) -> Recorder {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let requests = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&requests);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let log = Arc::clone(&log);
            let respond = respond.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| {
                    let log = Arc::clone(&log);
                    let respond = respond.clone();
                    async move {
                        let method = request.method().to_string();
                        let path = request
                            .uri()
                            .path_and_query()
                            .map(ToString::to_string)
                            .unwrap_or_default();
                        let headers = request
                            .headers()
                            .iter()
                            .map(|(name, value)| {
                                (
                                    name.as_str().to_owned(),
                                    value.to_str().expect("ASCII header").to_owned(),
                                )
                            })
                            .collect();
                        let body = request
                            .into_body()
                            .collect()
                            .await
                            .expect("body")
                            .to_bytes();
                        log.lock().expect("lock").push(Recorded {
                            method,
                            path,
                            headers,
                            body: String::from_utf8(body.to_vec()).expect("UTF-8 body"),
                        });
                        let mut response = Response::builder()
                            .status(StatusCode::from_u16(respond.status).expect("status"));
                        for (name, value) in &respond.headers {
                            response = response.header(name, value);
                        }
                        Ok::<_, Infallible>(
                            response
                                .body(Full::new(Bytes::from(respond.body.clone())))
                                .expect("response"),
                        )
                    }
                });
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    Recorder { addr, requests }
}

/// Accepts connections and never answers.
async fn stall() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let mut held: Vec<TcpStream> = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    addr
}

/// The byte-level answers of `gen_line_protocol.py:RAW_SCRIPTS`: status
/// line and headers, body chunks, pause between chunks, keep the
/// connection open afterwards.
fn raw_script(name: &str) -> (&'static [u8], Vec<&'static [u8]>, u64, bool) {
    match name {
        "truncated" => (
            b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\n",
            vec![b"abc"],
            0,
            false,
        ),
        "stall_body" => (
            b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n",
            vec![],
            0,
            true,
        ),
        "drip" => (
            b"HTTP/1.1 400 Bad Request\r\nContent-Length: 6\r\n\r\n",
            vec![b"s", b"l", b"o", b"w", b"l", b"y"],
            70,
            false,
        ),
        "not_utf8" => (
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n",
            vec![b"\xff\xfe"],
            0,
            false,
        ),
        other => panic!("unknown raw script {other}"),
    }
}

/// Reads one request (head and `Content-Length` body), then plays the
/// script byte by byte.
async fn raw(script: &str) -> SocketAddr {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (head, chunks, pause, hold) = raw_script(script);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let mut held: Vec<TcpStream> = Vec::new();
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut data = Vec::new();
            let mut buf = [0_u8; 65536];
            let mut expected = None;
            loop {
                if let Some(total) = expected
                    && data.len() >= total
                {
                    break;
                }
                let read = stream.read(&mut buf).await.unwrap_or(0);
                if read == 0 {
                    break;
                }
                data.extend_from_slice(&buf[..read]);
                if expected.is_none()
                    && let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n")
                {
                    let head_text = String::from_utf8_lossy(&data[..end]).to_ascii_lowercase();
                    let length = head_text
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .map_or(0, |value| value.trim().parse::<usize>().expect("length"));
                    expected = Some(end + 4 + length);
                }
            }
            let _ = stream.write_all(head).await;
            for chunk in &chunks {
                tokio::time::sleep(std::time::Duration::from_millis(pause)).await;
                let _ = stream.write_all(chunk).await;
            }
            if hold {
                held.push(stream);
            }
        }
    });
    addr
}

/// A port nothing listens on.
async fn closed() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    listener.local_addr().expect("addr")
}

// ------------------------------------------------------------ cases

fn point_of(value: &Value) -> MetricPoint {
    let optional = |key: &str| value[key].as_str().map(str::to_owned);
    MetricPoint {
        ts: DateTime::parse_from_rfc3339(s(value, "ts"))
            .expect("ts")
            .with_timezone(&Utc),
        metric: s(value, "metric").to_owned(),
        value: py_float(s(value, "value")).expect("float repr"),
        subject_type: s(value, "subject_type").to_owned(),
        subject_id: s(value, "subject_id").to_owned(),
        source_system: s(value, "source_system").to_owned(),
        collector_service: s(value, "collector_service").to_owned(),
        collector_host: s(value, "collector_host").to_owned(),
        source_instance: s(value, "source_instance").to_owned(),
        source_entity_id: optional("source_entity_id"),
        host: optional("host"),
        service: optional("service"),
        tags: value["tags"].as_array().map(|pairs| {
            pairs
                .iter()
                .map(|pair| (s_at(pair, 0), s_at(pair, 1)))
                .collect::<Tags>()
        }),
    }
}

async fn run_case(writer: &InfluxWriter, case: &Value) {
    let name = s(case, "name");
    let recorder = recorder(Respond::of(&case["respond"])).await;
    let stall = stall().await;
    let closed = closed().await;
    let raw_addr = match case["respond"].get("raw").and_then(Value::as_str) {
        Some(script) => raw(script).await,
        None => closed,
    };
    let env: BTreeMap<String, String> = case["env"]
        .as_object()
        .expect("env")
        .iter()
        .map(|(key, value)| {
            let value = value
                .as_str()
                .expect("env value")
                .replace("{server_noscheme}", &recorder.addr.to_string())
                .replace("{server}", &format!("http://{}", recorder.addr))
                .replace("{closed}", &format!("http://{closed}"))
                .replace("{stall}", &format!("http://{stall}"))
                .replace("{raw}", &format!("http://{raw_addr}"));
            (key.clone(), value)
        })
        .collect();
    let settings = Settings::from_lookup(|key| env.get(key).cloned()).expect("settings");
    let points: Vec<MetricPoint> = case["points"]
        .as_array()
        .expect("points")
        .iter()
        .map(point_of)
        .collect();

    take_records();
    let result = writer.write_points(&settings, &points).await;
    let records = take_records();

    let expected = &case["result"];
    match (expected.get("written"), &result) {
        (Some(written), Ok(actual)) => {
            assert_eq!(Some(*actual), written.as_u64(), "{name}");
        }
        (None, Err(err)) => {
            let error = &expected["error"];
            assert_eq!(err.python_class(), s(error, "class"), "{name}: {err}");
            if error.get("p2").is_none() {
                assert_eq!(err.to_string(), s(error, "message"), "{name}");
            }
        }
        _ => panic!("{name}: expected {expected}, got {result:?}"),
    }

    let requests = recorder.requests.lock().expect("lock").clone();
    let expected_requests = case["requests"].as_array().expect("requests");
    assert_eq!(
        requests.len(),
        expected_requests.len(),
        "{name}: {requests:?}"
    );
    for (actual, expected) in requests.iter().zip(expected_requests) {
        assert_eq!(actual.method, s(expected, "method"), "{name}");
        assert_eq!(actual.path, s(expected, "path"), "{name}");
        assert_eq!(actual.body, s(expected, "body"), "{name}");
        let expected_headers: BTreeMap<String, String> = expected["headers"]
            .as_array()
            .expect("headers")
            .iter()
            .map(|pair| (s_at(pair, 0).to_ascii_lowercase(), s_at(pair, 1)))
            .collect();
        let mut actual_headers = actual.headers.clone();
        actual_headers.remove("host");
        assert_eq!(actual_headers, expected_headers, "{name}");
    }

    let expected_records: Vec<(String, String)> = case["logs"]
        .as_array()
        .expect("logs")
        .iter()
        .map(|record| {
            let message = record
                .get("d10_message")
                .unwrap_or(&record["message"])
                .as_str()
                .expect("message");
            (s(record, "level").to_owned(), message.to_owned())
        })
        .collect();
    let records: Vec<(String, String)> = records
        .into_iter()
        .filter(|(_, message)| !message.starts_with("SyntaxWarning: "))
        .collect();
    assert_eq!(records, expected_records, "{name}");
}

#[tokio::test]
async fn write_points_replays_the_python_client() {
    let writer = InfluxWriter::new().expect("client");
    let fixture = fixture();
    let cases = fixture["writes"].as_array().expect("writes");
    assert!(cases.len() > 40);
    for case in cases {
        run_case(&writer, case).await;
    }
}

#[tokio::test]
async fn hash_measurement_warns_once() {
    let writer = InfluxWriter::new().expect("client");
    let recorder = recorder(Respond {
        status: 204,
        ..Respond::default()
    })
    .await;
    let url = format!("http://{}", recorder.addr);
    let env = [
        ("INFLUX_URL", url.as_str()),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
        ("INFLUX_MEASUREMENT", "#m"),
    ];
    let settings = Settings::from_lookup(|key| {
        env.iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| (*value).to_owned())
    })
    .expect("settings");
    let point = point_of(&fixture()["writes"][1]["points"][0]);
    take_records();
    for _ in 0..2 {
        assert_eq!(
            writer
                .write_points(&settings, std::slice::from_ref(&point))
                .await
                .unwrap(),
            1
        );
    }
    let warnings: Vec<_> = take_records()
        .into_iter()
        .filter(|(_, message)| message.starts_with("SyntaxWarning: "))
        .collect();
    // Once per process: another test may have warned first.
    assert!(warnings.len() <= 1, "{warnings:?}");
    assert_eq!(recorder.requests.lock().unwrap().len(), 2);
}

#[test]
fn written_lines_parse_as_line_protocol() {
    let fixture = fixture();
    let case = fixture["writes"]
        .as_array()
        .expect("writes")
        .iter()
        .find(|case| s(case, "name").starts_with("custom tags"))
        .expect("custom tags case");
    let body = s(&case["requests"][0], "body");
    let lines: Vec<_> = parse_lines(body)
        .collect::<Result<_, _>>()
        .expect("valid line protocol");
    assert_eq!(lines.len(), 1);
    let line = &lines[0];
    assert_eq!(line.series.measurement.as_str(), "graphyard_metrics");
    let tag = |key: &str| line.tag_value(key).map(|value| value.as_str().to_owned());
    assert_eq!(tag("spec_name").as_deref(), Some("My Spec, v=2"));
    // The space the client appends after a trailing backslash is read back
    // as an escaped space: InfluxDB stores `dir `, as with the Python client.
    assert_eq!(tag("trailing").as_deref(), Some("dir "));
    // Line protocol has no `\t`/`\n` escapes: InfluxDB stores the
    // client's two-character escapes literally.
    assert_eq!(tag("tab\\tkey").as_deref(), Some("new\\nline"));
    assert_eq!(tag("host").as_deref(), Some("macmini"));
    assert_eq!(tag("metric").as_deref(), Some("host.cpu_usage"));
    assert_eq!(tag("empty"), None);
    assert_eq!(line.field_value("value"), Some(&FieldValue::F64(0.5)));
    assert_eq!(line.timestamp, Some(1_792_570_530_123_456_000));
}

fn v2_settings(url: &str, timeout_ms: &str) -> Settings {
    let env = [
        ("INFLUX_URL", url),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
        ("INFLUX_TIMEOUT_MS", timeout_ms),
    ];
    Settings::from_lookup(|key| {
        env.iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| (*value).to_owned())
    })
    .expect("settings")
}

#[tokio::test]
async fn transport_errors_use_the_urllib3_forms() {
    let writer = InfluxWriter::new().expect("client");
    let point = point_of(&fixture()["writes"][1]["points"][0]);
    let points = std::slice::from_ref(&point);

    let closed = closed().await;
    let err = writer
        .write_points(&v2_settings(&format!("http://{closed}"), "1000"), points)
        .await
        .unwrap_err();
    assert_eq!(err.python_class(), "NewConnectionError");
    let prefix = format!(
        "HTTPConnection(host='127.0.0.1', port={}): Failed to establish a new connection: [Errno ",
        closed.port()
    );
    assert!(err.to_string().starts_with(&prefix), "{err}");
    assert!(err.to_string().ends_with("] Connection refused"), "{err}");

    let stall = stall().await;
    let err = writer
        .write_points(&v2_settings(&format!("http://{stall}"), "150"), points)
        .await
        .unwrap_err();
    assert_eq!(err.python_class(), "ReadTimeoutError");
    assert_eq!(
        err.to_string(),
        format!(
            "HTTPConnectionPool(host='127.0.0.1', port={}): Read timed out. (read timeout=0.15)",
            stall.port()
        )
    );

    let recorder = recorder(Respond {
        status: 400,
        headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
        body: "{\"code\":\"invalid\"}".to_owned(),
    })
    .await;
    let err = writer
        .write_points(
            &v2_settings(&format!("http://{}", recorder.addr), "1000"),
            points,
        )
        .await
        .unwrap_err();
    assert_eq!(err.python_class(), "ApiException");
    let text = err.to_string();
    assert!(
        text.starts_with("(400)\nReason: Bad Request\nHTTP response headers: HTTPHeaderDict({"),
        "{text}"
    );
    assert!(
        text.contains("'content-type': 'application/json'"),
        "{text}"
    );
    assert!(
        text.ends_with("})\nHTTP response body: {\"code\":\"invalid\"}\n"),
        "{text}"
    );
}

/// A TLS server with a self-signed certificate the system store does not
/// trust: the handshake fails, as urllib3's `SSLError`, which the Python
/// client re-raises as `ApiException(status=0)`.
#[tokio::test]
async fn tls_failures_are_the_client_api_exception() {
    use std::sync::Arc as StdArc;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

    graphyard_influx::install_crypto_provider();
    let certified = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()]).expect("cert");
    let cert = CertificateDer::from(certified.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .expect("server config");
    let acceptor = TlsAcceptor::from(StdArc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let _ = acceptor.accept(stream).await;
            });
        }
    });

    let writer = InfluxWriter::new().expect("client");
    let point = point_of(&fixture()["writes"][1]["points"][0]);
    let err = writer
        .write_points(
            &v2_settings(&format!("https://{addr}"), "2000"),
            std::slice::from_ref(&point),
        )
        .await
        .unwrap_err();
    assert_eq!(err.python_class(), "ApiException", "{err}");
    let text = err.to_string();
    assert!(text.starts_with("(0)\nReason: SSLError\n"), "{text}");
    assert!(text.to_ascii_lowercase().contains("certificate"), "{text}");
    assert!(text.ends_with('\n'), "{text}");
}

/// A server that reads the ClientHello and closes: the handshake ends with
/// an EOF, which Python's ssl module reports as an `SSLError` as well.
#[tokio::test]
async fn tls_handshake_eof_is_the_client_api_exception() {
    use tokio::io::AsyncReadExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0_u8; 4096];
            let _ = stream.read(&mut buf).await;
            drop(stream);
        }
    });
    let writer = InfluxWriter::new().expect("client");
    let point = point_of(&fixture()["writes"][1]["points"][0]);
    let err = writer
        .write_points(
            &v2_settings(&format!("https://{addr}"), "2000"),
            std::slice::from_ref(&point),
        )
        .await
        .unwrap_err();
    assert_eq!(err.python_class(), "ApiException", "{err}");
    let text = err.to_string();
    assert!(
        text.starts_with("(0)\nReason: SSLError\nEOF occurred in violation of protocol"),
        "{text}"
    );
}
