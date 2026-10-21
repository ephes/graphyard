//! Replay of the fixtures' executor scenarios: a scripted fake server per
//! case, a recording sink, and the comparison with what Python did.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};

use serde_json::Value;

use graphyard_core::model::MetricPoint;
use graphyard_core::pycompat::json_loads;

use crate::executors::{ExecutorResult, MetricSink, SinkError};
use crate::http::HttpContext;
use crate::http::status::standard_reason_phrase;
use crate::http::tests::logs;
use crate::http::tests::server::{Reply, Server};

/// The request headers the fixtures record.
const RECORDED_HEADERS: [&str; 6] = [
    "accept",
    "content-type",
    "content-length",
    "x-csrf-token",
    "cookie",
    "authorization",
];

/// A string field of a case.
pub(super) fn text<'a>(case: &'a Value, key: &str) -> &'a str {
    case[key]
        .as_str()
        .unwrap_or_else(|| panic!("field {key} is a string in {case}"))
}

/// Decode a `body_hex` field.
pub(super) fn hex_bytes(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("hex digit pair"))
        .collect()
}

/// The sink calls of one execution.
#[derive(Clone, Debug)]
pub(super) struct SinkCall {
    pub call: &'static str,
    pub points: Vec<MetricPoint>,
}

/// Stands in for the InfluxDB writer and the registry, like the fixture
/// generator's `_Sink`: records each call, returns `len(points)` or raises
/// `RuntimeError(<text>)`.
#[derive(Default)]
pub(super) struct RecordingSink {
    pub write_error: Option<String>,
    pub registry_error: Option<String>,
    pub calls: RefCell<Vec<SinkCall>>,
}

impl RecordingSink {
    fn from_case(case: &Value) -> Self {
        let error = |key: &str| case["sink"][key].as_str().map(str::to_owned);
        Self {
            write_error: error("write_error"),
            registry_error: error("registry_error"),
            calls: RefCell::new(Vec::new()),
        }
    }

    fn record(&self, call: &'static str, points: &[MetricPoint]) {
        self.calls.borrow_mut().push(SinkCall {
            call,
            points: points.to_vec(),
        });
    }
}

impl MetricSink for RecordingSink {
    fn write_points(&self, points: &[MetricPoint]) -> impl Future<Output = Result<u64, SinkError>> {
        self.record("write", points);
        let result = match &self.write_error {
            Some(message) => Err(SinkError::new("RuntimeError", message.clone())),
            None => Ok(points.len() as u64),
        };
        async move { result }
    }

    fn touch_registry(
        &self,
        points: &[MetricPoint],
    ) -> impl Future<Output = Result<(), SinkError>> {
        self.record("touch", points);
        let result = match &self.registry_error {
            Some(message) => Err(SinkError::new("RuntimeError", message.clone())),
            None => Ok(()),
        };
        async move { result }
    }
}

/// Replace the generator's origin with the fake server's.
pub(super) struct Localise {
    from: String,
    to: String,
}

impl Localise {
    pub fn text(&self, text: &str) -> String {
        text.replace(&self.from, &self.to)
    }

    fn json(&self, value: &Value) -> Value {
        let text = serde_json::to_string(value).expect("serialise");
        json_loads(&self.text(&text)).expect("localised JSON")
    }
}

fn encode_reply(reply: &Value, localise: &Localise) -> Reply {
    let status = reply["status"].as_u64().expect("status");
    let code = u16::try_from(status).expect("status code");
    let body = hex_bytes(text(reply, "body_hex"));
    let mut head = format!("HTTP/1.1 {status} {}\r\n", standard_reason_phrase(code));
    for pair in reply["headers"].as_array().expect("headers") {
        let name = pair[0].as_str().expect("header name");
        let value = localise.text(pair[1].as_str().expect("header value"));
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    let mut bytes = head.into_bytes();
    bytes.extend_from_slice(&body);
    Reply::Raw(bytes)
}

/// A server answering each route's replies in order (the last repeats),
/// 404 for anything else, like the generator's `MockTransport` handler.
/// Replies are encoded once the server's origin is known.
async fn scripted_server(case: &Value, origin: &str) -> (Server, Localise) {
    let routes: HashMap<String, Vec<Value>> = case["routes"]
        .as_object()
        .expect("routes")
        .iter()
        .map(|(target, replies)| (target.clone(), replies.as_array().expect("replies").clone()))
        .collect();
    let server_origin: Arc<OnceLock<String>> = Arc::new(OnceLock::new());
    let counters: Mutex<HashMap<String, usize>> = Mutex::new(HashMap::new());
    let from = origin.to_owned();
    let handler_origin = Arc::clone(&server_origin);
    let server = Server::start(move |request| {
        let Some(replies) = routes.get(&request.target) else {
            return Reply::Raw(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_vec(),
            );
        };
        let mut counters = counters.lock().expect("counters");
        let index = counters.entry(request.target.clone()).or_insert(0);
        let reply = &replies[(*index).min(replies.len() - 1)];
        *index += 1;
        let localise = Localise {
            from: from.clone(),
            to: handler_origin.get().expect("origin known").clone(),
        };
        encode_reply(reply, &localise)
    })
    .await;
    server_origin.set(server.origin()).expect("origin set once");
    let localise = Localise {
        from: origin.to_owned(),
        to: server.origin(),
    };
    (server, localise)
}

/// The executor under test.
pub(super) type Run = for<'a> fn(
    &'a HttpContext,
    &'a RecordingSink,
    &'a str,
    &'a Value,
) -> std::pin::Pin<Box<dyn Future<Output = ExecutorResult> + 'a>>;

/// Run one scenario and compare everything the fixture recorded.
pub(super) async fn replay(case: &Value, origin: &str, run: Run) {
    let name = text(case, "name");
    let (server, localise) = scripted_server(case, origin).await;
    let config = localise.json(&case["config"]);
    let sink = RecordingSink::from_case(case);
    logs::capture();
    let result = run(&HttpContext::new(), &sink, text(case, "spec_name"), &config).await;

    // Outcome or raised exception.
    let p2 = case["p2"].as_bool().unwrap_or(false);
    match (case.get("outcome"), &result) {
        (Some(expected), Ok(outcome)) => {
            assert_eq!(outcome.status.as_str(), expected[0], "status of {name}");
            assert_eq!(outcome.ingested, expected[1], "ingested of {name}");
            assert_eq!(outcome.skipped, expected[2], "skipped of {name}");
            if !p2 {
                let message = localise.text(expected[3].as_str().expect("message"));
                assert_eq!(outcome.message, message, "message of {name}");
            }
        }
        (None, Err(err)) => {
            let expected = &case["raised"];
            assert_eq!(
                err.python_class(),
                text(expected, "class"),
                "class of {name}"
            );
            assert_eq!(
                err.to_string(),
                text(expected, "message"),
                "message of {name}"
            );
        }
        (expected, actual) => panic!("{name}: expected {expected:?}, got {actual:?}"),
    }

    // Requests on the wire.
    let requests = server.requests();
    let expected_requests = case["requests"].as_array().expect("requests");
    assert_eq!(
        requests.len(),
        expected_requests.len(),
        "request count of {name}"
    );
    for (request, expected) in requests.iter().zip(expected_requests) {
        assert_eq!(request.method, text(expected, "method"), "{name}");
        assert_eq!(request.target, text(expected, "target"), "{name}");
        let headers: Vec<(String, String)> = RECORDED_HEADERS
            .iter()
            .filter_map(|header| {
                let values = request.header_values(header);
                (!values.is_empty()).then(|| ((*header).to_owned(), values.join(", ")))
            })
            .collect();
        let expected_headers: Vec<(String, String)> = expected["headers"]
            .as_array()
            .expect("headers")
            .iter()
            .map(|pair| {
                (
                    pair[0].as_str().expect("name").to_owned(),
                    pair[1].as_str().expect("value").to_owned(),
                )
            })
            .collect();
        assert_eq!(
            headers,
            expected_headers,
            "headers of {} in {name}",
            request.line()
        );
        assert_eq!(
            String::from_utf8_lossy(&request.body),
            text(expected, "body"),
            "body of {} in {name}",
            request.line()
        );
    }

    // Sink calls and their points.
    let calls = sink.calls.borrow();
    let expected_calls = case["calls"].as_array().expect("calls");
    assert_eq!(calls.len(), expected_calls.len(), "sink calls of {name}");
    for (call, expected) in calls.iter().zip(expected_calls) {
        assert_eq!(call.call, text(expected, "call"), "{name}");
        let expected_points = expected["points"].as_array().expect("points");
        assert_eq!(call.points.len(), expected_points.len(), "points of {name}");
        for (point, expected) in call.points.iter().zip(expected_points) {
            check_point(point, expected, &localise, name);
        }
        let first = &call.points[0];
        assert!(
            call.points.iter().all(|point| point.ts == first.ts),
            "{name}"
        );
        assert_eq!(first.ts.timestamp_subsec_nanos() % 1000, 0, "{name}");
    }

    // `HTTP Request:` lines.
    let expected_log: Vec<String> = case["log"]
        .as_array()
        .expect("log")
        .iter()
        .map(|line| localise.text(line.as_str().expect("log line")))
        .collect();
    assert_eq!(logs::lines(), expected_log, "log of {name}");
}

fn optional(value: &Value) -> Option<&str> {
    value.as_str()
}

fn check_point(point: &MetricPoint, expected: &Value, localise: &Localise, name: &str) {
    let context = format!("{} of {name}", point.metric);
    assert_eq!(point.metric, text(expected, "metric"), "{name}");
    match expected["value"].as_f64() {
        Some(value) => assert_eq!(point.value, value, "value of {context}"),
        // Measured timing values.
        None => assert!(point.value.is_finite() && point.value >= 0.0, "{context}"),
    }
    assert_eq!(
        point.subject_type,
        text(expected, "subject_type"),
        "{context}"
    );
    assert_eq!(point.subject_id, text(expected, "subject_id"), "{context}");
    assert_eq!(
        point.source_system,
        text(expected, "source_system"),
        "{context}"
    );
    assert_eq!(
        point.collector_service,
        text(expected, "collector_service"),
        "{context}"
    );
    assert_eq!(
        point.collector_host,
        text(expected, "collector_host"),
        "{context}"
    );
    assert_eq!(
        point.source_instance,
        text(expected, "source_instance"),
        "{context}"
    );
    assert_eq!(
        point.source_entity_id,
        optional(&expected["source_entity_id"]).map(|url| localise.text(url)),
        "{context}"
    );
    assert_eq!(
        point.host.as_deref(),
        optional(&expected["host"]),
        "{context}"
    );
    assert_eq!(
        point.service.as_deref(),
        optional(&expected["service"]),
        "{context}"
    );
    let tags: Option<Vec<(String, String)>> = point
        .tags
        .as_ref()
        .map(|tags| tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
    let expected_tags: Option<Vec<(String, String)>> = expected["tags"].as_array().map(|pairs| {
        pairs
            .iter()
            .map(|pair| {
                (
                    pair[0].as_str().expect("tag key").to_owned(),
                    pair[1].as_str().expect("tag value").to_owned(),
                )
            })
            .collect()
    });
    assert_eq!(tags, expected_tags, "tags of {context}");
}
