//! The scenarios of `fixtures/ha_json_executors.json`
//! (`fixtures/gen_ha_json_executors.py`): each Python executor run on a
//! scripted transport, replayed here against a fake server with the same
//! replies. Compared: the outcome (or the escaping `SpecConfigError`), the
//! requests sent, the points written and touched, and the log lines.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use serde_json::Value;

use graphyard_core::model::{MetricPoint, Tags};
use graphyard_core::pycompat::{json_loads, py_float};

use crate::executors::{
    ExecutorError, ExecutorResult, MetricSink, RunState, SinkError,
    execute_home_assistant_env_scan, execute_home_assistant_sensor, execute_http_json_metric,
};
use crate::http::HttpContext;
use crate::http::status::standard_reason_phrase;
use crate::http::tests::fixture::{hex_bytes, number, opt_text, text};
use crate::http::tests::logs;
use crate::http::tests::server::{Reply, Server};

/// A sink recording what the executor hands over, optionally failing (as
/// the generator's recorders), with a fixed clock.
pub(super) struct ClockedSink {
    pub now: DateTime<Utc>,
    pub written: RefCell<Option<Vec<MetricPoint>>>,
    pub touched: RefCell<Option<Vec<MetricPoint>>>,
    pub write_error: Option<String>,
    pub touch_error: Option<String>,
}

impl ClockedSink {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now,
            written: RefCell::new(None),
            touched: RefCell::new(None),
            write_error: None,
            touch_error: None,
        }
    }
}

impl MetricSink for ClockedSink {
    fn now(&self) -> DateTime<Utc> {
        self.now
    }

    async fn write_points(&self, points: &[MetricPoint]) -> Result<u64, SinkError> {
        *self.written.borrow_mut() = Some(points.to_vec());
        match &self.write_error {
            Some(message) => Err(SinkError::new("RuntimeError", message.clone())),
            None => Ok(points.len() as u64),
        }
    }

    async fn touch_registry(&self, points: &[MetricPoint]) -> Result<(), SinkError> {
        *self.touched.borrow_mut() = Some(points.to_vec());
        match &self.touch_error {
            Some(message) => Err(SinkError::new("RuntimeError", message.clone())),
            None => Ok(()),
        }
    }
}

const TEXT: &str = include_str!("../../../../../fixtures/ha_json_executors.json");

fn fixture() -> &'static Value {
    static FIXTURE: OnceLock<Value> = OnceLock::new();
    FIXTURE.get_or_init(|| serde_json::from_str(TEXT).expect("fixture is valid JSON"))
}

/// The host the Python scenarios used; replaced by the fake server's
/// address in configs and expected texts.
const FIXTURE_HOST: &str = "fixture.test";

fn reply_of(reply: &Value) -> Reply {
    let status = number(reply, "status");
    let code = u16::try_from(status).expect("status");
    let body = hex_bytes(text(reply, "body_hex"));
    let mut head = format!("HTTP/1.1 {status} {}\r\n", standard_reason_phrase(code));
    for pair in reply["headers"].as_array().expect("headers") {
        let name = pair[0].as_str().expect("header name");
        let value = pair[1].as_str().expect("header value");
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

/// A server answering each target with its scripted reply, 404 otherwise
/// (the generator's `MockTransport` handler).
async fn scripted_server(scenario: &Value) -> Server {
    let routes: HashMap<String, Reply> = scenario["routes"]
        .as_object()
        .expect("routes")
        .iter()
        .map(|(target, reply)| (target.clone(), reply_of(reply)))
        .collect();
    Server::start(move |request| {
        routes.get(&request.target).cloned().unwrap_or_else(|| {
            Reply::Raw(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_vec(),
            )
        })
    })
    .await
}

fn ts_of(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("fixture timestamp")
        .with_timezone(&Utc)
}

fn opt_string(value: &Value) -> Option<String> {
    value.as_str().map(str::to_owned)
}

/// A point of the fixture (`gen_normalize.py` encoding).
fn point_of(value: &Value) -> MetricPoint {
    MetricPoint {
        ts: ts_of(text(value, "ts")),
        metric: text(value, "metric").to_owned(),
        value: py_float(text(value, "value")).expect("value repr"),
        subject_type: text(value, "subject_type").to_owned(),
        subject_id: text(value, "subject_id").to_owned(),
        source_system: text(value, "source_system").to_owned(),
        collector_service: text(value, "collector_service").to_owned(),
        collector_host: text(value, "collector_host").to_owned(),
        source_instance: text(value, "source_instance").to_owned(),
        source_entity_id: opt_string(&value["source_entity_id"]),
        host: opt_string(&value["host"]),
        service: opt_string(&value["service"]),
        tags: value["tags"].as_array().map(|pairs| {
            pairs
                .iter()
                .map(|pair| {
                    (
                        pair[0].as_str().expect("tag key").to_owned(),
                        pair[1].as_str().expect("tag value").to_owned(),
                    )
                })
                .collect::<Tags>()
        }),
    }
}

fn points_of(value: &Value) -> Option<Vec<MetricPoint>> {
    value
        .as_array()
        .map(|points| points.iter().map(point_of).collect())
}

/// Tag order is part of the contract; `IndexMap` equality ignores it.
fn assert_points(actual: Option<&Vec<MetricPoint>>, expected: &Value, context: &str) {
    let expected = points_of(expected);
    assert_eq!(actual, expected.as_ref(), "{context}");
    if let (Some(actual), Some(expected)) = (actual, expected) {
        for (actual, expected) in actual.iter().zip(&expected) {
            let order = |point: &MetricPoint| -> Vec<String> {
                point
                    .tags
                    .iter()
                    .flatten()
                    .map(|(k, _)| k.clone())
                    .collect()
            };
            assert_eq!(order(actual), order(expected), "{context}: tag order");
        }
    }
}

pub(super) async fn run_executor(
    spec_type: &str,
    http: &HttpContext,
    sink: &ClockedSink,
    run: &mut RunState,
    name: &str,
    config: &Value,
) -> ExecutorResult {
    match spec_type {
        "home_assistant_sensor" => {
            execute_home_assistant_sensor(http, sink, run, name, config).await
        }
        "home_assistant_env_scan" => {
            execute_home_assistant_env_scan(http, sink, run, name, config).await
        }
        "http_json_metric" => execute_http_json_metric(http, sink, name, config).await,
        other => panic!("unknown spec type {other}"),
    }
}

fn check_outcome(
    scenario: &Value,
    result: &ExecutorResult,
    localise: &dyn Fn(&str) -> String,
    context: &str,
) {
    match (result, scenario.get("error")) {
        (Err(err @ ExecutorError::Config(_)), Some(expected)) => {
            assert_eq!(text(expected, "class"), err.python_class(), "{context}");
            assert_eq!(err.to_string(), text(expected, "message"), "{context}");
        }
        (Ok(outcome), None) => {
            let expected = scenario["outcome"].as_array().expect("outcome");
            assert_eq!(
                outcome.status.as_str(),
                expected[0].as_str().expect("status"),
                "{context}"
            );
            assert_eq!(Some(outcome.ingested), expected[1].as_u64(), "{context}");
            assert_eq!(Some(outcome.skipped), expected[2].as_u64(), "{context}");
            let message = localise(expected[3].as_str().expect("message"));
            match opt_text(scenario, "outcome_p2_prefix") {
                Some(prefix) => assert!(
                    outcome.message.starts_with(prefix) && outcome.message.len() > prefix.len(),
                    "{context}: {:?}",
                    outcome.message
                ),
                None => assert_eq!(outcome.message, message, "{context}"),
            }
        }
        (result, expected) => panic!("{context}: got {result:?}, expected error {expected:?}"),
    }
}

#[tokio::test]
async fn executor_scenarios_match_python() {
    let fixture = fixture();
    let now = ts_of(text(fixture, "now"));
    let scenarios = fixture["scenarios"].as_array().expect("scenarios");
    assert!(scenarios.len() >= 60, "fixture has the full corpus");
    let http = HttpContext::new();
    for scenario in scenarios {
        let name = text(scenario, "name");
        let server = scripted_server(scenario).await;
        let address = format!("127.0.0.1:{}", server.port());
        let localise = |text: &str| text.replace(FIXTURE_HOST, &address);
        let config = json_loads(&localise(text(scenario, "config"))).expect("config JSON");
        let mut sink = ClockedSink::new(now);
        sink.write_error = opt_text(scenario, "write_error").map(str::to_owned);
        sink.touch_error = opt_text(scenario, "touch_error").map(str::to_owned);
        logs::capture();
        let result = run_executor(
            text(scenario, "spec_type"),
            &http,
            &sink,
            &mut RunState::new(),
            text(scenario, "spec_name"),
            &config,
        )
        .await;
        check_outcome(scenario, &result, &localise, name);

        let requests = server.requests();
        let expected_requests = scenario["requests"].as_array().expect("requests");
        assert_eq!(requests.len(), expected_requests.len(), "{name}: requests");
        for (request, expected) in requests.iter().zip(expected_requests) {
            assert_eq!(request.method, text(expected, "method"), "{name}");
            assert_eq!(request.target, text(expected, "target"), "{name}");
            let single = |header: &str| request.header_values(header).first().copied();
            assert_eq!(
                single("authorization"),
                opt_text(expected, "authorization"),
                "{name}"
            );
            assert_eq!(single("accept"), opt_text(expected, "accept"), "{name}");
        }
        assert_points(
            sink.written.borrow().as_ref(),
            &scenario["written"],
            &format!("{name}: written"),
        );
        assert_points(
            sink.touched.borrow().as_ref(),
            &scenario["touched"],
            &format!("{name}: touched"),
        );
        let expected_log: Vec<String> = scenario["log"]
            .as_array()
            .expect("log")
            .iter()
            .map(|line| localise(line.as_str().expect("log line")))
            .collect();
        assert_eq!(logs::lines(), expected_log, "{name}: log");
    }
}
