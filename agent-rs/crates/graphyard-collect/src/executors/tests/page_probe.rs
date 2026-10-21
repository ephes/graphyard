//! `http_page_probe`: the `page_probe.json` scenarios and the executor-level
//! counterparts of `test_page_probe_deadline.py` and the page probe tests
//! of `test_metric_collection_specs.py` / `test_metric_dimensions.py` /
//! `test_metric_collection_spec_isolation.py`.

use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use graphyard_core::model::StatusLevel;

use super::support::{RecordingSink, Run, replay, text};
use crate::executors::page_probe::{STATUS_CODE_METRIC, SUCCESS_METRIC, TOTAL_METRIC};
use crate::executors::{ExecutorResult, execute_http_page_probe};
use crate::http::HttpContext;
use crate::http::tests::logs;
use crate::http::tests::server::{Gate, GateGuard, Reply, Server, StubLookup, response};

const FIXTURE: &str = include_str!("../../../../../fixtures/page_probe.json");

fn fixture() -> &'static Value {
    static FIXTURE_VALUE: OnceLock<Value> = OnceLock::new();
    FIXTURE_VALUE.get_or_init(|| serde_json::from_str(FIXTURE).expect("page_probe.json"))
}

const RUN: Run =
    |http, sink, name, config| Box::pin(execute_http_page_probe(http, sink, name, config));

#[tokio::test]
async fn page_probe_scenarios_match_python() {
    let fixture = fixture();
    let cases = fixture["executor"].as_array().expect("executor cases");
    assert!(cases.len() >= 25);
    for case in cases {
        replay(case, text(fixture, "origin"), RUN).await;
    }
}

async fn probe(context: &HttpContext, config: &Value) -> (ExecutorResult, RecordingSink) {
    let sink = RecordingSink::default();
    let result = execute_http_page_probe(context, &sink, "dripping probe", config).await;
    (result, sink)
}

/// The metric → value map of the single write call.
fn written_values(sink: &RecordingSink) -> Vec<(String, f64)> {
    let calls = sink.calls.borrow();
    assert_eq!(calls.len(), 2, "write then touch");
    assert_eq!(calls[0].call, "write");
    assert_eq!(calls[1].call, "touch");
    assert_eq!(calls[0].points, calls[1].points);
    calls[0]
        .points
        .iter()
        .map(|point| (point.metric.clone(), point.value))
        .collect()
}

fn failure_points() -> Vec<(String, f64)> {
    vec![
        (STATUS_CODE_METRIC.to_owned(), 0.0),
        (SUCCESS_METRIC.to_owned(), 0.0),
    ]
}

/// `_assert_deadline_failure`.
fn assert_deadline_failure(
    result: &ExecutorResult,
    sink: &RecordingSink,
    elapsed: Duration,
    total: f64,
) {
    assert!(
        elapsed.as_secs_f64() < total + 1.5,
        "returned after {elapsed:?}"
    );
    let outcome = result.as_ref().expect("outcome");
    assert_eq!(outcome.status, StatusLevel::Warning);
    assert_eq!(outcome.ingested, 2);
    assert!(
        outcome.message.contains("total_timeout_seconds"),
        "{outcome:?}"
    );
    assert_eq!(written_values(sink), failure_points());
}

fn drip(prefix: &[u8], drip: &[u8]) -> Reply {
    Reply::Drip {
        prefix: prefix.to_vec(),
        drip: drip.to_vec(),
        interval: Duration::from_millis(200),
        give_up: Duration::from_secs(8),
    }
}

#[tokio::test]
async fn deadline_bounds_dripping_headers() {
    let server = Server::start(|_| {
        drip(
            b"HTTP/1.1 200 OK\r\n",
            b"X-Drip: aaaaaaaaaaaaaaaaaaaaaaaaaa\r\n",
        )
    })
    .await;
    let config = json!({
        "url": format!("{}/slow", server.origin()),
        "subject_id": "dripping_site",
        "request_timeout_seconds": 1,
        "total_timeout_seconds": 1.5,
    });
    let started = Instant::now();
    let (result, sink) = probe(&HttpContext::new(), &config).await;
    assert_deadline_failure(&result, &sink, started.elapsed(), 1.5);
    assert_eq!(
        result.expect("outcome").message,
        "HTTP collector exceeded total_timeout_seconds=1.5"
    );
}

#[tokio::test]
async fn deadline_bounds_dripping_body() {
    let server = Server::start(|_| {
        drip(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 1000000\r\n\r\n",
            b"<p>slow</p>",
        )
    })
    .await;
    let config = json!({
        "url": format!("{}/slow", server.origin()),
        "subject_id": "dripping_site",
        "request_timeout_seconds": 1,
        "total_timeout_seconds": 1.5,
    });
    let started = Instant::now();
    let (result, sink) = probe(&HttpContext::new(), &config).await;
    assert_deadline_failure(&result, &sink, started.elapsed(), 1.5);
}

#[tokio::test]
async fn default_deadline_is_three_request_timeouts() {
    let server = Server::start(|_| {
        drip(
            b"HTTP/1.1 200 OK\r\n",
            b"X-Drip: aaaaaaaaaaaaaaaaaaaaaaaaaa\r\n",
        )
    })
    .await;
    let config = json!({
        "url": format!("{}/slow", server.origin()),
        "subject_id": "dripping_site",
        "request_timeout_seconds": 0.5,
    });
    let started = Instant::now();
    let (result, sink) = probe(&HttpContext::new(), &config).await;
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(1400), "{elapsed:?}");
    assert_deadline_failure(&result, &sink, elapsed, 1.5);
}

#[tokio::test]
async fn fast_page_is_unchanged() {
    let server = Server::start(|_| {
        response(
            "200 OK",
            &[("Content-Type", "text/html")],
            b"<html>ok</html>",
        )
    })
    .await;
    let config = json!({
        "url": format!("{}/", server.origin()),
        "subject_id": "dripping_site",
        "request_timeout_seconds": 2,
        "total_timeout_seconds": 5,
    });
    let (result, sink) = probe(&HttpContext::new(), &config).await;
    let outcome = result.expect("outcome");
    assert_eq!(outcome.status, StatusLevel::Ok);
    assert_eq!(outcome.message, "");
    let values = written_values(&sink);
    let value = |metric: &str| {
        values
            .iter()
            .find(|(name, _)| name == metric)
            .map(|(_, value)| *value)
            .expect("metric written")
    };
    assert_eq!(value(STATUS_CODE_METRIC), 200.0);
    assert_eq!(value(SUCCESS_METRIC), 1.0);
    assert_eq!(value("service.http_page_redirect_count"), 0.0);
    let total = value(TOTAL_METRIC);
    assert!((0.0..2.0).contains(&total), "{total}");
    assert!(value("service.http_page_ttfb_seconds") <= total);
}

#[tokio::test]
async fn abandoned_lookups_cap_new_probes() {
    // test_page_probe_caps_abandoned_workers (close wait 0.05 s, cap 2).
    let gate = Arc::new(Gate::default());
    let _release = GateGuard(Arc::clone(&gate));
    let lookup = Arc::new(StubLookup {
        gate: Some(Arc::clone(&gate)),
        ..StubLookup::loopback()
    });
    let context = HttpContext::new()
        .with_lookup(lookup.clone())
        .with_limits(Duration::from_millis(50), 2);
    let config = json!({
        "url": "https://example.invalid/",
        "subject_id": "dripping_site",
        "total_timeout_seconds": 0.1,
    });
    for _ in 0..2 {
        logs::capture();
        let (result, sink) = probe(&context, &config).await;
        let outcome = result.expect("outcome");
        assert_eq!(outcome.status, StatusLevel::Warning);
        assert_eq!(
            outcome.message,
            "HTTP collector exceeded total_timeout_seconds=0.1"
        );
        assert_eq!(written_values(&sink), failure_points());
        assert_eq!(
            logs::lines(),
            [
                "HTTP collector worker for https://example.invalid/ still running after it was cancelled"
            ]
        );
    }
    assert_eq!(lookup.calls.load(Ordering::SeqCst), 2);

    let started = Instant::now();
    let (result, sink) = probe(&context, &config).await;
    let elapsed = started.elapsed();
    let outcome = result.expect("outcome");
    assert!(elapsed < Duration::from_millis(100), "{elapsed:?}");
    assert_eq!(outcome.status, StatusLevel::Warning);
    assert_eq!(
        outcome.message,
        "2 earlier HTTP collectors are still running past total_timeout_seconds; not starting another"
    );
    assert_eq!(written_values(&sink), failure_points());
    assert_eq!(lookup.calls.load(Ordering::SeqCst), 2, "no third lookup");
    gate.open();
}

#[tokio::test]
async fn urls_httpcore_rejects_take_the_warning_path() {
    // `UnsupportedProtocol` is an `httpx.HTTPError` (http_errors.json);
    // MockTransport cannot show it, so the fixture has no such scenario.
    let cases = [
        (
            "ftp://probe.test/",
            "Request URL has an unsupported protocol 'ftp://'.",
        ),
        (
            "probe.test/",
            "Request URL is missing an 'http://' or 'https://' protocol.",
        ),
    ];
    for (url, message) in cases {
        let config = json!({"url": url, "subject_id": "example_site"});
        let (result, sink) = probe(&HttpContext::new(), &config).await;
        let outcome = result.expect("outcome");
        assert_eq!(outcome.status, StatusLevel::Warning, "{url}");
        assert_eq!(outcome.ingested, 2, "{url}");
        assert_eq!(outcome.message, message, "{url}");
        assert_eq!(written_values(&sink), failure_points());
    }
}

#[tokio::test]
async fn connection_failures_take_the_warning_path() {
    // test_http_page_probe_spec_timeout_records_failure_metrics: a transport
    // error records the two failure points.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("address").port();
    drop(listener);
    let config = json!({
        "url": format!("http://127.0.0.1:{port}/"),
        "subject_id": "example_site",
        "verify_tls": false,
    });
    let (result, sink) = probe(&HttpContext::new(), &config).await;
    let outcome = result.expect("outcome");
    assert_eq!(outcome.status, StatusLevel::Warning);
    assert!(outcome.message.starts_with("[Errno "), "{outcome:?}");
    assert_eq!(written_values(&sink), failure_points());
}
