//! The total deadline and the abandoned-lookup cap (PLAN.md 6.2, D-3):
//! counterparts of `test_page_probe_deadline.py`, the deadline parts of
//! `test_http_json_budget.py` and `test_home_assistant_budget.py`.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use graphyard_core::spec_config::{page_probe_total_timeout_config, request_timeout_config};

use super::logs;
use super::server::{Gate, GateGuard, Reply, Server, StubLookup, response};
use crate::http::client::{ClientOptions, HttpClient, HttpContext};
use crate::http::deadline::{Deadline, run_with_deadline};
use crate::http::error::{ErrorKind, HttpError};
use crate::http::probe::{PageProbeResult, finish_json, read_http_json, run_page_probe};

const DRIP_INTERVAL: Duration = Duration::from_millis(200);
const DRIP_GIVE_UP: Duration = Duration::from_secs(8);

fn drip(prefix: &[u8], drip: &[u8]) -> Reply {
    Reply::Drip {
        prefix: prefix.to_vec(),
        drip: drip.to_vec(),
        interval: DRIP_INTERVAL,
        give_up: DRIP_GIVE_UP,
    }
}

async fn probe_with_deadline(
    context: &HttpContext,
    url: &str,
    request_timeout: f64,
    total: f64,
) -> Result<PageProbeResult, HttpError> {
    let client = HttpClient::new(context, &ClientOptions::new(request_timeout, true));
    run_with_deadline(&client, url, total, |deadline| {
        let client = &client;
        async move { run_page_probe(client, url, 10_485_760, true, &deadline).await }
    })
    .await
}

async fn json_with_deadline(
    context: &HttpContext,
    url: &str,
    request_timeout: f64,
    total: f64,
) -> Result<Value, HttpError> {
    let client = HttpClient::new(context, &ClientOptions::new(request_timeout, true));
    run_with_deadline(&client, url, total, |deadline| {
        let client = &client;
        async move { read_http_json(client, url, 10_485_760, &deadline).await }
    })
    .await
}

/// `_assert_deadline_failure`: the deadline error, returned within the
/// deadline plus the bounded close wait and slack.
fn assert_deadline_failure<T: std::fmt::Debug>(
    result: &Result<T, HttpError>,
    elapsed: Duration,
    total: f64,
) {
    let err = result.as_ref().expect_err("deadline failure");
    assert_eq!(err.kind, ErrorKind::DeadlineExceeded, "{err:?}");
    assert!(err.message.contains("total_timeout_seconds"), "{err:?}");
    assert!(err.kind.is_page_probe_warning());
    assert!(
        elapsed.as_secs_f64() < total + 1.5,
        "returned after {elapsed:?}"
    );
}

#[tokio::test]
async fn page_probe_deadline_bounds_dripping_headers() {
    let server = Server::start(|_| {
        drip(
            b"HTTP/1.1 200 OK\r\n",
            b"X-Drip: aaaaaaaaaaaaaaaaaaaaaaaaaa\r\n",
        )
    })
    .await;
    let started = Instant::now();
    let result = probe_with_deadline(
        &HttpContext::new(),
        &format!("{}/slow", server.origin()),
        1.0,
        1.5,
    )
    .await;
    let elapsed = started.elapsed();
    assert_deadline_failure(&result, elapsed, 1.5);
    assert!(elapsed >= Duration::from_millis(1400), "{elapsed:?}");
    assert_eq!(
        result.unwrap_err().message,
        "HTTP collector exceeded total_timeout_seconds=1.5"
    );
}

#[tokio::test]
async fn page_probe_deadline_bounds_dripping_body() {
    let server = Server::start(|_| {
        drip(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 1000000\r\n\r\n",
            b"<p>slow</p>",
        )
    })
    .await;
    let started = Instant::now();
    let result = probe_with_deadline(
        &HttpContext::new(),
        &format!("{}/slow", server.origin()),
        1.0,
        1.5,
    )
    .await;
    assert_deadline_failure(&result, started.elapsed(), 1.5);
}

#[tokio::test]
async fn default_deadline_is_three_request_timeouts() {
    let config: Map<String, Value> =
        serde_json::from_str(r#"{"request_timeout_seconds": 0.5}"#).expect("config");
    let request_timeout = request_timeout_config(&config).expect("request timeout");
    let total = page_probe_total_timeout_config(&config, request_timeout).expect("total");
    assert!((total - 1.5).abs() < f64::EPSILON);
    let server = Server::start(|_| {
        drip(
            b"HTTP/1.1 200 OK\r\n",
            b"X-Drip: aaaaaaaaaaaaaaaaaaaaaaaaaa\r\n",
        )
    })
    .await;
    let started = Instant::now();
    let result = probe_with_deadline(
        &HttpContext::new(),
        &format!("{}/slow", server.origin()),
        request_timeout,
        total,
    )
    .await;
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_millis(1400), "{elapsed:?}");
    assert_deadline_failure(&result, elapsed, total);
}

#[tokio::test]
async fn fast_page_is_unchanged_by_the_deadline() {
    let server = Server::start(|_| {
        response(
            "200 OK",
            &[("Content-Type", "text/html")],
            b"<html>ok</html>",
        )
    })
    .await;
    let result = probe_with_deadline(
        &HttpContext::new(),
        &format!("{}/", server.origin()),
        2.0,
        5.0,
    )
    .await
    .expect("fast page");
    assert_eq!(result.status_code, 200);
    assert_eq!(result.redirect_count, 0);
    assert!((0.0..2.0).contains(&result.total_seconds));
    assert!(result.ttfb_seconds <= result.total_seconds);
}

#[tokio::test]
async fn dripping_json_hits_the_total_deadline() {
    for (request_timeout, total, explicit) in [(1.0, 0.4, true), (0.5, 1.5, false)] {
        let server = Server::start(|_| {
            drip(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{\"value\":",
                b" ",
            )
        })
        .await;
        let started = Instant::now();
        let result = json_with_deadline(
            &HttpContext::new(),
            &format!("{}/slow", server.origin()),
            request_timeout,
            total,
        )
        .await;
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
        if !explicit {
            assert!(elapsed >= Duration::from_millis(1400), "{elapsed:?}");
        }
        let err = result.expect_err("deadline");
        assert_eq!(err.kind, ErrorKind::DeadlineExceeded);
        assert!(err.message.contains("total_timeout_seconds"), "{err:?}");
    }
}

#[tokio::test]
async fn json_deadline_while_waiting_for_headers() {
    // test_home_assistant_budget "headers": the deadline passes before the
    // response headers are complete.
    let server = Server::start(|_| drip(b"HTTP/1.1 200 OK\r\n", b"X-Slow: 1\r\n")).await;
    let result = json_with_deadline(
        &HttpContext::new(),
        &format!("{}/api/states", server.origin()),
        10.0,
        0.5,
    )
    .await;
    assert_eq!(
        result.expect_err("deadline").message,
        "HTTP collector exceeded total_timeout_seconds=0.5"
    );
}

#[test]
fn deadline_passing_during_the_json_parse_fails_the_read() {
    // test_home_assistant_budget "parse": Python checks the deadline after
    // `json.loads`. Grow a body until parsing it takes measurable time, then
    // give the read a deadline that is still open before the parse but has
    // passed after it.
    let mut items = 50_000;
    let (body, parse_time) = loop {
        let body = format!("[{}]", vec!["{\"a\": [1, 2.5, \"x\"]}"; items].join(","));
        let started = Instant::now();
        finish_json(body.as_bytes(), &Deadline::none()).expect("valid JSON");
        let parse_time = started.elapsed();
        if parse_time >= Duration::from_millis(40) || items > 5_000_000 {
            break (body, parse_time);
        }
        items *= 2;
    };
    let deadline = Deadline::at(Instant::now() + parse_time / 4, 30.0);
    let err = finish_json(body.as_bytes(), &deadline).expect_err("deadline after the parse");
    assert_eq!(
        err.message,
        "HTTP collector exceeded total_timeout_seconds=30"
    );
    let passed = Deadline::at(Instant::now(), 30.0);
    assert_eq!(
        finish_json(b"{}", &passed)
            .expect_err("deadline before the parse")
            .kind,
        ErrorKind::DeadlineExceeded
    );
}

#[tokio::test]
async fn passed_deadline_sends_nothing() {
    let server = Server::start(|_| response("200 OK", &[], b"{}")).await;
    let client = HttpClient::new(&HttpContext::new(), &ClientOptions::new(10.0, true));
    let url = format!("{}/", server.origin());
    let passed = Deadline::at(Instant::now(), 1.0);
    let err = read_http_json(&client, &url, 100, &passed)
        .await
        .expect_err("deadline");
    assert_eq!(err.kind, ErrorKind::DeadlineExceeded);
    assert_eq!(server.accepted(), 0);
}

#[tokio::test]
async fn lookup_finishing_after_the_deadline_never_reaches_the_server() {
    // test_page_probe_cancels_connection_that_completes_after_deadline: the
    // lookup outlives the deadline and the close wait; the request is gone,
    // so nothing connects once it returns.
    let server = Server::start(|_| drip(b"HTTP/1.1 200 OK\r\n", b"X")).await;
    let lookup = Arc::new(StubLookup {
        delay: Duration::from_millis(1800),
        ..StubLookup::loopback()
    });
    let context = HttpContext::new().with_lookup(lookup.clone());
    let url = format!("http://slow.test:{}/", server.port());
    logs::capture();
    let started = Instant::now();
    let result = probe_with_deadline(&context, &url, 2.0, 0.3).await;
    let elapsed = started.elapsed();
    assert_deadline_failure(&result, elapsed, 0.3);
    assert!(
        elapsed >= Duration::from_millis(1300),
        "close wait: {elapsed:?}"
    );
    assert_eq!(context.live_abandoned(), 1);
    assert_eq!(
        logs::lines(),
        [format!(
            "HTTP collector worker for {url} still running after it was cancelled"
        )]
    );
    let give_up = Instant::now() + Duration::from_secs(4);
    while context.live_abandoned() > 0 && Instant::now() < give_up {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(context.live_abandoned(), 0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(server.accepted(), 0);
    assert_eq!(lookup.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn lookup_finishing_within_the_close_wait_is_not_abandoned() {
    let lookup = Arc::new(StubLookup {
        delay: Duration::from_millis(600),
        ..StubLookup::loopback()
    });
    let context = HttpContext::new().with_lookup(lookup);
    logs::capture();
    let result = probe_with_deadline(&context, "http://slow.test:9/", 2.0, 0.2).await;
    assert_eq!(
        result.expect_err("deadline").kind,
        ErrorKind::DeadlineExceeded
    );
    assert_eq!(context.live_abandoned(), 0);
    assert!(logs::lines().is_empty(), "{:?}", logs::lines());
}

#[tokio::test]
async fn abandoned_lookups_cap_new_operations() {
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
    let url = "https://example.invalid/";
    for expected_live in 1..=2 {
        let result = probe_with_deadline(&context, url, 10.0, 0.1).await;
        assert_eq!(
            result.expect_err("deadline").kind,
            ErrorKind::DeadlineExceeded
        );
        assert_eq!(context.live_abandoned(), expected_live);
    }
    assert_eq!(lookup.calls.load(Ordering::SeqCst), 2);

    let started = Instant::now();
    let result = probe_with_deadline(&context, url, 10.0, 0.1).await;
    let elapsed = started.elapsed();
    let err = result.expect_err("refused");
    assert_eq!(err.kind, ErrorKind::DeadlineExceeded);
    assert!(err.kind.is_page_probe_warning());
    assert_eq!(
        err.message,
        "2 earlier HTTP collectors are still running past total_timeout_seconds; not starting another"
    );
    assert!(elapsed < Duration::from_millis(100), "{elapsed:?}");
    assert_eq!(lookup.calls.load(Ordering::SeqCst), 2, "no third lookup");

    gate.open();
    let give_up = Instant::now() + Duration::from_secs(5);
    while context.live_abandoned() > 0 && Instant::now() < give_up {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(context.live_abandoned(), 0);
}
