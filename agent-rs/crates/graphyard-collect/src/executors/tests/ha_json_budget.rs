//! Executor-level budget, TLS and per-run tests with real sockets:
//! `test_http_json_budget.py`, `test_home_assistant_budget.py` (the modes
//! that need a clock: `headers`, `drip`) and
//! `test_metric_dimensions.py::test_home_assistant_env_scan_warning_cache_resets_between_runs`.
//! The byte budget, gzip and exact-cap modes are fixture scenarios
//! (`scenarios.rs`); the next-spec isolation is the run's (S8).

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use graphyard_core::model::{SpecOutcome, StatusLevel};

use super::ha_json::{ClockedSink, run_executor};
use crate::executors::{RunState, now_utc};
use crate::http::HttpContext;
use crate::http::resolve::Lookup;
use crate::http::tests::logs;
use crate::http::tests::server::{Reply, Server, StubLookup, response};
use crate::http::tests::transport::tls_material;

const DRIP_INTERVAL: Duration = Duration::from_millis(50);
const DRIP_GIVE_UP: Duration = Duration::from_secs(5);

fn drip(prefix: &[u8], drip: &[u8]) -> Reply {
    Reply::Drip {
        prefix: prefix.to_vec(),
        drip: drip.to_vec(),
        interval: DRIP_INTERVAL,
        give_up: DRIP_GIVE_UP,
    }
}

async fn run(
    spec_type: &str,
    http: &HttpContext,
    sink: &ClockedSink,
    run: &mut RunState,
    config: &Value,
) -> SpecOutcome {
    run_executor(spec_type, http, sink, run, "budget", config)
        .await
        .expect("no config error")
}

#[tokio::test]
async fn dripping_json_hits_the_total_deadline() {
    // test_dripping_json_total_deadline: the explicit deadline, and the
    // default of three request timeouts.
    for (config, message, at_least) in [
        (
            json!({"request_timeout_seconds": 1, "total_timeout_seconds": 0.4}),
            "HTTP collector exceeded total_timeout_seconds=0.4",
            None,
        ),
        (
            json!({"request_timeout_seconds": 0.5}),
            "HTTP collector exceeded total_timeout_seconds=1.5",
            Some(Duration::from_millis(1400)),
        ),
    ] {
        let server = Server::start(|_| {
            drip(
                b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{\"value\":",
                b" ",
            )
        })
        .await;
        let mut config = config;
        config["url"] = json!(format!("{}/slow", server.origin()));
        config["metric_path"] = json!("$.value");
        config["metric_name"] = json!("queue.depth");
        let sink = ClockedSink::new(now_utc());
        let started = Instant::now();
        let outcome = run(
            "http_json_metric",
            &HttpContext::new(),
            &sink,
            &mut RunState::new(),
            &config,
        )
        .await;
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
        if let Some(at_least) = at_least {
            assert!(elapsed >= at_least, "{elapsed:?}");
        }
        assert_eq!(
            outcome,
            SpecOutcome::new(StatusLevel::Critical, 0, 0, message)
        );
        assert!(sink.written.borrow().is_none());
    }
}

#[tokio::test]
async fn ha_deadline_covers_headers_and_body() {
    // test_ha_budgets_and_next_spec modes `headers` and `drip`, both spec
    // types: the request timeout (10 s) never fires, the total deadline does.
    let state = br#"{"entity_id": "sensor.temperature", "state": "7", "attributes": {}}"#;
    for spec_type in ["home_assistant_sensor", "home_assistant_env_scan"] {
        for mode in ["headers", "drip"] {
            let reply = if mode == "headers" {
                drip(b"HTTP/1.1 200 OK\r\n", b"X-Slow: 1\r\n")
            } else {
                drip(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        state.len() + 10
                    )
                    .as_bytes(),
                    b" ",
                )
            };
            let server = Server::start(move |_| reply.clone()).await;
            let config = json!({
                "base_url": server.origin(),
                "access_token": "test-ha",
                "entity_id": "sensor.temperature",
                "verify_tls": false,
                "total_timeout_seconds": 0.5,
            });
            let sink = ClockedSink::new(now_utc());
            let started = Instant::now();
            let outcome = run(
                spec_type,
                &HttpContext::new(),
                &sink,
                &mut RunState::new(),
                &config,
            )
            .await;
            let elapsed = started.elapsed();
            let context = format!("{spec_type} {mode}");
            assert!(elapsed < Duration::from_secs(2), "{context}: {elapsed:?}");
            assert_eq!(
                outcome,
                SpecOutcome::new(
                    StatusLevel::Critical,
                    0,
                    0,
                    "HTTP collector exceeded total_timeout_seconds=0.5"
                ),
                "{context}"
            );
            assert!(sink.written.borrow().is_none(), "{context}");
            let target = server.requests()[0].target.clone();
            let expected = if spec_type == "home_assistant_sensor" {
                "/api/states/sensor.temperature"
            } else {
                "/api/states"
            };
            assert_eq!(target, expected, "{context}");
        }
    }
}

#[tokio::test]
async fn json_stream_preserves_authentication_and_tls() {
    // test_json_stream_preserves_authentication_and_tls: verify_tls false
    // reaches a server whose certificate is not trusted, with the
    // configured authentication; verify_tls true refuses it.
    let (_root, acceptor) = tls_material();
    let server = Server::start_with(
        |_| response("200 OK", &[], br#"{"value": 7}"#),
        Some(acceptor),
    )
    .await;
    let lookup: Arc<dyn Lookup> = Arc::new(StubLookup::loopback());
    let http = HttpContext::new().with_lookup(lookup);
    let url = format!("https://tls.test:{}/", server.port());
    for (auth, expected) in [
        (json!({"bearer_token": "test-bearer"}), "Bearer test-bearer"),
        (
            json!({"basic_username": "user", "basic_password": "pass"}),
            "Basic dXNlcjpwYXNz",
        ),
    ] {
        let mut config = json!({
            "url": url,
            "metric_path": "$.value",
            "metric_name": "queue.depth",
            "verify_tls": false,
        });
        for (key, value) in auth.as_object().expect("auth") {
            config[key] = value.clone();
        }
        let sink = ClockedSink::new(now_utc());
        let outcome = run(
            "http_json_metric",
            &http,
            &sink,
            &mut RunState::new(),
            &config,
        )
        .await;
        assert_eq!(outcome, SpecOutcome::new(StatusLevel::Ok, 1, 0, ""));
        let request = server.requests().pop().expect("request");
        assert_eq!(request.header_values("authorization"), [expected]);
        assert_eq!(request.header_values("accept"), ["application/json"]);

        config["verify_tls"] = json!(true);
        let sink = ClockedSink::new(now_utc());
        let outcome = run(
            "http_json_metric",
            &http,
            &sink,
            &mut RunState::new(),
            &config,
        )
        .await;
        assert_eq!(outcome.status, StatusLevel::Critical, "{outcome:?}");
        assert!(sink.written.borrow().is_none());
    }
}

#[tokio::test]
async fn mapping_warnings_are_once_per_run() {
    // test_home_assistant_env_scan_warning_cache_resets_between_runs: the
    // missing-mapping warning appears once per run, again in the next run.
    let body = br#"[{"entity_id": "sensor.office_temperature", "state": "21.8",
        "last_updated": "2026-03-04T12:00:00Z", "attributes": {"device_class": "temperature"}}]"#;
    let server = Server::start(|_| response("200 OK", &[], body)).await;
    let config = json!({"base_url": server.origin(), "access_token": "token"});
    let http = HttpContext::new();
    let sink = ClockedSink::new(now_utc());
    let warning =
        "Spec budget missing config.subject_mapping; using default entity_name_slug mapping";
    let count = |lines: &[String]| lines.iter().filter(|line| *line == warning).count();

    logs::capture();
    let mut first_run = RunState::new();
    for _ in 0..2 {
        let outcome = run(
            "home_assistant_env_scan",
            &http,
            &sink,
            &mut first_run,
            &config,
        )
        .await;
        assert_eq!(outcome, SpecOutcome::new(StatusLevel::Ok, 1, 0, ""));
    }
    assert_eq!(count(&logs::lines()), 1);
    let mut second_run = RunState::new();
    run(
        "home_assistant_env_scan",
        &http,
        &sink,
        &mut second_run,
        &config,
    )
    .await;
    assert_eq!(count(&logs::lines()), 2);
}
