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
            "collector failure: _PageProbeDeadlineExceeded",
            None,
        ),
        (
            json!({"request_timeout_seconds": 0.5}),
            "collector failure: _PageProbeDeadlineExceeded",
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
                    "collector failure: _PageProbeDeadlineExceeded"
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

#[tokio::test]
async fn ha_work_envelope_refuses_before_matching_or_writing() {
    for (count, rules, entity) in [
        (4097, 0, "sensor.temperature".to_owned()),
        (1, 129, "sensor.temperature".to_owned()),
        (256, 128, "sensor.temperature".to_owned()),
        (128, 1, format!("sensor.{}", "a".repeat(32768))),
    ] {
        let body = serde_json::to_vec(&vec![
            json!({
                "entity_id": entity, "state": "7", "attributes": {"device_class": "temperature"}
            });
            count
        ])
        .unwrap();
        let server = Server::start(move |_| response("200 OK", &[], &body)).await;
        let config = json!({
            "base_url": server.origin(), "access_token": "synthetic",
            "metric_mapping": {"rules": vec![json!({"match_entity_id_regex": "never"}); rules]}
        });
        let sink = ClockedSink::new(now_utc());
        let mut state = RunState::new();
        let outcome = run(
            "home_assistant_env_scan",
            &HttpContext::new(),
            &sink,
            &mut state,
            &config,
        )
        .await;
        assert_eq!(
            outcome,
            SpecOutcome::new(
                StatusLevel::Critical,
                0,
                0,
                "collector failure: _HomeAssistantLimitExceeded"
            )
        );
        assert!(
            state.mappings.warned_keys().next().is_none(),
            "no normalization started"
        );
        assert!(sink.written.borrow().is_none());
        assert!(sink.touched.borrow().is_none());
    }
}

#[tokio::test]
async fn ha_output_envelope_refuses_tag_amplification_without_partial_writes() {
    let body = serde_json::to_vec(&vec![
        json!({
            "entity_id": "sensor.temperature", "state": "7", "attributes": {}
        });
        65
    ])
    .unwrap();
    let server = Server::start(move |_| response("200 OK", &[], &body)).await;
    let config = json!({
        "base_url": server.origin(), "access_token": "synthetic",
        "metric_mapping": {"rules": [{"match_entity_id_regex": ".*", "extra_tags": {"large": "x".repeat(128 * 1024)}}]}
    });
    let sink = ClockedSink::new(now_utc());
    let outcome = run(
        "home_assistant_env_scan",
        &HttpContext::new(),
        &sink,
        &mut RunState::new(),
        &config,
    )
    .await;
    assert_eq!(
        outcome,
        SpecOutcome::new(
            StatusLevel::Critical,
            0,
            0,
            "collector failure: _HomeAssistantLimitExceeded"
        )
    );
    assert!(sink.written.borrow().is_none());
    assert!(sink.touched.borrow().is_none());
}

#[tokio::test]
async fn ha_adversarial_mapping_failure_never_publishes_neighbors() {
    for mapping in ["subject_mapping", "metric_mapping", "entity_id_regex"] {
        let body = serde_json::to_vec(&json!([
            {"entity_id": "sensor.temperature", "state": "7", "attributes": {"device_class": "temperature"}},
            {"entity_id": format!("sensor.{}", "a".repeat(18)), "state": "8", "attributes": {}}
        ])).unwrap();
        let server = Server::start(move |_| response("200 OK", &[], &body)).await;
        let mut config = json!({"base_url": server.origin(), "access_token": "synthetic", "entity_id_regex": ".*"});
        if mapping == "entity_id_regex" {
            config[mapping] = json!("(a|aa)+\\1x");
        } else {
            config[mapping] = json!({"rules": [{"match_entity_id_regex": "sensor\\.(a|aa)+\\1x"}]});
        }
        let sink = ClockedSink::new(now_utc());
        let outcome = run(
            "home_assistant_env_scan",
            &HttpContext::new(),
            &sink,
            &mut RunState::new(),
            &config,
        )
        .await;
        assert_eq!(outcome.status, StatusLevel::Critical, "{mapping}");
        assert!(sink.written.borrow().is_none(), "{mapping}");
        assert!(sink.touched.borrow().is_none(), "{mapping}");
    }
}

#[tokio::test]
async fn ha_malformed_identity_is_counted_without_discarding_valid_neighbors() {
    for malformed in [
        json!("sensor."),
        json!("binary_sensor.!!"),
        Value::Null,
        json!(42),
    ] {
        let body = serde_json::to_vec(&json!([
            {"entity_id": "sensor.first_temperature", "state": "7", "attributes": {"device_class": "temperature"}},
            {"entity_id": malformed, "state": "8", "attributes": {"device_class": "temperature"}},
            {"entity_id": "sensor.last_temperature", "state": "9", "attributes": {"device_class": "temperature"}}
        ])).unwrap();
        let server = Server::start(move |_| response("200 OK", &[], &body)).await;
        let config = json!({"base_url": server.origin(), "access_token": "synthetic"});
        let sink = ClockedSink::new(now_utc());
        let outcome = run(
            "home_assistant_env_scan",
            &HttpContext::new(),
            &sink,
            &mut RunState::new(),
            &config,
        )
        .await;
        assert_eq!(outcome, SpecOutcome::new(StatusLevel::Ok, 2, 1, ""));
        {
            let points = sink.written.borrow();
            let points = points.as_ref().unwrap();
            assert_eq!(
                points.iter().map(|p| p.value).collect::<Vec<_>>(),
                [7.0, 9.0]
            );
            assert_eq!(sink.touched.borrow().as_ref().unwrap(), points);
        }

        let sink = ClockedSink::new(now_utc());
        let mut bad_config = config;
        bad_config["entity_id_regex"] = json!("(");
        let outcome = run(
            "home_assistant_env_scan",
            &HttpContext::new(),
            &sink,
            &mut RunState::new(),
            &bad_config,
        )
        .await;
        assert_eq!(outcome.status, StatusLevel::Critical);
        assert!(sink.written.borrow().is_none());
        assert!(sink.touched.borrow().is_none());
    }
}

#[tokio::test]
async fn cookie_jar_refusal_publishes_no_metrics() {
    let server = Server::start(|_| {
        let cookies: Vec<_> = (0..257).map(|i| format!("c{i}=x; Path=/")).collect();
        let headers: Vec<_> = cookies.iter().map(|v| ("Set-Cookie", v.as_str())).collect();
        response("200 OK", &headers, br#"{"value":7}"#)
    })
    .await;
    let config =
        json!({"url": server.origin(), "metric_path": "$.value", "metric_name": "queue.depth"});
    let sink = ClockedSink::new(now_utc());
    let outcome = run(
        "http_json_metric",
        &HttpContext::new(),
        &sink,
        &mut RunState::new(),
        &config,
    )
    .await;
    assert_eq!(
        outcome,
        SpecOutcome::new(
            StatusLevel::Critical,
            0,
            0,
            "collector cookie jar limit exceeded"
        )
    );
    assert!(sink.written.borrow().is_none());
    assert!(sink.touched.borrow().is_none());
}
