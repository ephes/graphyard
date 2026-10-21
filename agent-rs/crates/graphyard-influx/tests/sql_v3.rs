//! Modes `v3` and `auto` end to end against a fake InfluxDB replaying the
//! scripts of `fixtures/sql_v3.json`, and the SQL text and payload of
//! `_query_range_v3_sql`.

mod common;

use chrono::{DateTime, Utc};
use common::{
    FakeInflux, assert_requests, assert_result, capture_logs, client_for, e2e_range, expected_logs,
    list, load, take_logs, text,
};
use graphyard_influx::QueryRange;
use graphyard_influx::sql::sql_query;
use serde_json::Value;

fn fixture() -> Value {
    load(include_str!("../../../fixtures/sql_v3.json"))
}

fn time(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("fixture time")
        .to_utc()
}

#[test]
fn sql_text_matches_python() {
    let fixture = fixture();
    for case in list(&fixture, "sql_text") {
        let name = text(case, "name");
        let mut range = QueryRange::new(
            text(case, "metric"),
            time(text(case, "start")),
            time(text(case, "stop")),
        );
        let field = |key: &str| case.get(key).and_then(Value::as_str).map(str::to_owned);
        range.host = field("host");
        range.service = field("service");
        range.subject_type = field("subject_type");
        range.subject_id = field("subject_id");
        if let Some(tags) = case.get("tags").and_then(Value::as_object) {
            for (key, value) in tags {
                range
                    .tags
                    .insert(key.clone(), value.as_str().expect("tag").to_owned());
            }
        }
        let result = sql_query(&range, text(case, "measurement"));
        match case.get("error") {
            Some(error) => {
                let err = result.expect_err(name);
                assert_eq!(err.python_class(), text(error, "class"), "{name}");
                assert_eq!(err.to_string(), text(error, "message"), "{name}");
            }
            None => {
                let payload = &case["payload"];
                assert_eq!(result.expect(name), text(payload, "q"), "{name}");
                assert_eq!(text(payload, "db"), text(case, "bucket"), "{name}");
            }
        }
    }
}

/// Cases where Rust diverges on purpose (PLAN.md 2.4): the expected Rust
/// error instead of the Python result.
fn divergence(case: &Value) -> Option<(&'static str, &'static str)> {
    match case.get("divergence").and_then(Value::as_str) {
        // D-4: `NaN` is not JSON for serde_json.
        Some("D-4") => Some((
            "InfluxConfigurationError",
            "InfluxDB SQL query failed: invalid JSON",
        )),
        Some(other) => panic!("unknown divergence {other}"),
        None => None,
    }
}

async fn run_section(section: &str) {
    let fixture = fixture();
    let cases = list(&fixture, section);
    assert!(!cases.is_empty());
    for case in cases {
        let server = FakeInflux::start(list(case, "responses")).await;
        let client = client_for(case, &server);
        capture_logs();
        let started = std::time::Instant::now();
        let result = client.query_range(&e2e_range(case)).await;
        let logs = take_logs();
        if text(case, "name").starts_with("deadline") {
            // test_sql_deadline_*: the deadline cancels a blocked or
            // dripping read promptly.
            assert!(started.elapsed() < std::time::Duration::from_secs(3));
        }
        assert_result(case, &result, &server.server(), divergence(case));
        assert_eq!(
            logs,
            expected_logs(case, &server.server()),
            "{}",
            text(case, "name")
        );
        assert_requests(case, &server.recorded());
    }
}

#[tokio::test]
async fn v3_rows_match_python() {
    run_section("rows").await;
}

#[tokio::test]
async fn v3_errors_match_python() {
    run_section("errors").await;
}

#[tokio::test]
async fn auto_mode_matches_python() {
    run_section("auto").await;
}

/// test_sql_oversize_stops_reading_and_closes: the read stops at the chunk
/// that passes the cap instead of draining a slow body.
#[tokio::test]
async fn sql_cap_stops_reading_early() {
    let script = serde_json::json!({
        "status": 200,
        "reason": "OK",
        "headers": [],
        "body": format!("[{}]", " ".repeat(50)),
        "drip": true,
        "chunk_delay": 0.2,
    });
    let server = FakeInflux::start(&[script]).await;
    let case = serde_json::json!({
        "settings": {"INFLUX_API_MODE": "v3", "INFLUX_TOKEN": "t"},
        "patch": {"SQL_QUERY_MAX_BODY_BYTES": 3},
    });
    let client = client_for(&case, &server);
    let started = std::time::Instant::now();
    let err = client
        .query_range(&e2e_range(&case))
        .await
        .expect_err("cap");
    assert_eq!(
        err.to_string(),
        "InfluxDB SQL response exceeds the 10 MiB decoded byte limit"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}
