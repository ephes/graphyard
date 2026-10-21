//! `spec_config` against `fixtures/spec_config.json` (built by
//! `fixtures/gen_spec_config.py`).

use serde_json::Value;

use super::*;
use crate::fixture::{Field, load, strings};
use crate::pycompat::{json_loads, py_float};

fn fixture() -> Value {
    load(include_str!("../../../../fixtures/spec_config.json"))
}

fn config_of(case: &Value) -> Config {
    match json_loads(case.str_field("config")).expect("config text parses") {
        Value::Object(map) => map,
        other => panic!("config is not an object: {other}"),
    }
}

fn float_arg(case: &Value, key: &str) -> f64 {
    py_float(case.str_field(key)).expect("float argument is a repr")
}

/// Compare a helper result with the fixture's `value`/`error`, rendering the
/// Rust value like the generator renders the Python one.
fn assert_outcome<T>(case: &Value, actual: Result<T, SpecConfigError>, render: fn(&T) -> Value) {
    let context = case.str_field("config");
    match (case.get("error"), actual) {
        (Some(error), Err(actual)) => assert_eq!(actual.0, error.as_str().unwrap(), "{context}"),
        (None, Ok(actual)) => assert_eq!(render(&actual), *case.field("value"), "{context}"),
        (expected, actual) => panic!(
            "{context}: expected {expected:?}, got {:?}",
            actual.map(|value| render(&value))
        ),
    }
}

fn float_value(value: &f64) -> Value {
    Value::String(py_float_repr(*value))
}

#[test]
fn constants_match_python() {
    let fixture = fixture();
    let constants = fixture.field("constants");
    assert_eq!(
        constants.str_field("MAX_TIMEOUT_SECONDS"),
        py_float_repr(MAX_TIMEOUT_SECONDS)
    );
    assert_eq!(
        constants.str_field("DEFAULT_REQUEST_TIMEOUT_SECONDS"),
        py_float_repr(DEFAULT_REQUEST_TIMEOUT_SECONDS)
    );
    assert_eq!(
        constants.f64_field("DEFAULT_PAGE_PROBE_MAX_BODY_BYTES"),
        DEFAULT_PAGE_PROBE_MAX_BODY_BYTES as f64
    );
    assert_eq!(
        constants.f64_field("DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_FACTOR"),
        DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_FACTOR
    );
}

#[test]
fn positive_float_config_matches_python() {
    let fixture = fixture();
    for case in fixture.list_field("positive_float_config") {
        let actual = positive_float_config(
            &config_of(case),
            case.str_field("key"),
            float_arg(case, "default"),
        );
        assert_outcome(case, actual, float_value);
    }
}

#[test]
fn positive_int_config_matches_python() {
    let fixture = fixture();
    for case in fixture.list_field("positive_int_config") {
        let default: u64 = case.str_field("default").parse().unwrap();
        let actual = positive_int_config(&config_of(case), case.str_field("key"), default);
        match case.get("value") {
            // Python returns the exact int; Rust saturates beyond u64.
            Some(Value::String(expected)) if expected.parse::<u64>().is_err() => {
                assert_eq!(actual, Ok(u64::MAX), "{expected}");
            }
            _ => assert_outcome(case, actual, |value| Value::String(value.to_string())),
        }
    }
}

#[test]
fn bool_config_matches_python() {
    let fixture = fixture();
    for case in fixture.list_field("bool_config") {
        let actual = bool_config(
            &config_of(case),
            case.str_field("key"),
            case.bool_field("default"),
        );
        assert_outcome(case, actual, |value| Value::Bool(*value));
    }
}

#[test]
fn request_timeout_config_matches_python() {
    let fixture = fixture();
    for case in fixture.list_field("request_timeout_config") {
        assert_outcome(case, request_timeout_config(&config_of(case)), float_value);
    }
}

#[test]
fn page_probe_total_timeout_config_matches_python() {
    let fixture = fixture();
    for case in fixture.list_field("page_probe_total_timeout_config") {
        let actual = page_probe_total_timeout_config(
            &config_of(case),
            float_arg(case, "request_timeout_seconds"),
        );
        assert_outcome(case, actual, float_value);
    }
}

#[test]
fn validate_spec_config_matches_python() {
    let fixture = fixture();
    for case in fixture.list_field("validate_spec_config") {
        let config = json_loads(case.str_field("config")).unwrap();
        let spec_type = case.str_field("spec_type");
        assert_eq!(
            validate_spec_config(spec_type, &config),
            strings(case.list_field("errors")),
            "{spec_type} {}",
            case.str_field("config")
        );
    }
}

#[test]
fn every_python_test_config_of_the_slice_is_in_the_corpus() {
    // TESTMAP.md S2 rows: the configs of the Python tests must be fixture
    // cases, so a change of their outcome shows up here.
    let fixture = fixture();
    let validated: Vec<&str> = fixture
        .list_field("validate_spec_config")
        .iter()
        .map(|case| case.str_field("config"))
        .collect();
    for config in [
        r#"{"request_timeout_seconds": "5s"}"#,
        r#"{"request_timeout_seconds": null}"#,
        r#"{"request_timeout_seconds": 0}"#,
        r#"{"request_timeout_seconds": true}"#,
        r#"{"verify_tls": "false"}"#,
        r#"{"follow_redirects": "true"}"#,
        r#"{"max_body_bytes": 1.5}"#,
        r#"{"max_body_bytes": "\u00b2"}"#,
        r#"{"max_body_bytes": "\u0661"}"#,
        r#"{"request_timeout_seconds": 43200.0}"#,
        r#"{"request_timeout_seconds": "5s", "total_timeout_seconds": "abc"}"#,
        r#"{"request_timeout_seconds": 1e308}"#,
        r#"{"total_timeout_seconds": 1e308}"#,
        "[]",
    ] {
        assert!(validated.contains(&config), "{config}");
    }
    let token_object =
        json_loads(r#"{"request_timeout_seconds": {"$serde_json::private::Number": "5"}}"#)
            .unwrap();
    assert_eq!(
        validate_spec_config("http_page_probe", &token_object),
        [
            "config.request_timeout_seconds must be a number greater than 0, got \
             {'$serde_json::private::Number': '5'}"
        ]
    );
    let report_once = validate_spec_config(
        "http_json_metric",
        &serde_json::json!({"request_timeout_seconds": "5s", "total_timeout_seconds": "abc"}),
    );
    assert_eq!(
        report_once,
        [
            "config.request_timeout_seconds must be a number greater than 0, got '5s'",
            "config.total_timeout_seconds must be a number greater than 0, got 'abc'",
        ]
    );
}
