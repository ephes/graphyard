//! `settings` against `fixtures/settings.json` (built by
//! `fixtures/gen_settings.py`).

use std::collections::HashMap;
use std::ffi::OsString;

use serde_json::Value;

use super::*;
use crate::fixture::{Field, load, strings};

fn fixture() -> Value {
    load(include_str!("../../../../fixtures/settings.json"))
}

fn env_of(case: &Value) -> HashMap<String, String> {
    case.field("env")
        .as_object()
        .expect("env is an object")
        .iter()
        .map(|(key, value)| (key.clone(), value.as_str().expect("env value").to_owned()))
        .collect()
}

fn read(env: &HashMap<String, String>) -> Result<Settings, SettingsError> {
    Settings::from_lookup(|name| env.get(name).cloned())
}

/// The settings as the fixture renders them (integers as text).
fn rendered(settings: &Settings) -> Vec<(&'static str, String)> {
    vec![
        (
            "sqlite_timeout_seconds",
            settings.sqlite_timeout_seconds.to_string(),
        ),
        (
            "sqlite_busy_timeout_ms",
            settings.sqlite_busy_timeout_ms.to_string(),
        ),
        ("sqlite_journal_mode", settings.sqlite_journal_mode.clone()),
        ("sqlite_synchronous", settings.sqlite_synchronous.clone()),
        ("influx_url", settings.influx_url.clone()),
        ("influx_token", settings.influx_token.clone()),
        ("influx_org", settings.influx_org.clone()),
        ("influx_bucket", settings.influx_bucket.clone()),
        ("influx_measurement", settings.influx_measurement.clone()),
        ("influx_timeout_ms", settings.influx_timeout_ms.to_string()),
        ("influx_api_mode", settings.influx_api_mode.clone()),
        (
            "metric_collection_interval_seconds",
            settings.metric_collection_interval_seconds.to_string(),
        ),
        (
            "condition_eval_interval_seconds",
            settings.condition_eval_interval_seconds.to_string(),
        ),
        (
            "condition_data_stale_warning_seconds",
            settings.condition_data_stale_warning_seconds.to_string(),
        ),
    ]
}

fn assert_settings(expected: &Value, actual: &Settings, name: &str) {
    let expected = expected.as_object().expect("settings object");
    let actual = rendered(actual);
    let keys: Vec<&str> = expected.keys().map(String::as_str).collect();
    let fields: Vec<&str> = actual.iter().map(|(field, _)| *field).collect();
    assert_eq!(keys, fields, "{name}: settings fields");
    for (field, value) in actual {
        assert_eq!(
            expected[field].as_str(),
            Some(value.as_str()),
            "{name}: {field}"
        );
    }
}

#[test]
fn settings_match_python() {
    let fixture = fixture();
    let cases = fixture.list_field("cases");
    let defaults = cases
        .iter()
        .find(|case| case.str_field("name") == "defaults")
        .expect("defaults case");
    for case in cases {
        let name = case.str_field("name");
        let actual = read(&env_of(case));
        if case.get("divergence").is_some() {
            // D-7: a web-only variable fails Python's settings import; the
            // agent does not read it and starts with the other values.
            assert_eq!(case.str_field("divergence"), "D-7");
            assert!(case.get("error").is_some(), "{name}: Python fails");
            let settings = actual.unwrap_or_else(|err| panic!("{name}: {err}"));
            assert_settings(defaults.field("settings"), &settings, name);
            continue;
        }
        match case.get("error") {
            Some(error) => {
                let actual = actual.expect_err(name);
                assert_eq!(
                    actual.error.python_class(),
                    error.str_field("class"),
                    "{name}"
                );
                if error.get("p2").is_none() {
                    assert_eq!(actual.error.message, error.str_field("message"), "{name}");
                }
            }
            None => {
                let settings = actual.unwrap_or_else(|err| panic!("{name}: {err}"));
                assert_settings(case.field("settings"), &settings, name);
                let pragmas = settings.sqlite_pragmas();
                let expected = case.field("pragmas");
                assert_eq!(
                    pragmas.statements().to_vec(),
                    strings(expected.list_field("statements")),
                    "{name}"
                );
                assert_eq!(
                    pragmas.warnings,
                    strings(expected.list_field("warnings")),
                    "{name}"
                );
            }
        }
    }
}

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[test]
fn errors_name_the_variable_being_read() {
    let error = read(&env(&[
        ("GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS", "x"),
        ("GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS", "y"),
    ]))
    .unwrap_err();
    assert_eq!(error.variable, "GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS");
    assert_eq!(
        error.to_string(),
        "invalid environment variable GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS: ValueError: \
         invalid literal for int() with base 10: 'y'"
    );

    let proxied = read(&env(&[("INFLUX_TIMEOUT_MS", "$OTHER"), ("OTHER", "soon")])).unwrap_err();
    assert_eq!(proxied.variable, "INFLUX_TIMEOUT_MS");

    let inner = read(&env(&[
        ("GRAPHYARD_HA_POLL_INTERVAL_SECONDS", "often"),
        ("GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS", "90"),
    ]))
    .unwrap_err();
    assert_eq!(inner.variable, "GRAPHYARD_HA_POLL_INTERVAL_SECONDS");
}

#[test]
fn proxy_cycles_are_recursion_errors() {
    let error = read(&env(&[("INFLUX_ORG", "$INFLUX_ORG")])).unwrap_err();
    assert_eq!(error.variable, "INFLUX_ORG");
    assert_eq!(error.error.kind, PyErrorKind::RecursionError);
}

#[test]
fn api_mode_dispatch() {
    let mode = |value: &str| {
        read(&env(&[("INFLUX_API_MODE", value)]))
            .unwrap()
            .influx_api_mode()
    };
    assert_eq!(mode("V3"), InfluxApiMode::V3);
    assert_eq!(mode("auto"), InfluxApiMode::Auto);
    assert_eq!(mode("v2"), InfluxApiMode::V2);
    assert_eq!(mode(" v3 "), InfluxApiMode::V2);
    assert_eq!(mode(""), InfluxApiMode::V2);
    assert_eq!(
        read(&env(&[])).unwrap().influx_api_mode(),
        InfluxApiMode::Auto
    );
}

#[test]
fn environment_entries_are_read_lossily() {
    let vars = [
        (OsString::from("INFLUX_BUCKET"), OsString::from("metrics")),
        (
            OsString::from("INFLUX_TIMEOUT_MS"),
            OsString::from(" 2_500 "),
        ),
    ];
    let settings = Settings::from_vars(vars).unwrap();
    assert_eq!(settings.influx_bucket, "metrics");
    assert_eq!(settings.influx_timeout_ms, 2500.into());
}

#[cfg(unix)]
#[test]
fn non_utf8_values_are_replaced() {
    use std::os::unix::ffi::OsStringExt;
    let vars = [(
        OsString::from("INFLUX_ORG"),
        OsString::from_vec(b"org\xff".to_vec()),
    )];
    assert_eq!(Settings::from_vars(vars).unwrap().influx_org, "org\u{fffd}");
}
