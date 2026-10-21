//! `conditions` against `fixtures/conditions.json` (built by
//! `fixtures/gen_conditions.py`), plus the error texts and the source
//! plumbing the fixture cannot show.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use chrono::{DateTime, TimeZone, Utc};
use num_bigint::BigInt;
use serde_json::Value;

use super::*;
use crate::fixture::{Field, load};
use crate::model::Tags;
use crate::pycompat::{PyErrorKind, py_int};

fn fixture() -> Value {
    load(include_str!("../../../../fixtures/conditions.json"))
}

/// Drive a future that never waits (every source here is ready at once).
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("a stub sample source never waits"),
    }
}

fn datetime(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .unwrap_or_else(|err| panic!("fixture datetime {text:?}: {err}"))
        .with_timezone(&Utc)
}

/// A `repr(float)` text.
fn float(text: &str) -> f64 {
    text.parse()
        .unwrap_or_else(|err| panic!("fixture float {text:?}: {err}"))
}

fn optional_str(value: &Value, key: &str) -> Option<String> {
    let field = value.field(key);
    (!field.is_null()).then(|| field.as_str().expect("string or null").to_owned())
}

fn optional_float(value: &Value, key: &str) -> Option<f64> {
    optional_str(value, key).map(|text| float(&text))
}

fn optional_i64(value: &Value, key: &str) -> Option<i64> {
    let field = value.field(key);
    (!field.is_null()).then(|| field.as_i64().expect("integer or null"))
}

fn big(value: &Value) -> BigInt {
    py_int(&value.to_string()).expect("fixture integer")
}

fn rule(value: &Value) -> ConditionRule {
    ConditionRule {
        metric_name: value.str_field("metric_name").to_owned(),
        operator: value.str_field("operator").to_owned(),
        warning_threshold: optional_float(value, "warning_threshold"),
        critical_threshold: optional_float(value, "critical_threshold"),
        breach_minutes: value.field("breach_minutes").as_i64().expect("integer"),
        stale_after_seconds: optional_i64(value, "stale_after_seconds"),
        alert_when_stale: value.bool_field("alert_when_stale"),
    }
}

fn sample(value: &Value) -> MetricSample {
    let tags: Tags = value
        .field("tags")
        .as_object()
        .expect("tags object")
        .iter()
        .map(|(name, tag)| (name.clone(), tag.as_str().expect("tag value").to_owned()))
        .collect();
    MetricSample {
        ts: datetime(value.str_field("ts")),
        value: float(value.str_field("value")),
        host: value.str_field("host").to_owned(),
        metric: value.str_field("metric").to_owned(),
        service: optional_str(value, "service"),
        subject_type: optional_str(value, "subject_type"),
        subject_id: optional_str(value, "subject_id"),
        source_system: optional_str(value, "source_system"),
        source_instance: optional_str(value, "source_instance"),
        source_entity_id: optional_str(value, "source_entity_id"),
        collector_service: optional_str(value, "collector_service"),
        collector_host: optional_str(value, "collector_host"),
        tags,
    }
}

fn float_text(value: Option<f64>) -> Option<String> {
    value.map(py_float_repr)
}

fn assert_case(case: &Value) {
    let name = case.str_field("name");
    let rule = rule(case.field("condition"));
    let samples: Vec<MetricSample> = case.list_field("samples").iter().map(sample).collect();
    let now = datetime(case.str_field("now"));
    let stale = big(case.field("stale_warning_seconds"));
    let query_error = case.get("query_error").and_then(Value::as_str);

    let mut queried = Vec::new();
    let mut source = |at: DateTime<Utc>| {
        queried.push(at);
        let result = match query_error {
            Some(message) => Err(message.to_owned()),
            None => Ok(samples.clone()),
        };
        async move { result }
    };
    let outcome = ready(evaluate_condition(&rule, &stale, now, &mut source));
    assert_eq!(
        queried,
        [datetime(case.str_field("query_now"))],
        "{name}: query now"
    );

    match case.get("error") {
        Some(error) => {
            let err = outcome.expect_err(name);
            match &err {
                EvaluationError::Query(message) => {
                    assert_eq!(Some(message.as_str()), query_error, "{name}");
                    assert_eq!(err.to_string(), error.str_field("message"), "{name}");
                }
                EvaluationError::Evaluation(py) => {
                    assert_eq!(py.python_class(), error.str_field("class"), "{name}");
                    if error.get("p2").is_none() {
                        assert_eq!(err.to_string(), error.str_field("message"), "{name}");
                    }
                    let direct = evaluate_samples(&rule, &samples, now, &stale);
                    assert_eq!(direct.as_ref().err(), Some(py), "{name}: evaluate_samples");
                }
            }
        }
        None => {
            let expected = case.field("result");
            let result = outcome.unwrap_or_else(|err| panic!("{name}: {err}"));
            assert_eq!(
                result.status.as_str(),
                expected.str_field("status"),
                "{name}"
            );
            assert_eq!(result.message, expected.str_field("message"), "{name}");
            assert_eq!(
                float_text(result.last_value),
                optional_str(expected, "last_value"),
                "{name}: last_value"
            );
            assert_eq!(
                result.evaluated_at,
                datetime(expected.str_field("evaluated_at")),
                "{name}: evaluated_at"
            );
            let direct = evaluate_samples(&rule, &samples, now, &stale).expect(name);
            assert_eq!(direct.status, result.status, "{name}: evaluate_samples");
            assert_eq!(direct.message, result.message, "{name}: evaluate_samples");
        }
    }
}

#[test]
fn every_fixture_case_matches() {
    let fixture = fixture();
    let cases = fixture.list_field("cases");
    assert!(cases.len() > 100);
    for case in cases {
        assert_case(case);
    }
}

#[test]
fn total_seconds_rounds_like_cpython() {
    let fixture = fixture();
    for case in fixture.list_field("total_seconds") {
        let micros = case.field("micros").as_i64().expect("micros");
        let seconds = py_total_seconds(micros);
        assert_eq!(py_float_repr(seconds), case.str_field("repr"), "{micros}");
        assert_eq!(
            seconds.trunc() as i64,
            case.field("int").as_i64().expect("int"),
            "{micros}"
        );
    }
}

#[test]
fn every_python_test_of_the_slice_is_in_the_corpus() {
    // TESTMAP.md S5 rows: every `evaluate_condition` scenario of
    // tests/test_condition_evaluation.py is a fixture case.
    let fixture = fixture();
    let names: Vec<&str> = fixture
        .list_field("cases")
        .iter()
        .map(|case| case.str_field("name"))
        .collect();
    for test in [
        "test_evaluate_condition_no_samples_returns_warning",
        "test_evaluate_condition_stale_data_returns_warning",
        "test_evaluate_condition_critical_threshold_path",
        "test_evaluate_condition_warning_threshold_path",
        "test_evaluate_condition_grace_window_prevents_false_breach",
        "test_one_breaching_series_next_to_healthy_one_is_critical",
        "test_stale_series_next_to_fresh_one_is_warning",
        "test_healthy_multi_series_reports_newest_value",
        "test_series_split_by_collector_dimension",
        "test_five_minute_series_breaches_for_every_phase_offset",
        "test_five_minute_series_critical_for_every_evaluation",
        "test_five_minute_series_with_one_in_window_sample_below_is_ok",
        "test_anchor_below_threshold_means_breach_not_held_yet",
        "test_series_younger_than_breach_window_is_not_breached",
        "test_anchor_older_than_staleness_limit_is_not_carried_forward",
        "test_one_minute_series_with_anchor_still_breaches",
        "test_sample_at_window_start_supersedes_the_anchor",
        "test_breach_shorter_than_sampling_interval_uses_the_anchor_alone",
        "test_breach_shorter_than_sampling_interval_ok_when_anchor_is_below",
        "test_stale_sample_within_allowance_is_evaluated",
        "test_stale_sample_past_allowance_warns",
        "test_intermittent_condition_reports_stale_data_as_ok",
        "test_intermittent_condition_with_no_samples_is_ok",
        "test_intermittent_condition_still_alerts_on_fresh_breach",
        "test_default_condition_staleness_is_unchanged",
    ] {
        assert!(
            names
                .iter()
                .any(|name| *name == test || name.starts_with(&format!("{test}["))),
            "{test}"
        );
    }
}

fn humidity_rule() -> ConditionRule {
    ConditionRule {
        metric_name: "ha.sensor.office_humidity".to_owned(),
        operator: "gt".to_owned(),
        warning_threshold: Some(60.0),
        critical_threshold: Some(75.0),
        breach_minutes: 5,
        stale_after_seconds: None,
        alert_when_stale: true,
    }
}

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 4, 12, 0, 0).unwrap()
}

fn fresh(value: f64) -> Vec<MetricSample> {
    (0..=5)
        .rev()
        .map(|minutes| {
            MetricSample::new(
                now() - chrono::TimeDelta::minutes(minutes),
                value,
                "homeassistant",
                "ha.sensor.office_humidity",
                Some("homeassistant".to_owned()),
            )
        })
        .collect()
}

#[test]
fn overflow_errors_carry_the_cpython_texts() {
    // The fixture stores these library texts as P2 (class only); the Rust
    // side renders the texts CPython 3.12 prints.
    let stale = BigInt::from(600);
    for (breach_minutes, message) in [
        (
            1_440_000_000_000,
            "days=1000000000; must have magnitude <= 999999999",
        ),
        (
            -1_440_000_001_440,
            "days=-1000000001; must have magnitude <= 999999999",
        ),
        (1440 * (1 << 31), "Python int too large to convert to C int"),
        (i64::MAX, "Python int too large to convert to C int"),
        (1_439_999_999_999, "date value out of range"),
        (1_200_000_000, "date value out of range"),
    ] {
        let rule = ConditionRule {
            breach_minutes,
            ..humidity_rule()
        };
        let err = evaluate_samples(&rule, &fresh(80.0), now(), &stale).expect_err(message);
        assert_eq!(err.kind, PyErrorKind::OverflowError, "{breach_minutes}");
        assert_eq!(err.message, message, "{breach_minutes}");
    }
}

#[test]
fn unknown_operator_is_a_key_error_with_the_repr() {
    let rule = ConditionRule {
        operator: "it's".to_owned(),
        ..humidity_rule()
    };
    let err = evaluate_samples(&rule, &fresh(80.0), now(), &BigInt::from(600)).unwrap_err();
    assert_eq!(err.kind, PyErrorKind::KeyError);
    assert_eq!(err.to_string(), "\"it's\"");
}

#[test]
fn query_errors_display_as_the_source_error() {
    let mut source = |_: DateTime<Utc>| async { Err::<Vec<MetricSample>, _>("boom".to_owned()) };
    let err = ready(evaluate_condition(
        &humidity_rule(),
        &BigInt::from(600),
        now(),
        &mut source,
    ))
    .unwrap_err();
    assert_eq!(err, EvaluationError::Query("boom".to_owned()));
    assert_eq!(err.to_string(), "boom");
}

#[test]
fn now_is_truncated_to_microseconds_for_query_and_result() {
    let precise = now() + chrono::TimeDelta::nanoseconds(123_456_789);
    let expected = now() + chrono::TimeDelta::microseconds(123_456);
    let mut seen = Vec::new();
    let mut source = |at: DateTime<Utc>| {
        seen.push(at);
        async { Ok::<_, String>(Vec::new()) }
    };
    let result = ready(evaluate_condition(
        &humidity_rule(),
        &BigInt::from(600),
        precise,
        &mut source,
    ))
    .unwrap();
    assert_eq!(seen, [expected]);
    assert_eq!(result.evaluated_at, expected);
}

/// A named source type, as the agent will implement it.
struct Fixed(Vec<MetricSample>);

impl SampleSource for Fixed {
    type Error = std::convert::Infallible;

    async fn query_condition_window(
        &mut self,
        _now: DateTime<Utc>,
    ) -> Result<Vec<MetricSample>, Self::Error> {
        Ok(self.0.clone())
    }
}

#[test]
fn a_source_type_can_implement_the_trait() {
    let mut source = Fixed(fresh(80.0));
    let result = ready(evaluate_condition(
        &humidity_rule(),
        &BigInt::from(600),
        now(),
        &mut source,
    ))
    .unwrap();
    assert_eq!(result.status, StatusLevel::Critical);
    assert_eq!(result.message, "ha.sensor.office_humidity gt 75.0 for 5m");
    assert_eq!(result.last_value, Some(80.0));
}

#[test]
fn stale_limit_uses_the_allowance_unless_unset_or_zero() {
    let global = BigInt::from(600);
    for (allowance, expected) in [(None, 600), (Some(0), 600), (Some(3600), 3600)] {
        let rule = ConditionRule {
            stale_after_seconds: allowance,
            ..humidity_rule()
        };
        assert_eq!(rule.stale_limit_seconds(&global), BigInt::from(expected));
    }
}

#[test]
fn far_apart_timestamps_do_not_panic() {
    // Outside Python's datetime range (chrono allows more): the age
    // saturates instead of overflowing.
    let ancient = MetricSample::new(
        DateTime::<Utc>::MIN_UTC,
        80.0,
        "h",
        "ha.sensor.office_humidity",
        None,
    );
    let result = evaluate_samples(
        &humidity_rule(),
        &[ancient],
        DateTime::<Utc>::MAX_UTC,
        &BigInt::from(600),
    )
    .unwrap();
    assert_eq!(result.status, StatusLevel::Warning);
    assert!(result.message.starts_with("Latest sample is stale ("));
}
