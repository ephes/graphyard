//! `normalize` against `fixtures/normalize.json` (built by
//! `fixtures/gen_normalize.py`).

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::*;
use crate::fixture::{Field, load, strings};
use crate::pycompat::{json_loads, py_float, py_float_repr};

fn fixture() -> Value {
    load(include_str!("../../../../fixtures/normalize.json"))
}

fn assert_error(case: &Value, actual: &PyError, context: &str) {
    let expected = case.field("error");
    assert_eq!(
        actual.python_class(),
        expected.str_field("class"),
        "{context}"
    );
    assert_eq!(actual.message, expected.str_field("message"), "{context}");
}

fn optional_str(value: &Value) -> Option<String> {
    value.as_str().map(str::to_owned)
}

fn tags_of(value: &Value) -> Option<Tags> {
    value.as_array().map(|pairs| {
        pairs
            .iter()
            .map(|pair| {
                let pair = strings(pair.as_array().expect("tag pair"));
                (pair[0].to_owned(), pair[1].to_owned())
            })
            .collect()
    })
}

fn ts_of(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("fixture timestamp")
        .with_timezone(&Utc)
}

/// A fixture point (`gen_normalize.py:_encode_point`).
pub(crate) fn point_of(value: &Value) -> MetricPoint {
    MetricPoint {
        ts: ts_of(value.str_field("ts")),
        metric: value.str_field("metric").to_owned(),
        value: py_float(value.str_field("value")).expect("fixture float repr"),
        subject_type: value.str_field("subject_type").to_owned(),
        subject_id: value.str_field("subject_id").to_owned(),
        source_system: value.str_field("source_system").to_owned(),
        collector_service: value.str_field("collector_service").to_owned(),
        collector_host: value.str_field("collector_host").to_owned(),
        source_instance: value.str_field("source_instance").to_owned(),
        source_entity_id: optional_str(value.field("source_entity_id")),
        host: optional_str(value.field("host")),
        service: optional_str(value.field("service")),
        tags: tags_of(value.field("tags")),
    }
}

#[test]
fn reserved_tag_keys_match_python() {
    let fixture = fixture();
    assert_eq!(
        strings(fixture.list_field("reserved_tag_keys")),
        RESERVED_TAG_KEYS.to_vec()
    );
    assert!(is_reserved_tag_key("host"));
    assert!(!is_reserved_tag_key("spec_name"));
}

#[test]
fn normalize_subject_id_matches_python() {
    let fixture = fixture();
    let cases = fixture.list_field("subject_ids");
    assert!(cases.len() > 20);
    for case in cases {
        let raw = case.str_field("raw");
        let actual = normalize_subject_id(raw);
        match case.get("ok") {
            Some(expected) => {
                assert_eq!(actual.as_deref(), Ok(expected.as_str().unwrap()), "{raw:?}")
            }
            None => assert_error(case, &actual.expect_err(raw), raw),
        }
    }
}

#[test]
fn normalize_dimension_value_matches_python() {
    let fixture = fixture();
    for case in fixture.list_field("dimension_values") {
        let raw = case.str_field("raw");
        let field_name = case.str_field("field_name");
        let fallback = case.field("fallback").as_str();
        let context = format!("{field_name} {raw:?} fallback={fallback:?}");
        let actual = normalize_dimension_value(raw, field_name, fallback);
        match case.get("ok") {
            Some(expected) => {
                assert_eq!(
                    actual.as_deref(),
                    Ok(expected.as_str().unwrap()),
                    "{context}"
                );
            }
            None => assert_error(case, &actual.expect_err(&context), &context),
        }
    }
}

#[test]
fn parse_finite_float_matches_python() {
    let fixture = fixture();
    for case in fixture.list_field("parse_finite_float") {
        let text = case.str_field("json");
        let raw = json_loads(text).expect("fixture JSON text parses");
        let actual = parse_finite_float(&raw);
        match case.get("ok") {
            Some(expected) => {
                let value = actual.unwrap_or_else(|err| panic!("{text}: {err}"));
                assert_eq!(py_float_repr(value), expected.as_str().unwrap(), "{text}");
            }
            None => assert_error(case, &actual.expect_err(text), text),
        }
    }
}

#[test]
fn normalize_metric_point_matches_python() {
    let fixture = fixture();
    for case in fixture.list_field("points") {
        let name = case.str_field("name");
        let actual = normalize_metric_point(&point_of(case.field("point")));
        let Some(expected) = case.get("ok") else {
            assert_error(case, &actual.expect_err(name), name);
            continue;
        };
        let actual = actual.unwrap_or_else(|err| panic!("{name}: {err}"));
        assert_eq!(actual.ts, ts_of(expected.str_field("ts")), "{name}");
        assert_eq!(actual.metric, expected.str_field("metric"), "{name}");
        assert_eq!(
            py_float_repr(actual.value),
            expected.str_field("value"),
            "{name}"
        );
        assert_eq!(
            actual.subject_type.as_str(),
            expected.str_field("subject_type"),
            "{name}"
        );
        assert_eq!(
            actual.subject_id,
            expected.str_field("subject_id"),
            "{name}"
        );
        assert_eq!(
            actual.source_system,
            expected.str_field("source_system"),
            "{name}"
        );
        assert_eq!(
            actual.source_instance,
            expected.str_field("source_instance"),
            "{name}"
        );
        assert_eq!(
            actual.source_entity_id,
            optional_str(expected.field("source_entity_id")),
            "{name}"
        );
        assert_eq!(
            actual.collector_service,
            expected.str_field("collector_service"),
            "{name}"
        );
        assert_eq!(
            actual.collector_host,
            expected.str_field("collector_host"),
            "{name}"
        );
        assert_eq!(actual.host, optional_str(expected.field("host")), "{name}");
        assert_eq!(
            actual.service,
            optional_str(expected.field("service")),
            "{name}"
        );
        assert_eq!(Some(actual.tags), tags_of(expected.field("tags")), "{name}");
    }
}

#[test]
fn every_python_test_of_the_slice_is_in_the_corpus() {
    let fixture = fixture();
    let names: Vec<&str> = fixture
        .list_field("points")
        .iter()
        .map(|case| case.str_field("name"))
        .collect();
    for test in [
        "test_normalize_metric_point_rejects_host_mismatch",
        "test_normalize_metric_point_drops_host_for_non_host_subject",
        "test_normalize_metric_point_rejects_non_finite_value",
    ] {
        assert!(names.iter().any(|name| name.contains(test)), "{test}");
    }
    // test_parse_finite_float_rejects_non_finite / _accepts_numbers and the
    // "NaN" body of test_http_json_metric_spec_skips_non_finite_value.
    let inputs: Vec<&str> = fixture
        .list_field("parse_finite_float")
        .iter()
        .map(|case| case.str_field("json"))
        .collect();
    for text in [
        "\"nan\"",
        "\"inf\"",
        "\"-Infinity\"",
        "\"NaN\"",
        "\"1.5\"",
        "3",
    ] {
        assert!(inputs.contains(&text), "{text}");
    }
}
