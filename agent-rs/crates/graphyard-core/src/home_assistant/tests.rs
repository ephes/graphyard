//! `home_assistant` against `fixtures/ha_mapping.json` (built by
//! `fixtures/gen_ha_mapping.py`).

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::*;
use crate::fixture::{Field, load, strings};
use crate::logline::LogLevel;
use crate::normalize::tests::point_of;
use crate::pycompat::{json_loads, py_float, py_float_repr};

fn fixture() -> Value {
    load(include_str!("../../../../fixtures/ha_mapping.json"))
}

fn tags_of(value: &Value) -> Tags {
    value
        .as_array()
        .expect("tag pairs")
        .iter()
        .map(|pair| {
            let pair = strings(pair.as_array().expect("tag pair"));
            (pair[0].to_owned(), pair[1].to_owned())
        })
        .collect()
}

fn tag_pairs(tags: &Tags) -> Vec<(String, String)> {
    tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Compare the records of one call; Python does not escape line breaks
/// (D-10), so the expected text is escaped here, and a regex compile error
/// text is P2 (only the prefix is compared).
fn assert_logs(expected: &[Value], actual: &[LogEvent], context: &str) {
    assert_eq!(expected.len(), actual.len(), "{context}: {actual:?}");
    for (expected, actual) in expected.iter().zip(actual) {
        assert_eq!(actual.level, LogLevel::Warning, "{context}");
        assert_eq!(expected.str_field("level"), "WARNING", "{context}");
        if expected.get("p2").is_some() {
            let prefix = escape_log_value(expected.str_field("prefix"));
            assert!(
                actual.message.starts_with(&prefix) && actual.message.len() > prefix.len(),
                "{context}: {:?} does not start with {prefix:?}",
                actual.message
            );
        } else {
            assert_eq!(
                actual.message,
                escape_log_value(expected.str_field("message")),
                "{context}"
            );
        }
    }
}

fn assert_error(expected: &Value, actual: &PyError, context: &str) {
    assert_eq!(
        actual.python_class(),
        expected.str_field("class"),
        "{context}"
    );
    assert_eq!(actual.message, expected.str_field("message"), "{context}");
}

fn run_call(
    call: &Value,
    spec: HaSpec<'_>,
    now: DateTime<Utc>,
    state: &mut MappingState,
    context: &str,
) {
    let result = match call.str_field("fn") {
        "normalize" => {
            let payload = json_loads(call.str_field("payload")).expect("payload JSON");
            let payload = payload.as_object().expect("payload object");
            let extra_tags = tags_of(call.field("extra_tags"));
            normalize_state(
                payload,
                spec,
                call.str_field("metric_name_override"),
                &extra_tags,
                now,
                state,
            )
            .map(|point| {
                let expected = call.field("point");
                match (point, expected.is_null()) {
                    (None, true) => {}
                    (Some(point), false) => assert_eq!(point, point_of(expected), "{context}"),
                    (point, _) => panic!("{context}: got {point:?}, expected {expected}"),
                }
            })
        }
        "subject" => {
            resolve_subject_mapping(spec, call.str_field("entity_id"), state).map(|resolved| {
                let expected = call.field("subject");
                let expected = expected.as_array().map(|pair| {
                    let pair = strings(pair);
                    (pair[0].to_owned(), pair[1].to_owned())
                });
                assert_eq!(resolved, expected, "{context}");
            })
        }
        "metric" => {
            let value = py_float(call.str_field("value")).expect("value repr");
            resolve_metric_mapping(
                spec,
                call.str_field("entity_id"),
                call.str_field("metric_name"),
                value,
                state,
            )
            .map(|(name, value, tags)| {
                let expected = call.list_field("metric");
                assert_eq!(name, expected[0].as_str().expect("name"), "{context}");
                assert_eq!(
                    py_float_repr(value),
                    expected[1].as_str().expect("value"),
                    "{context}"
                );
                assert_eq!(
                    tag_pairs(&tags),
                    tag_pairs(&tags_of(&expected[2])),
                    "{context}"
                );
            })
        }
        other => panic!("unknown call {other}"),
    };
    match (result, call.get("error")) {
        (Ok(()), None) => {}
        (Err(err), Some(expected)) => assert_error(expected, &err, context),
        (result, expected) => panic!("{context}: got {result:?}, expected error {expected:?}"),
    }
    assert_logs(call.list_field("logs"), &state.take_events(), context);
}

#[test]
fn ha_mapping_cases_match_python() {
    let fixture = fixture();
    let now = DateTime::parse_from_rfc3339(fixture.str_field("now"))
        .expect("now")
        .with_timezone(&Utc);
    let cases = fixture.list_field("cases");
    assert!(cases.len() >= 25, "fixture has the full corpus");
    for case in cases {
        let config = json_loads(case.str_field("config")).expect("config JSON");
        let spec = HaSpec {
            name: case.str_field("spec_name"),
            config: config.as_object().expect("config object"),
        };
        let mut state = MappingState::new();
        for (index, call) in case.list_field("calls").iter().enumerate() {
            let context = format!("{} call {index}", case.str_field("name"));
            run_call(call, spec, now, &mut state, &context);
        }
        let mut warned: Vec<&str> = state.warned_keys().collect();
        warned.sort_unstable();
        assert_eq!(
            warned,
            strings(case.list_field("warned_keys")),
            "{}",
            case.str_field("name")
        );
    }
}

#[test]
fn production_env_scan_config_is_pinned() {
    let fixture = fixture();
    let production = &fixture.list_field("cases")[0];
    assert_eq!(
        production.str_field("name"),
        "Home Assistant Environment Scan"
    );
    let points: Vec<&Value> = production
        .list_field("calls")
        .iter()
        .map(|call| call.field("point"))
        .filter(|point| !point.is_null())
        .collect();
    assert_eq!(points.len(), 6);
}

#[test]
fn fullmatch_anchors_alternations_and_verbose_patterns() {
    let mut state = MappingState::new();
    assert!(state.fullmatch("a|ab", "ab").expect("match"));
    assert!(!state.fullmatch("a|ab", "abc").expect("match"));
    assert!(state.fullmatch("(?x) a b  # comment", "ab").expect("match"));
    assert!(
        state
            .fullmatch("(?i)(?x)sensor\\.temp # comment", "SENSOR.temp")
            .expect("match")
    );
    assert!(state.fullmatch("(?x)(?i) a # c", "A").expect("match"));
    assert!(!state.fullmatch("(?i)a # c", "a").expect("match"));
    assert!(state.fullmatch("(?i)a # c", "a # c").expect("match"));
    assert!(!verbose("(?:x)a"));
    assert!(!verbose("(?P<x>a)"));
    assert!(!state.fullmatch("abc", "xabc").expect("match"));
    assert!(state.fullmatch("(?i)ABC", "abc").expect("match"));
}

#[test]
fn regex_runtime_failures_are_errors() {
    // Catastrophic backtracking hits fancy-regex's backtrack limit; Python
    // would keep going (D-5).
    let mut state = MappingState::new();
    let regex = state.compile("(a|aa)+\\1x", false).expect("compiles");
    let err = regex_search(&regex, &"a".repeat(60)).expect_err("backtrack limit");
    assert!(err.message.starts_with("regex match failed: "), "{err:?}");
}

#[test]
fn compiled_regexes_are_cached_per_flags() {
    let mut state = MappingState::new();
    let first = state.compile("abc", true).expect("compiles");
    let second = state.compile("abc", true).expect("compiles");
    assert!(Arc::ptr_eq(&first, &second));
    assert!(regex_search(&first, "xABCx").expect("search"));
    let sensitive = state.compile("abc", false).expect("compiles");
    assert!(!regex_search(&sensitive, "ABC").expect("search"));
    assert!(state.compile("(unclosed", false).is_err());
}
