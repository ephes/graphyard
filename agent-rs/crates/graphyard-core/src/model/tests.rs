//! `model` against `fixtures/model.json` (built by `fixtures/gen_model.py`).

use chrono::{TimeZone, Utc};
use serde_json::Value;

use super::*;
use crate::fixture::{Field, load, strings};

fn fixture() -> Value {
    load(include_str!("../../../../fixtures/model.json"))
}

fn names<T: Copy>(all: &[T], as_str: fn(T) -> &'static str) -> Vec<&'static str> {
    all.iter().map(|&item| as_str(item)).collect()
}

#[test]
fn status_levels_match_python() {
    let fixture = fixture();
    let cases = fixture.list_field("status_levels");
    assert_eq!(cases.len(), StatusLevel::ALL.len());
    for (case, level) in cases.iter().zip(StatusLevel::ALL) {
        assert_eq!(case.str_field("value"), level.as_str());
        assert_eq!(case.f64_field("severity"), f64::from(level.severity()));
        assert_eq!(StatusLevel::parse(level.as_str()), Some(level));
        assert_eq!(level.to_string(), level.as_str());
    }
}

#[test]
fn subject_types_match_python() {
    let fixture = fixture();
    assert_eq!(
        strings(fixture.list_field("subject_types")),
        names(&SubjectType::ALL, SubjectType::as_str)
    );
    assert_eq!(
        strings(fixture.list_field("subject_types_all_sorted")),
        SubjectType::SORTED_NAMES
    );
    for kind in SubjectType::ALL {
        assert_eq!(SubjectType::parse(kind.as_str()), Some(kind));
        assert_eq!(kind.to_string(), kind.as_str());
    }
}

#[test]
fn spec_types_match_python_and_all_have_an_executor() {
    let fixture = fixture();
    let spec_types = names(&SpecType::ALL, SpecType::as_str);
    assert_eq!(strings(fixture.list_field("spec_types")), spec_types);
    assert_eq!(strings(fixture.list_field("spec_executors")), spec_types);
    for kind in SpecType::ALL {
        assert_eq!(SpecType::parse(kind.as_str()), Some(kind));
        assert_eq!(kind.to_string(), kind.as_str());
    }
}

#[test]
fn comparison_operators_match_python() {
    let fixture = fixture();
    assert_eq!(
        strings(fixture.list_field("comparison_operators")),
        names(&ComparisonOperator::ALL, ComparisonOperator::as_str)
    );
    let cases = fixture.list_field("comparisons");
    assert_eq!(cases.len(), ComparisonOperator::ALL.len() * 7);
    for case in cases {
        let operator = ComparisonOperator::parse(case.str_field("operator")).expect("operator");
        let (left, right) = (case.f64_field("left"), case.f64_field("right"));
        assert_eq!(
            operator.compare(left, right),
            case.bool_field("result"),
            "{operator} {left:?} {right:?}"
        );
    }
    let unknown = fixture.field("unknown_operator");
    assert_eq!(unknown.str_field("class"), "KeyError");
    assert_eq!(
        ComparisonOperator::parse(unknown.str_field("operator")),
        None
    );
}

#[test]
fn lookups_are_exact_like_the_python_dictionaries() {
    for text in ["OK", " ok", "ok ", "Critical", ""] {
        assert_eq!(StatusLevel::parse(text), None, "{text:?}");
    }
    for text in ["Host", " host", "network-device", ""] {
        assert_eq!(SubjectType::parse(text), None, "{text:?}");
    }
    for text in [
        "HTTP_JSON_METRIC",
        "http_json_metric ",
        "home_assistant",
        "",
    ] {
        assert_eq!(SpecType::parse(text), None, "{text:?}");
    }
    for text in [">", "GT", "ge", ""] {
        assert_eq!(ComparisonOperator::parse(text), None, "{text:?}");
    }
}

/// The dataclass fields in Python order: `(name, required)`. A field added,
/// removed, reordered or given a default in Python fails here and needs a
/// deliberate change of the Rust type.
fn assert_fields(fixture: &Value, class: &str, expected: &[(&str, bool)]) {
    let fields = fixture.field("dataclasses").list_field(class);
    let actual: Vec<(&str, bool)> = fields
        .iter()
        .map(|field| (field.str_field("name"), field.bool_field("required")))
        .collect();
    assert_eq!(actual, expected, "{class} fields");
}

fn default_of<'a>(fixture: &'a Value, class: &str, name: &str) -> &'a Value {
    fixture
        .field("dataclasses")
        .list_field(class)
        .iter()
        .find(|field| field.str_field("name") == name)
        .unwrap_or_else(|| panic!("{class}.{name} missing"))
        .field("default")
}

#[test]
fn metric_point_matches_the_python_dataclass() {
    let fixture = fixture();
    assert_fields(
        &fixture,
        "MetricPoint",
        &[
            ("ts", true),
            ("metric", true),
            ("value", true),
            ("subject_type", true),
            ("subject_id", true),
            ("source_system", true),
            ("collector_service", true),
            ("collector_host", true),
            ("source_instance", false),
            ("source_entity_id", false),
            ("host", false),
            ("service", false),
            ("tags", false),
        ],
    );
    let ts = Utc.with_ymd_and_hms(2026, 10, 21, 12, 0, 0).unwrap();
    let point = MetricPoint::new(ts, "m", 1.5, "host", "macmini", "s", "c", "h");
    assert_eq!(
        default_of(&fixture, "MetricPoint", "source_instance").as_str(),
        Some(point.source_instance.as_str())
    );
    for name in ["source_entity_id", "host", "service", "tags"] {
        assert!(
            default_of(&fixture, "MetricPoint", name).is_null(),
            "{name}"
        );
    }
    assert_eq!(point.source_entity_id, None);
    assert_eq!(point.host, None);
    assert_eq!(point.service, None);
    assert_eq!(point.tags, None);
    assert_eq!(
        (
            point.ts,
            point.metric.as_str(),
            point.value,
            point.subject_type.as_str(),
            point.subject_id.as_str(),
            point.source_system.as_str(),
            point.collector_service.as_str(),
            point.collector_host.as_str(),
        ),
        (ts, "m", 1.5, "host", "macmini", "s", "c", "h")
    );
}

#[test]
fn metric_sample_matches_the_python_dataclass() {
    let fixture = fixture();
    assert_fields(
        &fixture,
        "MetricSample",
        &[
            ("ts", true),
            ("value", true),
            ("host", true),
            ("metric", true),
            ("service", true),
            ("subject_type", false),
            ("subject_id", false),
            ("source_system", false),
            ("source_instance", false),
            ("source_entity_id", false),
            ("collector_service", false),
            ("collector_host", false),
            ("tags", false),
        ],
    );
    for name in [
        "subject_type",
        "subject_id",
        "source_system",
        "source_instance",
        "source_entity_id",
        "collector_service",
        "collector_host",
    ] {
        assert!(
            default_of(&fixture, "MetricSample", name).is_null(),
            "{name}"
        );
    }
    let tags_default = default_of(&fixture, "MetricSample", "tags");
    assert_eq!(tags_default.as_object().map(|tags| tags.len()), Some(0));

    let ts = Utc.with_ymd_and_hms(2026, 10, 21, 12, 0, 0).unwrap();
    let sample = MetricSample::new(ts, 2.0, "macmini", "m", None);
    assert_eq!(sample.subject_type, None);
    assert_eq!(sample.collector_host, None);
    assert!(sample.tags.is_empty());
}

#[test]
fn result_types_match_the_python_dataclasses() {
    let fixture = fixture();
    assert_fields(
        &fixture,
        "ConditionEvaluation",
        &[
            ("status", true),
            ("message", true),
            ("last_value", true),
            ("evaluated_at", true),
        ],
    );
    assert_fields(
        &fixture,
        "ConditionEvaluationRun",
        &[("total", true), ("failed", true)],
    );
    assert_fields(
        &fixture,
        "MetricCollectionSpecRun",
        &[
            ("total", true),
            ("failed", true),
            ("warning", true),
            ("ingested", true),
            ("skipped", true),
        ],
    );
    assert_eq!(
        SpecOutcome::new(StatusLevel::Warning, 0, 1, "invalid JSON payload"),
        SpecOutcome {
            status: StatusLevel::Warning,
            ingested: 0,
            skipped: 1,
            message: "invalid JSON payload".to_owned(),
        }
    );
}

#[test]
fn tags_keep_python_dict_order() {
    let mut tags = Tags::new();
    tags.insert("b".to_owned(), "1".to_owned());
    tags.insert("a".to_owned(), "2".to_owned());
    // Re-assigning a key keeps its position, like a dict.
    tags.insert("b".to_owned(), "3".to_owned());
    let pairs: Vec<(&str, &str)> = tags.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    assert_eq!(pairs, [("b", "3"), ("a", "2")]);
}
