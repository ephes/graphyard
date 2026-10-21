//! `influx.query_condition_window` against `fixtures/flux.json` (`window`
//! and `flux_text` cases): the lookback window, the tag filters, the host
//! filter warning, the errors in Python's order, and the Flux text.

mod common;

use chrono::{DateTime, Utc};
use common::{capture_logs, expected_logs, list, load, take_logs, text};
use graphyard_core::pycompat::py_isoformat_utc;
use graphyard_influx::flux::flux_query;
use graphyard_influx::{ConditionQuery, QueryRange, condition_window};
use num_bigint::BigInt;
use serde_json::Value;

fn fixture() -> Value {
    load(include_str!("../../../fixtures/flux.json"))
}

fn time(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("fixture time")
        .to_utc()
}

fn optional(value: &Value) -> Option<String> {
    value.as_str().map(str::to_owned)
}

#[test]
fn condition_window_matches_python() {
    let fixture = fixture();
    let cases = list(&fixture, "window");
    assert!(cases.len() >= 30);
    for case in cases {
        let name = text(case, "name");
        let condition = &case["condition"];
        let query = ConditionQuery {
            id: condition["id"].as_i64().expect("id"),
            name: text(condition, "name"),
            metric_name: text(condition, "metric_name"),
            host_filter: text(condition, "host_filter"),
            service_filter: text(condition, "service_filter"),
            subject_type_filter: text(condition, "subject_type_filter"),
            subject_id_filter: text(condition, "subject_id_filter"),
            tags_filter: &condition["tags_filter"],
            window_minutes: condition["window_minutes"].as_i64().expect("window"),
            breach_minutes: condition["breach_minutes"].as_i64().expect("breach"),
            stale_after_seconds: condition["stale_after_seconds"].as_i64(),
        };
        let stale_warning: BigInt = text(case, "stale_warning").parse().expect("integer");
        capture_logs();
        let result = condition_window(&query, &stale_warning, time(text(case, "now")));
        assert_eq!(take_logs(), expected_logs(case, ""), "{name}");
        if let Some(error) = case.get("error") {
            let err = result.expect_err(name);
            assert_eq!(err.python_class(), text(error, "class"), "{name}");
            assert_eq!(err.to_string(), text(error, "message"), "{name}");
            continue;
        }
        let range = result.unwrap_or_else(|err| panic!("{name}: {err}"));
        let expected = &case["range"];
        assert_eq!(range.metric, text(expected, "metric"), "{name}");
        assert_eq!(
            py_isoformat_utc(&range.start),
            text(expected, "start"),
            "{name}"
        );
        assert_eq!(
            py_isoformat_utc(&range.stop),
            text(expected, "stop"),
            "{name}"
        );
        assert_eq!(range.host, optional(&expected["host"]), "{name}");
        assert_eq!(range.service, optional(&expected["service"]), "{name}");
        let tags: Vec<(String, String)> = expected["tags"]
            .as_object()
            .expect("tags")
            .iter()
            .map(|(k, v)| (k.clone(), v.as_str().expect("tag").to_owned()))
            .collect();
        let actual: Vec<(String, String)> = range.tags.clone().into_iter().collect();
        assert_eq!(actual, tags, "{name}");
        assert_eq!(
            flux_query(&range, "graphyard", "graphyard_metrics"),
            text(case, "flux"),
            "{name}"
        );
    }
}

#[test]
fn flux_text_matches_python() {
    let fixture = fixture();
    for case in list(&fixture, "flux_text") {
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
        assert_eq!(
            flux_query(&range, text(case, "bucket"), text(case, "measurement")),
            text(case, "flux"),
            "{name}"
        );
    }
}
