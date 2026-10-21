//! `line_protocol` against `fixtures/line_protocol.json` (built by
//! `fixtures/gen_line_protocol.py`). The request side of the `writes` cases
//! (URL, headers, errors) is tested in `graphyard-influx`.

use chrono::{DateTime, Utc};
use serde_json::Value;

use super::*;
use crate::fixture::{Field, load, strings};
use crate::logline::LogLevel;
use crate::normalize::tests::point_of;
use crate::pycompat::py_float;

fn fixture() -> Value {
    load(include_str!("../../../../fixtures/line_protocol.json"))
}

fn ts_of(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("fixture timestamp")
        .with_timezone(&Utc)
}

#[test]
fn to_line_protocol_matches_the_python_client() {
    let fixture = fixture();
    let cases = fixture.list_field("lines");
    assert!(cases.len() > 40);
    for case in cases {
        let value = py_float(case.str_field("value")).expect("fixture float repr");
        let mut point = LinePoint::new(
            case.str_field("measurement"),
            value,
            ts_of(case.str_field("ts")),
        );
        for pair in case.list_field("tags") {
            let pair = strings(pair.as_array().expect("tag pair"));
            point.tag(pair[0], pair[1]);
        }
        assert_eq!(point.to_line_protocol(), case.str_field("line"), "{case}");
    }
}

/// The log records the fixture expects from Rust (D-10 applied).
fn expected_logs(case: &Value) -> Vec<LogEvent> {
    case.list_field("logs")
        .iter()
        .map(|record| {
            let level = match record.str_field("level") {
                "WARNING" => LogLevel::Warning,
                "ERROR" => LogLevel::Error,
                other => panic!("unexpected level {other}"),
            };
            let message = record
                .get("d10_message")
                .unwrap_or_else(|| record.field("message"));
            LogEvent {
                level,
                message: message.as_str().expect("message").to_owned(),
            }
        })
        .collect()
}

#[test]
fn prepare_write_matches_write_points() {
    let fixture = fixture();
    for case in fixture.list_field("writes") {
        let name = case.str_field("name");
        let measurement = case
            .field("env")
            .get("INFLUX_MEASUREMENT")
            .and_then(Value::as_str)
            .unwrap_or("graphyard_metrics");
        let points: Vec<MetricPoint> = case.list_field("points").iter().map(point_of).collect();
        let prepared = prepare_write(measurement, &points);
        assert_eq!(prepared.total, points.len(), "{name}");
        assert_eq!(
            prepared.written() + prepared.rejected,
            points.len(),
            "{name}"
        );

        let requests = case.list_field("requests");
        if let Some(request) = requests.first() {
            assert_eq!(requests.len(), 1, "{name}");
            assert_eq!(prepared.body(), request.str_field("body"), "{name}");
        } else if case.field("result").get("written").is_some() {
            // No request although the call succeeded: nothing to write.
            assert!(prepared.is_empty(), "{name}");
        }
        if let Some(written) = case.field("result").get("written") {
            assert_eq!(
                prepared.written() as u64,
                written.as_u64().expect("count"),
                "{name}"
            );
        }

        // write_points logs the rejected count only when it returns.
        let mut logs = prepared.events.clone();
        if case.field("result").get("written").is_some() {
            logs.extend(prepared.rejected_event());
        }
        assert_eq!(logs, expected_logs(case), "{name}");
    }
}

#[test]
fn every_python_test_of_the_slice_is_in_the_corpus() {
    let fixture = fixture();
    let names: Vec<&str> = fixture
        .list_field("writes")
        .iter()
        .map(|case| case.str_field("name"))
        .collect();
    for test in [
        "test_write_points_skips_invalid_points_in_batch",
        "test_write_points_does_not_count_non_finite_points",
    ] {
        assert!(names.iter().any(|name| name.contains(test)), "{test}");
    }
}

#[test]
fn escapes_follow_the_client_tables() {
    assert_eq!(
        escape_measurement("a b,c=d\te\nf\rg\\"),
        "a\\ b\\,c=d\\te\\nf\\rg\\"
    );
    assert_eq!(escape_key("a b,c=d"), "a\\ b\\,c\\=d");
    assert_eq!(escape_tag_value("dir\\"), "dir\\ ");
    assert_eq!(escape_tag_value(""), "");
    assert_eq!(format_float_field(21.0).as_deref(), Some("21"));
    assert_eq!(format_float_field(1e16).as_deref(), Some("1e+16"));
    assert_eq!(format_float_field(f64::NAN), None);
}

#[test]
fn rejected_event_reports_the_counts() {
    let prepared = PreparedWrite {
        lines: vec!["a".into()],
        total: 3,
        rejected: 2,
        events: Vec::new(),
    };
    assert_eq!(
        prepared.rejected_event(),
        Some(LogEvent::warning(
            "metrics_write_rejected category=normalization rejected_points=2 written_points=1 total_points=3"
        ))
    );
    let clean = PreparedWrite {
        rejected: 0,
        ..prepared
    };
    assert_eq!(clean.rejected_event(), None);
}
