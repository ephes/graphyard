//! `--influx-write-file` (PLAN.md 3.1): the shadow writer appends every
//! batch to a file, counts it as written and never sends a write request.

use std::net::TcpListener;

use chrono::{DateTime, Utc};
use graphyard_core::MetricPoint;
use graphyard_core::settings::Settings;
use graphyard_influx::{InfluxWriter, WriteError};
use influxdb_line_protocol::parse_lines;

fn settings(env: &[(&str, &str)]) -> Settings {
    Settings::from_lookup(|key| {
        env.iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| (*value).to_owned())
    })
    .expect("settings")
}

fn point(subject_id: &str, value: f64) -> MetricPoint {
    let ts: DateTime<Utc> = "2026-10-21T08:15:30.123456Z".parse().unwrap();
    MetricPoint::new(
        ts,
        "host.cpu_usage",
        value,
        "host",
        subject_id,
        "http",
        "graphyard",
        "macmini",
    )
}

/// A loopback port that accepts connections but is never read: a write
/// request would have to connect to it.
fn untouched_port() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    (listener, url)
}

#[tokio::test]
async fn batches_are_appended_and_counted_without_a_request() {
    let (listener, url) = untouched_port();
    let settings = settings(&[
        ("INFLUX_URL", &url),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
        ("INFLUX_API_MODE", "v2"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    std::fs::write(&path, "# earlier content\n").unwrap();
    let writer = InfluxWriter::to_file(&path).unwrap();

    // The invalid point is logged and skipped as in a real write.
    let invalid = point("", 1.0);
    let written = writer
        .write_points(
            &settings,
            &[point("macmini", 0.5), invalid, point("fractal", 2.0)],
        )
        .await
        .unwrap();
    assert_eq!(written, 2);
    assert_eq!(
        writer
            .write_points(&settings, &[point("macmini", 0.25)])
            .await
            .unwrap(),
        1
    );
    // Nothing valid: no record at all.
    assert_eq!(writer.write_points(&settings, &[]).await.unwrap(), 0);

    let text = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 6, "{text}");
    assert_eq!(lines[0], "# earlier content");
    for (comment, points) in [(lines[1], 2), (lines[4], 1)] {
        let rest = comment.strip_prefix("# write ").expect(comment);
        let (stamp, count) = rest.split_once(' ').unwrap();
        assert!(stamp.parse::<DateTime<Utc>>().is_ok(), "{stamp}");
        assert!(stamp.ends_with('Z'), "{stamp}");
        assert_eq!(count, format!("points={points}"));
    }
    let body = [lines[2], lines[3], lines[5]].join("\n");
    let parsed: Vec<_> = parse_lines(&body).collect::<Result<_, _>>().unwrap();
    assert_eq!(parsed.len(), 3);
    assert!(lines[2].starts_with("graphyard_metrics,"), "{}", lines[2]);
    assert!(lines[2].contains("subject_id=macmini"));
    assert!(lines[3].contains("subject_id=fractal"));
    assert!(lines[2].ends_with(" value=0.5 1792570530123456000"));

    // No connection ever reached the InfluxDB address.
    assert!(listener.accept().is_err());
}

#[tokio::test]
async fn configuration_errors_still_fail_the_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    let writer = InfluxWriter::to_file(&path).unwrap();

    let missing = settings(&[("INFLUX_API_MODE", "v2")]);
    let err = writer
        .write_points(&missing, &[point("macmini", 1.0)])
        .await
        .unwrap_err();
    assert!(matches!(err, WriteError::Configuration(_)), "{err:?}");
    assert_eq!(
        err.to_string(),
        "Influx setting missing: INFLUX_URL is required"
    );

    let negative = settings(&[
        ("INFLUX_URL", "http://127.0.0.1:9"),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
        ("INFLUX_TIMEOUT_MS", "-1"),
    ]);
    let err = writer
        .write_points(&negative, &[point("macmini", 1.0)])
        .await
        .unwrap_err();
    assert_eq!(err.python_class(), "ValueError");

    let scheme = settings(&[
        ("INFLUX_URL", "ftp://127.0.0.1"),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
    ]);
    let err = writer
        .write_points(&scheme, &[point("macmini", 1.0)])
        .await
        .unwrap_err();
    assert_eq!(err.python_class(), "URLSchemeUnknown");

    assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
}

#[test]
fn an_unwritable_path_fails_to_open() {
    let dir = tempfile::tempdir().unwrap();
    assert!(InfluxWriter::to_file(&dir.path().join("missing/writes.lp")).is_err());
}
