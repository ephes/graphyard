//! Opt-in oracle against disposable local InfluxDB 2.7.12 or 3.12.2.
//! See docs/agent-rs.md. Never points at an owner database.
use chrono::{DateTime, Utc};
use graphyard_core::{model::MetricPoint, settings::Settings};
use graphyard_influx::InfluxWriter;

fn point(ts: &str) -> MetricPoint {
    MetricPoint::new(
        DateTime::parse_from_rfc3339(ts)
            .unwrap()
            .with_timezone(&Utc),
        "timestamp_oracle",
        1.0,
        "host",
        "synthetic",
        "oracle",
        "oracle",
        "synthetic",
    )
}

#[tokio::test]
#[ignore = "requires disposable local InfluxDB; see docs/agent-rs.md"]
async fn real_timestamp_storage_domain() {
    let mode = std::env::var("GRAPHYARD_TIMESTAMP_ORACLE").expect("v2 or v3");
    let (port, max) = match mode.as_str() {
        "v2" => (18290, "2262-04-11T23:47:16.854775Z"),
        "v3" => (18289, "2262-04-11T23:39:59.999999Z"),
        _ => panic!("unsupported local oracle mode"),
    };
    let settings = Settings::from_vars([
        (
            "INFLUX_URL".into(),
            format!("http://127.0.0.1:{port}").into(),
        ),
        ("INFLUX_TOKEN".into(), "synthetic-oracle-token".into()),
        ("INFLUX_ORG".into(), "oracle".into()),
        ("INFLUX_BUCKET".into(), "oracle".into()),
    ])
    .unwrap();
    let writer = InfluxWriter::new().unwrap();
    // Python-compatible precision: the nearest supported microseconds.
    for ts in ["1677-09-21T00:12:43.145225Z", "1970-01-01T00:00:00Z", max] {
        assert_eq!(
            writer.write_points(&settings, &[point(ts)]).await.unwrap(),
            1,
            "{ts}"
        );
    }
    for ts in [
        "0001-01-01T00:00:00Z",
        "9999-12-31T23:59:59.999999Z",
        "1677-09-21T00:12:43.145224Z",
        "2262-04-11T23:47:16.854776Z",
    ] {
        assert!(
            writer.write_points(&settings, &[point(ts)]).await.is_err(),
            "{ts}"
        );
        // A mixed invalid batch must not be reported as successfully written.
        // v2 may partially accept it: no automatic replay is safe.
        assert!(
            writer
                .write_points(&settings, &[point("2026-11-14T00:00:00Z"), point(ts)])
                .await
                .is_err()
        );
    }
    if mode == "v3" {
        assert!(
            writer
                .write_points(&settings, &[point("2262-04-11T23:40:00Z")])
                .await
                .is_err()
        );
    }
}
