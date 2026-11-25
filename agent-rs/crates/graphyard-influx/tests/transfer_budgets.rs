//! Synthetic loopback regressions for transfer budgets and single submissions.
mod common;
use chrono::Utc;
use common::{FakeInflux, capture_logs, take_logs};
use graphyard_core::model::MetricPoint;
use graphyard_core::settings::Settings;
use graphyard_influx::{InfluxWriter, QueryClient, QueryRange};
use serde_json::json;
use std::time::{Duration, Instant};

fn settings(server: &FakeInflux) -> Settings {
    Settings::from_vars([
        ("INFLUX_URL".into(), server.url().into()),
        ("INFLUX_TOKEN".into(), "synthetic".into()),
        ("INFLUX_ORG".into(), "test".into()),
        ("INFLUX_BUCKET".into(), "test".into()),
        ("INFLUX_API_MODE".into(), "auto".into()),
    ])
    .unwrap()
}
fn point() -> MetricPoint {
    MetricPoint {
        ts: Utc::now(),
        metric: "m".into(),
        value: 2.0,
        subject_type: "host".into(),
        subject_id: "test".into(),
        source_system: "test".into(),
        source_instance: "default".into(),
        collector_service: "test".into(),
        collector_host: "test".into(),
        host: None,
        service: None,
        source_entity_id: None,
        tags: None,
    }
}

fn query_range() -> QueryRange {
    let stop = Utc::now();
    QueryRange::new("m", stop - chrono::Duration::seconds(1), stop)
}

#[tokio::test]
async fn authorization_header_admission_redacts_credentials() {
    const SENTINEL: &str = "synthetic-secret-marker";
    let server =
        FakeInflux::start(&[json!({"status": 200, "reason": "OK", "headers": [], "body": ""})])
            .await;
    let mut settings = settings(&server);
    settings.influx_api_mode = "v2".into();
    settings.influx_token = format!("{SENTINEL}\nrejected");
    capture_logs();

    let query_error = QueryClient::new(&settings)
        .query_range(&query_range())
        .await
        .unwrap_err();
    assert_eq!(query_error.python_class(), "ValueError");
    assert!(!format!("{query_error} {query_error:?}").contains(SENTINEL));
    assert_eq!(
        query_error.to_string(),
        "Invalid InfluxDB Authorization header value"
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    for writer in [
        InfluxWriter::new().unwrap(),
        InfluxWriter::to_file(&path).unwrap(),
    ] {
        let write_error = writer
            .write_points(&settings, &[point()])
            .await
            .unwrap_err();
        assert_eq!(write_error.python_class(), "ValueError");
        assert!(!format!("{write_error} {write_error:?}").contains(SENTINEL));
        assert_eq!(
            write_error.to_string(),
            "Invalid InfluxDB Authorization header value"
        );
    }
    assert_eq!(std::fs::metadata(path).unwrap().len(), 0);

    settings.influx_token = format!("{SENTINEL}🦀");
    let encoding_error = QueryClient::new(&settings)
        .query_range(&query_range())
        .await
        .unwrap_err();
    assert_eq!(encoding_error.python_class(), "UnicodeEncodeError");
    assert!(!format!("{encoding_error:?}").contains(SENTINEL));
    assert_eq!(
        encoding_error.to_string(),
        format!(
            "InfluxDB Authorization header cannot be encoded as Latin-1 at position {}",
            "Token ".len() + SENTINEL.len()
        )
    );
    assert!(
        take_logs()
            .iter()
            .all(|(_, message)| !message.contains(SENTINEL) && !message.contains('🦀'))
    );
    assert!(server.recorded().is_empty());
}

#[tokio::test]
async fn caps_success_and_error_bodies_without_fallback_or_replay() {
    for write in [false, true] {
        for status in [200, 404, 500] {
            let server = FakeInflux::start(&[
                json!({"status":status,"reason":"OK", "headers":[], "body":"x".repeat(5000)}),
            ])
            .await;
            let settings = settings(&server);
            let message = if write {
                InfluxWriter::new()
                    .unwrap()
                    .with_response_limits(Duration::from_secs(2), 2048)
                    .write_points(&settings, &[point()])
                    .await
                    .unwrap_err()
                    .to_string()
            } else {
                let err = QueryClient::new(&settings)
                    .with_flux_limits(Duration::from_secs(2), 2048)
                    .query_range(&query_range())
                    .await
                    .unwrap_err();
                assert!(!err.is_v2_query_missing());
                err.to_string()
            };
            assert!(message.contains("decoded byte limit"), "{message}");
            if write {
                assert!(message.contains("acceptance uncertain; not retried"));
                assert!(message.len() < 1200);
            }
            assert_eq!(server.recorded().len(), 1);
        }
    }
}

#[tokio::test]
async fn deadlines_stop_continuous_trickle_including_error_bodies() {
    for write in [false, true] {
        for status in [200, 500] {
            let server = FakeInflux::start(&[json!({"status":status,"reason":"OK", "headers":[], "body":"x".repeat(100), "drip":true,"chunk_delay":0.02})]).await;
            let settings = settings(&server);
            let started = Instant::now();
            let message = if write {
                InfluxWriter::new()
                    .unwrap()
                    .with_response_limits(Duration::from_millis(100), 1000)
                    .write_points(&settings, &[point()])
                    .await
                    .unwrap_err()
                    .to_string()
            } else {
                QueryClient::new(&settings)
                    .with_flux_limits(Duration::from_millis(100), 1000)
                    .query_range(&query_range())
                    .await
                    .unwrap_err()
                    .to_string()
            };
            assert!(message.contains("total deadline"), "{message}");
            assert!(started.elapsed() < Duration::from_secs(2));
            assert_eq!(server.recorded().len(), 1);
        }
    }
}

#[tokio::test]
async fn oversized_request_refused_before_network() {
    let server =
        FakeInflux::start(&[json!({"status":204,"reason":"No Content","headers":[],"body":""})])
            .await;
    let mut p = point();
    p.source_entity_id = Some("x".repeat(256));
    let points = vec![p; 25000];
    assert!(
        InfluxWriter::new()
            .unwrap()
            .write_points(&settings(&server), &points)
            .await
            .is_err()
    );
    assert!(server.requests.lock().unwrap().is_empty());
}
