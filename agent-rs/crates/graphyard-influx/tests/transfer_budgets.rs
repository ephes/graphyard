//! Synthetic loopback regressions for transfer budgets and single submissions.
mod common;
use chrono::Utc;
use common::FakeInflux;
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
                    .query_range(&QueryRange::new("m", Utc::now(), Utc::now()))
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
                    .query_range(&QueryRange::new("m", Utc::now(), Utc::now()))
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
