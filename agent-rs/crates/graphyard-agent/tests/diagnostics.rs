//! Synthetic faults through the production run boundary and real SQLite publications.
mod common;

use std::cell::RefCell;
use std::sync::Once;

use common::{TestDb, settings};
use graphyard_agent::runs::{
    ConditionSamples, SpecExecutors, SystemClock, run_conditions, run_metric_specs,
};
use graphyard_collect::executors::{ExecutorError, ExecutorResult, RunState};
use graphyard_core::{MetricSample, SpecOutcome, SpecType, StatusLevel};
use graphyard_influx::{ConditionQuery, QueryError};
use graphyard_store::Store;
use num_bigint::BigInt;
use serde_json::Value;

const MARKER: &str = "invented-query-password-body-marker";
struct Capture;
thread_local! { static LINES: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) }; }
impl log::Log for Capture {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        LINES.with(|lines| lines.borrow_mut().push(record.args().to_string()));
    }
    fn flush(&self) {}
}
fn capture() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        log::set_logger(&Capture).unwrap();
        log::set_max_level(log::LevelFilter::Info);
    });
    LINES.with(|lines| lines.borrow_mut().clear());
}
fn lines() -> Vec<String> {
    LINES.with(|lines| lines.borrow().clone())
}

struct Faults;
impl SpecExecutors for Faults {
    async fn execute(
        &self,
        _: SpecType,
        name: &str,
        _: &Value,
        _: &mut RunState,
    ) -> ExecutorResult {
        if name == "a bad" {
            Err(ExecutorError::Raised {
                class: "RuntimeError".into(),
                message: format!("https://user:{MARKER}@example.test/?key={MARKER}"),
            })
        } else {
            Ok(SpecOutcome::new(StatusLevel::Ok, 0, 0, ""))
        }
    }
}
#[tokio::test]
async fn collector_failure_is_private_in_rows_heartbeat_and_logs() {
    capture();
    let db = TestDb::new();
    for name in ["a bad", "b good"] {
        common::insert_spec(&db, name, "http_json_metric", &serde_json::json!({}));
    }
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let run = run_metric_specs(&store, &Faults, &SystemClock, false)
        .await
        .unwrap();
    assert_eq!((run.total, run.failed), (2, 1));
    let error = db.text("metric_collection_spec", "a bad", "last_error");
    assert_eq!(error, "collector failure: RuntimeError");
    assert!(
        !db.text("pipeline_heartbeat", "metric_collectors", "last_error")
            .contains(MARKER)
    );
    assert!(!lines().join("\n").contains(MARKER));
    assert_eq!(
        db.text("metric_collection_spec", "b good", "last_status"),
        "ok"
    );
    store.close();
}

struct Samples {
    fail: bool,
}
impl ConditionSamples for Samples {
    async fn query_condition_window(
        &self,
        query: &ConditionQuery<'_>,
        _: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<MetricSample>, QueryError> {
        if self.fail && query.id == 1 {
            Err(QueryError::Config(MARKER.into()))
        } else {
            Ok(Vec::new())
        }
    }
}
fn conditions(db: &TestDb) {
    db.raw().execute_batch("INSERT INTO condition_definition (id, name, enabled, metric_name, host_filter, service_filter, tags_filter, operator, warning_threshold, critical_threshold, window_minutes, breach_minutes, status, message, created_at, updated_at, subject_id_filter, subject_type_filter, alert_when_stale) VALUES (1, 'a bad', 1, 'host.test', '', '', '{}', 'gte', 80, 90, 30, 5, 'ok', '', '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '', 1), (2, 'b good', 1, 'host.test', '', '', '{}', 'gte', 80, 90, 30, 5, 'ok', '', '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '', 1);").unwrap();
}
async fn condition_fault(stage: &str, fallback_fails: bool) {
    capture();
    let db = TestDb::new();
    conditions(&db);
    if stage == "publication" {
        let predicate = if fallback_fails {
            "NEW.id = 1"
        } else {
            "NEW.id = 1 AND NEW.message NOT LIKE 'evaluation error:%'"
        };
        db.raw().execute_batch(&format!("CREATE TRIGGER publication_failure BEFORE UPDATE ON condition_definition WHEN {predicate} BEGIN SELECT RAISE(FAIL, '{MARKER}'); END;")).unwrap();
    }
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let run = run_conditions(
        &store,
        &Samples {
            fail: stage == "evaluation",
        },
        &SystemClock,
        &BigInt::from(60),
        None,
    )
    .await
    .unwrap();
    assert_eq!((run.total, run.failed), (2, 1));
    let logs = lines();
    let failures: Vec<_> = logs
        .iter()
        .filter(|line| line.starts_with("Condition failure "))
        .collect();
    assert_eq!(failures.len(), 1, "{logs:?}");
    let line = failures[0];
    assert!(line.contains("condition_id=1"));
    assert!(line.contains(&format!("stage={stage}")));
    assert!(line.contains(if stage == "evaluation" {
        "category=InfluxConfigurationError"
    } else {
        "category=IntegrityError"
    }));
    assert!(line.contains(&format!("failure_published={}", !fallback_fails)));
    assert!(!logs.join("\n").contains(MARKER));
    assert!(
        db.value("condition_definition", "b good", "last_evaluated")
            != rusqlite::types::Value::Null
    );
    store.close();
}
#[tokio::test]
async fn evaluation_failure_is_diagnosed() {
    condition_fault("evaluation", false).await;
}
#[tokio::test]
async fn publication_failure_is_diagnosed() {
    condition_fault("publication", false).await;
}
#[tokio::test]
async fn failed_fallback_does_not_stop_next_condition() {
    condition_fault("publication", true).await;
}

#[tokio::test]
async fn http_failure_keeps_status_but_hides_url_and_response_body() {
    use graphyard_agent::collectors::{AgentSink, Collectors};
    use graphyard_collect::http::HttpContext;
    use graphyard_influx::InfluxWriter;
    use std::io::{BufRead, BufReader, Write};
    use std::net::TcpListener;
    use std::sync::Arc;

    capture();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = BufReader::new(socket.try_clone().unwrap());
            loop {
                let mut line = String::new();
                request.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            write!(
                socket,
                "HTTP/1.1 503 {MARKER}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{MARKER}",
                MARKER.len()
            )
            .unwrap();
        }
    });
    let db = TestDb::new();
    common::insert_spec(
        &db,
        "a bad",
        "http_json_metric",
        &serde_json::json!({
            "url": format!("http://user:{MARKER}@127.0.0.1:{port}/?key={MARKER}"),
            "metric_path": "value", "metric_name": "test.value"
        }),
    );
    let config = settings(&[]);
    let store = Store::open(db.path(), &config).unwrap();
    let collectors = Collectors {
        http: HttpContext::new(),
        sink: AgentSink {
            settings: Arc::new(config),
            store: store.clone(),
            writer: Arc::new(InfluxWriter::to_file(&db.dir.path().join("points.lp")).unwrap()),
        },
    };
    let result = run_metric_specs(&store, &collectors, &SystemClock, false)
        .await
        .unwrap();
    assert_eq!(result.failed, 1);
    assert_eq!(
        db.text("metric_collection_spec", "a bad", "last_error"),
        "collector HTTP error: status=503"
    );
    assert!(
        !db.text("pipeline_heartbeat", "metric_collectors", "last_error")
            .contains(MARKER)
    );
    assert!(!lines().join("\n").contains(MARKER));
    conditions(&db);
    let url = format!("http://user:{MARKER}@127.0.0.1:{port}");
    let query_config = settings(&[
        ("INFLUX_URL", &url),
        ("INFLUX_TOKEN", "synthetic"),
        ("INFLUX_ORG", "synthetic"),
        ("INFLUX_API_MODE", "v3"),
    ]);
    let samples = graphyard_agent::collectors::InfluxSamples(graphyard_influx::QueryClient::new(
        &query_config,
    ));
    let condition_run = run_conditions(
        &store,
        &samples,
        &SystemClock,
        &BigInt::from(60),
        Some(&BigInt::from(1)),
    )
    .await
    .unwrap();
    assert_eq!(condition_run.failed, 1);
    assert!(!lines().join("\n").contains(MARKER));
    store.close();
    server.join().unwrap();
}

struct EditedFailure<'a>(&'a TestDb, &'a str);
impl ConditionSamples for EditedFailure<'_> {
    async fn query_condition_window(
        &self,
        query: &ConditionQuery<'_>,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<MetricSample>, QueryError> {
        if query.id != 1 {
            return Ok(Vec::new());
        }
        match self.1 {
            "deleted" => self
                .0
                .execute("DELETE FROM condition_definition WHERE id = 1"),
            "edited" => self
                .0
                .execute("UPDATE condition_definition SET metric_name = 'changed' WHERE id = 1"),
            "disabled" => self
                .0
                .execute("UPDATE condition_definition SET enabled = 0 WHERE id = 1"),
            "superseded" => {
                self.0
                    .raw()
                    .execute(
                        "UPDATE condition_definition SET last_evaluated = ? WHERE id = 1",
                        [now.format("%Y-%m-%d %H:%M:%S%.6f").to_string()],
                    )
                    .unwrap();
            }
            "unchanged" => {}
            _ => unreachable!(),
        }
        Err(QueryError::Config(MARKER.into()))
    }
}
#[tokio::test]
async fn failure_diagnostic_reports_actual_publication() {
    for change in ["deleted", "edited", "disabled", "superseded", "unchanged"] {
        capture();
        let db = TestDb::new();
        conditions(&db);
        let store = Store::open(db.path(), &settings(&[])).unwrap();
        let result = run_conditions(
            &store,
            &EditedFailure(&db, change),
            &SystemClock,
            &BigInt::from(60),
            None,
        )
        .await
        .unwrap();
        assert_eq!((result.total, result.failed), (2, 1));
        let log = lines().join("\n");
        assert!(
            log.contains(&format!("failure_published={}", change == "unchanged")),
            "{change}: {log}"
        );
        assert_ne!(
            db.value("condition_definition", "b good", "last_evaluated"),
            rusqlite::types::Value::Null
        );
        store.close();
    }
}
