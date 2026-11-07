//! The real AgentSink at the Influx/SQLite publication boundary.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use graphyard_agent::collectors::{AgentSink, Collectors};
use graphyard_agent::runs::{SystemClock, run_metric_specs};
use graphyard_collect::http::HttpContext;
use graphyard_influx::InfluxWriter;
use graphyard_store::Store;
use serde_json::json;
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use common::{FakeServer, TestDb, insert_spec, settings};

#[derive(Clone, Copy)]
enum FirstWrite {
    Reject,
    Disconnect,
    Accept,
}

/// Consume and record complete writes before rejecting, disconnecting or
/// acknowledging them. Later writes succeed, so a sibling can prove isolation.
struct InfluxServer {
    url: String,
    bodies: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<()>,
}

impl InfluxServer {
    async fn start(first: FirstWrite) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let recorded = bodies.clone();
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                assert!(line.starts_with("POST /api/v2/write?"), "{line}");
                let mut length = None;
                loop {
                    line.clear();
                    assert!(reader.read_line(&mut line).await.unwrap() > 0);
                    if line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        length = Some(value.trim().parse::<usize>().unwrap());
                    }
                }
                let mut body = vec![0; length.unwrap()];
                reader.read_exact(&mut body).await.unwrap();
                let attempt = {
                    let mut bodies = recorded.lock().unwrap();
                    bodies.push(String::from_utf8(body).unwrap());
                    bodies.len()
                };
                let response = match (attempt, first) {
                    (1, FirstWrite::Disconnect) => continue,
                    (1, FirstWrite::Reject) => "HTTP/1.1 503 Service Unavailable",
                    _ => "HTTP/1.1 204 No Content",
                };
                reader
                    .get_mut()
                    .write_all(
                        format!("{response}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
            }
        });
        Self { url, bodies, task }
    }

    fn bodies(&self) -> Vec<String> {
        self.bodies.lock().unwrap().clone()
    }
}

impl Drop for InfluxServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn collectors(db: &TestDb, influx: &InfluxServer) -> (Store, Collectors) {
    let settings = settings(&[
        ("INFLUX_URL", &influx.url),
        ("INFLUX_TOKEN", "synthetic"),
        ("INFLUX_ORG", "test"),
        ("INFLUX_BUCKET", "metrics"),
        ("INFLUX_TIMEOUT_MS", "2000"),
    ]);
    let store = Store::open(db.path(), &settings).unwrap();
    let collectors = Collectors {
        http: HttpContext::new(),
        sink: AgentSink {
            settings: Arc::new(settings),
            writer: Arc::new(InfluxWriter::new().unwrap()),
            store: store.clone(),
        },
    };
    (store, collectors)
}

fn specs(db: &TestDb, source: &FakeServer) {
    for (name, id) in [("a failing", "failed"), ("b sibling", "sibling")] {
        insert_spec(
            db,
            name,
            "http_json_metric",
            &json!({
                "url": source.url("/value"),
                "metric_path": "$.value",
                "metric_name": "test.value",
                "subject_type": "host",
                "subject_id": id,
                "host_id": id,
                "collector_host": id,
                "service_id": id,
            }),
        );
    }
}

fn assert_failure_and_sibling(db: &TestDb, category: &str) {
    let table = "metric_collection_spec";
    assert_eq!(db.text(table, "a failing", "last_status"), "critical");
    assert_eq!(
        db.text(table, "a failing", "last_error"),
        format!("collector failure: {category}")
    );
    assert!(!matches!(
        db.value(table, "a failing", "last_run_at"),
        rusqlite::types::Value::Null
    ));
    assert!(matches!(
        db.value(table, "a failing", "next_run_time"),
        rusqlite::types::Value::Integer(next) if next > 0
    ));
    assert_eq!(db.text(table, "b sibling", "last_status"), "ok");
    assert_eq!(
        db.text("pipeline_heartbeat", "metric_collectors", "status"),
        "warning"
    );
    // Only the sibling's rows survived. In particular the failed spec left
    // neither a subject/host nor a service, even on a late service INSERT error.
    for (table, column) in [
        ("subject_registry", "subject_id"),
        ("host_registry", "host_id"),
        ("service_registry", "service_id"),
    ] {
        assert_eq!(db.count(table), 1, "{table}");
        let id: String = db
            .raw()
            .query_row(&format!("SELECT {column} FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(id, "sibling", "{table}");
    }
}

async fn rejected_write(first: FirstWrite, category: &str) {
    let influx = InfluxServer::start(first).await;
    let source = FakeServer::start();
    let db = TestDb::new();
    specs(&db, &source);
    let (store, collectors) = collectors(&db, &influx);
    let run = tokio::time::timeout(
        Duration::from_secs(15),
        run_metric_specs(&store, &collectors, &SystemClock, true),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!((run.total, run.failed, run.ingested), (2, 1, 1));
    assert_failure_and_sibling(&db, category);
    let bodies = influx.bodies();
    assert_eq!(bodies.len(), 2, "one attempt per spec, without replay");
    assert!(bodies[0].contains("subject_id=failed,"), "{bodies:?}");
    assert!(bodies[1].contains("subject_id=sibling,"), "{bodies:?}");
    assert_eq!(source.requests(), ["GET /value", "GET /value"]);
    store.close();
}

#[tokio::test]
async fn rejected_influx_write_does_not_publish_registry_or_stop_sibling() {
    rejected_write(FirstWrite::Reject, "ApiException").await;
}

#[tokio::test]
async fn disconnected_influx_write_does_not_publish_registry_or_retry() {
    rejected_write(FirstWrite::Disconnect, "Exception").await;
}

#[tokio::test]
async fn registry_failure_after_ack_is_atomic_and_does_not_replay_write() {
    let influx = InfluxServer::start(FirstWrite::Accept).await;
    let source = FakeServer::start();
    let db = TestDb::new();
    specs(&db, &source);
    // Service creation happens after subject and host creation. RAISE(ABORT)
    // rejects just this statement; the Store must roll back the transaction.
    db.execute(
        "CREATE TRIGGER reject_registry BEFORE INSERT ON service_registry
         WHEN NEW.service_id = 'failed'
         BEGIN SELECT RAISE(ABORT, 'synthetic registry failure'); END;",
    );
    let (store, collectors) = collectors(&db, &influx);
    let run = tokio::time::timeout(
        Duration::from_secs(15),
        run_metric_specs(&store, &collectors, &SystemClock, true),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!((run.total, run.failed, run.ingested), (2, 1, 1));
    assert_failure_and_sibling(&db, "IntegrityError");
    let bodies = influx.bodies();
    assert_eq!(bodies.len(), 2, "one acknowledged write per spec");
    assert!(bodies[0].contains("subject_id=failed,"), "{bodies:?}");
    assert!(bodies[1].contains("subject_id=sibling,"), "{bodies:?}");

    db.execute(
        "DROP TRIGGER reject_registry;
         UPDATE metric_collection_spec SET enabled = 0 WHERE name = 'b sibling';",
    );
    // A later explicit invocation can send the already-accepted point again;
    // there is no exactly-once guarantee across invocations.
    let retry = tokio::time::timeout(
        Duration::from_secs(15),
        run_metric_specs(&store, &collectors, &SystemClock, false),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!((retry.total, retry.failed, retry.ingested), (1, 0, 1));
    assert_eq!(
        db.text("metric_collection_spec", "a failing", "last_status"),
        "ok"
    );
    for table in ["subject_registry", "host_registry", "service_registry"] {
        assert_eq!(db.count(table), 2, "{table}");
    }
    let bodies = influx.bodies();
    assert_eq!(bodies.len(), 3);
    assert!(bodies[2].contains("subject_id=failed,"), "{bodies:?}");
    store.close();
}
