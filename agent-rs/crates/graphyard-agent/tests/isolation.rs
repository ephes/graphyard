//! Executor isolation with the real collectors (PLAN.md 3.4):
//! `test_metric_collection_spec_isolation.py::test_bad_spec_does_not_stop_later_specs`
//! and `::test_unexpected_collector_exception_is_isolated`, and the next
//! spec after one that hits its deadline (`test_http_json_budget.py`,
//! `test_home_assistant_budget.py`).

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use graphyard_agent::collectors::{AgentSink, Collectors};
use graphyard_agent::runs::{SpecExecutors, SystemClock, run_metric_specs};
use graphyard_collect::executors::{ExecutorError, ExecutorResult, RunState};
use graphyard_collect::http::HttpContext;
use graphyard_core::SpecType;
use graphyard_influx::InfluxWriter;
use graphyard_store::Store;
use serde_json::{Value, json};

use common::{FakeServer, TestDb, settings};

fn json_config(url: &str, extra: &Value) -> Value {
    let mut config = json!({
        "url": url,
        "metric_path": "$.value",
        "metric_name": "service.example_value",
        "service_id": "example",
    });
    for (key, value) in extra.as_object().unwrap() {
        config[key] = value.clone();
    }
    config
}

struct Setup {
    db: TestDb,
    store: Store,
    collectors: Collectors,
    writes: std::path::PathBuf,
}

fn setup(db: TestDb) -> Setup {
    let writes = db.dir.path().join("writes.lp");
    let settings = settings(&[
        ("INFLUX_URL", "http://127.0.0.1:9"),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
    ]);
    let store = Store::open(db.path(), &settings).unwrap();
    let collectors = Collectors {
        http: HttpContext::new(),
        sink: AgentSink {
            settings: Arc::new(settings),
            writer: Arc::new(InfluxWriter::to_file(&writes).unwrap()),
            store: store.clone(),
        },
    };
    Setup {
        db,
        store,
        collectors,
        writes,
    }
}

#[tokio::test]
async fn a_bad_spec_does_not_stop_later_specs() {
    let server = FakeServer::start();
    let db = TestDb::new();
    let url = server.url("/value");
    common::insert_spec(
        &db,
        "a bad timeout",
        "http_json_metric",
        &json_config(&url, &json!({"request_timeout_seconds": "5s"})),
    );
    common::insert_spec(
        &db,
        "b good",
        "http_json_metric",
        &json_config(&url, &json!({})),
    );
    let setup = setup(db);

    let run = run_metric_specs(&setup.store, &setup.collectors, &SystemClock, true)
        .await
        .unwrap();
    assert_eq!((run.total, run.failed, run.ingested), (2, 1, 1));

    let spec = "metric_collection_spec";
    let db = &setup.db;
    assert_eq!(db.text(spec, "a bad timeout", "last_status"), "critical");
    assert!(
        db.text(spec, "a bad timeout", "last_error")
            .contains("request_timeout_seconds")
    );
    assert!(!matches!(
        db.value(spec, "a bad timeout", "last_run_at"),
        rusqlite::types::Value::Null
    ));
    assert_eq!(db.text(spec, "b good", "last_status"), "ok");
    let heartbeat = "pipeline_heartbeat";
    assert_eq!(db.text(heartbeat, "metric_collectors", "status"), "warning");
    assert!(
        db.text(heartbeat, "metric_collectors", "last_error")
            .contains("a bad timeout")
    );
    // The good spec's point was written and its service registered.
    let written = std::fs::read_to_string(&setup.writes).unwrap();
    assert!(written.starts_with("# write "), "{written}");
    assert!(
        written.contains("metric=service.example_value"),
        "{written}"
    );
    assert!(written.contains(" value=7 "), "{written}");
    assert_eq!(db.count("service_registry"), 1);

    // The bad spec is no longer due, so the next tick does not abort on it.
    let next = run_metric_specs(&setup.store, &setup.collectors, &SystemClock, true)
        .await
        .unwrap();
    assert_eq!(next.total, 0);
    assert_eq!(server.requests(), ["GET /value"]);
    setup.store.close();
}

/// The real collectors, except that `a crashing` raises.
struct Crashing<'a>(&'a Collectors);

impl SpecExecutors for Crashing<'_> {
    async fn execute(
        &self,
        spec_type: SpecType,
        name: &str,
        config: &Value,
        run: &mut RunState,
    ) -> ExecutorResult {
        if name == "a crashing" {
            return Err(ExecutorError::Raised {
                class: "RuntimeError".to_owned(),
                message: "collector bug".to_owned(),
            });
        }
        self.0.execute(spec_type, name, config, run).await
    }
}

#[tokio::test]
async fn an_unexpected_collector_exception_is_isolated() {
    let server = FakeServer::start();
    let db = TestDb::new();
    let url = server.url("/value");
    common::insert_spec(
        &db,
        "a crashing",
        "http_json_metric",
        &json_config(&url, &json!({})),
    );
    common::insert_spec(
        &db,
        "b good",
        "http_json_metric",
        &json_config(&url, &json!({})),
    );
    let setup = setup(db);

    let run = run_metric_specs(
        &setup.store,
        &Crashing(&setup.collectors),
        &SystemClock,
        false,
    )
    .await
    .unwrap();
    setup.store.close();
    assert_eq!((run.total, run.failed), (2, 1));
    let spec = "metric_collection_spec";
    assert_eq!(setup.db.text(spec, "a crashing", "last_status"), "critical");
    assert_eq!(
        setup.db.text(spec, "a crashing", "last_error"),
        "collector raised RuntimeError: collector bug"
    );
    assert!(matches!(
        setup.db.value(spec, "a crashing", "next_run_time"),
        rusqlite::types::Value::Integer(next) if next > 0
    ));
    assert_eq!(setup.db.text(spec, "b good", "last_status"), "ok");
}

#[tokio::test]
async fn the_spec_after_one_that_hits_its_deadline_still_runs() {
    // A server that accepts and never answers: the first spec runs into its
    // total deadline, the next one is collected normally.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let silent_url = format!("http://{}/value", silent.local_addr().unwrap());
    let server = FakeServer::start();
    let db = TestDb::new();
    common::insert_spec(
        &db,
        "a stalled",
        "http_json_metric",
        &json_config(
            &silent_url,
            &json!({"request_timeout_seconds": 2, "total_timeout_seconds": 0.5}),
        ),
    );
    common::insert_spec(
        &db,
        "b good",
        "http_json_metric",
        &json_config(&server.url("/value"), &json!({})),
    );
    let setup = setup(db);

    let started = Instant::now();
    let run = run_metric_specs(&setup.store, &setup.collectors, &SystemClock, true)
        .await
        .unwrap();
    setup.store.close();
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!((run.total, run.failed, run.ingested), (2, 1, 1));
    let spec = "metric_collection_spec";
    assert_eq!(
        setup.db.text(spec, "a stalled", "last_error"),
        "HTTP collector exceeded total_timeout_seconds=0.5"
    );
    assert_eq!(setup.db.text(spec, "b good", "last_status"), "ok");
    drop(silent);
}
