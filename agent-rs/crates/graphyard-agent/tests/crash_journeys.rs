//! Fresh-process death after claim commit and after acknowledged sink acceptance.
//! All endpoints, credentials, rows and clocks are synthetic.
mod common;

use std::io::{BufRead as _, BufReader, Write as _};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use chrono::{DateTime, Utc};
use graphyard_agent::collectors::AgentSink;
use graphyard_agent::runs::{Clock, SpecExecutors, run_metric_specs};
use graphyard_collect::executors::{
    ExecutorResult, MetricSink, RunState, SinkError, execute_http_json_metric,
};
use graphyard_collect::http::HttpContext;
use graphyard_core::{MetricPoint, SpecType};
use graphyard_influx::InfluxWriter;
use graphyard_store::Store;
use serde_json::{Value, json};

use common::{FakeServer, TestDb, insert_spec, settings};

const START: i64 = 1_800_000_000;
struct Fixed(i64);
impl Clock for Fixed {
    fn now(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(self.0, 0).unwrap()
    }
}

fn gate(label: &str) {
    println!("R81:{label}");
    std::io::stdout().flush().unwrap();
    let mut release = String::new();
    std::io::stdin().read_line(&mut release).unwrap();
    assert_eq!(
        release, "continue\n",
        "parent should kill at the announced gate"
    );
}
struct GatedSink {
    real: AgentSink,
    phase: String,
}
impl MetricSink for GatedSink {
    fn now(&self) -> DateTime<Utc> {
        Fixed(START).now()
    }

    async fn write_points(&self, points: &[MetricPoint]) -> Result<u64, SinkError> {
        let count = self.real.write_points(points).await?;
        if self.phase == "accepted" {
            gate("ACCEPTED");
        }
        Ok(count)
    }
    async fn touch_registry(&self, points: &[MetricPoint]) -> Result<(), SinkError> {
        self.real.touch_registry(points).await
    }
}
struct GatedCollector {
    http: HttpContext,
    sink: GatedSink,
}
impl SpecExecutors for GatedCollector {
    async fn execute(
        &self,
        kind: SpecType,
        name: &str,
        config: &Value,
        _: &mut RunState,
    ) -> ExecutorResult {
        assert_eq!(kind, SpecType::HttpJsonMetric);
        if self.sink.phase == "claim" {
            gate("CLAIMED");
        }
        execute_http_json_metric(&self.http, &self.sink, name, config).await
    }
}

/// Launched only by this test executable's parent, never by a production flag.
#[test]
fn crash_child() {
    let Ok(db) = std::env::var("R81_CRASH_DB") else {
        return;
    };
    let influx = std::env::var("R81_CRASH_INFLUX").unwrap();
    let phase = std::env::var("R81_CRASH_PHASE").unwrap();
    let now = std::env::var("R81_CRASH_NOW").unwrap().parse().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let config = settings(&[
            ("INFLUX_URL", &influx),
            ("INFLUX_TOKEN", "synthetic"),
            ("INFLUX_ORG", "test"),
            ("INFLUX_BUCKET", "metrics"),
            ("INFLUX_TIMEOUT_MS", "2000"),
        ]);
        let store = Store::open(std::path::Path::new(&db), &config).unwrap();
        let collector = GatedCollector {
            http: HttpContext::new(),
            sink: GatedSink {
                real: AgentSink {
                    settings: Arc::new(config),
                    writer: Arc::new(InfluxWriter::new().unwrap()),
                    store: store.clone(),
                },
                phase,
            },
        };
        let result = run_metric_specs(&store, &collector, &Fixed(now), true)
            .await
            .unwrap();
        println!(
            "R81:DONE total={} failed={} ingested={}",
            result.total, result.failed, result.ingested
        );
        std::io::stdout().flush().unwrap();
        store.close();
    });
}

struct ChildRun {
    child: Child,
    lines: mpsc::Receiver<String>,
    reader: Option<std::thread::JoinHandle<()>>,
}
impl ChildRun {
    fn start(db: &TestDb, server: &FakeServer, phase: &str, now: i64) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child", "--nocapture"])
            .env_clear()
            .env("R81_CRASH_DB", db.path())
            .env("R81_CRASH_INFLUX", server.url(""))
            .env("R81_CRASH_PHASE", phase)
            .env("R81_CRASH_NOW", now.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            lines,
            reader: Some(reader),
        }
    }
    fn event(&self, expected: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()))
                .expect("child did not reach its explicit process gate");
            if let Some((_, event)) = line.split_once("R81:") {
                assert_eq!(event, expected);
                return;
            }
        }
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        assert!(!self.child.wait().unwrap().success());
    }
    fn finish(mut self, total: u64) {
        self.event(&format!("DONE total={total} failed=0 ingested={total}"));
        assert!(self.child.wait().unwrap().success());
    }
}
impl Drop for ChildRun {
    fn drop(&mut self) {
        // Also clean up after a failed assertion; never leave a gated child alive.
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

fn accepted(server: &FakeServer) -> usize {
    server
        .requests()
        .iter()
        .filter(|r| r.starts_with("POST /api/v2/write?"))
        .count()
}
fn schedule(db: &TestDb) -> i64 {
    db.raw()
        .query_row(
            "SELECT next_run_time FROM metric_collection_spec",
            [],
            |r| r.get(0),
        )
        .unwrap()
}
fn assert_unfinished(db: &TestDb) {
    assert_eq!(
        db.text("metric_collection_spec", "synthetic", "last_status"),
        "warning"
    );
    assert_eq!(
        db.text("metric_collection_spec", "synthetic", "last_error"),
        ""
    );
    assert!(matches!(
        db.value("metric_collection_spec", "synthetic", "last_run_at"),
        rusqlite::types::Value::Null
    ));
    for table in [
        "pipeline_heartbeat",
        "subject_registry",
        "host_registry",
        "service_registry",
    ] {
        assert_eq!(
            db.count(table),
            0,
            "no healthy/completed bookkeeping from killed attempt: {table}"
        );
    }
}
fn journey(phase: &str, initial_writes: usize) {
    let db = TestDb::new();
    let server = FakeServer::start();
    insert_spec(
        &db,
        "synthetic",
        "http_json_metric",
        &json!({
            "url": server.url("/value"), "metric_path": "$.value", "metric_name": "synthetic.value",
            "subject_type": "host", "subject_id": "synthetic", "host_id": "synthetic",
            "collector_host": "synthetic", "service_id": "synthetic",
        }),
    );
    db.execute("UPDATE metric_collection_spec SET interval_seconds=1");
    let mut original = ChildRun::start(&db, &server, phase, START);
    original.event(if phase == "claim" {
        "CLAIMED"
    } else {
        "ACCEPTED"
    });
    let lease = schedule(&db);
    assert!(
        lease > START,
        "claim must be durably committed before the gate"
    );
    assert_eq!(accepted(&server), initial_writes);
    if phase == "claim" {
        assert!(server.requests().is_empty());
    }
    original.kill();
    assert_eq!(schedule(&db), lease, "process death must retain the claim");
    assert_unfinished(&db);
    ChildRun::start(&db, &server, "", lease - 1).finish(0);
    assert_eq!(accepted(&server), initial_writes);
    assert_eq!(schedule(&db), lease);
    assert_unfinished(&db);
    ChildRun::start(&db, &server, "", lease).finish(1);
    assert_eq!(
        accepted(&server),
        initial_writes + 1,
        "one recovery delivery, no internal replay"
    );
    let deliveries: Vec<_> = server
        .bodies()
        .into_iter()
        .filter(|b| !b.is_empty())
        .collect();
    assert_eq!(deliveries.len(), initial_writes + 1);
    let line = std::str::from_utf8(&deliveries[0]).unwrap();
    assert!(
        line.starts_with("graphyard_metrics,") && line.contains("metric=synthetic.value,"),
        "{line}"
    );
    if initial_writes == 1 {
        assert_eq!(
            deliveries[0], deliveries[1],
            "the same synthetic observation may be delivered twice"
        );
    }
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| *r == "GET /value")
            .count(),
        initial_writes + 1
    );
    assert_eq!(
        db.text("metric_collection_spec", "synthetic", "last_status"),
        "ok"
    );
    assert!(!matches!(
        db.value("metric_collection_spec", "synthetic", "last_run_at"),
        rusqlite::types::Value::Null
    ));
    assert_eq!(
        db.text("pipeline_heartbeat", "metric_collectors", "status"),
        "ok"
    );
    for table in [
        "pipeline_heartbeat",
        "subject_registry",
        "host_registry",
        "service_registry",
    ] {
        assert_eq!(db.count(table), 1);
    }
    assert_eq!(schedule(&db), lease + 1);
    ChildRun::start(&db, &server, "", lease).finish(0);
    assert_eq!(accepted(&server), initial_writes + 1);
}

#[test]
fn killed_after_claim_restarts_only_at_lease_expiry() {
    journey("claim", 0);
}

#[test]
fn killed_after_sink_acceptance_records_one_uncertain_attempt_and_one_recovery() {
    journey("accepted", 1);
}
