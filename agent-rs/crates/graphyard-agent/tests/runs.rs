//! The metrics and conditions runs (PLAN.md 3.4, 3.5) against
//! `fixtures/runs.json` (`gen_runs.py`): the Python runs with stubbed
//! executors and condition samples, replayed here with the same scripts.

mod common;

use std::cell::Cell;
use std::path::PathBuf;

use graphyard_agent::runs::{
    Clock, ConditionSamples, RunError, SpecExecutors, run_conditions, run_metric_specs,
};
use graphyard_collect::executors::{ExecutorError, ExecutorResult, RunState};
use graphyard_core::spec_config::SpecConfigError;
use graphyard_core::utc::UtcDateTime;
use graphyard_core::{MetricSample, SpecOutcome, SpecType, StatusLevel};
use graphyard_influx::{ConditionQuery, QueryError};
use graphyard_store::Store;
use num_bigint::BigInt;
use serde_json::Value;

use common::{RUNS_JSON, TestDb, assert_rows, settings};

const TABLES: [&str; 3] = [
    "metric_collection_spec",
    "condition_definition",
    "pipeline_heartbeat",
];

struct FixedClock(UtcDateTime);

impl Clock for FixedClock {
    fn now(&self) -> UtcDateTime {
        self.0
    }
}

/// The scripts of a scenario: what each spec's executor or each
/// condition's query does.
struct Scripted {
    scripts: Value,
    db: PathBuf,
    now: UtcDateTime,
}

impl Scripted {
    /// The script of `name`, after its `delete` side effect.
    fn script(&self, name: &str) -> &Value {
        let script = &self.scripts[name];
        if let Some(delete) = script.get("delete") {
            rusqlite::Connection::open(&self.db)
                .unwrap()
                .execute(
                    &format!(
                        "DELETE FROM \"{}\" WHERE name = ?",
                        delete[0].as_str().unwrap()
                    ),
                    [delete[1].as_str().unwrap()],
                )
                .unwrap();
        }
        script
    }
}

fn raised(script: &Value) -> Option<(&str, &str)> {
    let raised = script.get("raise")?;
    Some((
        raised["class"].as_str().unwrap(),
        raised["message"].as_str().unwrap(),
    ))
}

impl SpecExecutors for Scripted {
    async fn execute(
        &self,
        _spec_type: SpecType,
        name: &str,
        _config: &Value,
        _run: &mut RunState,
    ) -> ExecutorResult {
        let script = self.script(name);
        if let Some((class, message)) = raised(script) {
            return Err(if class == "SpecConfigError" {
                ExecutorError::Config(SpecConfigError(message.to_owned()))
            } else {
                ExecutorError::Raised {
                    class: class.to_owned(),
                    message: message.to_owned(),
                }
            });
        }
        let outcome = &script["outcome"];
        Ok(SpecOutcome::new(
            StatusLevel::parse(outcome[0].as_str().unwrap()).unwrap(),
            outcome[1].as_u64().unwrap(),
            outcome[2].as_u64().unwrap(),
            outcome[3].as_str().unwrap(),
        ))
    }
}

impl ConditionSamples for Scripted {
    async fn query_condition_window(
        &self,
        condition: &ConditionQuery<'_>,
        now: UtcDateTime,
    ) -> Result<Vec<MetricSample>, QueryError> {
        assert_eq!(now, self.now);
        let script = self.script(condition.name);
        if let Some((class, message)) = raised(script) {
            assert_eq!(class, "InfluxConfigurationError");
            return Err(QueryError::Config(message.to_owned()));
        }
        Ok(script["samples"]
            .as_array()
            .unwrap()
            .iter()
            .map(|sample| {
                MetricSample::new(
                    sample["ts"].as_str().unwrap().parse().unwrap(),
                    sample["value"].as_f64().unwrap(),
                    sample["host"].as_str().unwrap(),
                    sample["metric"].as_str().unwrap(),
                    None,
                )
            })
            .collect())
    }
}

fn check_error(err: &RunError, expected: &Value, name: &str) {
    assert_eq!(err.python_class(), expected["class"], "{name}");
    assert_eq!(
        err.to_string(),
        expected["message"].as_str().unwrap(),
        "{name}"
    );
}

#[tokio::test]
async fn every_run_scenario_matches_python() {
    let fixture: Value = serde_json::from_str(RUNS_JSON).unwrap();
    let now: UtcDateTime = fixture["now"].as_str().unwrap().parse().unwrap();
    assert_eq!(now.timestamp(), fixture["now_ts"].as_i64().unwrap());
    let scenarios = fixture["scenarios"].as_array().unwrap();
    assert_eq!(scenarios.len(), 15);
    for scenario in scenarios {
        let name = scenario["name"].as_str().unwrap();
        let db = TestDb::new();
        for table in TABLES {
            let rows = scenario["seed"]
                .get(table)
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            db.seed(table, &rows);
        }
        let store = Store::open(db.path(), &settings(&[])).unwrap();
        let scripted = Scripted {
            scripts: scenario["scripts"].clone(),
            db: db.path.clone(),
            now,
        };
        let clock = FixedClock(now);
        let expected = &scenario["result"];
        if scenario["kind"] == "metrics" {
            let due_only = scenario["due_only"].as_bool().unwrap();
            match run_metric_specs(&store, &scripted, &clock, due_only).await {
                Ok(run) => {
                    let actual = [
                        run.total,
                        run.failed,
                        run.warning,
                        run.ingested,
                        run.skipped,
                    ];
                    let wanted = ["total", "failed", "warning", "ingested", "skipped"]
                        .map(|key| expected[key].as_u64().unwrap());
                    assert_eq!(actual, wanted, "{name}");
                }
                Err(err) => check_error(&err, &scenario["error"], name),
            }
        } else {
            let stale = BigInt::from(600);
            // `evaluate_conditions_once(condition_id=...)` (S15a); the id
            // may lie beyond 64 bits.
            let condition_id: Option<BigInt> = scenario
                .get("condition_id")
                .map(|id| id.to_string().parse().unwrap());
            match run_conditions(&store, &scripted, &clock, &stale, condition_id.as_ref()).await {
                Ok(run) => {
                    assert_eq!(run.total, expected["total"].as_u64().unwrap(), "{name}");
                    assert_eq!(run.failed, expected["failed"].as_u64().unwrap(), "{name}");
                }
                Err(err) => check_error(&err, &scenario["error"], name),
            }
        }
        assert_eq!(
            scenario.get("result").is_some(),
            scenario.get("error").is_none()
        );
        store.close();
        for table in TABLES {
            assert_rows(
                &db.rows(table),
                scenario["after"][table].as_array().unwrap(),
                &format!("{name}: {table}"),
            );
        }
    }
}

/// Executors that raise, panic or succeed by spec name.
struct Faulty;

impl SpecExecutors for Faulty {
    async fn execute(
        &self,
        _spec_type: SpecType,
        name: &str,
        _config: &Value,
        _run: &mut RunState,
    ) -> ExecutorResult {
        match name {
            "a panicking" => panic!("index out of bounds"),
            "b formatted panic" => panic!("{} {}", "formatted", 42),
            _ => Ok(SpecOutcome::new(StatusLevel::Ok, 1, 0, "")),
        }
    }
}

#[tokio::test]
async fn a_panicking_executor_is_isolated() {
    let db = TestDb::new();
    let config = serde_json::json!({});
    for name in ["a panicking", "b formatted panic", "c good"] {
        common::insert_spec(&db, name, "http_json_metric", &config);
    }
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
    let run = run_metric_specs(&store, &Faulty, &clock, true)
        .await
        .unwrap();
    store.close();
    assert_eq!((run.total, run.failed, run.ingested), (3, 2, 1));
    let spec = "metric_collection_spec";
    assert_eq!(
        db.text(spec, "a panicking", "last_error"),
        "collector failure: panic"
    );
    assert_eq!(
        db.text(spec, "b formatted panic", "last_error"),
        "collector failure: panic"
    );
    assert_eq!(db.text(spec, "c good", "last_status"), "ok");
    assert_eq!(
        db.text("pipeline_heartbeat", "metric_collectors", "status"),
        "warning"
    );
}

#[tokio::test]
async fn metric_heartbeat_samples_first_errors_and_counts_all_outcomes() {
    let db = TestDb::new();
    let now = "2026-10-21T08:15:30Z".parse().unwrap();
    let mut scripts = serde_json::Map::new();
    for id in 0..256 {
        let name = format!("spec-{id:03}");
        common::insert_spec(&db, &name, "http_json_metric", &serde_json::json!({}));
        let (status, message) = match id {
            0 => ("warning", ""),
            1 => ("critical", ""),
            _ if id % 2 == 0 => ("critical", "failed"),
            _ => ("warning", "partial"),
        };
        scripts.insert(
            name,
            serde_json::json!({"outcome": [status, 1, 2, message]}),
        );
    }
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let executor = Scripted {
        scripts: Value::Object(scripts),
        db: db.path.clone(),
        now,
    };
    let run = run_metric_specs(&store, &executor, &FixedClock(now), true)
        .await
        .unwrap();
    store.close();

    assert_eq!((run.total, run.failed, run.warning), (256, 128, 128));
    let heartbeat = |column| db.text("pipeline_heartbeat", "metric_collectors", column);
    assert_eq!(heartbeat("status"), "warning");
    assert_eq!(
        heartbeat("last_error"),
        "spec-001: ; spec-002: failed; spec-003: partial; spec-004: failed; spec-005: partial"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&heartbeat("details")).unwrap(),
        serde_json::json!({"total": 256, "failed": 128, "warning": 128, "ingested": 256, "skipped": 512})
    );
}

#[derive(Default)]
struct RecordingCollector(Cell<u32>);

impl SpecExecutors for RecordingCollector {
    async fn execute(
        &self,
        _spec_type: SpecType,
        _name: &str,
        _config: &Value,
        _run: &mut RunState,
    ) -> ExecutorResult {
        self.0.set(self.0.get() + 1);
        Ok(SpecOutcome::new(StatusLevel::Ok, 1, 0, ""))
    }
}

#[tokio::test]
async fn overflowing_next_run_time_never_claims_or_collects() {
    for due_only in [true, false] {
        let db = TestDb::new();
        let table = "metric_collection_spec";
        common::insert_spec(&db, "synthetic", "http_json_metric", &serde_json::json!({}));
        db.execute(&format!(
            "UPDATE {table} SET interval_seconds = {}",
            i64::MAX
        ));
        let before = db.rows(table);
        let store = Store::open(db.path(), &settings(&[])).unwrap();
        let executor = RecordingCollector::default();
        let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());

        let run = run_metric_specs(&store, &executor, &clock, due_only)
            .await
            .unwrap();
        assert_eq!((run.total, run.failed, run.ingested), (1, 1, 0));
        store.close();

        assert_eq!(executor.0.get(), 0, "collector calls (due_only={due_only})");
        assert_eq!(db.rows(table), before, "spec rows (due_only={due_only})");
        assert_eq!(
            db.text("pipeline_heartbeat", "metric_collectors", "status"),
            "critical"
        );
    }
}

#[tokio::test]
async fn stored_json_python_cannot_load_fails_only_its_spec_or_condition() {
    // D-11: Python fails the whole load on a 4300-digit integer; Rust marks
    // only the affected row. A lone surrogate is `not valid JSON` in Rust.
    let db = TestDb::new();
    let huge = format!(r#"{{"n": {}}}"#, "9".repeat(4301));
    db.execute(&format!(
        "INSERT INTO metric_collection_spec (name, enabled, spec_type, interval_seconds, \
         next_run_time, config, last_status, last_error, created_at, updated_at) VALUES \
         ('a huge', 1, 'http_json_metric', 60, 0, '{huge}', 'ok', '', '2026-01-01 00:00:00', \
         '2026-01-01 00:00:00'), \
         ('b surrogate', 1, 'http_json_metric', 60, 0, '{{\"u\": \"\\ud800\"}}', 'ok', '', \
         '2026-01-01 00:00:00', '2026-01-01 00:00:00'), \
         ('c good', 1, 'http_json_metric', 60, 0, '{{}}', 'ok', '', '2026-01-01 00:00:00', \
         '2026-01-01 00:00:00')"
    ));
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
    let run = run_metric_specs(&store, &Faulty, &clock, true)
        .await
        .unwrap();
    store.close();
    assert_eq!((run.total, run.failed), (3, 2));
    let spec = "metric_collection_spec";
    assert!(db.text(spec, "a huge", "last_error").starts_with(
        "Exceeds the limit (4300 digits) for integer string conversion: value has 4301 digits"
    ));
    assert!(
        db.text(spec, "b surrogate", "last_error")
            .starts_with("config is not valid JSON: ")
    );
    assert_eq!(db.text(spec, "c good", "last_status"), "ok");
}

#[tokio::test]
async fn overflowing_schedule_does_not_starve_later_due_specs() {
    let db = TestDb::new();
    for name in ["a huge interval", "b healthy"] {
        common::insert_spec(&db, name, "http_json_metric", &serde_json::json!({}));
    }
    db.execute(&format!(
        "UPDATE metric_collection_spec SET interval_seconds = {} WHERE name = 'a huge interval'",
        i64::MAX
    ));
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
    let executor = RecordingCollector::default();
    let run = run_metric_specs(&store, &executor, &clock, true)
        .await
        .unwrap();
    store.close();

    assert_eq!((run.total, run.failed, run.ingested), (2, 1, 1));
    assert_eq!(executor.0.get(), 1);
    assert_eq!(
        db.text("metric_collection_spec", "b healthy", "last_status"),
        "ok"
    );
    let heartbeat = |column| db.text("pipeline_heartbeat", "metric_collectors", column);
    assert_eq!(heartbeat("status"), "warning");
    assert_eq!(
        heartbeat("last_error"),
        "spec_id=1: next run time exceeds SQLite INTEGER range"
    );
}

#[tokio::test]
async fn later_claim_failure_marks_committed_prefix_as_partial() {
    let db = TestDb::new();
    let specs = "metric_collection_spec";
    for name in ["a healthy", "b blocked", "c unattempted"] {
        common::insert_spec(&db, name, "http_json_metric", &serde_json::json!({}));
    }
    db.execute(
        "INSERT INTO pipeline_heartbeat (name, status, last_success, last_error, details, updated_at)
         VALUES ('metric_collectors', 'ok', '2026-10-20 08:15:30', '', '{}', '2026-10-20 08:15:30');
         CREATE TRIGGER reject_later_claim BEFORE UPDATE OF next_run_time ON metric_collection_spec
         WHEN OLD.name = 'b blocked' BEGIN SELECT RAISE(ABORT, 'synthetic claim failure'); END;",
    );
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
    let executor = RecordingCollector::default();
    let err = run_metric_specs(&store, &executor, &clock, true)
        .await
        .unwrap_err();

    assert!(matches!(err, RunError::Store(_)));
    assert!(err.to_string().contains("synthetic claim failure"));
    assert_eq!(executor.0.get(), 1);
    assert_eq!(db.text(specs, "a healthy", "last_status"), "ok");
    assert_eq!(
        db.value(specs, "a healthy", "next_run_time"),
        rusqlite::types::Value::Integer(clock.now().timestamp() + 60)
    );
    for name in ["b blocked", "c unattempted"] {
        assert_eq!(
            db.value(specs, name, "last_run_at"),
            rusqlite::types::Value::Null
        );
        assert_eq!(
            db.value(specs, name, "next_run_time"),
            rusqlite::types::Value::Integer(0)
        );
    }
    let heartbeat = |column| db.text("pipeline_heartbeat", "metric_collectors", column);
    assert_eq!(heartbeat("status"), "warning");
    assert_eq!(
        heartbeat("last_error"),
        "partial run: collector store failure"
    );
    assert_eq!(heartbeat("last_success"), "2026-10-20 08:15:30");
    assert_eq!(
        serde_json::from_str::<Value>(&heartbeat("details")).unwrap(),
        serde_json::json!({"total": 1, "failed": 0, "warning": 0, "ingested": 1, "skipped": 0, "partial": 1})
    );

    db.execute(
        "CREATE TRIGGER reject_heartbeat BEFORE UPDATE ON pipeline_heartbeat
         BEGIN SELECT RAISE(ABORT, 'synthetic heartbeat failure'); END;",
    );
    let next_clock = FixedClock(clock.now() + std::time::Duration::from_secs(120));
    let err = run_metric_specs(&store, &executor, &next_clock, true)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("synthetic claim failure"));
    assert_eq!(executor.0.get(), 2);
    store.close();
}

#[tokio::test]
async fn later_claim_failure_keeps_schedule_overflow_evidence() {
    let db = TestDb::new();
    for name in ["a unrepresentable", "b blocked"] {
        common::insert_spec(&db, name, "http_json_metric", &serde_json::json!({}));
    }
    db.execute(&format!(
        "UPDATE metric_collection_spec SET interval_seconds = {} WHERE name = 'a unrepresentable';
         CREATE TRIGGER reject_claim BEFORE UPDATE OF next_run_time ON metric_collection_spec
         BEGIN SELECT RAISE(ABORT, 'synthetic claim failure'); END;",
        i64::MAX
    ));
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
    let executor = RecordingCollector::default();
    let err = run_metric_specs(&store, &executor, &clock, true)
        .await
        .unwrap_err();
    store.close();

    assert!(matches!(err, RunError::Store(_)));
    assert!(err.to_string().contains("synthetic claim failure"));
    assert_eq!(executor.0.get(), 0);
    let heartbeat = |column| db.text("pipeline_heartbeat", "metric_collectors", column);
    assert_eq!(heartbeat("status"), "critical");
    assert_eq!(
        heartbeat("last_error"),
        "partial run: collector store failure; spec_id=1: next run time exceeds SQLite INTEGER range"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&heartbeat("details")).unwrap(),
        serde_json::json!({"total": 1, "failed": 1, "warning": 0, "ingested": 0, "skipped": 0, "partial": 1})
    );
}

#[tokio::test]
async fn claim_failure_without_any_outcome_leaves_heartbeat_untouched() {
    let db = TestDb::new();
    common::insert_spec(&db, "blocked", "http_json_metric", &serde_json::json!({}));
    db.execute(
        "CREATE TRIGGER reject_claim BEFORE UPDATE OF next_run_time ON metric_collection_spec
         BEGIN SELECT RAISE(ABORT, 'synthetic claim failure'); END;",
    );
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
    let executor = RecordingCollector::default();
    let err = run_metric_specs(&store, &executor, &clock, true)
        .await
        .unwrap_err();
    store.close();

    assert!(matches!(err, RunError::Store(_)));
    assert!(err.to_string().contains("synthetic claim failure"));
    assert_eq!(executor.0.get(), 0);
    assert_eq!(db.count("pipeline_heartbeat"), 0);
}

struct DeferredCollector {
    entered: tokio::sync::Notify,
    finish: tokio::sync::Notify,
    calls: std::sync::atomic::AtomicUsize,
    db: PathBuf,
    claim_until: i64,
}

impl SpecExecutors for DeferredCollector {
    async fn execute(
        &self,
        _kind: SpecType,
        _name: &str,
        _config: &Value,
        _state: &mut RunState,
    ) -> ExecutorResult {
        use std::sync::atomic::Ordering;
        let next: i64 = rusqlite::Connection::open(&self.db)
            .unwrap()
            .query_row(
                "SELECT next_run_time FROM metric_collection_spec",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(next, self.claim_until, "claim must precede collection");
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            self.finish.notified().await;
        }
        Ok(SpecOutcome::new(StatusLevel::Ok, 1, 0, ""))
    }
}

#[tokio::test]
async fn two_due_runners_execute_one_collector() {
    let db = TestDb::new();
    common::insert_spec(&db, "synthetic", "http_json_metric", &serde_json::json!({}));
    db.execute("UPDATE metric_collection_spec SET interval_seconds=600");
    let a = Store::open(db.path(), &settings(&[])).unwrap();
    let b = Store::open(db.path(), &settings(&[])).unwrap();
    let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
    let collector = DeferredCollector {
        entered: tokio::sync::Notify::new(),
        finish: tokio::sync::Notify::new(),
        calls: std::sync::atomic::AtomicUsize::new(0),
        db: db.path.clone(),
        claim_until: clock.0.timestamp() + 300,
    };
    let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::join!(run_metric_specs(&a, &collector, &clock, true), async {
            collector.entered.notified().await;
            let result = run_metric_specs(&b, &collector, &clock, true).await;
            collector.finish.notify_one();
            result
        })
    })
    .await
    .unwrap();
    assert_eq!(first.unwrap().total, 1);
    assert_eq!(second.unwrap().total, 0);
    assert_eq!(collector.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    a.close();
    b.close();
}

struct ClaimAfterLoadClock {
    db: PathBuf,
    now: UtcDateTime,
    calls: std::sync::atomic::AtomicUsize,
}

impl Clock for ClaimAfterLoadClock {
    fn now(&self) -> UtcDateTime {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            // Another owner claims after load_specs, before this runner's CAS.
            rusqlite::Connection::open(&self.db)
                .unwrap()
                .execute(
                    "UPDATE metric_collection_spec SET next_run_time = ?",
                    [self.now.timestamp() + 300],
                )
                .unwrap();
        }
        self.now
    }
}

#[tokio::test]
async fn a_claim_lost_after_loading_skips_collection_and_heartbeat() {
    let db = TestDb::new();
    common::insert_spec(&db, "synthetic", "http_json_metric", &serde_json::json!({}));
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let clock = ClaimAfterLoadClock {
        db: db.path.clone(),
        now: "2026-10-21T08:15:30Z".parse().unwrap(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    };
    let result = run_metric_specs(&store, &Faulty, &clock, true)
        .await
        .unwrap();
    assert_eq!(result, graphyard_core::MetricCollectionSpecRun::default());
    assert_eq!(clock.calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(db.count("pipeline_heartbeat"), 0);
    store.close();
}

struct EditingCollector<'a> {
    db: &'a TestDb,
    edit: &'a str,
}
impl SpecExecutors for EditingCollector<'_> {
    async fn execute(&self, _: SpecType, _: &str, _: &Value, _: &mut RunState) -> ExecutorResult {
        self.db.execute(self.edit);
        Ok(SpecOutcome::new(StatusLevel::Critical, 2, 3, "old failure"))
    }
}

#[tokio::test]
async fn definition_edits_and_deletion_reject_collection_completion() {
    for due_only in [true, false] {
        for edit in [
            "UPDATE metric_collection_spec SET config = '{\"synthetic\":1}'",
            "UPDATE metric_collection_spec SET spec_type = 'http_page_probe'",
            "UPDATE metric_collection_spec SET interval_seconds = 900",
            "UPDATE metric_collection_spec SET enabled = 0",
            "UPDATE metric_collection_spec SET name = 'renamed'",
            "UPDATE metric_collection_spec SET next_run_time = 9999999999",
            "DELETE FROM metric_collection_spec",
        ] {
            let db = TestDb::new();
            common::insert_spec(&db, "synthetic", "http_json_metric", &serde_json::json!({}));
            let store = Store::open(db.path(), &settings(&[])).unwrap();
            let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
            let result = run_metric_specs(
                &store,
                &EditingCollector { db: &db, edit },
                &clock,
                due_only,
            )
            .await
            .unwrap();
            assert_eq!(
                result,
                graphyard_core::MetricCollectionSpecRun::default(),
                "{edit}"
            );
            assert_eq!(db.count("pipeline_heartbeat"), 0, "{edit}");
            if !edit.starts_with("DELETE") {
                assert_eq!(
                    db.raw()
                        .query_row("SELECT last_run_at FROM metric_collection_spec", [], |r| {
                            r.get::<_, Option<String>>(0)
                        })
                        .unwrap(),
                    None
                );
            }
            store.close();
        }
    }
}

struct ReclaimedCollector<'a> {
    store: &'a Store,
    old_failed: bool,
    now: UtcDateTime,
}
impl SpecExecutors for ReclaimedCollector<'_> {
    async fn execute(&self, _: SpecType, _: &str, _: &Value, _: &mut RunState) -> ExecutorResult {
        struct Fresh(bool);
        impl SpecExecutors for Fresh {
            async fn execute(
                &self,
                _: SpecType,
                _: &str,
                _: &Value,
                _: &mut RunState,
            ) -> ExecutorResult {
                Ok(SpecOutcome::new(
                    if self.0 {
                        StatusLevel::Ok
                    } else {
                        StatusLevel::Critical
                    },
                    1,
                    0,
                    "new",
                ))
            }
        }
        let newer = FixedClock(self.now + std::time::Duration::from_secs(300));
        let run = run_metric_specs(self.store, &Fresh(self.old_failed), &newer, true)
            .await
            .unwrap();
        assert_eq!(run.total, 1);
        Ok(SpecOutcome::new(
            if self.old_failed {
                StatusLevel::Critical
            } else {
                StatusLevel::Ok
            },
            8,
            9,
            "old",
        ))
    }
}
#[tokio::test]
async fn superseded_collection_cannot_replace_newer_heartbeat() {
    for old_failed in [true, false] {
        let db = TestDb::new();
        common::insert_spec(&db, "synthetic", "http_json_metric", &serde_json::json!({}));
        db.execute("UPDATE metric_collection_spec SET interval_seconds = 600");
        let a = Store::open(db.path(), &settings(&[])).unwrap();
        let b = Store::open(db.path(), &settings(&[])).unwrap();
        let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
        let result = run_metric_specs(
            &a,
            &ReclaimedCollector {
                store: &b,
                old_failed,
                now: clock.0,
            },
            &clock,
            true,
        )
        .await
        .unwrap();
        assert_eq!(result, graphyard_core::MetricCollectionSpecRun::default());
        let expected = if old_failed { "ok" } else { "critical" };
        assert_eq!(
            db.text("metric_collection_spec", "synthetic", "last_status"),
            expected
        );
        assert_eq!(
            db.text("pipeline_heartbeat", "metric_collectors", "status"),
            expected
        );
        let details: Value =
            serde_json::from_str(&db.text("pipeline_heartbeat", "metric_collectors", "details"))
                .unwrap();
        assert_eq!(details["ingested"], 1);
        a.close();
        b.close();
    }
}

#[tokio::test]
async fn unchanged_truthy_enabled_collection_settles() {
    for due_only in [true, false] {
        let db = TestDb::new();
        common::insert_spec(&db, "synthetic", "http_json_metric", &serde_json::json!({}));
        db.execute("UPDATE metric_collection_spec SET enabled = 2, interval_seconds = 600");
        let store = Store::open(db.path(), &settings(&[])).unwrap();
        let clock = FixedClock("2026-10-21T08:15:30Z".parse().unwrap());
        let result = run_metric_specs(&store, &Faulty, &clock, due_only)
            .await
            .unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(
            db.text("metric_collection_spec", "synthetic", "last_status"),
            "ok"
        );
        assert_eq!(
            db.value("metric_collection_spec", "synthetic", "next_run_time"),
            rusqlite::types::Value::Integer(clock.0.timestamp() + 600)
        );
        store.close();
    }
}
