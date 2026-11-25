//! A short schedule must not expire while a collector is still live.
mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use graphyard_agent::runs::{Clock, SpecExecutors, SystemClock, run_metric_specs};
use graphyard_collect::executors::{ExecutorResult, RunState};
use graphyard_core::utc::UtcDateTime;
use graphyard_core::{SpecOutcome, SpecType, StatusLevel};
use graphyard_store::Store;
use serde_json::{Value, json};
use tokio::sync::Notify;

use common::{TestDb, insert_spec, settings};

struct Deferred {
    entered: Notify,
    finish: Notify,
    calls: AtomicUsize,
}
impl Deferred {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            finish: Notify::new(),
            calls: AtomicUsize::new(0),
        }
    }
}
impl SpecExecutors for Deferred {
    async fn execute(&self, _: SpecType, _: &str, _: &Value, _: &mut RunState) -> ExecutorResult {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.entered.notify_one();
            self.finish.notified().await;
        }
        Ok(SpecOutcome::new(StatusLevel::Ok, 1, 0, ""))
    }
}
struct Fixed(UtcDateTime);
impl Clock for Fixed {
    fn now(&self) -> UtcDateTime {
        self.0
    }
}
fn short_spec(db: &TestDb) {
    insert_spec(db, "synthetic", "http_json_metric", &json!({}));
    db.execute("UPDATE metric_collection_spec SET interval_seconds=1");
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

#[tokio::test]
async fn live_collector_outlasts_short_interval_without_a_second_execution() {
    let db = TestDb::new();
    short_spec(&db);
    let a = Store::open(db.path(), &settings(&[])).unwrap();
    let b = Store::open(db.path(), &settings(&[])).unwrap();
    let collector = Deferred::new();
    let (first, second) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(
            run_metric_specs(&a, &collector, &SystemClock, true),
            async {
                collector.entered.notified().await;
                let first_claim = schedule(&db);
                // Longer than both the one-second interval and the minimum lease.
                tokio::time::sleep(Duration::from_secs(4)).await;
                let renewed = schedule(&db);
                let second = run_metric_specs(&b, &collector, &SystemClock, true).await;
                collector.finish.notify_one();
                assert!(renewed > first_claim, "the live owner must renew its claim");
                second
            }
        )
    })
    .await
    .unwrap();
    assert_eq!(first.unwrap().total, 1);
    assert_eq!(second.unwrap().total, 0);
    assert_eq!(collector.calls.load(Ordering::SeqCst), 1);
    a.close();
    b.close();
}

#[tokio::test]
async fn abandoned_collection_is_claimable_only_after_its_persisted_lease() {
    let db = TestDb::new();
    short_spec(&db);
    let a = Store::open(db.path(), &settings(&[])).unwrap();
    let b = Store::open(db.path(), &settings(&[])).unwrap();
    let collector = Deferred::new();
    {
        let pending = run_metric_specs(&a, &collector, &SystemClock, true);
        tokio::pin!(pending);
        tokio::select! {
            _ = collector.entered.notified() => {},
            result = &mut pending => panic!("collector should be deferred: {result:?}"),
        }
        // Dropping the run stops renewals; only its persisted lease remains.
    }
    let until = schedule(&db);
    let before = Fixed(UtcDateTime::from_seconds(until - 1));
    let expired = Fixed(UtcDateTime::from_seconds(until));
    assert_eq!(
        run_metric_specs(&b, &collector, &before, true)
            .await
            .unwrap()
            .total,
        0
    );
    assert_eq!(
        run_metric_specs(&b, &collector, &expired, true)
            .await
            .unwrap()
            .total,
        1
    );
    assert_eq!(collector.calls.load(Ordering::SeqCst), 2);
    a.close();
    b.close();
}

#[tokio::test]
async fn renewal_and_completion_cannot_extend_or_publish_a_replacement_claim() {
    let db = TestDb::new();
    short_spec(&db);
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    db.execute("UPDATE metric_collection_spec SET last_error = printf('%.*c', 1048576, 'x')");
    let spec = store.load_specs(None).await.unwrap().remove(0);
    assert_eq!(spec.last_error.len(), 1_048_576);
    let claim = spec.claim_identity();
    let id = spec.id;
    drop(spec);
    assert!(
        store
            .claim_due_spec(claim.clone(), 0, 100, 103)
            .await
            .unwrap()
    );
    assert!(
        store
            .renew_spec_claim(claim.clone(), 103, 104)
            .await
            .unwrap()
    );
    db.execute("UPDATE metric_collection_spec SET last_status = 'warning', last_error = ''");
    assert!(
        store
            .renew_spec_claim(claim.clone(), 104, 105)
            .await
            .unwrap()
    );
    // A different store/process owns the replacement once the old claim expires.
    let other = Store::open(db.path(), &settings(&[])).unwrap();
    assert!(
        other
            .claim_due_spec(claim.clone(), 105, 105, 108)
            .await
            .unwrap()
    );
    assert!(
        !store
            .renew_spec_claim(claim.clone(), 105, 109)
            .await
            .unwrap()
    );
    assert!(
        !store
            .finish_claimed_spec(
                claim.clone(),
                graphyard_store::SpecRunUpdate {
                    id,
                    last_run_at: UtcDateTime::from_seconds(100),
                    last_status: StatusLevel::Ok,
                    last_error: String::new(),
                    next_run_time: 101,
                },
                105
            )
            .await
            .unwrap()
    );
    assert_eq!(schedule(&db), 108);
    db.execute("UPDATE metric_collection_spec SET enabled=0");
    assert!(!other.renew_spec_claim(claim, 108, 111).await.unwrap());
    store.close();
    other.close();
}

/// Records the config each execution received.
struct Recording(std::sync::Mutex<Vec<Value>>);
impl SpecExecutors for Recording {
    async fn execute(
        &self,
        _: SpecType,
        _: &str,
        config: &Value,
        _: &mut RunState,
    ) -> ExecutorResult {
        self.0.lock().unwrap().push(config.clone());
        Ok(SpecOutcome::new(StatusLevel::Ok, 0, 0, ""))
    }
}

/// Replaces the config (keeping the schedule) on the per-spec clock read,
/// i.e. after the load and before the claim.
struct EditBeforeClaim<'a> {
    db: &'a TestDb,
    calls: AtomicUsize,
}
impl Clock for EditBeforeClaim<'_> {
    fn now(&self) -> UtcDateTime {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            self.db.execute(
                "UPDATE metric_collection_spec SET \
                 config='{\"url\": \"http://127.0.0.1:9/new\", \"bearer_token\": \"new-token\"}'",
            );
        }
        UtcDateTime::from_seconds(100)
    }
}

#[tokio::test]
async fn claim_refuses_a_definition_edited_after_load() {
    let db = TestDb::new();
    insert_spec(
        &db,
        "synthetic",
        "http_json_metric",
        &json!({"url": "http://127.0.0.1:9/old", "bearer_token": "old-token"}),
    );
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let collector = Recording(std::sync::Mutex::new(Vec::new()));
    let clock = EditBeforeClaim {
        db: &db,
        calls: AtomicUsize::new(0),
    };
    let stale = run_metric_specs(&store, &collector, &clock, true)
        .await
        .unwrap();
    assert_eq!(stale.total, 0);
    assert!(
        collector.0.lock().unwrap().is_empty(),
        "the old URL/bearer must not reach the collector"
    );
    // The slot was not claimed: the edited spec is still due and runs with
    // its current definition.
    assert_eq!(schedule(&db), 0);
    let fresh = run_metric_specs(
        &store,
        &collector,
        &Fixed(UtcDateTime::from_seconds(100)),
        true,
    )
    .await
    .unwrap();
    assert_eq!(fresh.total, 1);
    assert_eq!(
        *collector.0.lock().unwrap(),
        vec![json!({"url": "http://127.0.0.1:9/new", "bearer_token": "new-token"})]
    );
    store.close();
}

fn edit_and_revert(db: &TestDb) {
    // Django saves updated_at with each definition edit while the admin
    // preserves the runner's schedule.
    db.execute(
        "UPDATE metric_collection_spec SET config = '{\"changed\": true}',          updated_at = '2026-10-21 08:15:30.000001'",
    );
    db.execute(
        "UPDATE metric_collection_spec SET config = '{}',          updated_at = '2026-10-21 08:15:30.000002'",
    );
}

#[tokio::test]
async fn edits_and_reverts_fence_claim_and_renewal() {
    let db = TestDb::new();
    short_spec(&db);
    db.execute("UPDATE metric_collection_spec SET updated_at = '2026-01-02T04:04:05+01:00'");
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let before = store
        .load_specs(None)
        .await
        .unwrap()
        .remove(0)
        .claim_identity();
    assert!(
        store
            .claim_due_spec(before.clone(), 0, 100, 103)
            .await
            .unwrap()
    );
    edit_and_revert(&db);
    assert!(
        !store
            .renew_spec_claim(before.clone(), 103, 104)
            .await
            .unwrap()
    );
    assert!(!store.claim_due_spec(before, 103, 103, 106).await.unwrap());
    let current = store
        .load_specs(None)
        .await
        .unwrap()
        .remove(0)
        .claim_identity();
    assert!(store.claim_due_spec(current, 103, 103, 106).await.unwrap());
    store.close();
}

#[tokio::test]
async fn edits_and_reverts_fence_inflight_completion() {
    for due_only in [true, false] {
        let db = TestDb::new();
        short_spec(&db);
        let store = Store::open(db.path(), &settings(&[])).unwrap();
        let collector = Deferred::new();
        let clock = Fixed(UtcDateTime::from_seconds(100));
        let (run, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                run_metric_specs(&store, &collector, &clock, due_only),
                async {
                    collector.entered.notified().await;
                    edit_and_revert(&db);
                    collector.finish.notify_one();
                }
            )
        })
        .await
        .unwrap();
        assert_eq!(run.unwrap().total, 0);
        assert_eq!(db.count("pipeline_heartbeat"), 0);
        assert_eq!(
            db.value("metric_collection_spec", "synthetic", "last_run_at"),
            rusqlite::types::Value::Null
        );
        let due = Fixed(UtcDateTime::from_seconds(schedule(&db).max(101)));
        assert_eq!(
            run_metric_specs(&store, &collector, &due, due_only)
                .await
                .unwrap()
                .total,
            1
        );
        assert_eq!(db.count("pipeline_heartbeat"), 1);
        store.close();
    }
}
