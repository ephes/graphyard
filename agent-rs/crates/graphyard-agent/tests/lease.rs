//! A short schedule must not expire while a collector is still live.
mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use graphyard_agent::runs::{Clock, SpecExecutors, SystemClock, run_metric_specs};
use graphyard_collect::executors::{ExecutorResult, RunState};
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
struct Fixed(DateTime<Utc>);
impl Clock for Fixed {
    fn now(&self) -> DateTime<Utc> {
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
    let before = Fixed(DateTime::from_timestamp(until - 1, 0).unwrap());
    let expired = Fixed(DateTime::from_timestamp(until, 0).unwrap());
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
    let spec = store.load_specs(None).await.unwrap().remove(0);
    assert!(store.claim_due_spec(spec.id, 0, 100, 103).await.unwrap());
    assert!(
        store
            .renew_spec_claim(spec.clone(), 103, 104)
            .await
            .unwrap()
    );
    // A different store/process owns the replacement once the old claim expires.
    let other = Store::open(db.path(), &settings(&[])).unwrap();
    assert!(other.claim_due_spec(spec.id, 104, 104, 107).await.unwrap());
    assert!(
        !store
            .renew_spec_claim(spec.clone(), 104, 108)
            .await
            .unwrap()
    );
    assert!(
        !store
            .finish_claimed_spec(
                spec.clone(),
                graphyard_store::SpecRunUpdate {
                    id: spec.id,
                    last_run_at: DateTime::from_timestamp(100, 0).unwrap(),
                    last_status: StatusLevel::Ok,
                    last_error: String::new(),
                    next_run_time: 101,
                },
                104
            )
            .await
            .unwrap()
    );
    assert_eq!(schedule(&db), 107);
    db.execute("UPDATE metric_collection_spec SET enabled=0");
    assert!(!other.renew_spec_claim(spec, 107, 110).await.unwrap());
    store.close();
    other.close();
}
