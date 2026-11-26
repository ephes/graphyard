//! A short schedule must not expire while a collector is still live.
mod common;

use std::future::{Future, poll_fn};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::task::Poll;
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
#[derive(Clone, Copy)]
struct Fixed(UtcDateTime);
impl Clock for Fixed {
    fn now(&self) -> UtcDateTime {
        self.0
    }
}

#[derive(Clone)]
struct AdjustableClock {
    seconds: Arc<AtomicI64>,
    read: Arc<Notify>,
}

impl AdjustableClock {
    fn new(seconds: i64) -> Self {
        Self {
            seconds: Arc::new(AtomicI64::new(seconds)),
            read: Arc::new(Notify::new()),
        }
    }
}

impl Clock for AdjustableClock {
    fn now(&self) -> UtcDateTime {
        let seconds = self.seconds.load(Ordering::SeqCst);
        self.read.notify_one();
        UtcDateTime::from_seconds(seconds)
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

struct Ordered {
    entered: Notify,
    resume: Notify,
    names: std::sync::Mutex<Vec<String>>,
}

impl SpecExecutors for Ordered {
    async fn execute(
        &self,
        _: SpecType,
        name: &str,
        _: &Value,
        _: &mut RunState,
    ) -> ExecutorResult {
        self.names.lock().unwrap().push(name.to_owned());
        if name == "a first" {
            self.entered.notify_one();
            self.resume.notified().await;
        }
        Ok(SpecOutcome::new(StatusLevel::Ok, 0, 0, ""))
    }
}

#[tokio::test]
async fn pending_sweep_skips_full_config_reads_for_an_intervening_lease() {
    let db = TestDb::new();
    for name in ["d last", "b leased", "bb edited", "c next", "a first"] {
        insert_spec(&db, name, "http_json_metric", &json!({}));
    }
    db.execute(
        "PRAGMA ignore_check_constraints = ON;
         UPDATE metric_collection_spec SET next_run_time = 50 WHERE name != 'b leased';
         UPDATE metric_collection_spec SET config = X'00' WHERE name = 'b leased';",
    );
    let a = Store::open(db.path(), &settings(&[])).unwrap();
    let b = Store::open(db.path(), &settings(&[])).unwrap();
    let first = Ordered {
        entered: Notify::new(),
        resume: Notify::new(),
        names: std::sync::Mutex::new(Vec::new()),
    };
    let other = Deferred::new();
    let now = Fixed(UtcDateTime::from_seconds(100));
    let earlier = Fixed(UtcDateTime::from_seconds(45));
    let completed = Notify::new();

    let (result, second) = tokio::time::timeout(Duration::from_secs(5), async {
        let first_run = async {
            let result = run_metric_specs(&a, &first, &now, true).await;
            completed.notify_one();
            result
        };
        tokio::join!(first_run, async {
            first.entered.notified().await;
            // Runner A has seen the candidate. A full config read at scan
            // time would have tried to decode its BLOB and counted a failure.
            db.execute("UPDATE metric_collection_spec SET config = '{}' WHERE name = 'b leased'");
            let pending = run_metric_specs(&b, &other, &earlier, true);
            tokio::pin!(pending);
            tokio::select! {
                _ = other.entered.notified() => {},
                result = &mut pending => panic!("second collector should be deferred: {result:?}"),
            }
            // Eager hydration at A's execution boundary would also fail.
            db.execute(
                "PRAGMA ignore_check_constraints = ON;
                     UPDATE metric_collection_spec SET config = X'00' WHERE name = 'b leased';",
            );
            db.execute(
                "UPDATE metric_collection_spec SET config = '{\"edited\": true}', \
                 updated_at = '2026-02-01 00:00:00' WHERE name = 'bb edited'",
            );
            first.resume.notify_one();
            // Keep the lease alive until runner A has passed the candidate.
            completed.notified().await;
            db.execute("UPDATE metric_collection_spec SET config = '{}' WHERE name = 'b leased'");
            other.finish.notify_one();
            pending.await
        })
    })
    .await
    .unwrap();

    let result = result.unwrap();
    assert_eq!((result.total, result.failed), (3, 0));
    assert_eq!(
        *first.names.lock().unwrap(),
        ["a first", "c next", "d last"]
    );
    assert_eq!(second.unwrap().total, 1);
    assert_eq!(other.calls.load(Ordering::SeqCst), 1);
    a.close();
    b.close();
}

#[tokio::test]
async fn due_sweep_pages_in_name_order_with_one_horizon() {
    let db = TestDb::new();
    let names: Vec<_> = (0..130).map(|n| format!("spec {n:03}")).collect();
    for name in names.iter().rev() {
        insert_spec(&db, name, "http_json_metric", &json!({}));
    }
    insert_spec(&db, "zz future", "http_json_metric", &json!({}));
    db.execute("UPDATE metric_collection_spec SET next_run_time = 101 WHERE name = 'zz future'");
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let collector = Ordered {
        entered: Notify::new(),
        resume: Notify::new(),
        names: std::sync::Mutex::new(Vec::new()),
    };
    let clock = Fixed(UtcDateTime::from_seconds(100));
    let run = run_metric_specs(&store, &collector, &clock, true)
        .await
        .unwrap();
    assert_eq!(run.total, 130);
    assert_eq!(*collector.names.lock().unwrap(), names);
    store.close();
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
async fn renewal_delayed_until_expiry_cancels_collection_and_leaves_the_slot_due() {
    let db = TestDb::new();
    short_spec(&db);
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let collector = Deferred::new();
    let clock = AdjustableClock::new(100);
    let (run, ()) = tokio::time::timeout(Duration::from_secs(9), async {
        tokio::join!(run_metric_specs(&store, &collector, &clock, true), async {
            collector.entered.notified().await;
            assert_eq!(schedule(&db), 103);
            clock.read.notified().await; // Drain the scan/claim reads.
            let writer = db.raw();
            writer.execute_batch("BEGIN IMMEDIATE").unwrap();
            clock.seconds.store(101, Ordering::SeqCst);
            clock.read.notified().await; // Renewal starts while the claim is live.
            clock.seconds.store(103, Ordering::SeqCst);
            writer.execute_batch("COMMIT").unwrap();
            collector.finish.notify_one();
        })
    })
    .await
    .unwrap();
    assert_eq!(run.unwrap().total, 0);
    assert_eq!(schedule(&db), 103);
    assert_eq!(db.count("pipeline_heartbeat"), 0);
    assert_eq!(
        run_metric_specs(&store, &collector, &clock, true)
            .await
            .unwrap()
            .total,
        1
    );
    assert_eq!(collector.calls.load(Ordering::SeqCst), 2);
    store.close();
}

#[tokio::test]
async fn completion_delayed_until_expiry_leaves_the_slot_due() {
    let db = TestDb::new();
    short_spec(&db);
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let collector = Deferred::new();
    let clock = AdjustableClock::new(100);
    let run = tokio::time::timeout(Duration::from_secs(9), async {
        let pending = run_metric_specs(&store, &collector, &clock, true);
        tokio::pin!(pending);
        tokio::select! {
            _ = collector.entered.notified() => {},
            result = &mut pending => panic!("collector should be deferred: {result:?}"),
        }
        assert_eq!(schedule(&db), 103);
        let writer = db.raw();
        writer.execute_batch("BEGIN IMMEDIATE").unwrap();
        collector.finish.notify_one();
        // Poll the completion through submission to the blocked store.
        assert!(
            poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        clock.seconds.store(103, Ordering::SeqCst);
        writer.execute_batch("COMMIT").unwrap();
        pending.await.unwrap()
    })
    .await
    .unwrap();
    assert_eq!(run.total, 0);
    assert_eq!(schedule(&db), 103);
    assert_eq!(
        db.value("metric_collection_spec", "synthetic", "last_run_at"),
        rusqlite::types::Value::Null
    );
    assert_eq!(db.count("pipeline_heartbeat"), 0);
    assert_eq!(
        run_metric_specs(&store, &collector, &clock, true)
            .await
            .unwrap()
            .total,
        1
    );
    assert_eq!(collector.calls.load(Ordering::SeqCst), 2);
    store.close();
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
            .renew_spec_claim(claim.clone(), 103, 104, || 101)
            .await
            .unwrap()
    );
    db.execute("UPDATE metric_collection_spec SET last_status = 'warning', last_error = ''");
    assert!(
        store
            .renew_spec_claim(claim.clone(), 104, 105, || 102)
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
            .renew_spec_claim(claim.clone(), 105, 109, || 104)
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
                105,
                || 104,
            )
            .await
            .unwrap()
    );
    assert_eq!(schedule(&db), 108);
    db.execute("UPDATE metric_collection_spec SET enabled=0");
    assert!(
        !other
            .renew_spec_claim(claim, 108, 111, || 106)
            .await
            .unwrap()
    );
    store.close();
    other.close();
}

#[tokio::test]
async fn a_truthy_but_decoded_disabled_flag_fences_lease_writes() {
    let db = TestDb::new();
    short_spec(&db);
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let spec = store.load_specs(None).await.unwrap().remove(0);
    let claim = spec.claim_identity();
    let update = graphyard_store::SpecRunUpdate {
        id: spec.id,
        last_run_at: UtcDateTime::from_seconds(100),
        last_status: StatusLevel::Ok,
        last_error: String::new(),
        next_run_time: 101,
    };

    db.execute("UPDATE metric_collection_spec SET enabled = 2");
    let admitted = store
        .claim_due_spec(claim.clone(), 0, 100, 103)
        .await
        .unwrap();
    db.execute("UPDATE metric_collection_spec SET enabled = 1, next_run_time = 0");
    assert!(
        store
            .claim_due_spec(claim.clone(), 0, 100, 103)
            .await
            .unwrap()
    );

    // Simulate a legacy import which changes the flag without a new revision.
    db.execute("UPDATE metric_collection_spec SET enabled = 2");
    let renewed = store
        .renew_spec_claim(claim.clone(), 103, 104, || 101)
        .await
        .unwrap();
    let published = store
        .finish_claimed_spec(claim, update, if renewed { 104 } else { 103 }, || 101)
        .await
        .unwrap();
    assert_eq!((admitted, renewed, published), (false, false, false));
    assert_eq!(schedule(&db), 103);
    store.close();
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
#[derive(Clone)]
struct EditBeforeClaim {
    db: Arc<TestDb>,
    calls: Arc<AtomicUsize>,
}
impl Clock for EditBeforeClaim {
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
    let db = Arc::new(TestDb::new());
    insert_spec(
        &db,
        "synthetic",
        "http_json_metric",
        &json!({"url": "http://127.0.0.1:9/old", "bearer_token": "old-token"}),
    );
    let store = Store::open(db.path(), &settings(&[])).unwrap();
    let collector = Recording(std::sync::Mutex::new(Vec::new()));
    let clock = EditBeforeClaim {
        db: db.clone(),
        calls: Arc::new(AtomicUsize::new(0)),
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
            .renew_spec_claim(before.clone(), 103, 104, || 101)
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

/// Hold a read transaction until the writer is blocked on it, then pass the
/// lease deadline and release.
///
/// A fresh reader is refused (SQLITE_BUSY) once the writer holds SQLite's
/// PENDING lock. An IMMEDIATE writer takes it only at COMMIT, after reading
/// the clock; an EXCLUSIVE writer takes it at BEGIN, before reading the clock.
/// Either way the writer is then waiting for `reader`.
async fn release_reader_after_expiry(
    path: std::path::PathBuf,
    reader: rusqlite::Connection,
    clock: Arc<AtomicI64>,
) {
    let probe = rusqlite::Connection::open(path).unwrap();
    probe.busy_timeout(Duration::ZERO).unwrap();
    loop {
        match probe.query_row("SELECT count(*) FROM metric_collection_spec", [], |row| {
            row.get::<_, i64>(0)
        }) {
            Ok(_) => tokio::time::sleep(Duration::from_millis(5)).await,
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::DatabaseBusy =>
            {
                break;
            }
            Err(error) => panic!("probe read failed: {error}"),
        }
    }
    clock.store(103, Ordering::SeqCst);
    reader.execute_batch("COMMIT").unwrap();
}

#[tokio::test]
async fn a_reader_held_through_expiry_cannot_delay_a_checked_lease_write_past_the_deadline() {
    for publish in [false, true] {
        let db = TestDb::new();
        short_spec(&db);
        // A rollback journal: there a writer's COMMIT waits for readers.
        let store = Store::open(
            db.path(),
            &settings(&[("GRAPHYARD_SQLITE_JOURNAL_MODE", "DELETE")]),
        )
        .unwrap();
        let spec = store.load_specs(None).await.unwrap().remove(0);
        let claim = spec.claim_identity();
        let id = spec.id;
        drop(spec);
        assert!(
            store
                .claim_due_spec(claim.clone(), 0, 100, 103)
                .await
                .unwrap()
        );
        let reader = db.raw();
        reader.execute_batch("BEGIN").unwrap();
        let rows: i64 = reader
            .query_row("SELECT count(*) FROM metric_collection_spec", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(rows, 1);
        let clock = Arc::new(AtomicI64::new(102));
        let read = clock.clone();
        let now = move || read.load(Ordering::SeqCst);
        let wrote = tokio::time::timeout(Duration::from_secs(9), async {
            if publish {
                let update = graphyard_store::SpecRunUpdate {
                    id,
                    last_run_at: UtcDateTime::from_seconds(102),
                    last_status: StatusLevel::Ok,
                    last_error: String::new(),
                    next_run_time: 104,
                };
                tokio::join!(
                    store.finish_claimed_spec(claim, update, 103, now),
                    release_reader_after_expiry(db.path().to_owned(), reader, clock),
                )
                .0
            } else {
                tokio::join!(
                    store.renew_spec_claim(claim, 103, 106, now),
                    release_reader_after_expiry(db.path().to_owned(), reader, clock),
                )
                .0
            }
        })
        .await
        .unwrap()
        .unwrap();
        assert!(!wrote, "publish={publish}");
        assert_eq!(schedule(&db), 103);
        assert_eq!(
            db.value("metric_collection_spec", "synthetic", "last_run_at"),
            rusqlite::types::Value::Null
        );
        store.close();
    }
}
