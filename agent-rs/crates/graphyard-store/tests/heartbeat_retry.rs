//! `record_heartbeat`'s lock retry (PLAN.md 5.4): up to three attempts,
//! retrying on SQLite `SQLITE_BUSY` after 50 ms and 100 ms.

mod common;

use std::time::{Duration, Instant};

use common::{TestDb, settings};
use graphyard_core::StatusLevel;
use graphyard_core::utc::UtcDateTime;
use graphyard_store::{HeartbeatRecord, Store, StoreError};

fn at() -> UtcDateTime {
    UtcDateTime::from_micros(1_792_829_730_123_456)
}

fn record() -> HeartbeatRecord {
    HeartbeatRecord {
        name: "condition_evaluator".to_owned(),
        status: StatusLevel::Ok,
        last_error: String::new(),
        details: vec![("evaluated".to_owned(), 1), ("failed".to_owned(), 0)],
        success: true,
    }
}

/// A store that gives up waiting for a lock after 20 ms.
fn impatient_store(db: &TestDb) -> Store {
    let settings = settings(&[
        ("GRAPHYARD_SQLITE_TIMEOUT_SECONDS", "0"),
        ("GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS", "20"),
    ]);
    Store::open(db.path(), &settings).unwrap()
}

fn heartbeat_count(db: &TestDb) -> i64 {
    db.raw()
        .query_row("SELECT count(*) FROM pipeline_heartbeat", [], |row| {
            row.get(0)
        })
        .unwrap()
}

#[tokio::test]
async fn a_lock_held_throughout_fails_after_three_attempts() {
    let db = TestDb::new();
    let store = impatient_store(&db);
    let holder = db.raw();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();

    let started = Instant::now();
    let err = store.record_heartbeat(record(), at()).await.unwrap_err();
    let elapsed = started.elapsed();

    assert!(err.is_database_locked(), "{err:?}");
    assert_eq!(err.to_string(), "database is locked");
    // Three busy waits of 20 ms plus the sleeps of 50 ms and 100 ms.
    assert!(elapsed >= Duration::from_millis(210), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    holder.execute_batch("ROLLBACK").unwrap();
    assert_eq!(heartbeat_count(&db), 0);
    store.close();
}

#[tokio::test]
async fn a_lock_released_between_attempts_succeeds() {
    let db = TestDb::new();
    let store = impatient_store(&db);
    let path = db.path().to_owned();
    let (locked, wait_locked) = std::sync::mpsc::channel();
    let holder = std::thread::spawn(move || {
        let connection = rusqlite::Connection::open(path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked.send(()).unwrap();
        // Longer than the first attempt and its 50 ms sleep, shorter than
        // all three attempts.
        std::thread::sleep(Duration::from_millis(60));
        connection.execute_batch("COMMIT").unwrap();
    });
    wait_locked.recv().unwrap();

    store.record_heartbeat(record(), at()).await.unwrap();

    holder.join().unwrap();
    assert_eq!(heartbeat_count(&db), 1);
    store.close();
}

#[tokio::test]
async fn other_errors_are_not_retried() {
    let db = TestDb::new();
    db.raw()
        .execute(
            "INSERT INTO pipeline_heartbeat (name, status, last_success, last_error, details, \
             updated_at) VALUES ('condition_evaluator', 'ok', '2026-02-30 00:00:00', '', '{}', \
             '2026-01-01 00:00:00')",
            [],
        )
        .unwrap();
    let store = db.store();

    let started = Instant::now();
    let err = store.record_heartbeat(record(), at()).await.unwrap_err();

    assert!(!err.is_database_locked());
    assert_eq!(err.to_string(), "day is out of range for month");
    assert!(started.elapsed() < Duration::from_millis(50));
    store.close();
}

#[test]
fn heartbeat_retry_uses_sqlite_result_code() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/heartbeat/retry_codes.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let code = i32::try_from(case["code"].as_i64().unwrap()).unwrap();
        let message = case["message"].as_str().unwrap().to_owned();
        let retryable = case["retryable"].as_bool().unwrap();
        let err = StoreError::from(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            Some(message),
        ));
        assert_eq!(err.is_database_locked(), retryable, "SQLite code {code}");
    }
}
