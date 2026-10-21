//! Shutdown (PLAN.md 3.7, 4.1, D-1): a `getaddrinfo` that never returns
//! does not delay the exit, and an SQLite statement the DB thread is running
//! completes before the agent exits.

mod common;

use std::net::IpAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use graphyard_agent::app::{AgentConfig, Shutdown, run_agent};
use graphyard_agent::cli::Loops;
use graphyard_collect::http::{HttpContext, Lookup, LookupError};
use graphyard_influx::InfluxWriter;
use num_bigint::BigInt;
use rusqlite::types::Value as SqlValue;
use serde_json::json;

use common::{TestDb, settings};

/// A resolver whose lookups never return (for the test's lifetime).
struct Stuck;

impl Lookup for Stuck {
    fn lookup(&self, _host: &str) -> Result<Vec<IpAddr>, LookupError> {
        std::thread::sleep(Duration::from_secs(600));
        Err(LookupError("unreachable".to_owned()))
    }
}

fn config(db: &TestDb, http: HttpContext, busy_timeout_ms: &str) -> AgentConfig {
    AgentConfig {
        db: db.path.clone(),
        loops: Loops {
            run_metrics: true,
            metrics_interval: BigInt::from(60),
            run_conditions: false,
            condition_interval: BigInt::from(60),
            run_once: true,
        },
        settings: settings(&[("GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS", busy_timeout_ms)]),
        http,
        writer: InfluxWriter::to_file(&db.dir.path().join("writes.lp")).unwrap(),
    }
}

async fn signal_after(millis: u64, signal: Shutdown) -> Shutdown {
    tokio::time::sleep(Duration::from_millis(millis)).await;
    signal
}

#[test]
fn sigterm_does_not_wait_for_a_blocked_lookup() {
    let db = TestDb::new();
    common::insert_spec(
        &db,
        "stuck lookup",
        "http_json_metric",
        &json!({"url": "http://stuck.invalid/value", "metric_path": "value", "metric_name": "x.y"}),
    );
    let http = HttpContext::new().with_lookup(Arc::new(Stuck));
    let started = Instant::now();
    let code = run_agent(config(&db, http, "30000"), || {
        Ok(signal_after(300, Shutdown::Terminate))
    });
    assert_eq!(code, ExitCode::SUCCESS);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
    // The spec was cancelled before its result was written.
    assert_eq!(
        db.value("metric_collection_spec", "stuck lookup", "last_run_at"),
        SqlValue::Null
    );
    assert_eq!(db.count("pipeline_heartbeat"), 0);
}

#[test]
fn shutdown_waits_for_the_statement_the_db_thread_is_running() {
    let db = TestDb::new();
    common::insert_spec(&db, "unknown", "legacy_type", &json!({}));
    // Another connection holds the write lock for 1.5 s: the spec update
    // waits on the busy timeout when the signal arrives.
    let holder = db.raw();
    holder
        .execute_batch("PRAGMA journal_mode = WAL; BEGIN IMMEDIATE")
        .unwrap();
    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(1500));
        holder.execute_batch("COMMIT").unwrap();
    });
    let started = Instant::now();
    let code = run_agent(config(&db, HttpContext::new(), "10000"), || {
        Ok(signal_after(300, Shutdown::Interrupt))
    });
    let elapsed = started.elapsed();
    release.join().unwrap();
    assert_eq!(code, ExitCode::SUCCESS);
    assert!(elapsed >= Duration::from_millis(1400), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    // The statement completed; the heartbeat after it never started.
    assert_eq!(
        db.text("metric_collection_spec", "unknown", "last_error"),
        "unsupported spec_type: legacy_type"
    );
    assert_eq!(db.count("pipeline_heartbeat"), 0);
}

#[test]
fn run_once_ends_without_a_signal() {
    let db = TestDb::new();
    common::insert_spec(&db, "unknown", "legacy_type", &json!({}));
    let code = run_agent(config(&db, HttpContext::new(), "1000"), || {
        Ok(std::future::pending::<Shutdown>())
    });
    assert_eq!(code, ExitCode::SUCCESS);
    assert_eq!(
        db.text("pipeline_heartbeat", "metric_collectors", "status"),
        "critical"
    );
}

#[test]
fn a_signal_during_a_blocked_startup_ends_the_agent() {
    // Another connection holds an exclusive lock: opening (the WAL switch)
    // and the schema check wait on the busy timeout when the signal comes.
    let db = TestDb::new();
    let holder = db.raw();
    holder.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let started = Instant::now();
    let code = run_agent(config(&db, HttpContext::new(), "20000"), || {
        Ok(signal_after(300, Shutdown::Terminate))
    });
    let elapsed = started.elapsed();
    holder.execute_batch("COMMIT").unwrap();
    assert_eq!(code, ExitCode::SUCCESS);
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(3), "{elapsed:?}");
    assert_eq!(db.count("pipeline_heartbeat"), 0);
}

#[test]
fn a_failed_signal_registration_exits_1() {
    let db = TestDb::new();
    let code = run_agent(config(&db, HttpContext::new(), "1000"), || {
        Err::<std::future::Pending<Shutdown>, _>(std::io::Error::other("no signals"))
    });
    assert_eq!(code, ExitCode::FAILURE);
    assert_eq!(db.count("pipeline_heartbeat"), 0);
}
