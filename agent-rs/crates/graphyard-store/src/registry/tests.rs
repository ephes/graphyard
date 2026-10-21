//! The registry touch at connection level: the statement sequence Django
//! sends for every `registry_cases` entry of `fixtures/store_ops.json`
//! (verb, table, inside the registry transaction or not), the row images,
//! and the cases that need a concurrent writer between the autocommit check
//! and the transaction.

use std::cell::RefCell;

use rusqlite::trace::{TraceEvent, TraceEventCodes};
use serde_json::{Value, json};

#[path = "../../tests/common/mod.rs"]
mod common;

use common::{REGISTRY_TABLES, STORE_OPS_JSON, TestDb, assert_rows, load, metric_point, now};

use super::*;

thread_local! {
    static TRACE: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn record(event: TraceEvent<'_>) {
    if let TraceEvent::Stmt(_, sql) = event {
        TRACE.with(|trace| trace.borrow_mut().push(sql.to_owned()));
    }
}

/// Start recording the statements `connection` runs on this thread.
fn trace(connection: &Connection) {
    TRACE.with(|trace| trace.borrow_mut().clear());
    connection.trace_v2(TraceEventCodes::SQLITE_TRACE_STMT, Some(record));
}

/// The recorded registry statements as the fixture describes them: the
/// verb, the first registry table the SQL names, and whether it ran inside
/// the transaction. Trigger programs (`-- TRIGGER ...`) and statements
/// naming no registry table (BEGIN, SAVEPOINT, ...) are not listed; BEGIN
/// and COMMIT/ROLLBACK delimit the transaction.
fn recorded_statements() -> Vec<Value> {
    let sqls = TRACE.with(|trace| trace.borrow().clone());
    let mut in_transaction = false;
    let mut statements = Vec::new();
    for sql in sqls {
        let sql = sql.trim_start();
        if sql.starts_with("--") {
            continue;
        }
        let verb = sql
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_uppercase();
        match verb.as_str() {
            "BEGIN" => in_transaction = true,
            "COMMIT" | "END" => in_transaction = false,
            "ROLLBACK" if !sql.to_uppercase().contains(" TO ") => in_transaction = false,
            _ => {}
        }
        let table = REGISTRY_TABLES
            .iter()
            .filter_map(|table| sql.find(table).map(|at| (at, *table)))
            .min();
        if let Some((_, table)) = table {
            statements.push(json!({
                "verb": verb,
                "table": table,
                "in_transaction": in_transaction,
            }));
        }
    }
    statements
}

fn open(db: &TestDb) -> Connection {
    let connection = Connection::open(db.path()).unwrap();
    connection
        .execute_batch("PRAGMA foreign_keys = ON; PRAGMA legacy_alter_table = OFF")
        .unwrap();
    connection
}

fn points(case: &Value) -> Vec<MetricPoint> {
    case["op"]["points"]
        .as_array()
        .unwrap()
        .iter()
        .map(metric_point)
        .collect()
}

fn case_now(case: &Value) -> DbDateTime {
    DbDateTime::from_utc(now(case["op"]["now"].as_str().unwrap()))
}

/// The touch in its two phases with the case's concurrent insert between
/// them.
fn run(db: &TestDb, connection: &mut Connection, case: &Value) -> Result<(), StoreError> {
    let batch = RegistryBatch::collect(&points(case)).map_err(StoreError::Point)?;
    if batch.is_empty() {
        return Ok(());
    }
    let now = case_now(case);
    let needed = registry_needs_write(connection, &batch, now)?;
    db.insert_after_check(case);
    if needed {
        write_registry(connection, &batch, now)?;
    }
    Ok(())
}

#[test]
fn every_registry_case_sends_the_django_statements() {
    let fixture = load(STORE_OPS_JSON);
    let cases = fixture["registry_cases"].as_array().unwrap();
    assert!(cases.len() >= 30);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let db = TestDb::new();
        db.seed_registry(case);
        let mut connection = open(&db);
        trace(&connection);
        let result = run(&db, &mut connection, case);
        let statements = recorded_statements();
        assert_eq!(
            Value::Array(statements),
            case["statements"],
            "{name}: statements"
        );
        match (result, case.get("error")) {
            (Ok(()), None) => {}
            (Err(err), Some(expected)) => {
                assert_eq!(
                    common::python_class(&err),
                    expected["class"].as_str().unwrap(),
                    "{name}"
                );
                assert_eq!(
                    err.to_string(),
                    expected["message"].as_str().unwrap(),
                    "{name}"
                );
            }
            (Ok(()), Some(expected)) => panic!("{name}: expected {expected}"),
            (Err(err), None) => panic!("{name}: unexpected {err:?}"),
        }
        assert!(connection.is_autocommit(), "{name}: transaction left open");
        for table in REGISTRY_TABLES {
            assert_rows(
                &db.rows(table),
                case["after"][table].as_array().unwrap(),
                &format!("{name}: {table}"),
            );
        }
    }
}

#[test]
fn steady_state_takes_no_write_lock() {
    // Another connection holds the write lock: the touch of rows that are
    // all present and recent still completes (reads only, no BEGIN).
    let fixture = load(STORE_OPS_JSON);
    let case = fixture["registry_cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "steady state reads three tables and writes nothing")
        .unwrap();
    let db = TestDb::new();
    db.seed_registry(case);
    let holder = db.raw();
    holder.execute_batch("BEGIN IMMEDIATE").unwrap();
    let mut connection = open(&db);
    connection.busy_timeout(std::time::Duration::ZERO).unwrap();
    let batch = RegistryBatch::collect(&points(case)).unwrap();
    touch_registry(&mut connection, &batch, case_now(case)).unwrap();
    holder.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn the_batch_keeps_first_seen_order_and_the_last_source_system() {
    let at = now("2026-10-21 08:15:30");
    let point = |subject_type: &str, subject_id: &str, source: &str, service: Option<&str>| {
        let mut point = MetricPoint::new(
            at,
            "m",
            1.0,
            subject_type,
            subject_id,
            source,
            "vector",
            "collector",
        );
        point.service = service.map(str::to_owned);
        point
    };
    let batch = RegistryBatch::collect(&[
        point("host", "b", "one", Some("mail")),
        point("service", "a", "two", Some("mail")),
        point("host", "a", "three", None),
        point("host", "b", "four", Some("web")),
    ])
    .unwrap();
    let subjects: Vec<(&str, &str, Option<&str>)> = batch
        .subjects
        .iter()
        .map(|((kind, id), source)| (kind.as_str(), id.as_str(), source.as_deref()))
        .collect();
    assert_eq!(
        subjects,
        [
            ("host", "b", Some("four")),
            ("service", "a", Some("two")),
            ("host", "a", Some("three")),
        ]
    );
    let hosts: Vec<(&str, usize)> = batch
        .host_ids
        .iter()
        .map(|(id, index)| (id.as_str(), *index))
        .collect();
    assert_eq!(hosts, [("b", 0), ("a", 2)]);
    let mail: Vec<(&str, usize)> = batch.service_hosts["mail"]
        .iter()
        .map(|(host, index)| (host.as_str(), *index))
        .collect();
    assert_eq!(mail, [("b", 0), ("collector", 1)]);
    assert_eq!(
        batch.service_hosts.keys().collect::<Vec<_>>(),
        ["mail", "web"]
    );
}

#[test]
fn staleness_is_sixty_seconds_or_null() {
    let at = now("2026-10-21 08:15:30.5").naive_utc();
    let seen = |text: &str| Some(DbDateTime::from_utc(now(text)));
    assert!(is_stale(None, at));
    assert!(is_stale(seen("2026-10-21 08:14:30.5"), at));
    assert!(!is_stale(seen("2026-10-21 08:14:30.500001"), at));
    assert!(!is_stale(seen("2026-10-21 09:00:00"), at));
    assert!(is_stale(seen("0001-01-01 00:00:00"), at));
}
