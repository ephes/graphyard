//! Store operations against `fixtures/store_ops.json` (built by
//! `fixtures/gen_store.py` from the Django ORM calls; PLAN.md 5.3, 5.4,
//! 5.5): the same starting rows, the same operation, identical row images
//! afterwards and the same loaded values or exception. The registry
//! statement sequences are checked by the unit tests of `src/registry`.

mod common;

use common::{
    REGISTRY_TABLES, STORE_OPS_JSON, TestDb, assert_rows, load, metric_point, now, python_class,
};
use graphyard_core::{ConditionEvaluation, StatusLevel};
use graphyard_store::{
    ConditionRow, DbDateTime, HeartbeatRecord, SpecRow, SpecRunUpdate, StoreError,
};
use serde_json::{Value, json};

fn text(value: Option<DbDateTime>) -> Value {
    value.map_or(Value::Null, |value| json!(value.to_db_text().unwrap()))
}

fn float(value: Option<f64>) -> Value {
    value.map_or(Value::Null, |value| json!(value))
}

fn spec_values(spec: &SpecRow) -> Value {
    json!({
        "id": spec.id,
        "name": spec.name,
        "enabled": spec.enabled,
        "spec_type": spec.spec_type,
        "interval_seconds": spec.interval_seconds,
        "next_run_time": spec.next_run_time,
        "last_run_at": text(spec.last_run_at),
        "last_status": spec.last_status,
        "last_error": spec.last_error,
        "created_at": text(spec.created_at),
        "updated_at": text(spec.updated_at),
        "config": spec.config,
    })
}

/// The definition a runner loaded before claiming (`op.definition`).
fn captured_spec(id: i64, definition: &Value) -> SpecRow {
    SpecRow {
        id,
        name: definition["name"].as_str().unwrap().to_owned(),
        enabled: true,
        spec_type: definition["spec_type"].as_str().unwrap().to_owned(),
        interval_seconds: definition["interval_seconds"].as_i64().unwrap(),
        next_run_time: 0,
        config: definition["config"].as_str().unwrap().to_owned(),
        last_run_at: None,
        last_status: String::new(),
        last_error: String::new(),
        created_at: None,
        updated_at: None,
    }
}

#[tokio::test]
async fn metric_spec_load_preserves_projection_and_reports_first_bad_row() {
    let db = TestDb::new();
    let config = json!({"synthetic": "x".repeat(4096)}).to_string();
    let mut expected = Vec::new();
    let mut due = Vec::new();
    let mut connection = db.raw();
    let tx = connection.transaction().unwrap();
    for id in (1..=512).rev() {
        let name = format!("spec-{id:03}");
        let enabled = id % 3;
        let next_run_time = id % 2;
        tx.execute(
            "INSERT INTO metric_collection_spec
             (id, name, enabled, spec_type, interval_seconds, next_run_time, config,
              last_run_at, last_status, last_error, created_at, updated_at)
             VALUES (?, ?, ?, 'http_json_metric', 60, ?, ?, NULL, 'warning', 'synthetic',
                     '2026-01-01 00:00:00', '2026-01-02 00:00:00')",
            rusqlite::params![id, name, enabled, next_run_time, config],
        )
        .unwrap();
        if enabled != 0 {
            let row = json!({
                "id": id, "name": name, "enabled": enabled == 1, "spec_type": "http_json_metric",
                "interval_seconds": 60, "next_run_time": next_run_time, "config": config,
                "last_run_at": null, "last_status": "warning", "last_error": "synthetic",
                "created_at": "2026-01-01 00:00:00", "updated_at": "2026-01-02 00:00:00",
            });
            if next_run_time == 0 {
                due.push(row.clone());
            }
            expected.push(row);
        }
    }
    tx.commit().unwrap();
    let store = db.store();
    for (due_before, expected) in [(None, expected), (Some(0), due)] {
        let actual = store.load_specs(due_before).await.unwrap();
        assert!(
            actual
                .iter()
                .map(spec_values)
                .eq(expected.into_iter().rev())
        );
    }

    // Both malformed rows are enabled and due. The first has a bad datetime,
    // the second a bad SQLite storage type for last_error.
    connection
        .execute_batch(
            "UPDATE metric_collection_spec SET updated_at = '2026-02-30 00:00:00' WHERE id = 502;
             UPDATE metric_collection_spec SET last_error = x'FF' WHERE id = 506;",
        )
        .unwrap();
    for due_before in [None, Some(0)] {
        assert!(matches!(
            store.load_specs(due_before).await,
            Err(StoreError::DateTime(_))
        ));
    }
    store.close();
}

fn condition_values(condition: &ConditionRow) -> Value {
    json!({
        "id": condition.id,
        "name": condition.name,
        "enabled": condition.enabled,
        "metric_name": condition.metric_name,
        "host_filter": condition.host_filter,
        "service_filter": condition.service_filter,
        "subject_type_filter": condition.subject_type_filter,
        "subject_id_filter": condition.subject_id_filter,
        "operator": condition.operator,
        "warning_threshold": float(condition.warning_threshold),
        "critical_threshold": float(condition.critical_threshold),
        "window_minutes": condition.window_minutes,
        "breach_minutes": condition.breach_minutes,
        "stale_after_seconds": condition.stale_after_seconds,
        "alert_when_stale": condition.alert_when_stale,
        "status": condition.status,
        "message": condition.message,
        "last_value": float(condition.last_value),
        "last_evaluated": text(condition.last_evaluated),
        "created_at": text(condition.created_at),
        "updated_at": text(condition.updated_at),
        "tags_filter": condition.tags_filter,
    })
}

/// Compare loaded values; numbers by value (Python writes `3.0` for a
/// float, serde `3.0` too, but `0.8` may print differently).
fn assert_loaded(actual: &[Value], expected: &Value, context: &str) {
    let expected = expected.as_array().unwrap();
    assert_eq!(actual.len(), expected.len(), "{context}: loaded rows");
    for (row, expected_row) in actual.iter().zip(expected) {
        let expected_row = expected_row.as_object().unwrap();
        let row = row.as_object().unwrap();
        assert_eq!(row.len(), expected_row.len(), "{context}: loaded fields");
        for (key, expected_value) in expected_row {
            let value = &row[key];
            let same = match (value, expected_value) {
                (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
                (a, b) => a == b,
            };
            assert!(
                same,
                "{context}: {key}: rust {value}, python {expected_value}"
            );
        }
    }
}

fn status(op: &Value, key: &str) -> StatusLevel {
    StatusLevel::parse(op[key].as_str().unwrap()).unwrap()
}

async fn run_case(case: &Value) {
    let name = case["name"].as_str().unwrap();
    let table = case["table"].as_str().unwrap();
    let op = &case["op"];
    let db = TestDb::new();
    db.seed(table, case["before"].as_array().unwrap());
    let store = db.store();
    let at = now(op["now"].as_str().unwrap());
    let definition = if table == "condition_definition"
        && op["kind"]
            .as_str()
            .unwrap()
            .starts_with("update_condition_")
    {
        Some(
            store
                .load_conditions(op["id"].as_i64())
                .await
                .unwrap()
                .remove(0),
        )
    } else {
        None
    };
    if op.get("delete_first").and_then(Value::as_bool) == Some(true) {
        db.raw()
            .execute(
                &format!("DELETE FROM \"{table}\" WHERE id = ?"),
                [op["id"].as_i64().unwrap()],
            )
            .unwrap();
    }
    let result: Result<Option<Vec<Value>>, StoreError> = match op["kind"].as_str().unwrap() {
        "load_specs" => store
            .load_specs(op["due_before"].as_i64())
            .await
            .map(|specs| Some(specs.iter().map(spec_values).collect())),
        "update_spec" => store
            .update_spec_run(SpecRunUpdate {
                id: op["id"].as_i64().unwrap(),
                last_run_at: at,
                last_status: status(op, "last_status"),
                last_error: op["last_error"].as_str().unwrap().to_owned(),
                next_run_time: op["next_run_time"].as_i64().unwrap(),
            })
            .await
            .map(|()| None),
        "claim_spec" => store
            .claim_due_spec(
                captured_spec(op["id"].as_i64().unwrap(), &op["definition"]).claim_identity(),
                op["expected_due"].as_i64().unwrap(),
                op["now_ts"].as_i64().unwrap(),
                op["claim_until"].as_i64().unwrap(),
            )
            .await
            .map(|_| None),
        "finish_claimed_spec" => store
            .finish_claimed_spec(
                store
                    .load_specs(None)
                    .await
                    .unwrap()
                    .into_iter()
                    .find(|s| s.id == op["id"].as_i64().unwrap())
                    .unwrap()
                    .claim_identity(),
                SpecRunUpdate {
                    id: op["id"].as_i64().unwrap(),
                    last_run_at: at,
                    last_status: status(op, "last_status"),
                    last_error: op["last_error"].as_str().unwrap().to_owned(),
                    next_run_time: op["next_run_time"].as_i64().unwrap(),
                },
                op["claim_until"].as_i64().unwrap(),
            )
            .await
            .map(|_| None),
        "load_conditions" => store
            .load_conditions(op["condition_id"].as_i64())
            .await
            .map(|conditions| Some(conditions.iter().map(condition_values).collect())),
        "update_condition_success" => store
            .update_condition_success(
                definition.clone().unwrap(),
                ConditionEvaluation {
                    status: status(op, "status"),
                    message: op["message"].as_str().unwrap().to_owned(),
                    last_value: op["last_value"].as_f64(),
                    evaluated_at: at,
                },
            )
            .await
            .map(|_| None),
        "update_condition_failure" => store
            .update_condition_failure(
                definition.unwrap(),
                status(op, "status"),
                op["message"].as_str().unwrap().to_owned(),
                at,
            )
            .await
            .map(|_| None),
        "record_heartbeat" => store
            .record_heartbeat(
                HeartbeatRecord {
                    name: op["name"].as_str().unwrap().to_owned(),
                    status: status(op, "status"),
                    last_error: op["last_error"].as_str().unwrap().to_owned(),
                    details: op["details"]
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(key, value)| (key.clone(), value.as_i64().unwrap()))
                        .collect(),
                    success: op["success"].as_bool().unwrap(),
                },
                at,
            )
            .await
            .map(|()| None),
        other => panic!("unknown operation {other}"),
    };
    match (result, case.get("error")) {
        (Ok(loaded), None) => {
            if let Some(loaded) = loaded {
                assert_loaded(&loaded, &case["loaded"], name);
            } else {
                assert!(case.get("loaded").is_none(), "{name}: expected loaded rows");
            }
        }
        (Err(err), Some(expected)) => {
            assert_eq!(
                python_class(&err),
                expected["class"].as_str().unwrap(),
                "{name}"
            );
            // Every message the store reproduces equals Python's, P1 or P2.
            assert_eq!(
                err.to_string(),
                expected["message"].as_str().unwrap(),
                "{name}"
            );
        }
        (Ok(_), Some(expected)) => panic!("{name}: expected {expected}"),
        (Err(err), None) => panic!("{name}: unexpected {err:?}"),
    }
    assert_rows(&db.rows(table), case["after"].as_array().unwrap(), name);
    store.close();
}

#[tokio::test]
async fn store_operations_match_the_django_orm() {
    let fixture = load(STORE_OPS_JSON);
    let cases = fixture["cases"].as_array().unwrap();
    assert!(cases.len() >= 30);
    for case in cases {
        run_case(case).await;
    }
}

/// One `registry_cases` entry through [`graphyard_store::Store::touch_registry`].
async fn run_registry_case(case: &Value) {
    let name = case["name"].as_str().unwrap();
    let op = &case["op"];
    let db = TestDb::new();
    db.seed_registry(case);
    let store = db.store();
    let points = op["points"]
        .as_array()
        .unwrap()
        .iter()
        .map(metric_point)
        .collect();
    let result = store
        .touch_registry(points, now(op["now"].as_str().unwrap()))
        .await;
    match (result, case.get("error")) {
        (Ok(()), None) => {}
        (Err(err), Some(expected)) => {
            assert_eq!(
                python_class(&err),
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
    for table in REGISTRY_TABLES {
        assert_rows(
            &db.rows(table),
            case["after"][table].as_array().unwrap(),
            &format!("{name}: {table}"),
        );
    }
    store.close();
}

#[tokio::test]
async fn registry_touch_matches_the_django_orm() {
    let fixture = load(STORE_OPS_JSON);
    let cases = fixture["registry_cases"].as_array().unwrap();
    assert!(cases.len() >= 30);
    for case in cases {
        // A concurrent insert between the check and the transaction needs
        // the split phases: `src/registry/tests.rs` runs those cases.
        if case.get("insert_after_check").is_some() {
            continue;
        }
        run_registry_case(case).await;
    }
}

#[tokio::test]
async fn independent_stores_race_for_one_due_slot() {
    let fixture = load(STORE_OPS_JSON);
    let row = fixture["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == "claim due slot")
        .unwrap();
    let db = TestDb::new();
    db.seed("metric_collection_spec", row["before"].as_array().unwrap());
    let first = db.store();
    let second = db.store();
    let spec = captured_spec(1, &row["op"]["definition"]).claim_identity();
    let (a, b) = tokio::join!(
        first.claim_due_spec(spec.clone(), 100, 100, 400),
        second.claim_due_spec(spec.clone(), 100, 100, 400),
    );
    assert_ne!(a.unwrap(), b.unwrap());
    assert!(
        !first
            .claim_due_spec(spec.clone(), 400, 399, 699)
            .await
            .unwrap()
    );
    assert!(second.claim_due_spec(spec, 400, 400, 700).await.unwrap());
    first.close();
    second.close();
}

fn enabled_condition_fixture() -> Value {
    let fixtures = load(STORE_OPS_JSON);
    fixtures["cases"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|case| case["table"] == "condition_definition")
        .flat_map(|case| case["before"].as_array().unwrap())
        .find(|row| row["enabled"] == 1)
        .unwrap()
        .clone()
}

#[tokio::test]
async fn condition_definition_edits_fence_successes_and_failures() {
    let row = enabled_condition_fixture();
    for edit in [
        "warning_threshold = 12345",
        "tags_filter = '{\"synthetic\": \"changed\"}'",
        "enabled = 0",
    ] {
        for failure in [false, true] {
            let db = TestDb::new();
            db.seed("condition_definition", std::slice::from_ref(&row));
            let store = db.store();
            let definition = store.load_conditions(None).await.unwrap().remove(0);
            db.raw()
                .execute(&format!("UPDATE condition_definition SET {edit}"), [])
                .unwrap();
            let at = now("2026-10-21 08:15:30.123456");
            if failure {
                store
                    .update_condition_failure(
                        definition,
                        StatusLevel::Critical,
                        "synthetic failure".into(),
                        at,
                    )
                    .await
                    .unwrap();
            } else {
                store
                    .update_condition_success(
                        definition,
                        ConditionEvaluation {
                            status: StatusLevel::Critical,
                            message: "synthetic result".into(),
                            last_value: Some(999.),
                            evaluated_at: at,
                        },
                    )
                    .await
                    .unwrap();
            }
            let current: (String, String, Option<f64>, Option<String>) = db
                .raw()
                .query_row(
                    "SELECT status, message, last_value, last_evaluated FROM condition_definition",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
                )
                .unwrap();
            assert_eq!(current.0, row["status"].as_str().unwrap(), "{edit}");
            assert_eq!(current.1, row["message"].as_str().unwrap());
            assert_eq!(current.2, row["last_value"].as_f64());
            assert_eq!(current.3.as_deref(), row["last_evaluated"].as_str());
        }
    }
}

#[tokio::test]
async fn condition_watermark_orders_legacy_text_by_utc_at_microsecond_precision() {
    let at = now("2026-10-21 08:15:30.123456");
    let microsecond = chrono::TimeDelta::microseconds(1);
    for legacy in [
        "2026-10-21 07:15:30.123456-01:00",
        "2026-10-21T08:15:30.123456Z",
    ] {
        for failure in [false, true] {
            let db = TestDb::new();
            let mut row = enabled_condition_fixture();
            row["last_evaluated"] = Value::Null;
            row["last_value"] = json!(42.0);
            row["status"] = json!("ok");
            row["message"] = json!("current");
            db.seed("condition_definition", &[row]);
            let store = db.store();
            let definition = store.load_conditions(None).await.unwrap().remove(0);
            let connection = db.raw();
            // A different writer publishes after this evaluator loads the definition.
            connection
                .execute(
                    "UPDATE condition_definition SET last_evaluated = ?",
                    [legacy],
                )
                .unwrap();
            let current = || {
                connection.query_row(
                    "SELECT status, message, last_value, last_evaluated FROM condition_definition",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                ).unwrap()
            };

            for (attempted_at, accepted) in [
                (at - microsecond, false),
                (at, false),
                (at + microsecond, true),
            ] {
                let published = if failure {
                    store
                        .update_condition_failure(
                            definition.clone(),
                            StatusLevel::Critical,
                            "attempt".into(),
                            attempted_at,
                        )
                        .await
                        .unwrap()
                } else {
                    store
                        .update_condition_success(
                            definition.clone(),
                            ConditionEvaluation {
                                status: StatusLevel::Critical,
                                message: "attempt".into(),
                                last_value: Some(99.),
                                evaluated_at: attempted_at,
                            },
                        )
                        .await
                        .unwrap()
                };
                assert_eq!(published, accepted, "{legacy}, {failure}, {attempted_at}");
                let expected = if accepted {
                    (
                        "critical".to_owned(),
                        "attempt".to_owned(),
                        if failure { 42. } else { 99. },
                        "2026-10-21 08:15:30.123457".to_owned(),
                    )
                } else {
                    (
                        "ok".to_owned(),
                        "current".to_owned(),
                        42.,
                        legacy.to_owned(),
                    )
                };
                assert_eq!(current(), expected);
            }
            store.close();
        }
    }
}
