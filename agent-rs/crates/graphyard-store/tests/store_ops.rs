//! Store operations against `fixtures/store_ops.json` (built by
//! `fixtures/gen_store.py` from the Django ORM calls; PLAN.md 5.3, 5.4):
//! the same starting rows, the same operation, identical row images
//! afterwards and the same loaded values or exception.

mod common;

use common::{STORE_OPS_JSON, TestDb, assert_rows, load, now};
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

fn python_class(err: &StoreError) -> &'static str {
    match err {
        StoreError::DateTime(err) => err.class,
        StoreError::NotUpdated => "NotUpdated",
        other => panic!("unexpected store error {other:?}"),
    }
}

async fn run_case(case: &Value) {
    let name = case["name"].as_str().unwrap();
    let table = case["table"].as_str().unwrap();
    let op = &case["op"];
    let db = TestDb::new();
    db.seed(table, case["before"].as_array().unwrap());
    let store = db.store();
    let at = now(op["now"].as_str().unwrap());
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
        "load_conditions" => store
            .load_conditions(op["condition_id"].as_i64())
            .await
            .map(|conditions| Some(conditions.iter().map(condition_values).collect())),
        "update_condition_success" => store
            .update_condition_success(
                op["id"].as_i64().unwrap(),
                ConditionEvaluation {
                    status: status(op, "status"),
                    message: op["message"].as_str().unwrap().to_owned(),
                    last_value: op["last_value"].as_f64(),
                    evaluated_at: at,
                },
            )
            .await
            .map(|()| None),
        "update_condition_failure" => store
            .update_condition_failure(
                op["id"].as_i64().unwrap(),
                status(op, "status"),
                op["message"].as_str().unwrap().to_owned(),
            )
            .await
            .map(|()| None),
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
