//! Test databases built from the Django schema fixture and helpers for the
//! Python-generated fixtures (PLAN.md 5.1, 9.1).

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDateTime, Utc};
use graphyard_core::settings::Settings;
use graphyard_core::{MetricPoint, Tags};
use graphyard_store::{Store, StoreError};
use rusqlite::Connection;
use rusqlite::types::Value as SqlValue;
use serde_json::Value;

pub const SCHEMA_SQL: &str = include_str!("../../../../fixtures/schema/schema.sql");
pub const AGENT_TABLES_JSON: &str = include_str!("../../../../fixtures/schema/agent_tables.json");
pub const STORE_OPS_JSON: &str = include_str!("../../../../fixtures/store_ops.json");
pub const DATETIME_CODEC_JSON: &str = include_str!("../../../../fixtures/datetime_codec.json");

/// The registry tables in seeding order (PLAN.md 5.5).
pub const REGISTRY_TABLES: [&str; 3] = ["subject_registry", "host_registry", "service_registry"];

/// The settings of an empty environment (the defaults of `base.py`).
pub fn default_settings() -> Settings {
    settings(&[])
}

/// The settings of an environment holding exactly `vars`.
pub fn settings(vars: &[(&str, &str)]) -> Settings {
    let vars: std::collections::HashMap<String, String> = vars
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect();
    Settings::from_lookup(|name| vars.get(name).cloned()).unwrap()
}

/// Parse a fixture file.
pub fn load(text: &str) -> Value {
    serde_json::from_str(text).expect("fixture file is valid JSON")
}

/// A temporary database migrated to the committed Django schema.
pub struct TestDb {
    _dir: tempfile::TempDir,
    pub path: PathBuf,
}

impl TestDb {
    pub fn new() -> TestDb {
        TestDb::with_schema(SCHEMA_SQL)
    }

    pub fn with_schema(schema: &str) -> TestDb {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(schema).unwrap();
        TestDb { _dir: dir, path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The agent's store with test settings (journal mode as Python's test
    /// settings: the default WAL).
    pub fn store(&self) -> Store {
        Store::open(&self.path, &default_settings()).unwrap()
    }

    /// A second connection, as the web app or a test fixture would open it.
    pub fn raw(&self) -> Connection {
        let connection = Connection::open(&self.path).unwrap();
        connection
            .execute_batch("PRAGMA foreign_keys = ON")
            .unwrap();
        connection
    }

    /// Replace every row of `table` with the fixture rows.
    pub fn seed(&self, table: &str, rows: &[Value]) {
        let connection = self.raw();
        connection
            .execute(&format!("DELETE FROM \"{table}\""), [])
            .unwrap();
        for row in rows {
            insert_row(&connection, table, row);
        }
    }

    /// Seed the three registry tables of a `registry_cases` entry and run
    /// its `setup_sql` (triggers).
    pub fn seed_registry(&self, case: &Value) {
        for table in REGISTRY_TABLES {
            self.seed(table, case["before"][table].as_array().unwrap());
        }
        let connection = self.raw();
        for statement in case
            .get("setup_sql")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            connection
                .execute_batch(statement.as_str().unwrap())
                .unwrap();
        }
    }

    /// Insert the `insert_after_check` rows of a registry case (a concurrent
    /// writer between the autocommit check and the transaction).
    pub fn insert_after_check(&self, case: &Value) {
        let Some(tables) = case.get("insert_after_check").and_then(Value::as_object) else {
            return;
        };
        let connection = self.raw();
        for (table, rows) in tables {
            for row in rows.as_array().unwrap() {
                insert_row(&connection, table, row);
            }
        }
    }

    /// Every row of `table` ordered by id: `(column, value)` pairs.
    pub fn rows(&self, table: &str) -> Vec<Vec<(String, SqlValue)>> {
        let connection = self.raw();
        let mut statement = connection
            .prepare(&format!("SELECT * FROM \"{table}\" ORDER BY id"))
            .unwrap();
        let columns: Vec<String> = statement
            .column_names()
            .into_iter()
            .map(str::to_owned)
            .collect();
        statement
            .query_map([], |row| {
                Ok(columns
                    .iter()
                    .enumerate()
                    .map(|(index, column)| (column.clone(), row.get::<_, SqlValue>(index).unwrap()))
                    .collect())
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }
}

/// Insert one fixture row into `table`.
pub fn insert_row(connection: &Connection, table: &str, row: &Value) {
    let row = row.as_object().expect("fixture row is an object");
    let columns: Vec<String> = row.keys().map(|key| format!("\"{key}\"")).collect();
    let marks: Vec<&str> = row.keys().map(|_| "?").collect();
    let values: Vec<SqlValue> = row.values().map(sql_value).collect();
    connection
        .execute(
            &format!(
                "INSERT INTO \"{table}\" ({}) VALUES ({})",
                columns.join(", "),
                marks.join(", ")
            ),
            rusqlite::params_from_iter(values),
        )
        .unwrap();
}

/// A fixture `MetricPoint` (every dataclass field, `ts` in ISO form).
pub fn metric_point(value: &Value) -> MetricPoint {
    let text = |key: &str| value[key].as_str().unwrap().to_owned();
    let optional = |key: &str| value[key].as_str().map(str::to_owned);
    MetricPoint {
        ts: DateTime::parse_from_rfc3339(value["ts"].as_str().unwrap())
            .unwrap()
            .with_timezone(&Utc),
        metric: text("metric"),
        value: value["value"].as_f64().unwrap(),
        subject_type: text("subject_type"),
        subject_id: text("subject_id"),
        source_system: text("source_system"),
        collector_service: text("collector_service"),
        collector_host: text("collector_host"),
        source_instance: text("source_instance"),
        source_entity_id: optional("source_entity_id"),
        host: optional("host"),
        service: optional("service"),
        tags: value["tags"].as_object().map(|tags| {
            tags.iter()
                .map(|(key, value)| (key.clone(), value.as_str().unwrap().to_owned()))
                .collect::<Tags>()
        }),
    }
}

/// The Python exception class of a store error, where the store reproduces
/// one.
pub fn python_class(err: &StoreError) -> &'static str {
    match err {
        StoreError::DateTime(err) => err.class,
        StoreError::NotUpdated => "NotUpdated",
        StoreError::Point(err) => err.python_class(),
        StoreError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
            if failure.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            "IntegrityError"
        }
        other => panic!("unexpected store error {other:?}"),
    }
}

/// A fixture value as the SQLite value Python's `sqlite3` stored: integers
/// (no fraction or exponent in the JSON text) are INTEGER, other numbers
/// REAL, `{"blob_hex": ...}` a BLOB.
pub fn sql_value(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Object(blob) => {
            let hex = blob["blob_hex"].as_str().expect("blob_hex");
            SqlValue::Blob(
                (0..hex.len())
                    .step_by(2)
                    .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).unwrap())
                    .collect(),
            )
        }
        Value::String(text) => SqlValue::Text(text.clone()),
        Value::Number(number) => {
            let text = number.to_string();
            if text.contains(['.', 'e', 'E']) {
                SqlValue::Real(text.parse().unwrap())
            } else {
                SqlValue::Integer(text.parse().unwrap())
            }
        }
        other => panic!("unexpected fixture cell {other}"),
    }
}

/// Assert that the table rows equal the fixture's row images.
pub fn assert_rows(actual: &[Vec<(String, SqlValue)>], expected: &[Value], context: &str) {
    assert_eq!(actual.len(), expected.len(), "{context}: row count");
    for (row, expected_row) in actual.iter().zip(expected) {
        let expected_row = expected_row.as_object().unwrap();
        let columns: Vec<&str> = row.iter().map(|(column, _)| column.as_str()).collect();
        let expected_columns: Vec<&str> = expected_row.keys().map(String::as_str).collect();
        assert_eq!(columns, expected_columns, "{context}: columns");
        for (column, value) in row {
            assert_eq!(
                value,
                &sql_value(&expected_row[column]),
                "{context}: column {column}"
            );
        }
    }
}

/// A fixture `now` (`YYYY-MM-DD HH:MM:SS[.ffffff]`, UTC).
pub fn now(text: &str) -> DateTime<Utc> {
    let format = if text.contains('.') {
        "%Y-%m-%d %H:%M:%S%.f"
    } else {
        "%Y-%m-%d %H:%M:%S"
    };
    NaiveDateTime::parse_from_str(text, format)
        .unwrap()
        .and_utc()
}
