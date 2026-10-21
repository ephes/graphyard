//! Shared helpers of the agent tests: databases built from the Django
//! schema fixture, fixture row images and a recording fake HTTP server.

#![allow(dead_code)]

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use graphyard_core::settings::Settings;
use rusqlite::Connection;
use rusqlite::types::Value as SqlValue;
use serde_json::Value;

pub const SCHEMA_SQL: &str = include_str!("../../../../fixtures/schema/schema.sql");
pub const RUNS_JSON: &str = include_str!("../../../../fixtures/runs.json");

/// The settings of an environment holding exactly `vars`.
pub fn settings(vars: &[(&str, &str)]) -> Settings {
    Settings::from_lookup(|name| {
        vars.iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| (*value).to_owned())
    })
    .unwrap()
}

/// A temporary database migrated to the committed Django schema.
pub struct TestDb {
    pub dir: tempfile::TempDir,
    pub path: PathBuf,
}

impl TestDb {
    pub fn new() -> TestDb {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db.sqlite3");
        Connection::open(&path)
            .unwrap()
            .execute_batch(SCHEMA_SQL)
            .unwrap();
        TestDb { dir, path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn raw(&self) -> Connection {
        Connection::open(&self.path).unwrap()
    }

    /// Replace every row of `table` with the fixture rows.
    pub fn seed(&self, table: &str, rows: &[Value]) {
        let connection = self.raw();
        connection
            .execute(&format!("DELETE FROM \"{table}\""), [])
            .unwrap();
        for row in rows {
            let row = row.as_object().unwrap();
            let columns: Vec<String> = row.keys().map(|key| format!("\"{key}\"")).collect();
            let marks: Vec<&str> = row.keys().map(|_| "?").collect();
            connection
                .execute(
                    &format!(
                        "INSERT INTO \"{table}\" ({}) VALUES ({})",
                        columns.join(", "),
                        marks.join(", ")
                    ),
                    rusqlite::params_from_iter(row.values().map(sql_value)),
                )
                .unwrap();
        }
    }

    /// Run SQL on a second connection.
    pub fn execute(&self, sql: &str) {
        self.raw().execute_batch(sql).unwrap();
    }

    /// Every row of `table` ordered by id.
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

    /// One column of the row `name` of `table`.
    pub fn value(&self, table: &str, name: &str, column: &str) -> SqlValue {
        self.raw()
            .query_row(
                &format!("SELECT \"{column}\" FROM \"{table}\" WHERE name = ?"),
                [name],
                |row| row.get(0),
            )
            .unwrap()
    }

    pub fn text(&self, table: &str, name: &str, column: &str) -> String {
        match self.value(table, name, column) {
            SqlValue::Text(text) => text,
            other => panic!("{table}.{column} of {name}: {other:?}"),
        }
    }

    pub fn count(&self, table: &str) -> i64 {
        self.raw()
            .query_row(&format!("SELECT count(*) FROM \"{table}\""), [], |row| {
                row.get(0)
            })
            .unwrap()
    }
}

/// A spec row with an `http_json_metric` config (`created_at`/`updated_at`
/// as Django writes them).
pub fn insert_spec(db: &TestDb, name: &str, spec_type: &str, config: &Value) {
    db.raw()
        .execute(
            "INSERT INTO metric_collection_spec (name, enabled, spec_type, interval_seconds, \
             next_run_time, config, last_run_at, last_status, last_error, created_at, \
             updated_at) VALUES (?, 1, ?, 60, 0, ?, NULL, 'warning', '', \
             '2026-01-02 03:04:05.678901', '2026-01-02 03:04:05.678901')",
            rusqlite::params![name, spec_type, config.to_string()],
        )
        .unwrap();
}

/// A fixture cell as the SQLite value Python stored.
pub fn sql_value(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
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

/// A fake HTTP/1.1 server on a std thread: `GET /value` answers
/// `{"value": 7}`, `POST /api/v2/query` an empty annotated CSV, anything
/// else 204. Every request is recorded as `<METHOD> <target>`.
pub struct FakeServer {
    pub addr: SocketAddr,
    pub requests: Arc<Mutex<Vec<String>>>,
}

impl FakeServer {
    pub fn start() -> FakeServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut request_line = String::new();
                if reader.read_line(&mut request_line).is_err() {
                    continue;
                }
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
                        break;
                    }
                    if let Some((name, value)) = line.split_once(':')
                        && name.eq_ignore_ascii_case("content-length")
                    {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0; length];
                let _ = reader.read_exact(&mut body);
                let mut parts = request_line.split_whitespace();
                let method = parts.next().unwrap_or_default().to_owned();
                let target = parts.next().unwrap_or_default().to_owned();
                recorded.lock().unwrap().push(format!("{method} {target}"));
                let (status, content_type, body) = if target.starts_with("/value") {
                    ("200 OK", "application/json", "{\"value\": 7}")
                } else if target.starts_with("/api/v2/query") {
                    ("200 OK", "text/csv; charset=utf-8", "")
                } else {
                    ("204 No Content", "text/plain", "")
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
                     Connection: close\r\n\r\n{body}",
                    body.len()
                );
            }
        });
        FakeServer { addr, requests }
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}
