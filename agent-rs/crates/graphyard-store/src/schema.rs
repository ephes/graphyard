//! Schema contract verification (PLAN.md 5.1, D-2).
//!
//! The Rust agent runs no DDL. Before the first run it verifies that the
//! Django migrations created every table and column of the six agent tables
//! and that the minimum graphyard migration is recorded; `--check-schema`
//! runs only this verification on a read-only connection ([`check_schema`]).
//!
//! The required columns are every column of the six tables: Django loads
//! whole rows (and converts their datetimes), the store does the same.
//! `agent-rs/fixtures/schema/agent_tables.json`, generated from a migrated
//! database, pins the same contract; the store tests fail when these
//! constants drift from it.

use std::fmt;
use std::path::Path;

use rusqlite::{Connection, OptionalExtension};

use graphyard_core::settings::Settings;

use crate::actor::{Store, open_read_only};
use crate::error::StoreError;

/// The minimum Django migration the agent needs, `(app, name)`: the last
/// graphyard migration that changes an agent table.
pub const MIN_GRAPHYARD_MIGRATION: (&str, &str) = ("graphyard", "0010_condition_stale_allowance");

/// Every agent table with its columns in `PRAGMA table_info` order.
pub const REQUIRED_COLUMNS: [(&str, &[&str]); 6] = [
    (
        "metric_collection_spec",
        &[
            "id",
            "name",
            "enabled",
            "spec_type",
            "interval_seconds",
            "next_run_time",
            "config",
            "last_run_at",
            "last_status",
            "last_error",
            "created_at",
            "updated_at",
        ],
    ),
    (
        "condition_definition",
        &[
            "id",
            "name",
            "enabled",
            "metric_name",
            "host_filter",
            "service_filter",
            "tags_filter",
            "operator",
            "warning_threshold",
            "critical_threshold",
            "window_minutes",
            "breach_minutes",
            "status",
            "last_evaluated",
            "message",
            "last_value",
            "created_at",
            "updated_at",
            "subject_id_filter",
            "subject_type_filter",
            "alert_when_stale",
            "stale_after_seconds",
        ],
    ),
    (
        "pipeline_heartbeat",
        &[
            "id",
            "name",
            "status",
            "last_success",
            "last_error",
            "details",
            "updated_at",
        ],
    ),
    (
        "subject_registry",
        &[
            "id",
            "subject_type",
            "subject_id",
            "display_name",
            "source_system",
            "last_seen_at",
        ],
    ),
    (
        "host_registry",
        &[
            "id",
            "host_id",
            "display_name",
            "grafana_url",
            "enabled",
            "last_seen_at",
        ],
    ),
    (
        "service_registry",
        &[
            "id",
            "service_id",
            "display_name",
            "grafana_url",
            "enabled",
            "last_seen_at",
            "host_id",
        ],
    ),
];

/// Outcome of the schema verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaReport {
    /// Required tables that do not exist.
    pub missing_tables: Vec<String>,
    /// Required `(table, column)` pairs missing from existing tables.
    pub missing_columns: Vec<(String, String)>,
    /// Whether the `django_migrations` table is missing.
    pub missing_migrations_table: bool,
    /// Whether [`MIN_GRAPHYARD_MIGRATION`] is not recorded.
    pub missing_migration: bool,
    /// `PRAGMA journal_mode` (reported; `--check-schema` never changes it).
    pub journal_mode: String,
}

impl SchemaReport {
    /// Whether the database satisfies the contract.
    pub fn is_ok(&self) -> bool {
        self.missing_tables.is_empty() && self.missing_columns.is_empty() && !self.missing_migration
    }

    /// The single error line naming every problem and the remedy, `None`
    /// when the schema is fine.
    pub fn error_message(&self) -> Option<String> {
        (!self.is_ok()).then(|| self.to_string())
    }
}

impl fmt::Display for SchemaReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (app, name) = MIN_GRAPHYARD_MIGRATION;
        if self.is_ok() {
            return write!(
                f,
                "database schema satisfies the agent contract ({app}.{name}, journal_mode={})",
                self.journal_mode
            );
        }
        let mut problems = Vec::new();
        if !self.missing_tables.is_empty() {
            problems.push(format!(
                "missing tables: {}",
                self.missing_tables.join(", ")
            ));
        }
        if !self.missing_columns.is_empty() {
            let columns: Vec<String> = self
                .missing_columns
                .iter()
                .map(|(table, column)| format!("{table}.{column}"))
                .collect();
            problems.push(format!("missing columns: {}", columns.join(", ")));
        }
        if self.missing_migration {
            let detail = if self.missing_migrations_table {
                " (table django_migrations does not exist)"
            } else {
                ""
            };
            problems.push(format!("missing migration: {app}.{name}{detail}"));
        }
        write!(
            f,
            "database schema is older than the agent contract; {}; run `manage.py migrate` \
             before starting the agent",
            problems.join("; ")
        )
    }
}

fn table_columns(connection: &Connection, table: &str) -> Result<Vec<String>, StoreError> {
    let mut statement = connection.prepare("SELECT name FROM pragma_table_info(?)")?;
    let rows = statement.query_map([table], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

/// Verify the contract on `connection` (read-only: no DDL, no pragma writes).
pub(crate) fn verify_schema(connection: &Connection) -> Result<SchemaReport, StoreError> {
    let mut missing_tables = Vec::new();
    let mut missing_columns = Vec::new();
    for (table, required) in REQUIRED_COLUMNS {
        // Every table has at least one column, so no rows means no table.
        let present = table_columns(connection, table)?;
        if present.is_empty() {
            missing_tables.push(table.to_owned());
            continue;
        }
        for column in required {
            if !present.iter().any(|name| name == column) {
                missing_columns.push((table.to_owned(), (*column).to_owned()));
            }
        }
    }
    let missing_migrations_table = table_columns(connection, "django_migrations")?.is_empty();
    let missing_migration = missing_migrations_table || {
        let (app, name) = MIN_GRAPHYARD_MIGRATION;
        connection
            .query_row(
                "SELECT 1 FROM django_migrations WHERE app = ? AND name = ?",
                [app, name],
                |_| Ok(()),
            )
            .optional()?
            .is_none()
    };
    let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    Ok(SchemaReport {
        missing_tables,
        missing_columns,
        missing_migrations_table,
        missing_migration,
        journal_mode,
    })
}

/// `--check-schema`: open `path` read-only (never creating it) and verify
/// the contract.
///
/// # Errors
/// The database cannot be opened or queried.
pub fn check_schema(path: &Path, settings: &Settings) -> Result<SchemaReport, StoreError> {
    let connection = open_read_only(path, settings)?;
    verify_schema(&connection)
}

impl Store {
    /// Verify the contract on the agent's connection (startup, before the
    /// first run; D-2).
    ///
    /// # Errors
    /// The database cannot be queried.
    pub async fn verify_schema(&self) -> Result<SchemaReport, StoreError> {
        self.call(|connection| verify_schema(connection)).await
    }
}
