//! Schema contract verification (PLAN.md 5.1, D-2).
//!
//! The Rust agent runs no DDL. Before the first run it verifies that the
//! Django migrations created every table/column and its required storage and
//! identity keys, and that the minimum graphyard migration is recorded. All
//! introspection shares one deferred read snapshot; `--check-schema`
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
/// graphyard migration that changes an agent table or has an unattributed
/// operation (0015 changes condition field validators).
pub const MIN_GRAPHYARD_MIGRATION: (&str, &str) = (
    "graphyard",
    "0015_alter_conditiondefinition_breach_minutes_and_more",
);

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

/// Storage properties generated from the migrated Django fixture.
pub struct ColumnContract {
    pub name: &'static str,
    pub affinity: &'static str,
    pub not_null: bool,
    pub primary_key: i64,
}

/// Required storage and unconditional unique identity keys of an agent table.
pub struct TableContract {
    pub table: &'static str,
    pub columns: &'static [ColumnContract],
    pub unique_keys: &'static [&'static [&'static str]],
}

include!("../../../fixtures/schema/storage_contract.rs");

/// Outcome of the schema verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaReport {
    /// Required tables that do not exist.
    pub missing_tables: Vec<String>,
    /// Required `(table, column)` pairs missing from existing tables.
    pub missing_columns: Vec<(String, String)>,
    /// Storage/key properties that differ from the migrated contract.
    pub incompatible_storage: Vec<String>,
    /// Whether the `django_migrations` table is missing.
    pub missing_migrations_table: bool,
    /// Whether the marker for [`MIN_GRAPHYARD_MIGRATION`] is absent or
    /// cannot be verified because the migration table is missing or incompatible.
    pub missing_migration: bool,
    /// `PRAGMA journal_mode` (reported; `--check-schema` never changes it).
    pub journal_mode: String,
}

impl SchemaReport {
    /// Whether the database satisfies the contract.
    pub fn is_ok(&self) -> bool {
        self.missing_tables.is_empty()
            && self.missing_columns.is_empty()
            && self.incompatible_storage.is_empty()
            && !self.missing_migration
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
        if !self.incompatible_storage.is_empty() {
            problems.push(format!(
                "incompatible storage: {}",
                self.incompatible_storage.join(", ")
            ));
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
            "database schema does not satisfy the agent contract; {}; stop the agent and run `manage.py migrate` \
             or restore the migrated schema before starting the agent",
            problems.join("; ")
        )
    }
}

fn table_kind(connection: &Connection, table: &str) -> Result<Option<String>, StoreError> {
    Ok(connection
        .query_row(
            "SELECT type FROM pragma_table_list(?) WHERE schema = 'main'",
            [table],
            |row| row.get(0),
        )
        .optional()?)
}

fn table_columns(connection: &Connection, table: &str) -> Result<Vec<String>, StoreError> {
    let mut statement = connection.prepare("SELECT name FROM pragma_table_info(?, 'main')")?;
    let rows = statement.query_map([table], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn affinity(declared: &str) -> &'static str {
    let declared = declared.to_ascii_uppercase();
    if declared.contains("INT") {
        "INTEGER"
    } else if ["CHAR", "CLOB", "TEXT"]
        .iter()
        .any(|part| declared.contains(part))
    {
        "TEXT"
    } else if declared.is_empty() || declared.contains("BLOB") {
        "BLOB"
    } else if ["REAL", "FLOA", "DOUB"]
        .iter()
        .any(|part| declared.contains(part))
    {
        "REAL"
    } else {
        "NUMERIC"
    }
}

fn verify_storage(
    connection: &Connection,
    contract: &TableContract,
    problems: &mut Vec<String>,
) -> Result<(), StoreError> {
    let mut statement = connection
        .prepare("SELECT name, type, \"notnull\", pk FROM pragma_table_info(?, 'main')")?;
    let columns = statement
        .query_map([contract.table], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut statement = connection
        .prepare("SELECT name, \"unique\", origin, partial FROM pragma_index_list(?, 'main')")?;
    let indexes = statement
        .query_map([contract.table], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, bool>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for required in contract.columns {
        let Some((_, declared, not_null, pk)) =
            columns.iter().find(|(name, ..)| name == required.name)
        else {
            continue; // Already reported as a missing column.
        };
        // The store inserts rows without an id and needs SQLite's rowid alias,
        // not just a unique BIGINT key (or INTEGER PRIMARY KEY DESC index).
        let rowid_key = required.primary_key == 0
            || (declared.eq_ignore_ascii_case("INTEGER")
                && !indexes.iter().any(|(_, _, origin, _)| origin == "pk")
                && columns.iter().filter(|(_, _, _, pk)| *pk > 0).count() == 1);
        if affinity(declared) != required.affinity
            || *not_null != required.not_null
            || *pk != required.primary_key
            || !rowid_key
        {
            problems.push(format!(
                "{}.{} (affinity/nullability/primary key)",
                contract.table, required.name
            ));
        }
    }
    let mut unique_keys = Vec::new();
    for (index, unique, _, partial) in indexes {
        if !unique || partial {
            continue;
        }
        let mut statement = connection.prepare(
            "SELECT name, coll FROM pragma_index_xinfo(?, 'main') WHERE key = 1 ORDER BY seqno",
        )?;
        let columns = statement
            .query_map([index], |row| {
                Ok((row.get::<_, Option<String>>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        // Expression and nonbinary indexes do not provide the migrated key.
        if columns
            .iter()
            .any(|(name, collation)| name.is_none() || !collation.eq_ignore_ascii_case("BINARY"))
        {
            continue;
        }
        let mut names: Vec<String> = columns.into_iter().filter_map(|(name, _)| name).collect();
        names.sort();
        unique_keys.push(names);
    }
    for key in contract.unique_keys {
        if !unique_keys.iter().any(|present| present == key) {
            problems.push(format!("{} unique ({})", contract.table, key.join(", ")));
        }
    }
    Ok(())
}

fn verify_registry_extensions(
    connection: &Connection,
    contract: &TableContract,
    problems: &mut Vec<String>,
) -> Result<(), StoreError> {
    // Fixed-column inserts omit extensions. Generated and nullable columns
    // can be omitted without supplying a default.
    let mut statement = connection.prepare(
        r#"SELECT name, dflt_value FROM pragma_table_xinfo(?, 'main')
            WHERE hidden = 0 AND "notnull" = 1 ORDER BY cid"#,
    )?;
    let columns = statement.query_map([contract.table], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
    })?;
    for column in columns {
        let (name, default) = column?;
        if contract
            .columns
            .iter()
            .any(|required| required.name == name)
        {
            continue;
        }
        // The catalog stores default SQL. Detect a literal NULL without
        // evaluating SQL from a restored database.
        if default.as_deref().is_none_or(|sql| {
            sql.trim_matches(|ch: char| ch.is_ascii_whitespace() || matches!(ch, '(' | ')'))
                .eq_ignore_ascii_case("NULL")
        }) {
            problems.push(format!(
                "{}.{} (required column has no non-NULL default)",
                contract.table,
                name.escape_debug()
            ));
        }
    }
    Ok(())
}

fn verify_service_host_foreign_key(
    connection: &Connection,
    problems: &mut Vec<String>,
) -> Result<(), StoreError> {
    // A composite constraint is not a reference on host_id alone. A NULL
    // parent column denotes the primary key, verified separately as id.
    let present: bool = connection.query_row(
        r#"SELECT EXISTS (
            SELECT 1 FROM pragma_foreign_key_list('service_registry', 'main')
            GROUP BY id
            HAVING count(*) = 1
                AND max("from" = 'host_id' COLLATE NOCASE
                    AND "table" = 'host_registry' COLLATE NOCASE
                    AND ("to" IS NULL OR "to" = 'id' COLLATE NOCASE))
        )"#,
        [],
        |row| row.get(0),
    )?;
    if !present {
        problems.push("service_registry.host_id (expected foreign key to host_registry.id)".into());
    }
    Ok(())
}

/// Verify the contract on `connection` (read-only: no DDL, no pragma writes).
pub(crate) fn verify_schema(connection: &Connection) -> Result<SchemaReport, StoreError> {
    verify_schema_with_hook(connection, |_| {})
}

fn verify_schema_with_hook(
    connection: &Connection,
    after_table: impl FnMut(&str),
) -> Result<SchemaReport, StoreError> {
    // Deferred BEGIN: pin one read generation at the first schema query. Never
    // reserve the writer, run DDL or change connection/journal settings.
    let transaction = connection.unchecked_transaction()?;
    let report = inspect_schema(&transaction, after_table)?;
    transaction.commit()?;
    Ok(report)
}

fn inspect_schema(
    connection: &Connection,
    mut after_table: impl FnMut(&str),
) -> Result<SchemaReport, StoreError> {
    let mut missing_tables = Vec::new();
    let mut missing_columns = Vec::new();
    let mut incompatible_storage = Vec::new();
    let mut tables = Vec::new();
    for (table, required) in REQUIRED_COLUMNS {
        match table_kind(connection, table)?.as_deref() {
            None => {
                missing_tables.push(table.to_owned());
                continue;
            }
            Some("table") => tables.push(table),
            Some(kind) => {
                incompatible_storage.push(format!("{table} (expected table, found {kind})"));
                continue;
            }
        }
        let present = table_columns(connection, table)?;
        for column in required {
            if !present.iter().any(|name| name == column) {
                missing_columns.push((table.to_owned(), (*column).to_owned()));
            }
        }
    }
    for contract in REQUIRED_STORAGE {
        if tables.contains(&contract.table) {
            verify_storage(connection, contract, &mut incompatible_storage)?;
            if matches!(
                contract.table,
                "host_registry" | "service_registry" | "subject_registry"
            ) {
                verify_registry_extensions(connection, contract, &mut incompatible_storage)?;
            }
            after_table(contract.table);
        }
    }
    if tables.contains(&"service_registry") {
        verify_service_host_foreign_key(connection, &mut incompatible_storage)?;
    }
    let migrations_kind = table_kind(connection, "django_migrations")?;
    let missing_migrations_table = migrations_kind.is_none();
    let missing_migration = if migrations_kind.as_deref() == Some("table") {
        let (app, name) = MIN_GRAPHYARD_MIGRATION;
        connection
            .query_row(
                "SELECT 1 FROM main.django_migrations WHERE app = ? AND name = ?",
                [app, name],
                |_| Ok(()),
            )
            .optional()?
            .is_none()
    } else {
        if let Some(kind) = migrations_kind {
            incompatible_storage.push(format!("django_migrations (expected table, found {kind})"));
        }
        true
    };
    let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    Ok(SchemaReport {
        missing_tables,
        missing_columns,
        missing_migrations_table,
        missing_migration,
        incompatible_storage,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    const SCHEMA: &str = include_str!("../../../fixtures/schema/schema.sql");

    #[test]
    fn concurrent_rebuild_never_fabricates_readiness_from_two_invalid_generations() {
        for remove_marker in [false, true] {
            check_concurrent_rebuild(remove_marker);
        }
    }

    fn check_concurrent_rebuild(remove_marker: bool) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("snapshot.sqlite3");
        let writer = Connection::open(&path).unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL").unwrap();
        // Generation A: the first table is good, the last table lacks its key.
        writer
            .execute_batch(&SCHEMA.replace(
                "\"service_id\" varchar(128) NOT NULL UNIQUE",
                "\"service_id\" varchar(128) NOT NULL",
            ))
            .unwrap();
        let settings = Settings::from_lookup(|_| None).unwrap();
        let reader = open_read_only(&path, &settings).unwrap();
        let (checked_tx, checked_rx) = mpsc::channel();
        let (rebuilt_tx, rebuilt_rx) = mpsc::channel();
        let rebuild = std::thread::spawn(move || {
            checked_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            // Generation B: restore the last key, remove the already-checked
            // first key and migration marker, all in one atomic rebuild.
            let mut ddl = String::from(
                "BEGIN; DROP TABLE metric_collection_spec; DROP TABLE service_registry;\n",
            );
            for line in SCHEMA.lines() {
                if line.starts_with("CREATE TABLE \"metric_collection_spec\"") {
                    ddl.push_str(&line.replace(" NOT NULL UNIQUE", " NOT NULL"));
                } else if line.starts_with("CREATE TABLE \"service_registry\"") {
                    ddl.push_str(line);
                }
            }
            if remove_marker {
                let (app, name) = MIN_GRAPHYARD_MIGRATION;
                ddl.push_str(&format!(
                    "DELETE FROM django_migrations WHERE app = '{app}' AND name = '{name}';"
                ));
            }
            ddl.push_str("COMMIT;");
            writer.execute_batch(&ddl).unwrap();
            rebuilt_tx.send(()).unwrap();
        });
        let report = verify_schema_with_hook(&reader, |table| {
            if table == "metric_collection_spec" {
                checked_tx.send(()).unwrap();
                rebuilt_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        })
        .unwrap();
        rebuild.join().unwrap();
        assert_eq!(
            report.incompatible_storage,
            ["service_registry unique (service_id)"]
        );
        assert!(!report.missing_migration);
        assert!(!report.is_ok());
        assert!(reader.is_autocommit());
        // A later verification sees all of B, not the first read's snapshot.
        let later = verify_schema(&reader).unwrap();
        assert_eq!(
            later.incompatible_storage,
            ["metric_collection_spec unique (name)"]
        );
        assert_eq!(later.missing_migration, remove_marker);
    }
}
