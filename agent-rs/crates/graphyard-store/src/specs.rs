//! `metric_collection_spec`: the metrics run's load and per-spec update
//! (`services.py:run_metric_collection_specs_once`; PLAN.md 3.4, 5.4).

use chrono::{DateTime, Utc};
use graphyard_core::StatusLevel;
use rusqlite::{Connection, params};

use crate::actor::Store;
use crate::datetime::DbDateTime;
use crate::error::StoreError;
use crate::rows::{bool_column, datetime_column, now_text};

/// A spec row as Django loads it.
#[derive(Clone, Debug, PartialEq)]
pub struct SpecRow {
    pub id: i64,
    pub name: String,
    /// The `bool` converter's value (a stored `2` is selected by `WHERE
    /// enabled` but loads as false).
    pub enabled: bool,
    pub spec_type: String,
    pub interval_seconds: i64,
    pub next_run_time: i64,
    /// The stored JSON text, unparsed: the executor parses it (D-11).
    pub config: String,
    pub last_run_at: Option<DbDateTime>,
    pub last_status: String,
    pub last_error: String,
    pub created_at: Option<DbDateTime>,
    pub updated_at: Option<DbDateTime>,
}

/// The run result written per spec (`spec.save(update_fields=[...])`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpecRunUpdate {
    pub id: i64,
    /// The run's `now`.
    pub last_run_at: DateTime<Utc>,
    pub last_status: StatusLevel,
    pub last_error: String,
    /// `now_ts + max(1, interval_seconds)`, computed by the caller.
    pub next_run_time: i64,
}

type RawSpec = (
    i64,
    String,
    bool,
    String,
    i64,
    i64,
    String,
    Option<String>,
    String,
    String,
    Option<String>,
    Option<String>,
);

const SELECT_SPECS: &str = "SELECT id, name, CAST(enabled AS TEXT), spec_type, interval_seconds, \
     next_run_time, config, CAST(last_run_at AS TEXT), last_status, last_error, \
     CAST(created_at AS TEXT), CAST(updated_at AS TEXT) FROM metric_collection_spec";

/// `MetricCollectionSpec.objects.filter(enabled=True)` (plus
/// `next_run_time__lte=due_before`) ordered by name, every row converted.
pub(crate) fn load_specs(
    connection: &Connection,
    due_before: Option<i64>,
) -> Result<Vec<SpecRow>, StoreError> {
    let map = |row: &rusqlite::Row<'_>| -> rusqlite::Result<RawSpec> {
        Ok((
            row.get(0)?,
            row.get(1)?,
            bool_column(row, 2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
            row.get(6)?,
            row.get(7)?,
            row.get(8)?,
            row.get(9)?,
            row.get(10)?,
            row.get(11)?,
        ))
    };
    let raw: Vec<RawSpec> = match due_before {
        None => {
            let mut statement =
                connection.prepare(&format!("{SELECT_SPECS} WHERE enabled ORDER BY name ASC"))?;
            statement
                .query_map([], map)?
                .collect::<rusqlite::Result<_>>()?
        }
        Some(now_ts) => {
            let mut statement = connection.prepare(&format!(
                "{SELECT_SPECS} WHERE (enabled AND next_run_time <= ?) ORDER BY name ASC"
            ))?;
            statement
                .query_map([now_ts], map)?
                .collect::<rusqlite::Result<_>>()?
        }
    };
    raw.into_iter()
        .map(
            |(
                id,
                name,
                enabled,
                spec_type,
                interval_seconds,
                next_run_time,
                config,
                last_run_at,
                last_status,
                last_error,
                created_at,
                updated_at,
            )| {
                Ok(SpecRow {
                    id,
                    name,
                    enabled,
                    spec_type,
                    interval_seconds,
                    next_run_time,
                    config,
                    last_run_at: datetime_column(last_run_at.as_deref())?,
                    last_status,
                    last_error,
                    created_at: datetime_column(created_at.as_deref())?,
                    updated_at: datetime_column(updated_at.as_deref())?,
                })
            },
        )
        .collect()
}

/// `spec.save(update_fields=["last_run_at", "last_status", "last_error",
/// "next_run_time"])`: one autocommit UPDATE; `updated_at` is not touched.
pub(crate) fn update_spec_run(
    connection: &Connection,
    update: &SpecRunUpdate,
) -> Result<(), StoreError> {
    let changed = connection.execute(
        "UPDATE metric_collection_spec SET last_run_at = ?, last_status = ?, last_error = ?, \
         next_run_time = ? WHERE id = ?",
        params![
            now_text(update.last_run_at)?,
            update.last_status.as_str(),
            update.last_error,
            update.next_run_time,
            update.id,
        ],
    )?;
    if changed == 0 {
        return Err(StoreError::NotUpdated);
    }
    Ok(())
}

impl Store {
    /// Load the enabled specs ordered by name; with `due_before`, only those
    /// whose `next_run_time <= due_before` (`due_only=True` with `now_ts`).
    ///
    /// # Errors
    /// SQLite errors, and a stored datetime Django's loader rejects (the
    /// whole load fails, as in Python).
    pub async fn load_specs(&self, due_before: Option<i64>) -> Result<Vec<SpecRow>, StoreError> {
        self.call(move |connection| load_specs(connection, due_before))
            .await
    }

    /// Write one spec's run result.
    ///
    /// # Errors
    /// [`StoreError::NotUpdated`] when the row is gone, SQLite errors.
    pub async fn update_spec_run(&self, update: SpecRunUpdate) -> Result<(), StoreError> {
        self.call(move |connection| update_spec_run(connection, &update))
            .await
    }
}
