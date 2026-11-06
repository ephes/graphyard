//! `condition_definition`: the conditions run's load and per-condition
//! updates (`services.py:evaluate_conditions_once`; PLAN.md 3.5, 5.4).

use chrono::{DateTime, Utc};
use graphyard_core::{ConditionEvaluation, StatusLevel};
use rusqlite::{Connection, params};

use crate::actor::Store;
use crate::datetime::DbDateTime;
use crate::error::StoreError;
use crate::rows::{bool_column, datetime_column, now_text};

/// A condition row as Django loads it.
#[derive(Clone, Debug, PartialEq)]
pub struct ConditionRow {
    pub id: i64,
    pub name: String,
    pub enabled: bool,
    pub metric_name: String,
    pub host_filter: String,
    pub service_filter: String,
    pub subject_type_filter: String,
    pub subject_id_filter: String,
    /// The stored JSON text, unparsed: the evaluation parses it (D-11).
    pub tags_filter: String,
    pub operator: String,
    pub warning_threshold: Option<f64>,
    pub critical_threshold: Option<f64>,
    pub window_minutes: i64,
    pub breach_minutes: i64,
    pub stale_after_seconds: Option<i64>,
    /// The `bool` converter's value (true only for a stored `1`).
    pub alert_when_stale: bool,
    pub status: String,
    pub message: String,
    pub last_value: Option<f64>,
    pub last_evaluated: Option<DbDateTime>,
    pub created_at: Option<DbDateTime>,
    pub updated_at: Option<DbDateTime>,
}

const SELECT_CONDITIONS: &str = "SELECT id, name, CAST(enabled AS TEXT), metric_name, \
     host_filter, service_filter, subject_type_filter, subject_id_filter, tags_filter, operator, \
     warning_threshold, critical_threshold, window_minutes, breach_minutes, stale_after_seconds, \
     CAST(alert_when_stale AS TEXT), status, message, last_value, CAST(last_evaluated AS TEXT), \
     CAST(created_at AS TEXT), CAST(updated_at AS TEXT) FROM condition_definition";

/// The raw row: the datetime columns follow as text, converted after the
/// query (a conversion error fails the load).
struct RawCondition {
    row: ConditionRow,
    last_evaluated: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
}

fn raw_condition(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawCondition> {
    Ok(RawCondition {
        row: ConditionRow {
            id: row.get(0)?,
            name: row.get(1)?,
            enabled: bool_column(row, 2)?,
            metric_name: row.get(3)?,
            host_filter: row.get(4)?,
            service_filter: row.get(5)?,
            subject_type_filter: row.get(6)?,
            subject_id_filter: row.get(7)?,
            tags_filter: row.get(8)?,
            operator: row.get(9)?,
            warning_threshold: row.get(10)?,
            critical_threshold: row.get(11)?,
            window_minutes: row.get(12)?,
            breach_minutes: row.get(13)?,
            stale_after_seconds: row.get(14)?,
            alert_when_stale: bool_column(row, 15)?,
            status: row.get(16)?,
            message: row.get(17)?,
            last_value: row.get(18)?,
            last_evaluated: None,
            created_at: None,
            updated_at: None,
        },
        last_evaluated: row.get(19)?,
        created_at: row.get(20)?,
        updated_at: row.get(21)?,
    })
}

/// `ConditionDefinition.objects.filter(enabled=True)` (plus
/// `id=condition_id`) ordered by name, every row converted.
pub(crate) fn load_conditions(
    connection: &Connection,
    condition_id: Option<i64>,
) -> Result<Vec<ConditionRow>, StoreError> {
    let raw: Vec<RawCondition> = match condition_id {
        None => {
            let mut statement = connection.prepare(&format!(
                "{SELECT_CONDITIONS} WHERE enabled ORDER BY name ASC"
            ))?;
            statement
                .query_map([], raw_condition)?
                .collect::<rusqlite::Result<_>>()?
        }
        Some(id) => {
            let mut statement = connection.prepare(&format!(
                "{SELECT_CONDITIONS} WHERE (enabled AND id = ?) ORDER BY name ASC"
            ))?;
            statement
                .query_map([id], raw_condition)?
                .collect::<rusqlite::Result<_>>()?
        }
    };
    raw.into_iter()
        .map(|raw| {
            let mut row = raw.row;
            row.last_evaluated = datetime_column(raw.last_evaluated.as_deref())?;
            row.created_at = datetime_column(raw.created_at.as_deref())?;
            row.updated_at = datetime_column(raw.updated_at.as_deref())?;
            Ok(row)
        })
        .collect()
}

/// Publish a success only if its attempt is newer than the stored watermark.
pub(crate) fn update_condition_success(
    connection: &Connection,
    id: i64,
    evaluation: &ConditionEvaluation,
) -> Result<(), StoreError> {
    connection.execute(
        "UPDATE condition_definition SET status = ?, message = ?, last_value = ?, \
         last_evaluated = ? WHERE id = ? AND (last_evaluated IS NULL OR last_evaluated < ?)",
        params![
            evaluation.status.as_str(),
            evaluation.message,
            evaluation.last_value,
            now_text(evaluation.evaluated_at)?,
            id,
            now_text(evaluation.evaluated_at)?,
        ],
    )?;
    Ok(())
}

/// Publish a failure with the same attempt watermark, preserving last_value.
pub(crate) fn update_condition_failure(
    connection: &Connection,
    id: i64,
    status: StatusLevel,
    message: &str,
    attempted_at: DateTime<Utc>,
) -> Result<(), StoreError> {
    connection.execute(
        "UPDATE condition_definition SET status = ?, message = ?, last_evaluated = ? \
         WHERE id = ? AND (last_evaluated IS NULL OR last_evaluated < ?)",
        params![
            status.as_str(),
            message,
            now_text(attempted_at)?,
            id,
            now_text(attempted_at)?
        ],
    )?;
    Ok(())
}

// Result fields and timestamps are intentionally excluded from the definition.
fn same_definition(a: &ConditionRow, b: &ConditionRow) -> bool {
    a.enabled == b.enabled
        && a.metric_name == b.metric_name
        && a.host_filter == b.host_filter
        && a.service_filter == b.service_filter
        && a.subject_type_filter == b.subject_type_filter
        && a.subject_id_filter == b.subject_id_filter
        && a.tags_filter == b.tags_filter
        && a.operator == b.operator
        && a.warning_threshold == b.warning_threshold
        && a.critical_threshold == b.critical_threshold
        && a.window_minutes == b.window_minutes
        && a.breach_minutes == b.breach_minutes
        && a.stale_after_seconds == b.stale_after_seconds
        && a.alert_when_stale == b.alert_when_stale
}

impl Store {
    /// Load the enabled conditions ordered by name; with `condition_id`,
    /// only that one (`evaluate_conditions_once(condition_id=...)`).
    ///
    /// # Errors
    /// SQLite errors, and a stored datetime Django's loader rejects (the
    /// whole load fails, as in Python).
    pub async fn load_conditions(
        &self,
        condition_id: Option<i64>,
    ) -> Result<Vec<ConditionRow>, StoreError> {
        self.call(move |connection| load_conditions(connection, condition_id))
            .await
    }

    /// Write a successful evaluation.
    ///
    /// # Errors
    /// SQLite errors. Stale attempts and deleted rows are no-ops.
    pub async fn update_condition_success(
        &self,
        definition: ConditionRow,
        evaluation: ConditionEvaluation,
    ) -> Result<(), StoreError> {
        self.call(move |connection| {
            let tx =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            if load_conditions(&tx, Some(definition.id))?
                .first()
                .is_some_and(|current| same_definition(current, &definition))
            {
                update_condition_success(&tx, definition.id, &evaluation)?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }

    /// Write a failed evaluation's status and message.
    ///
    /// # Errors
    /// SQLite errors. Stale attempts and deleted rows are no-ops.
    pub async fn update_condition_failure(
        &self,
        definition: ConditionRow,
        status: StatusLevel,
        message: String,
        attempted_at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        self.call(move |connection| {
            let tx =
                connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            if load_conditions(&tx, Some(definition.id))?
                .first()
                .is_some_and(|current| same_definition(current, &definition))
            {
                update_condition_failure(&tx, definition.id, status, &message, attempted_at)?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
    }
}
