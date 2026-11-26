//! `metric_collection_spec`: the metrics run's load and per-spec update
//! (`services.py:run_metric_collection_specs_once`; PLAN.md 3.4, 5.4).

use std::sync::Arc;
use std::vec::IntoIter;

use graphyard_core::StatusLevel;
use graphyard_core::utc::UtcDateTime;
use rusqlite::types::{Value, ValueRef};
use rusqlite::{Connection, TransactionBehavior, params};

use crate::actor::Store;
use crate::datetime::DbDateTime;
use crate::error::StoreError;
use crate::rows::{bool_column, datetime_column, now_text};

/// A spec row as Django loads it.
#[derive(Debug, PartialEq)]
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
    /// Exact SQLite text of `updated_at`, including legacy timestamp formats.
    pub updated_at_raw: Option<String>,
}

/// The spec identity shared by the claim, renewal and completion CAS.
pub struct SpecClaimIdentity {
    id: i64,
    name: String,
    spec_type: String,
    interval_seconds: i64,
    config: String,
    updated_at_raw: Option<String>,
}

impl SpecRow {
    /// Capture the fields used by the lease CAS, without the previous result.
    pub fn claim_identity(&self) -> Arc<SpecClaimIdentity> {
        Arc::new(SpecClaimIdentity {
            id: self.id,
            name: self.name.clone(),
            spec_type: self.spec_type.clone(),
            interval_seconds: self.interval_seconds,
            config: self.config.clone(),
            updated_at_raw: self.updated_at_raw.clone(),
        })
    }
}

/// The run result written per spec (`spec.save(update_fields=[...])`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpecRunUpdate {
    pub id: i64,
    /// The run's `now`.
    pub last_run_at: UtcDateTime,
    pub last_status: StatusLevel,
    pub last_error: String,
    /// `now_ts + max(1, interval_seconds)`, computed by the caller.
    pub next_run_time: i64,
}

fn decode_spec(row: &rusqlite::Row<'_>) -> Result<SpecRow, StoreError> {
    let mut spec = SpecRow {
        id: row.get(0)?,
        name: row.get(1)?,
        enabled: bool_column(row, 2)?,
        spec_type: row.get(3)?,
        interval_seconds: row.get(4)?,
        next_run_time: row.get(5)?,
        config: row.get(CONFIG_COLUMN)?,
        last_run_at: datetime_column(row.get::<_, Option<String>>(6)?.as_deref())?,
        last_status: row.get(7)?,
        last_error: row.get(8)?,
        created_at: datetime_column(row.get::<_, Option<String>>(9)?.as_deref())?,
        updated_at: None,
        updated_at_raw: row.get(10)?,
    };
    spec.updated_at = datetime_column(spec.updated_at_raw.as_deref())?;
    Ok(spec)
}

/// Why a candidate cannot be executed by this sweep.
pub enum PendingSpecIssue {
    /// Carry only the key so diagnostics cannot expose stored values.
    Invalid {
        id: i64,
        reason: &'static str,
    },
    Superseded,
}

pub type PendingSpec = Result<SpecRow, PendingSpecIssue>;

fn decode_pending_spec(row: &rusqlite::Row<'_>) -> Result<PendingSpec, StoreError> {
    if row.get_ref(CONFIG_COLUMN)? == ValueRef::Null {
        return Ok(Err(PendingSpecIssue::Invalid {
            id: row.get(0)?,
            reason: "stored collector config exceeds 1 MiB",
        }));
    }
    match decode_spec(row) {
        Ok(spec) => Ok(Ok(spec)),
        Err(
            StoreError::DateTime(_)
            | StoreError::Sqlite(
                rusqlite::Error::InvalidColumnType(..)
                | rusqlite::Error::FromSqlConversionFailure(..)
                | rusqlite::Error::Utf8Error(..)
                | rusqlite::Error::IntegralValueOutOfRange(..),
            ),
        ) => Ok(Err(PendingSpecIssue::Invalid {
            id: row.get(0)?,
            reason: "invalid stored collector metadata",
        })),
        Err(error) => Err(error),
    }
}

const SPEC_FIELDS: &str = "id, name, CAST(enabled AS TEXT), spec_type, interval_seconds, \
     next_run_time, CAST(last_run_at AS TEXT), last_status, last_error, \
     CAST(created_at AS TEXT), CAST(updated_at AS TEXT)";
const CONFIG_COLUMN: usize = 11;
const MAX_COLLECTOR_CONFIG_BYTES: i64 = 1024 * 1024;
// config is NOT NULL on disk. SQLite can get a TEXT or BLOB column's byte
// length from its record metadata without loading the value.
const BOUNDED_CONFIG: &str =
    "CASE WHEN octet_length(config) > :config_limit THEN NULL ELSE config END";

// Match bool_column's byte-for-byte interpretation, including restored rows.
const COLLECTOR_ENABLED: &str = "CAST(enabled AS TEXT) COLLATE BINARY = '1'";

// Each config is capped at 1 MiB. Keep the page small even when all are full.
const MANUAL_PAGE_SIZE: usize = 16;

#[derive(Clone)]
struct ManualSpecKey {
    id: i64,
    name: Vec<u8>,
    name_is_text: bool,
}

/// A manual sweep that holds only one page of eligible configs at a time.
#[derive(Default)]
pub struct ManualSpecScan {
    page: IntoIter<PendingSpec>,
    after: Option<ManualSpecKey>,
    last_page: bool,
}

impl ManualSpecScan {
    /// Whether the sweep can have another candidate.
    pub fn has_pending(&self) -> bool {
        !self.page.as_slice().is_empty() || !self.last_page
    }

    /// Advance by one candidate, returning None at the end of the sweep.
    ///
    /// # Errors
    /// Store and query failures. Unreadable rows are returned separately.
    pub async fn next(&mut self, store: &Store) -> Result<Option<PendingSpec>, StoreError> {
        if self.page.as_slice().is_empty() && !self.last_page {
            let after = self.after.clone();
            *self = store
                .call(move |connection| load_manual_page(connection, after))
                .await?;
        }
        Ok(self.page.next())
    }
}

fn load_manual_page(
    connection: &Connection,
    after: Option<ManualSpecKey>,
) -> Result<ManualSpecScan, StoreError> {
    let pagination = if after.is_some() {
        "AND (name, id) > (CASE WHEN :name_is_text THEN CAST(:name AS TEXT) ELSE :name END, :id)"
    } else {
        ""
    };
    let mut statement = connection.prepare_cached(&format!(
        "SELECT {SPEC_FIELDS}, {BOUNDED_CONFIG}, CAST(name AS BLOB), typeof(name) = 'text'
         FROM metric_collection_spec WHERE {COLLECTOR_ENABLED} {pagination}
         ORDER BY name ASC, id ASC LIMIT :page_size"
    ))?;
    let limit = MANUAL_PAGE_SIZE as i64;
    let mut rows = match after {
        Some(key) => statement.query(rusqlite::named_params! {
            ":config_limit": MAX_COLLECTOR_CONFIG_BYTES, ":page_size": limit,
            ":name_is_text": key.name_is_text, ":name": key.name, ":id": key.id,
        })?,
        None => statement.query(rusqlite::named_params! {
            ":config_limit": MAX_COLLECTOR_CONFIG_BYTES, ":page_size": limit,
        })?,
    };
    let mut page = Vec::new();
    let mut after = None;
    while let Some(row) = rows.next()? {
        // Retain the raw key even when the last row's name cannot be decoded.
        after = Some(ManualSpecKey {
            id: row.get(0)?,
            name: row.get(12)?,
            name_is_text: row.get(13)?,
        });
        page.push(decode_pending_spec(row)?);
    }
    Ok(ManualSpecScan {
        last_page: page.len() < MANUAL_PAGE_SIZE,
        page: page.into_iter(),
        after,
    })
}

const DUE_PAGE_SIZE: usize = 128;

struct DueCandidate {
    id: i64,
    name: Vec<u8>,
    name_is_text: bool,
    due: Value,
    spec_type: Vec<u8>,
    interval: Vec<u8>,
    revision: Option<Vec<u8>>,
}

/// A paged name-ordered due sweep with a fixed time horizon.
pub struct DueSpecScan {
    due_before: i64,
    page: IntoIter<DueCandidate>,
    after: Option<(Vec<u8>, bool)>,
    last_page: bool,
}

impl DueSpecScan {
    pub fn new(due_before: i64) -> Self {
        Self {
            due_before,
            page: Vec::new().into_iter(),
            after: None,
            last_page: false,
        }
    }

    /// Whether the sweep can have another candidate.
    pub fn has_pending(&self) -> bool {
        !self.page.as_slice().is_empty() || !self.last_page
    }

    /// Advance by one candidate, returning None at the end of the sweep.
    ///
    /// # Errors
    /// Store and query failures. Unreadable rows are returned separately.
    pub async fn next(&mut self, store: &Store) -> Result<Option<PendingSpec>, StoreError> {
        if self.page.as_slice().is_empty() && !self.last_page {
            let due_before = self.due_before;
            let after = self.after.clone();
            let mut page = store
                .call(move |connection| load_due_page(connection, due_before, after))
                .await?;
            self.last_page = page.len() <= DUE_PAGE_SIZE;
            page.truncate(DUE_PAGE_SIZE);
            self.after = page.last().map(|row| (row.name.clone(), row.name_is_text));
            self.page = page.into_iter();
        }
        let Some(candidate) = self.page.next() else {
            return Ok(None);
        };
        store
            .call(move |connection| load_pending_spec(connection, candidate))
            .await
            .map(Some)
    }
}

fn load_due_page(
    connection: &Connection,
    due_before: i64,
    after: Option<(Vec<u8>, bool)>,
) -> Result<Vec<DueCandidate>, StoreError> {
    // Carry raw bytes so even a malformed TEXT value can serve as a cursor.
    // name is unique and its index can satisfy the keyset lookup and ordering.
    const SELECT: &str = "SELECT id, CAST(name AS BLOB), typeof(name) = 'text', next_run_time, \
        CAST(spec_type AS BLOB), CAST(interval_seconds AS BLOB), CAST(updated_at AS BLOB) \
        FROM metric_collection_spec";
    let pagination = if after.is_some() {
        "AND name > CASE WHEN ?3 THEN CAST(?4 AS TEXT) ELSE ?4 END"
    } else {
        ""
    };
    let mut statement = connection.prepare_cached(&format!(
        "{SELECT} WHERE {COLLECTOR_ENABLED} AND next_run_time <= ?1 \
             {pagination} ORDER BY name ASC LIMIT ?2"
    ))?;
    // Look ahead one key to distinguish a full final page from more work.
    let limit = (DUE_PAGE_SIZE + 1) as i64;
    let mut rows = match after {
        Some((name, is_text)) => statement.query(params![due_before, limit, is_text, name])?,
        None => statement.query(params![due_before, limit])?,
    };
    let mut page = Vec::new();
    while let Some(row) = rows.next()? {
        page.push(DueCandidate {
            id: row.get(0)?,
            name: row.get(1)?,
            name_is_text: row.get(2)?,
            due: row.get(3)?,
            spec_type: row.get(4)?,
            interval: row.get(5)?,
            revision: row.get(6)?,
        });
    }
    Ok(page)
}

fn load_pending_spec(
    connection: &Connection,
    candidate: DueCandidate,
) -> Result<PendingSpec, StoreError> {
    // Only materialize config if the slot and definition seen on this page
    // still match. The later claim CAS also fences edits to the loaded config.
    let mut statement = connection.prepare_cached(&format!(
        "SELECT {SPEC_FIELDS}, {BOUNDED_CONFIG} FROM metric_collection_spec \
         WHERE id = ? AND {COLLECTOR_ENABLED} AND next_run_time = ? \
         AND CAST(name AS BLOB) = ? AND (typeof(name) = 'text') = ? \
         AND CAST(spec_type AS BLOB) = ? AND CAST(interval_seconds AS BLOB) = ? \
         AND CAST(updated_at AS BLOB) IS ?"
    ))?;
    let mut rows = statement.query(params![
        MAX_COLLECTOR_CONFIG_BYTES,
        candidate.id,
        candidate.due,
        candidate.name,
        candidate.name_is_text,
        candidate.spec_type,
        candidate.interval,
        candidate.revision,
    ])?;
    match rows.next()? {
        Some(row) => decode_pending_spec(row),
        None => Ok(Err(PendingSpecIssue::Superseded)),
    }
}

/// `MetricCollectionSpec.objects.filter(enabled=True)` (plus
/// `next_run_time__lte=due_before`) ordered by name, every row converted.
pub(crate) fn load_specs(
    connection: &Connection,
    due_before: Option<i64>,
) -> Result<Vec<SpecRow>, StoreError> {
    match due_before {
        None => {
            let mut statement =
                connection.prepare(&format!("SELECT {SPEC_FIELDS}, config FROM metric_collection_spec WHERE enabled ORDER BY name ASC"))?;
            statement.query_and_then([], decode_spec)?.collect()
        }
        Some(now_ts) => {
            let mut statement = connection.prepare(&format!(
                "SELECT {SPEC_FIELDS}, config FROM metric_collection_spec WHERE (enabled AND next_run_time <= ?) ORDER BY name ASC"
            ))?;
            statement.query_and_then([now_ts], decode_spec)?.collect()
        }
    }
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

fn claim_due_spec(
    connection: &Connection,
    definition: &SpecClaimIdentity,
    expected_due: i64,
    now: i64,
    until: i64,
) -> Result<bool, StoreError> {
    Ok(connection.execute(
        &format!(
            "UPDATE metric_collection_spec SET next_run_time = ? \
         WHERE id = ? AND {COLLECTOR_ENABLED} AND next_run_time = ? AND next_run_time <= ? \
         AND name = ? AND spec_type = ? AND interval_seconds = ? AND config = ? \
         AND CAST(updated_at AS TEXT) IS ?"
        ),
        params![
            until,
            definition.id,
            expected_due,
            now,
            definition.name,
            definition.spec_type,
            definition.interval_seconds,
            definition.config,
            definition.updated_at_raw,
        ],
    )? == 1)
}

impl Store {
    /// Claim one due slot before collection, only for the captured
    /// definition and its update timestamp.
    /// Only the winning caller may execute; an edit since the load leaves the
    /// slot due for a run with the current definition.
    ///
    /// # Errors
    /// SQLite errors.
    pub async fn claim_due_spec(
        &self,
        definition: Arc<SpecClaimIdentity>,
        expected_due: i64,
        now: i64,
        until: i64,
    ) -> Result<bool, StoreError> {
        self.call(move |connection| {
            claim_due_spec(connection, &definition, expected_due, now, until)
        })
        .await
    }

    /// Extend only the exact live claim and captured definition. A stale owner
    /// cannot extend a replacement or continue work after an edit/disable.
    /// Read `now` on the database thread after taking the writer reservation.
    ///
    /// # Errors
    /// SQLite errors. Expired, superseded or deleted claims return false.
    pub async fn renew_spec_claim(
        &self,
        definition: Arc<SpecClaimIdentity>,
        expected_schedule: i64,
        until: i64,
        now: impl FnOnce() -> i64 + Send + 'static,
    ) -> Result<bool, StoreError> {
        self.call(move |connection| {
            let transaction =
                connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if expected_schedule <= now() {
                return Ok(false);
            }
            let renewed = transaction.execute(
                &format!(
                    "UPDATE metric_collection_spec SET next_run_time = ? \
                 WHERE id = ? AND {COLLECTOR_ENABLED} AND next_run_time = ? \
                 AND name = ? AND spec_type = ? AND interval_seconds = ? AND config = ? \
                 AND CAST(updated_at AS TEXT) IS ?"
                ),
                params![
                    until,
                    definition.id,
                    expected_schedule,
                    definition.name,
                    definition.spec_type,
                    definition.interval_seconds,
                    definition.config,
                    definition.updated_at_raw,
                ],
            )? == 1;
            transaction.commit()?;
            Ok(renewed)
        })
        .await
    }

    /// Publish only for the captured definition and scheduling owner.
    /// Manual runs use the originally observed schedule instead of a lease.
    ///
    /// # Errors
    /// SQLite errors. Superseded or deleted rows return false.
    pub async fn finish_claimed_spec(
        &self,
        definition: Arc<SpecClaimIdentity>,
        update: SpecRunUpdate,
        expected_schedule: i64,
    ) -> Result<bool, StoreError> {
        if definition.id != update.id {
            return Ok(false);
        }
        self.call(move |connection| {
            Ok(connection.execute(
                &format!("UPDATE metric_collection_spec SET last_run_at = ?, last_status = ?, last_error = ?, \
                 next_run_time = ? WHERE id = ? AND {COLLECTOR_ENABLED} AND next_run_time = ? \
                 AND name = ? AND spec_type = ? \
                 AND interval_seconds = ? AND config = ? AND CAST(updated_at AS TEXT) IS ?"),
                params![
                    now_text(update.last_run_at)?, update.last_status.as_str(),
                    update.last_error, update.next_run_time, update.id, expected_schedule,
                    definition.name,
                    definition.spec_type, definition.interval_seconds, definition.config,
                    definition.updated_at_raw,
                ],
            )? == 1)
        }).await
    }

    /// Load specs using Django’s SQL truthiness filter for ORM parity.
    /// With `due_before`, return only those
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
