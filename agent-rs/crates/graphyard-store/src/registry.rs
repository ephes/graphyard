//! Registry touch: `services.py:touch_registry_from_points(points)` without a
//! bound subject (graphyard `4d5baf1`; PLAN.md 5.5).
//!
//! One call works on the distinct rows a batch of points names, not per
//! point: normalise every point (a `ValueError` fails the call before any
//! read), load the subject, host and service rows in autocommit, and return
//! without a write when no row is missing and nothing changed or went stale
//! (`last_seen_at` older than 60 s). Otherwise one `BEGIN IMMEDIATE`
//! transaction reloads the rows, creates the missing ones like Django's
//! `get_or_create` and runs at most one UPDATE per table for `last_seen_at`
//! plus one per changed source system or re-parented service. Row images and
//! statement sequences are pinned by the `registry_cases` of
//! `fixtures/store_ops.json`.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, NaiveDateTime, TimeDelta, Utc};
use graphyard_core::model::{MetricPoint, SubjectType};
use graphyard_core::normalize::normalize_metric_point;
use graphyard_core::pycompat::PyError;
use indexmap::IndexMap;
use rusqlite::types::{Type, Value};
use rusqlite::{Connection, ErrorCode, Params, Transaction, TransactionBehavior, params_from_iter};

use crate::actor::Store;
use crate::datetime::{DateTimeError, DbDateTime, parse_column};
use crate::error::StoreError;
use crate::rows::bool_column;

/// `REGISTRY_LAST_SEEN_UPDATE_INTERVAL`: a row whose `last_seen_at` is at
/// least this old (or NULL) is refreshed.
pub const REGISTRY_LAST_SEEN_UPDATE_INTERVAL: TimeDelta = TimeDelta::seconds(60);

/// `_RegistryBatch`: the distinct rows one batch names, in first-seen order.
#[derive(Debug, Default)]
pub(crate) struct RegistryBatch {
    /// `(subject_type, subject_id)` -> the last non-empty source system.
    subjects: IndexMap<(String, String), Option<String>>,
    /// Host subject id -> index of the first point that registers it.
    host_ids: IndexMap<String, usize>,
    /// Service id -> `(candidate host, point index)` per point naming it: the
    /// point's own subject id for a host subject, else its collector host.
    service_hosts: IndexMap<String, Vec<(String, usize)>>,
}

impl RegistryBatch {
    /// `_collect_registry_batch`.
    ///
    /// # Errors
    /// The first point's normalisation `ValueError`.
    pub(crate) fn collect(points: &[MetricPoint]) -> Result<Self, PyError> {
        let mut batch = RegistryBatch::default();
        for (index, point) in points.iter().enumerate() {
            let normalized = normalize_metric_point(point)?;
            let key = (
                normalized.subject_type.as_str().to_owned(),
                normalized.subject_id.clone(),
            );
            // `normalized.source_system or previous`.
            let source_system = Some(normalized.source_system.clone())
                .filter(|source| !source.is_empty())
                .or_else(|| batch.subjects.get(&key).cloned().flatten());
            let is_host = normalized.subject_type == SubjectType::Host;
            batch.subjects.insert(key, source_system);
            if is_host {
                batch
                    .host_ids
                    .entry(normalized.subject_id.clone())
                    .or_insert(index);
            }
            if let Some(service) = normalized.service {
                let host_id = if is_host {
                    normalized.subject_id
                } else {
                    normalized.collector_host
                };
                batch
                    .service_hosts
                    .entry(service)
                    .or_default()
                    .push((host_id, index));
            }
        }
        Ok(batch)
    }

    fn is_empty(&self) -> bool {
        self.subjects.is_empty()
    }
}

/// A loaded `subject_registry` row: the fields the touch reads (Django
/// loads, decodes and converts every column).
#[derive(Clone, Debug)]
struct SubjectRow {
    id: i64,
    /// `(subject_type, subject_id)`.
    key: (String, String),
    /// As stored: only a TEXT value equal to the batch value is unchanged,
    /// like Python's `!=` between the loaded value and a `str`.
    source_system: Value,
    last_seen_at: Option<DbDateTime>,
}

/// A loaded `host_registry` row.
#[derive(Clone, Debug)]
struct HostRow {
    id: i64,
    host_id: String,
    last_seen_at: Option<DbDateTime>,
}

/// A loaded `service_registry` row.
#[derive(Clone, Debug)]
struct ServiceRow {
    id: i64,
    service_id: String,
    /// The stored foreign key (NULL for no host).
    host_id: Value,
    last_seen_at: Option<DbDateTime>,
}

/// `_RegistryRows`.
#[derive(Debug, Default)]
struct RegistryRows {
    subjects: HashMap<(String, String), SubjectRow>,
    hosts: HashMap<String, HostRow>,
    services: HashMap<String, ServiceRow>,
    /// The hosts of the load; a host this batch creates (or finds in
    /// `get_or_create`) is not one of them.
    preexisting_hosts: HashSet<String>,
}

const SELECT_SUBJECTS: &str = "SELECT id, subject_type, subject_id, display_name, source_system, \
     CAST(last_seen_at AS TEXT) FROM subject_registry";
const SELECT_HOSTS: &str = "SELECT id, host_id, display_name, grafana_url, CAST(enabled AS TEXT), \
     CAST(last_seen_at AS TEXT) FROM host_registry";
const SELECT_SERVICES: &str = "SELECT id, service_id, display_name, grafana_url, \
     CAST(enabled AS TEXT), CAST(last_seen_at AS TEXT), host_id FROM service_registry";

/// `last_seen_at` (selected as text) through Django's datetime conversion.
/// A conversion error travels as a `FromSqlConversionFailure` and becomes
/// [`StoreError::DateTime`] again in [`query_rows`].
fn last_seen(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<DbDateTime>> {
    let text: Option<String> = row.get(index)?;
    parse_column(text.as_deref())
        .map_err(|err| rusqlite::Error::FromSqlConversionFailure(index, Type::Text, Box::new(err)))
}

/// A column Django loads but the touch does not use: decoded like Python's
/// `sqlite3` (invalid UTF-8 text fails the load).
fn decoded(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<()> {
    row.get::<_, Value>(index).map(drop)
}

fn read_subject(row: &rusqlite::Row<'_>) -> rusqlite::Result<SubjectRow> {
    decoded(row, 3)?;
    Ok(SubjectRow {
        id: row.get(0)?,
        key: (row.get(1)?, row.get(2)?),
        source_system: row.get(4)?,
        last_seen_at: last_seen(row, 5)?,
    })
}

fn read_host(row: &rusqlite::Row<'_>) -> rusqlite::Result<HostRow> {
    decoded(row, 2)?;
    decoded(row, 3)?;
    bool_column(row, 4)?;
    Ok(HostRow {
        id: row.get(0)?,
        host_id: row.get(1)?,
        last_seen_at: last_seen(row, 5)?,
    })
}

fn read_service(row: &rusqlite::Row<'_>) -> rusqlite::Result<ServiceRow> {
    decoded(row, 2)?;
    decoded(row, 3)?;
    bool_column(row, 4)?;
    Ok(ServiceRow {
        id: row.get(0)?,
        service_id: row.get(1)?,
        last_seen_at: last_seen(row, 5)?,
        host_id: row.get(6)?,
    })
}

fn store_error(err: rusqlite::Error) -> StoreError {
    match err {
        rusqlite::Error::FromSqlConversionFailure(index, kind, inner) => {
            match inner.downcast::<DateTimeError>() {
                Ok(err) => StoreError::DateTime(*err),
                Err(inner) => StoreError::Sqlite(rusqlite::Error::FromSqlConversionFailure(
                    index, kind, inner,
                )),
            }
        }
        other => StoreError::Sqlite(other),
    }
}

/// Every row of `sql`, each read and converted (the first failure fails
/// the load, like Django's queryset iteration).
fn query_rows<T, P: Params>(
    connection: &Connection,
    sql: &str,
    params: P,
    read: fn(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
) -> Result<Vec<T>, StoreError> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(params, read)?;
    rows.collect::<rusqlite::Result<Vec<T>>>()
        .map_err(store_error)
}

/// `?, ?, ...` for `count` parameters.
fn marks(count: usize) -> String {
    vec!["?"; count].join(", ")
}

/// The distinct values in first-seen order.
fn distinct<'a>(values: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
    let mut seen = HashSet::new();
    values.filter(|value| seen.insert(*value)).collect()
}

/// `_load_registry_rows`, in its query order: hosts (batch hosts and
/// non-empty service candidates), subjects (`subject_type IN (...) AND
/// subject_id IN (...)`: every row of that cross product is converted, then
/// the rows are narrowed to the batch's pairs), services. Django runs no
/// query for an empty `__in` list (`EmptyResultSet`).
fn load_rows(connection: &Connection, batch: &RegistryBatch) -> Result<RegistryRows, StoreError> {
    let candidates = batch
        .service_hosts
        .values()
        .flatten()
        .map(|(host_id, _)| host_id.as_str());
    let host_ids = distinct(
        batch
            .host_ids
            .keys()
            .map(String::as_str)
            .chain(candidates)
            .filter(|host_id| !host_id.is_empty()),
    );
    let mut hosts = HashMap::new();
    if !host_ids.is_empty() {
        let sql = format!(
            "{SELECT_HOSTS} WHERE host_id IN ({})",
            marks(host_ids.len())
        );
        for host in query_rows(connection, &sql, params_from_iter(&host_ids), read_host)? {
            hosts.insert(host.host_id.clone(), host);
        }
    }

    let subject_types = distinct(batch.subjects.keys().map(|(kind, _)| kind.as_str()));
    let subject_ids = distinct(batch.subjects.keys().map(|(_, id)| id.as_str()));
    let mut subjects = HashMap::new();
    if !subject_types.is_empty() {
        let sql = format!(
            "{SELECT_SUBJECTS} WHERE subject_type IN ({}) AND subject_id IN ({})",
            marks(subject_types.len()),
            marks(subject_ids.len())
        );
        let params = params_from_iter(subject_types.iter().chain(&subject_ids));
        for subject in query_rows(connection, &sql, params, read_subject)? {
            if batch.subjects.contains_key(&subject.key) {
                subjects.insert(subject.key.clone(), subject);
            }
        }
    }

    let mut services = HashMap::new();
    if !batch.service_hosts.is_empty() {
        let sql = format!(
            "{SELECT_SERVICES} WHERE service_id IN ({})",
            marks(batch.service_hosts.len())
        );
        let params = params_from_iter(batch.service_hosts.keys());
        for service in query_rows(connection, &sql, params, read_service)? {
            services.insert(service.service_id.clone(), service);
        }
    }

    Ok(RegistryRows {
        preexisting_hosts: hosts.keys().cloned().collect(),
        subjects,
        hosts,
        services,
    })
}

/// `_service_target_host`: the host of the last candidate, in batch order,
/// whose row is known and that either existed before the batch or is
/// registered by a host point at the same or an earlier index.
fn service_target_host<'a>(
    service_id: &str,
    batch: &RegistryBatch,
    rows: &'a RegistryRows,
) -> Option<&'a HostRow> {
    batch.service_hosts[service_id]
        .iter()
        .rev()
        .find_map(|(host_id, index)| {
            let host = rows.hosts.get(host_id).filter(|_| !host_id.is_empty())?;
            let registered = rows.preexisting_hosts.contains(host_id)
                || batch
                    .host_ids
                    .get(host_id)
                    .is_some_and(|first| first <= index);
            registered.then_some(host)
        })
}

/// `_last_seen_is_stale`.
fn is_stale(value: Option<DbDateTime>, now: NaiveDateTime) -> bool {
    value.is_none_or(|seen| {
        now.signed_duration_since(seen.naive_utc()) >= REGISTRY_LAST_SEEN_UPDATE_INTERVAL
    })
}

/// `_RegistryPlan`: row updates by primary key; every write also moves
/// `last_seen_at`.
#[derive(Debug, Default, PartialEq, Eq)]
struct RegistryPlan {
    subject_seen: Vec<i64>,
    subject_source: IndexMap<i64, String>,
    host_seen: Vec<i64>,
    service_seen: Vec<i64>,
    service_reparent: IndexMap<i64, i64>,
}

impl RegistryPlan {
    fn is_empty(&self) -> bool {
        self.subject_seen.is_empty()
            && self.subject_source.is_empty()
            && self.host_seen.is_empty()
            && self.service_seen.is_empty()
            && self.service_reparent.is_empty()
    }
}

/// `_plan_registry_updates` over the rows that exist.
fn plan_updates(batch: &RegistryBatch, rows: &RegistryRows, now: NaiveDateTime) -> RegistryPlan {
    let mut plan = RegistryPlan::default();
    for (key, source_system) in &batch.subjects {
        let Some(subject) = rows.subjects.get(key) else {
            continue;
        };
        match source_system {
            Some(source)
                if !source.is_empty()
                    && !matches!(&subject.source_system, Value::Text(stored) if stored == source) =>
            {
                plan.subject_source.insert(subject.id, source.clone());
            }
            _ if is_stale(subject.last_seen_at, now) => plan.subject_seen.push(subject.id),
            _ => {}
        }
    }
    for host_id in batch.host_ids.keys() {
        if let Some(host) = rows.hosts.get(host_id)
            && is_stale(host.last_seen_at, now)
        {
            plan.host_seen.push(host.id);
        }
    }
    for service_id in batch.service_hosts.keys() {
        let Some(service) = rows.services.get(service_id) else {
            continue;
        };
        match service_target_host(service_id, batch, rows) {
            Some(target) if service.host_id != Value::Integer(target.id) => {
                plan.service_reparent.insert(service.id, target.id);
            }
            _ if is_stale(service.last_seen_at, now) => plan.service_seen.push(service.id),
            _ => {}
        }
    }
    plan
}

/// `_registry_needs_write`.
fn needs_write(batch: &RegistryBatch, rows: &RegistryRows, now: NaiveDateTime) -> bool {
    rows.subjects.len() < batch.subjects.len()
        || batch
            .host_ids
            .keys()
            .any(|host_id| !rows.hosts.contains_key(host_id))
        || rows.services.len() < batch.service_hosts.len()
        || !plan_updates(batch, rows, now).is_empty()
}

/// Django's `get_or_create` inside the registry transaction: select; else
/// insert inside a savepoint; on an `IntegrityError` select again and
/// re-raise when there is still no row.
fn get_or_create<T>(
    transaction: &mut Transaction<'_>,
    get: &dyn Fn(&Connection) -> Result<Option<T>, StoreError>,
    create: &dyn Fn(&Connection) -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    if let Some(found) = get(transaction)? {
        return Ok(found);
    }
    let savepoint = transaction.savepoint()?;
    match create(&savepoint) {
        Ok(created) => {
            savepoint.commit()?;
            Ok(created)
        }
        Err(StoreError::Sqlite(err))
            if err.sqlite_error_code() == Some(ErrorCode::ConstraintViolation) =>
        {
            // ROLLBACK TO and RELEASE the savepoint.
            savepoint.finish()?;
            get(transaction)?.ok_or(StoreError::Sqlite(err))
        }
        Err(err) => Err(err),
    }
}

/// `_create_missing_registry_rows`, in the order subjects, hosts, services.
fn create_missing(
    transaction: &mut Transaction<'_>,
    batch: &RegistryBatch,
    rows: &mut RegistryRows,
    now: DbDateTime,
    now_text: &str,
) -> Result<(), StoreError> {
    for ((subject_type, subject_id), source_system) in &batch.subjects {
        let key = (subject_type.clone(), subject_id.clone());
        if rows.subjects.contains_key(&key) {
            continue;
        }
        let subject = get_or_create(
            transaction,
            &|connection| {
                let sql =
                    format!("{SELECT_SUBJECTS} WHERE subject_type = ? AND subject_id = ? LIMIT 21");
                let found = query_rows(connection, &sql, [subject_type, subject_id], read_subject)?;
                Ok(found.into_iter().next())
            },
            &|connection| {
                connection.execute(
                    "INSERT INTO subject_registry \
                     (subject_type, subject_id, display_name, source_system, last_seen_at) \
                     VALUES (?, ?, ?, ?, ?)",
                    rusqlite::params![
                        subject_type,
                        subject_id,
                        subject_id,
                        source_system,
                        now_text
                    ],
                )?;
                Ok(SubjectRow {
                    id: connection.last_insert_rowid(),
                    key: key.clone(),
                    source_system: source_system.clone().map_or(Value::Null, Value::Text),
                    last_seen_at: Some(now),
                })
            },
        )?;
        rows.subjects.insert(key, subject);
    }
    for host_id in batch.host_ids.keys() {
        if rows.hosts.contains_key(host_id) {
            continue;
        }
        let host = get_or_create(
            transaction,
            &|connection| {
                let sql = format!("{SELECT_HOSTS} WHERE host_id = ? LIMIT 21");
                Ok(query_rows(connection, &sql, [host_id], read_host)?
                    .into_iter()
                    .next())
            },
            &|connection| {
                connection.execute(
                    "INSERT INTO host_registry \
                     (host_id, display_name, grafana_url, enabled, last_seen_at) \
                     VALUES (?, ?, '', 1, ?)",
                    [host_id, host_id, now_text],
                )?;
                Ok(HostRow {
                    id: connection.last_insert_rowid(),
                    host_id: host_id.clone(),
                    last_seen_at: Some(now),
                })
            },
        )?;
        rows.hosts.insert(host_id.clone(), host);
    }
    for service_id in batch.service_hosts.keys() {
        if rows.services.contains_key(service_id) {
            continue;
        }
        let target = service_target_host(service_id, batch, rows).map(|host| host.id);
        let service = get_or_create(
            transaction,
            &|connection| {
                let sql = format!("{SELECT_SERVICES} WHERE service_id = ? LIMIT 21");
                Ok(query_rows(connection, &sql, [service_id], read_service)?
                    .into_iter()
                    .next())
            },
            &|connection| {
                connection.execute(
                    "INSERT INTO service_registry \
                     (service_id, host_id, display_name, grafana_url, enabled, last_seen_at) \
                     VALUES (?, ?, ?, '', 1, ?)",
                    rusqlite::params![service_id, target, service_id, now_text],
                )?;
                Ok(ServiceRow {
                    id: connection.last_insert_rowid(),
                    service_id: service_id.clone(),
                    host_id: target.map_or(Value::Null, Value::Integer),
                    last_seen_at: Some(now),
                })
            },
        )?;
        rows.services.insert(service_id.clone(), service);
    }
    Ok(())
}

/// `UPDATE <table> SET last_seen_at = ? WHERE id IN (...)` (a queryset
/// `update()`).
fn update_seen(
    connection: &Connection,
    table: &str,
    ids: &[i64],
    now_text: &str,
) -> Result<(), StoreError> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut values: Vec<Value> = vec![Value::Text(now_text.to_owned())];
    values.extend(ids.iter().copied().map(Value::Integer));
    connection.execute(
        &format!(
            "UPDATE {table} SET last_seen_at = ? WHERE id IN ({})",
            marks(ids.len())
        ),
        params_from_iter(values),
    )?;
    Ok(())
}

/// The write phase: one `BEGIN IMMEDIATE` transaction that reloads the
/// rows, creates the missing ones and runs the planned updates.
pub(crate) fn write_registry(
    connection: &mut Connection,
    batch: &RegistryBatch,
    now: DbDateTime,
) -> Result<(), StoreError> {
    let now_text = now.to_db_text()?;
    let mut transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // Reload under the write lock so the plan sees committed concurrent work.
    let mut rows = load_rows(&transaction, batch)?;
    create_missing(&mut transaction, batch, &mut rows, now, &now_text)?;
    let plan = plan_updates(batch, &rows, now.naive_utc());

    update_seen(
        &transaction,
        "subject_registry",
        &plan.subject_seen,
        &now_text,
    )?;
    for (id, source_system) in &plan.subject_source {
        transaction.execute(
            "UPDATE subject_registry SET source_system = ?, last_seen_at = ? WHERE id = ?",
            rusqlite::params![source_system, now_text, id],
        )?;
    }
    update_seen(&transaction, "host_registry", &plan.host_seen, &now_text)?;
    update_seen(
        &transaction,
        "service_registry",
        &plan.service_seen,
        &now_text,
    )?;
    for (id, host_id) in &plan.service_reparent {
        transaction.execute(
            "UPDATE service_registry SET host_id = ?, last_seen_at = ? WHERE id = ?",
            rusqlite::params![host_id, now_text, id],
        )?;
    }
    transaction.commit()?;
    Ok(())
}

/// The read-only check in autocommit: whether the batch needs the write
/// phase (a row is missing, changes, or its `last_seen_at` is stale).
pub(crate) fn registry_needs_write(
    connection: &Connection,
    batch: &RegistryBatch,
    now: DbDateTime,
) -> Result<bool, StoreError> {
    let rows = load_rows(connection, batch)?;
    Ok(needs_write(batch, &rows, now.naive_utc()))
}

/// `touch_registry_from_points(points)` on a collected batch at `now`.
pub(crate) fn touch_registry(
    connection: &mut Connection,
    batch: &RegistryBatch,
    now: DbDateTime,
) -> Result<(), StoreError> {
    if batch.is_empty() || !registry_needs_write(connection, batch, now)? {
        return Ok(());
    }
    write_registry(connection, batch, now)
}

impl Store {
    /// `touch_registry_from_points(points)` (no bound subject, PLAN.md 5.5)
    /// at `now`. An empty list returns at once; a point that fails
    /// normalisation fails the call before any registry read.
    ///
    /// # Errors
    /// [`StoreError::Point`] for a normalisation `ValueError`, a stored
    /// datetime Django's loader rejects, SQLite errors (an `IntegrityError`
    /// of a create rolls the whole transaction back).
    pub async fn touch_registry(
        &self,
        points: Vec<MetricPoint>,
        now: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let batch = RegistryBatch::collect(&points).map_err(StoreError::Point)?;
        if batch.is_empty() {
            return Ok(());
        }
        // Django's clock value has microsecond precision.
        let now = DbDateTime::from_utc(now);
        self.call(move |connection| touch_registry(connection, &batch, now))
            .await
    }
}

#[cfg(test)]
mod tests;
