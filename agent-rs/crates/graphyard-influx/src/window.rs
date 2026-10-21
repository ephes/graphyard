//! The query window of a condition (`influx.py:query_condition_window`,
//! PLAN.md 6.5) and the range a query covers (`query_range`'s arguments).

use chrono::{DateTime, Utc};
use graphyard_core::Tags;
use graphyard_core::logline::escape_log_value;
use graphyard_core::normalize::normalize_subject_id;
use graphyard_core::pycompat::{
    PyError, date_out_of_range, py_datetime_add, py_int_true_divide, py_str, py_strip,
    py_timedelta_minutes, py_type_name,
};
use num_bigint::BigInt;
use num_traits::{FromPrimitive, ToPrimitive};
use serde_json::Value;

use crate::error::QueryError;

/// The condition fields `query_condition_window` reads (a borrowed view of
/// the `condition_definition` row).
#[derive(Clone, Copy, Debug)]
pub struct ConditionQuery<'a> {
    pub id: i64,
    pub name: &'a str,
    pub metric_name: &'a str,
    pub host_filter: &'a str,
    pub service_filter: &'a str,
    pub subject_type_filter: &'a str,
    pub subject_id_filter: &'a str,
    /// The decoded `tags_filter` JSON (any JSON value; Python calls
    /// `.items()` on it).
    pub tags_filter: &'a Value,
    pub window_minutes: i64,
    pub breach_minutes: i64,
    pub stale_after_seconds: Option<i64>,
}

impl ConditionQuery<'_> {
    /// `ConditionDefinition.stale_limit_seconds()`: the condition's
    /// allowance when set and non-zero, else the global setting (the rule of
    /// `graphyard_core::conditions::ConditionRule::stale_limit_seconds`).
    pub fn stale_limit_seconds(&self, stale_warning_seconds: &BigInt) -> BigInt {
        match self.stale_after_seconds {
            Some(seconds) if seconds != 0 => BigInt::from(seconds),
            _ => stale_warning_seconds.clone(),
        }
    }
}

/// The arguments of `influx.query_range`. `None` and `""` filters are both
/// left out of the query (Python tests truthiness).
#[derive(Clone, Debug, PartialEq)]
pub struct QueryRange {
    pub metric: String,
    pub start: DateTime<Utc>,
    pub stop: DateTime<Utc>,
    pub host: Option<String>,
    pub service: Option<String>,
    pub subject_type: Option<String>,
    pub subject_id: Option<String>,
    /// Tag filters in insertion order.
    pub tags: Tags,
}

impl QueryRange {
    /// A range over `metric` without filters.
    pub fn new(metric: impl Into<String>, start: DateTime<Utc>, stop: DateTime<Utc>) -> Self {
        Self {
            metric: metric.into(),
            start,
            stop,
            host: None,
            service: None,
            subject_type: None,
            subject_id: None,
            tags: Tags::new(),
        }
    }

    /// The set filters in Python's order: host, service, subject type,
    /// subject id (each only when non-empty).
    pub(crate) fn dimension_filters(&self) -> Vec<(&'static str, &str)> {
        [
            ("host", &self.host),
            ("service", &self.service),
            ("subject_type", &self.subject_type),
            ("subject_id", &self.subject_id),
        ]
        .into_iter()
        .filter_map(|(key, value)| match value.as_deref() {
            Some(value) if !value.is_empty() => Some((key, value)),
            _ => None,
        })
        .collect()
    }
}

/// `math.ceil(value / 60)` for a Python `int` (float division, then the
/// ceiling of the float as an exact integer).
fn ceil_minutes(seconds: &BigInt) -> Result<BigInt, PyError> {
    let minutes = py_int_true_divide(seconds, 60)?.ceil();
    Ok(BigInt::from_f64(minutes).unwrap_or_default())
}

/// `now - timedelta(minutes=minutes)` with CPython's checks: the
/// `timedelta` constructor (days must fit a C `int` and be at most
/// 999999999 in magnitude) and the datetime range.
fn minus_minutes(now: DateTime<Utc>, minutes: &BigInt) -> Result<DateTime<Utc>, PyError> {
    // Beyond `i64` the day count cannot fit a C `int` either.
    let minutes = minutes
        .to_i64()
        .ok_or_else(|| PyError::overflow_error("Python int too large to convert to C int"))?;
    let delta = py_timedelta_minutes(minutes)?;
    let start = py_datetime_add(now.timestamp_micros(), -delta)?;
    DateTime::from_timestamp_micros(start).ok_or_else(date_out_of_range)
}

/// `influx.query_condition_window` up to the `query_range` call: the
/// lookback window ending at `now`, the tag filters and the host filter.
/// Logs `Ignoring host_filter for non-host condition id=<id> name=<name>`
/// when a host filter is dropped.
///
/// # Errors
/// The exceptions Python raises on the way, in its order: `OverflowError`
/// of the lookback arithmetic, `AttributeError` for a `tags_filter` that is
/// not an object, `ValueError` of `normalize_subject_id`.
pub fn condition_window(
    condition: &ConditionQuery<'_>,
    stale_warning_seconds: &BigInt,
    now: DateTime<Utc>,
) -> Result<QueryRange, QueryError> {
    let anchor_minutes = ceil_minutes(stale_warning_seconds)?;
    let stale_minutes = ceil_minutes(&condition.stale_limit_seconds(stale_warning_seconds))?;
    let lookback = [
        BigInt::from(condition.window_minutes),
        BigInt::from(condition.breach_minutes) + anchor_minutes,
        stale_minutes,
    ]
    .into_iter()
    .reduce(|best, candidate| if candidate > best { candidate } else { best })
    .unwrap_or_default();
    let start = minus_minutes(now, &lookback)?;

    let Value::Object(filter) = condition.tags_filter else {
        return Err(QueryError::python(
            "AttributeError",
            format!(
                "'{}' object has no attribute 'items'",
                py_type_name(condition.tags_filter)
            ),
        ));
    };
    let mut tags: Tags = filter
        .iter()
        .map(|(key, value)| (key.clone(), py_str(value)))
        .collect();
    let subject_type = py_strip(condition.subject_type_filter).to_lowercase();
    if !condition.subject_type_filter.is_empty() {
        tags.insert("subject_type".to_owned(), subject_type.clone());
    }
    if !condition.subject_id_filter.is_empty() {
        tags.insert(
            "subject_id".to_owned(),
            normalize_subject_id(condition.subject_id_filter)?,
        );
    }

    let mut host = (!condition.host_filter.is_empty()).then(|| condition.host_filter.to_owned());
    if host.is_some() && !condition.subject_type_filter.is_empty() && subject_type != "host" {
        log::warn!(
            "Ignoring host_filter for non-host condition id={} name={}",
            condition.id,
            escape_log_value(condition.name)
        );
        host = None;
    }

    Ok(QueryRange {
        metric: condition.metric_name.to_owned(),
        start,
        stop: now,
        host,
        service: (!condition.service_filter.is_empty())
            .then(|| condition.service_filter.to_owned()),
        subject_type: None,
        subject_id: None,
        tags,
    })
}
