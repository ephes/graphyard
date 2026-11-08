//! InfluxDB v3: the SQL text of `influx.py:_query_range_v3_sql` and the
//! decoding of the JSON rows it reads (PLAN.md 6.5).

use chrono::{DateTime, Utc};
use graphyard_core::pycompat::{PyErrorKind, py_fromisoformat, py_str};
use graphyard_core::{MetricSample, Tags};
use serde_json::Value;

use crate::error::QueryError;
use crate::flux::{flux_timestamp, sample_value};
use crate::window::QueryRange;

/// `influx._sql_escape`: single quotes doubled.
pub fn sql_escape(value: &str) -> String {
    value.replace('\'', "''")
}

/// `influx._sql_identifier`: `"<name>"` for `[A-Za-z_][A-Za-z0-9_]*`.
///
/// # Errors
/// `InfluxConfigurationError: Unsupported SQL identifier: <name>`.
pub fn sql_identifier(name: &str) -> Result<String, QueryError> {
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        return Err(QueryError::config(format!(
            "Unsupported SQL identifier: {name}"
        )));
    }
    Ok(format!("\"{name}\""))
}

/// The SQL text of `_query_range_v3_sql` (byte-identical).
///
/// # Errors
/// An unsupported tag key (checked first, in tag order) or measurement name.
pub fn sql_query(range: &QueryRange, measurement: &str) -> Result<String, QueryError> {
    let mut clauses = vec![
        format!("time >= '{}'", sql_escape(&flux_timestamp(&range.start))),
        format!("time < '{}'", sql_escape(&flux_timestamp(&range.stop))),
        format!("metric = '{}'", sql_escape(&range.metric)),
    ];
    for (key, value) in range.dimension_filters() {
        clauses.push(format!("{key} = '{}'", sql_escape(value)));
    }
    for (key, value) in &range.tags {
        clauses.push(format!(
            "{} = '{}'",
            sql_identifier(key)?,
            sql_escape(value)
        ));
    }
    let measurement = sql_identifier(measurement)?;
    Ok(format!(
        "select * from {measurement} where {} order by time asc",
        clauses.join(" and ")
    ))
}

/// `influx._parse_influx_ts(value)`: `Z` replaced by `+00:00`,
/// `datetime.fromisoformat`, naive taken as UTC, converted to UTC.
///
/// Invalid timestamps are rejected by the row loop.
fn parse_influx_ts(raw: &str) -> Result<Option<DateTime<Utc>>, QueryError> {
    match py_fromisoformat(&raw.replace('Z', "+00:00")) {
        Err(err) if err.kind == PyErrorKind::ValueError => Ok(None),
        Err(err) => Err(err.into()),
        Ok(dt) => Ok(Some(dt.to_utc()?)),
    }
}

/// Columns `_query_range_v3_sql` does not turn into custom tags.
const NON_TAG_COLUMNS: [&str; 12] = [
    "time",
    "value",
    "host",
    "service",
    "metric",
    "subject_type",
    "subject_id",
    "source_system",
    "source_instance",
    "source_entity_id",
    "collector_service",
    "collector_host",
];

/// The samples `_query_range_v3_sql` builds from the decoded response.
///
/// # Errors
/// `Invalid response from InfluxDB v3 query API` when the document is not a
/// list; missing or malformed row/time/value evidence.
pub fn sql_samples(rows: &Value, metric_name: &str) -> Result<Vec<MetricSample>, QueryError> {
    let Value::Array(rows) = rows else {
        return Err(QueryError::config(
            "Invalid response from InfluxDB v3 query API",
        ));
    };
    let mut samples = Vec::new();
    for row in rows {
        let invalid_row = || QueryError::config("Invalid InfluxDB query row");
        let Value::Object(row) = row else {
            return Err(invalid_row());
        };
        let present = |key: &str| row.get(key).filter(|value| !value.is_null());
        let (Some(value), Some(Value::String(time))) = (present("value"), present("time")) else {
            return Err(invalid_row());
        };
        let invalid_time = || QueryError::config("Invalid InfluxDB query timestamp");
        let ts = parse_influx_ts(time)
            .map_err(|_| invalid_time())?
            .ok_or_else(invalid_time)?;
        let value = sample_value(&py_str(value))?;
        let optional = |key: &str| present(key).map(py_str);
        let tags: Tags = row
            .iter()
            .filter(|(key, value)| !value.is_null() && !NON_TAG_COLUMNS.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), py_str(value)))
            .collect();
        samples.push(MetricSample {
            ts,
            value,
            host: row.get("host").map(py_str).unwrap_or_default(),
            metric: row
                .get("metric")
                .map_or_else(|| metric_name.to_owned(), py_str),
            service: optional("service"),
            subject_type: optional("subject_type"),
            subject_id: optional("subject_id"),
            source_system: optional("source_system"),
            source_instance: optional("source_instance"),
            source_entity_id: optional("source_entity_id"),
            collector_service: optional("collector_service"),
            collector_host: optional("collector_host"),
            tags,
        });
    }
    Ok(samples)
}
