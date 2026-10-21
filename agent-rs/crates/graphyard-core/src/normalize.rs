//! Metric point normalisation (PLAN.md section 6.4).
//!
//! Python source: `influx.py` — `normalize_subject_id`,
//! `_normalize_dimension_value`, `parse_finite_float`,
//! `normalize_metric_point` and `_RESERVED_TAG_KEYS`. Pinned by
//! `fixtures/normalize.json` (`fixtures/gen_normalize.py`).
//!
//! Every failure is the `ValueError` Python raises, with its exact message
//! (P1): executors report these texts as spec messages and `write_points`
//! logs them. `str.lower()` is Rust's `to_lowercase` (full Unicode mapping
//! like CPython; the Unicode database versions may differ on new code
//! points, P2) and `str.strip()`/regex `\s` are [`py_isspace`], so the
//! ASCII-only results of the patterns below are the same as Python's.

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::model::{MetricPoint, SubjectType, Tags};
use crate::pycompat::{
    PyError, py_float, py_float_repr, py_isspace, py_repr, py_str, py_str_repr, py_strip,
};

/// `influx.py:_RESERVED_TAG_KEYS`, sorted: custom tags with these keys are
/// ignored by `write_points` (with a warning).
pub const RESERVED_TAG_KEYS: [&str; 10] = [
    "collector_host",
    "collector_service",
    "host",
    "metric",
    "service",
    "source_entity_id",
    "source_instance",
    "source_system",
    "subject_id",
    "subject_type",
];

/// Whether `key` is in `_RESERVED_TAG_KEYS`.
pub fn is_reserved_tag_key(key: &str) -> bool {
    RESERVED_TAG_KEYS.contains(&key)
}

/// A point after `normalize_metric_point`: the dimensions in their
/// normalised form, the subject type validated, custom tags as given
/// (`{str(k): str(v)}`, empty when the point had none).
#[derive(Clone, Debug, PartialEq)]
pub struct NormalizedPoint {
    pub ts: DateTime<Utc>,
    pub metric: String,
    pub value: f64,
    pub subject_type: SubjectType,
    pub subject_id: String,
    pub source_system: String,
    pub source_instance: String,
    pub source_entity_id: Option<String>,
    pub collector_service: String,
    pub collector_host: String,
    pub host: Option<String>,
    pub service: Option<String>,
    pub tags: Tags,
}

/// `normalize_subject_id(raw)`: `strip().lower()`, every run of characters
/// outside `[a-z0-9]` replaced by one `_`, leading and trailing `_` removed.
///
/// # Errors
/// `ValueError` `Invalid subject_id <repr>: expected lowercase snake_case
/// identifier` when nothing is left.
pub fn normalize_subject_id(raw: &str) -> Result<String, PyError> {
    let lowered = py_strip(raw).to_lowercase();
    // re.sub(r"[^a-z0-9]+", "_", ...): `_` itself is outside the class, so
    // every run of `_` in the result is one replaced run and the second
    // `re.sub(r"_+", "_", ...)` changes nothing.
    let mut normalized = String::with_capacity(lowered.len());
    let mut in_run = false;
    for c in lowered.chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            normalized.push(c);
            in_run = false;
        } else if !in_run {
            normalized.push('_');
            in_run = true;
        }
    }
    // What is left matches `[a-z0-9]+(?:_[a-z0-9]+)*` exactly when it is
    // not empty.
    let normalized = normalized.trim_matches('_');
    if normalized.is_empty() {
        return Err(PyError::value_error(format!(
            "Invalid subject_id {}: expected lowercase snake_case identifier",
            py_str_repr(raw)
        )));
    }
    Ok(normalized.to_owned())
}

/// `_normalize_dimension_value(raw, field_name=..., fallback=...)`:
/// `strip().lower()`, the fallback when empty, every whitespace run replaced
/// by `_`, then `[a-z0-9][a-z0-9_.-]*` required. Python's `raw or ""` makes
/// `None` the same as `""`.
///
/// # Errors
/// `ValueError` `<field_name> is required` (empty without fallback) or
/// `Invalid <field_name> <repr of raw>: expected lowercase token with
/// [a-z0-9_.-]`.
pub fn normalize_dimension_value(
    raw: &str,
    field_name: &str,
    fallback: Option<&str>,
) -> Result<String, PyError> {
    let lowered = py_strip(raw).to_lowercase();
    let value = if lowered.is_empty() {
        match fallback {
            Some(fallback) => fallback.to_owned(),
            None => return Err(PyError::value_error(format!("{field_name} is required"))),
        }
    } else {
        lowered
    };
    let mut token = String::with_capacity(value.len());
    let mut in_space = false;
    for c in value.chars() {
        if py_isspace(c) {
            if !in_space {
                token.push('_');
                in_space = true;
            }
        } else {
            token.push(c);
            in_space = false;
        }
    }
    let mut chars = token.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "_.-".contains(c));
    if !valid {
        return Err(PyError::value_error(format!(
            "Invalid {field_name} {}: expected lowercase token with [a-z0-9_.-]",
            py_str_repr(raw)
        )));
    }
    Ok(token)
}

/// `parse_finite_float(raw)` for a decoded JSON value: `float(str(raw))`,
/// non-finite results rejected.
///
/// # Errors
/// The `ValueError` of `float()` (`could not convert string to float: …`,
/// also for `None`, booleans and containers, whose `str()` is not a number)
/// or `metric value must be finite, got <repr of raw>`.
pub fn parse_finite_float(raw: &Value) -> Result<f64, PyError> {
    let value = py_float(&py_str(raw))?;
    if !value.is_finite() {
        return Err(PyError::value_error(format!(
            "metric value must be finite, got {}",
            py_repr(raw)
        )));
    }
    Ok(value)
}

/// `str(x).strip()` of an optional text, empty mapped to `None`.
fn stripped_or_none(raw: Option<&str>) -> Option<String> {
    raw.map(py_strip)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

/// `normalize_metric_point(item)`.
///
/// Checks in Python's order: metric, subject type, subject id, source
/// system, source instance (fallback `default`), collector service,
/// collector host, the host compatibility tag, the value.
///
/// # Errors
/// The first `ValueError` of that order.
pub fn normalize_metric_point(item: &MetricPoint) -> Result<NormalizedPoint, PyError> {
    let metric = py_strip(&item.metric);
    if metric.is_empty() {
        return Err(PyError::value_error("metric is required"));
    }

    let subject_type = SubjectType::parse(&py_strip(&item.subject_type).to_lowercase())
        .ok_or_else(|| {
            PyError::value_error(format!(
                "Unknown subject_type {}; expected one of [{}]",
                py_str_repr(&item.subject_type),
                SubjectType::SORTED_NAMES
                    .map(|name| format!("'{name}'"))
                    .join(", ")
            ))
        })?;

    let subject_id = normalize_subject_id(&item.subject_id)?;
    let source_system = normalize_dimension_value(&item.source_system, "source_system", None)?;
    let source_instance = normalize_dimension_value(
        &item.source_instance,
        "source_instance",
        Some(MetricPoint::DEFAULT_SOURCE_INSTANCE),
    )?;
    let collector_service =
        normalize_dimension_value(&item.collector_service, "collector_service", None)?;
    let collector_host = normalize_dimension_value(&item.collector_host, "collector_host", None)?;
    let source_entity_id = stripped_or_none(item.source_entity_id.as_deref());

    let host = if subject_type == SubjectType::Host {
        match item.host.as_deref() {
            Some(host) if !py_strip(host).is_empty() => {
                let normalized_host = normalize_subject_id(host)?;
                if normalized_host != subject_id {
                    return Err(PyError::value_error(
                        "host compatibility tag must equal subject_id when subject_type=host",
                    ));
                }
                Some(normalized_host)
            }
            _ => Some(subject_id.clone()),
        }
    } else {
        None
    };

    let service = stripped_or_none(item.service.as_deref());
    let tags = item.tags.clone().unwrap_or_default();

    if !item.value.is_finite() {
        return Err(PyError::value_error(format!(
            "metric value must be finite, got {}",
            py_float_repr(item.value)
        )));
    }

    Ok(NormalizedPoint {
        ts: item.ts,
        metric: metric.to_owned(),
        value: item.value,
        subject_type,
        subject_id,
        source_system,
        source_instance,
        source_entity_id,
        collector_service,
        collector_host,
        host,
        service,
        tags,
    })
}

#[cfg(test)]
pub(crate) mod tests;
