//! Typed knobs of a metric collection spec `config` (PLAN.md section 6.3).
//!
//! Python source: `spec_config.py` (`positive_float_config`,
//! `positive_int_config`, `bool_config`, `request_timeout_config`,
//! `page_probe_total_timeout_config`, `_supported_timeout`, `_describe`,
//! `validate_spec_config`). Pinned by `fixtures/spec_config.json`
//! (`fixtures/gen_spec_config.py`); every [`SpecConfigError`] message is P1.
//!
//! `config` is the spec's JSON object as `json.loads` decodes it (see
//! [`crate::pycompat`] for numbers). The executors call these helpers before
//! any transport is built; `validate_spec_config` is the web's model
//! validation and is ported because it shares the helpers and its message
//! order is pinned by the same fixtures.

use std::fmt;

use num_traits::{Signed, ToPrimitive};
use serde_json::{Map, Value};

use crate::pycompat::{
    PyNumber, describe_text, py_describe, py_float, py_float_of, py_float_repr, py_number, py_strip,
};

/// `MAX_TIMEOUT_SECONDS`: the largest accepted timeout (one day).
pub const MAX_TIMEOUT_SECONDS: f64 = 86400.0;
/// `DEFAULT_REQUEST_TIMEOUT_SECONDS`.
pub const DEFAULT_REQUEST_TIMEOUT_SECONDS: f64 = 10.0;
/// `DEFAULT_PAGE_PROBE_MAX_BODY_BYTES` (10 MiB).
pub const DEFAULT_PAGE_PROBE_MAX_BODY_BYTES: u64 = 10 * 1024 * 1024;
/// `DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_FACTOR`.
pub const DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_FACTOR: f64 = 3.0;

/// The spec types whose total deadline defaults to three request timeouts
/// in `validate_spec_config` (the executors that run under the deadline).
const DEADLINE_SPEC_TYPES: [&str; 4] = [
    "http_page_probe",
    "http_json_metric",
    "home_assistant_sensor",
    "home_assistant_env_scan",
];

/// A spec config value has the wrong type or range (`SpecConfigError`, a
/// `ValueError`); the message is what Python stores as `last_error`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpecConfigError(pub String);

impl fmt::Display for SpecConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SpecConfigError {}

/// A spec's config object.
pub type Config = Map<String, Value>;

/// `float(raw.strip() if isinstance(raw, str) else raw)` for the accepted
/// types (`int`, `float`, `str`, not `bool`); `None` when Python's
/// conversion raises or the type is not accepted.
fn float_of_accepted(raw: &Value) -> Option<f64> {
    match raw {
        // `float(int)` raises `OverflowError` beyond the double range.
        Value::Number(_) => py_float_of(raw).ok(),
        Value::String(text) => py_float(py_strip(text)).ok(),
        _ => None,
    }
}

/// `positive_float_config`: a finite number > 0; numbers and numeric strings
/// are accepted, booleans are not. A missing key uses `default`, which is
/// checked like a configured value.
///
/// # Errors
/// `config.<key> must be a number greater than 0, got <_describe(raw)>`.
pub fn positive_float_config(
    config: &Config,
    key: &str,
    default: f64,
) -> Result<f64, SpecConfigError> {
    let raw = config.get(key);
    let value = match raw {
        Some(raw) => float_of_accepted(raw),
        None => Some(default),
    };
    match value {
        Some(value) if value.is_finite() && value > 0.0 => Ok(value),
        _ => {
            // `_describe(raw)`, where `raw` is the default for a missing key.
            let described = raw.map_or_else(|| describe_text(py_float_repr(default)), py_describe);
            Err(SpecConfigError(format!(
                "config.{key} must be a number greater than 0, got {described}"
            )))
        }
    }
}

/// `positive_int_config`: an integer >= 1; JSON integers and strings of 1 to
/// 18 ASCII digits (after `strip()`) are accepted, booleans and floats are
/// not. A missing key returns `default` unchecked. Integers beyond `u64`
/// saturate to `u64::MAX`, which no byte count reaches.
///
/// # Errors
/// `config.<key> must be a positive integer, got <_describe(raw)>`.
pub fn positive_int_config(
    config: &Config,
    key: &str,
    default: u64,
) -> Result<u64, SpecConfigError> {
    let Some(raw) = config.get(key) else {
        return Ok(default);
    };
    let value = match raw {
        Value::Number(number) => match py_number(number) {
            PyNumber::Int(int) if int.is_positive() => Some(int.to_u64().unwrap_or(u64::MAX)),
            _ => None,
        },
        Value::String(text) => {
            let stripped = py_strip(text);
            let digits = stripped.len();
            if (1..=18).contains(&digits) && stripped.bytes().all(|b| b.is_ascii_digit()) {
                stripped.parse::<u64>().ok().filter(|value| *value >= 1)
            } else {
                None
            }
        }
        _ => None,
    };
    value.ok_or_else(|| {
        SpecConfigError(format!(
            "config.{key} must be a positive integer, got {}",
            py_describe(raw)
        ))
    })
}

/// `bool_config`: a real JSON boolean (`"false"` is rejected); a missing key
/// returns `default`.
///
/// # Errors
/// `config.<key> must be true or false, got <_describe(raw)>`.
pub fn bool_config(config: &Config, key: &str, default: bool) -> Result<bool, SpecConfigError> {
    match config.get(key) {
        None => Ok(default),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(raw) => Err(SpecConfigError(format!(
            "config.{key} must be true or false, got {}",
            py_describe(raw)
        ))),
    }
}

/// `_supported_timeout`: at most [`MAX_TIMEOUT_SECONDS`].
fn supported_timeout(key: &str, value: f64) -> Result<f64, SpecConfigError> {
    if value > MAX_TIMEOUT_SECONDS {
        return Err(SpecConfigError(format!(
            "config.{key} must be <= {} seconds",
            py_float_repr(MAX_TIMEOUT_SECONDS)
        )));
    }
    Ok(value)
}

/// `request_timeout_config`: `request_timeout_seconds` (default 10), the
/// per-operation HTTP timeout.
///
/// # Errors
/// The messages of [`positive_float_config`] and `_supported_timeout`.
pub fn request_timeout_config(config: &Config) -> Result<f64, SpecConfigError> {
    let value = positive_float_config(
        config,
        "request_timeout_seconds",
        DEFAULT_REQUEST_TIMEOUT_SECONDS,
    )?;
    supported_timeout("request_timeout_seconds", value)
}

/// `page_probe_total_timeout_config`: `total_timeout_seconds`, the
/// whole-operation deadline, defaulting to three request timeouts.
///
/// # Errors
/// The messages of [`positive_float_config`] (also for an infinite derived
/// default) and `_supported_timeout`.
pub fn page_probe_total_timeout_config(
    config: &Config,
    request_timeout_seconds: f64,
) -> Result<f64, SpecConfigError> {
    let value = positive_float_config(
        config,
        "total_timeout_seconds",
        request_timeout_seconds * DEFAULT_PAGE_PROBE_TOTAL_TIMEOUT_FACTOR,
    )?;
    supported_timeout("total_timeout_seconds", value)
}

/// `validate_spec_config`: the problems of a config's typed knobs in check
/// order (request timeout, total timeout, `verify_tls`, `follow_redirects`,
/// `max_body_bytes`), empty when valid; a non-object config is the single
/// problem `config must be an object`.
pub fn validate_spec_config(spec_type: &str, config: &Value) -> Vec<String> {
    let Value::Object(config) = config else {
        return vec!["config must be an object".to_owned()];
    };
    let total_timeout_check = || {
        if !DEADLINE_SPEC_TYPES.contains(&spec_type) {
            return positive_float_config(config, "total_timeout_seconds", 1.0);
        }
        // Report the request error once, while still checking an explicit total.
        let request_timeout =
            request_timeout_config(config).unwrap_or(DEFAULT_REQUEST_TIMEOUT_SECONDS);
        page_probe_total_timeout_config(config, request_timeout)
    };
    [
        request_timeout_config(config).err(),
        total_timeout_check().err(),
        bool_config(config, "verify_tls", true).err(),
        bool_config(config, "follow_redirects", true).err(),
        positive_int_config(config, "max_body_bytes", DEFAULT_PAGE_PROBE_MAX_BODY_BYTES).err(),
    ]
    .into_iter()
    .flatten()
    .map(|error| error.0)
    .collect()
}

#[cfg(test)]
mod tests;
