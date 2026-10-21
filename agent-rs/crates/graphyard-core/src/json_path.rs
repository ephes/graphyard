//! The metric path of an `http_json_metric` spec (PLAN.md 6.3).
//!
//! Python source: `services.py:_resolve_json_path`. Pinned by
//! `fixtures/json_path.json` (`fixtures/gen_json_path.py`).

use num_traits::ToPrimitive;
use serde_json::Value;

use crate::pycompat::{PyError, py_int};

/// `_resolve_json_path(payload, path)`: `$` is the payload itself; `$.a.0.b`
/// walks dict keys and list indexes (empty segments are skipped); any other
/// path, a missing key, an index out of range or a step into a scalar is
/// `None`. A JSON `null` at the path is `None` as well, which the executor
/// reports as not found, like Python.
///
/// A list index is a segment of ASCII digits (D-6: Python's `isdigit()`
/// also takes other Unicode digits).
///
/// # Errors
/// The `ValueError` of `int()` for an index segment of more than 4300
/// digits.
pub fn resolve_json_path<'a>(payload: &'a Value, path: &str) -> Result<Option<&'a Value>, PyError> {
    if path == "$" {
        return Ok(not_null(payload));
    }
    let Some(rest) = path.strip_prefix("$.") else {
        return Ok(None);
    };
    let mut current = payload;
    for part in rest.split('.').filter(|part| !part.is_empty()) {
        current = match current {
            Value::Object(map) => match map.get(part) {
                Some(value) => value,
                None => return Ok(None),
            },
            Value::Array(items) if part.bytes().all(|byte| byte.is_ascii_digit()) => {
                // `int(part)`: exact, with CPython's digit limit. An index
                // beyond `usize` is out of range like any other.
                let index = py_int(part)?;
                match index.to_usize().and_then(|index| items.get(index)) {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            _ => return Ok(None),
        };
    }
    Ok(not_null(current))
}

fn not_null(value: &Value) -> Option<&Value> {
    (!value.is_null()).then_some(value)
}

#[cfg(test)]
mod tests;
