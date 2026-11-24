//! The metric path of an `http_json_metric` spec (PLAN.md 6.3).
//!
//! Python source: `services.py:_resolve_json_path`. Pinned by
//! `fixtures/json_path.json` (`fixtures/gen_json_path.py`).

use serde_json::Value;

use crate::pycompat::{MAX_INT_STR_DIGITS, PyError, decimal_digit, int_limit_error};

/// `_resolve_json_path(payload, path)`: `$` is the payload itself; `$.a.0.b`
/// walks dict keys and list indexes (empty segments are skipped); any other
/// path, a missing key, an index out of range or a step into a scalar is
/// `None`. A JSON `null` at the path is `None` as well, which the executor
/// reports as not found, like Python.
///
/// A list index is a segment of Unicode decimal digits (category Nd),
/// matching Python 3.12.
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
            Value::Array(items) => match array_index(part)?.and_then(|index| items.get(index)) {
                Some(value) => value,
                None => return Ok(None),
            },
            _ => return Ok(None),
        };
    }
    Ok(not_null(current))
}

fn array_index(part: &str) -> Result<Option<usize>, PyError> {
    let mut digits = 0;
    let mut index = Some(0_usize);
    for c in part.chars() {
        let Some(digit) = decimal_digit(c) else {
            return Ok(None);
        };
        digits += 1;
        // Keep validating the segment after overflow: the digit limit
        // still applies when every character is a decimal digit.
        index = index.and_then(|value| value.checked_mul(10)?.checked_add(digit as usize));
    }
    if digits > MAX_INT_STR_DIGITS {
        return Err(int_limit_error(digits));
    }
    Ok(index)
}

fn not_null(value: &Value) -> Option<&Value> {
    (!value.is_null()).then_some(value)
}

#[cfg(test)]
mod tests;
