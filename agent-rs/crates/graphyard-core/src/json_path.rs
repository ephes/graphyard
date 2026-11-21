//! The metric path of an `http_json_metric` spec (PLAN.md 6.3).
//!
//! Python source: `services.py:_resolve_json_path`. Pinned by
//! `fixtures/json_path.json` (`fixtures/gen_json_path.py`).

use serde_json::Value;

use crate::pycompat::{MAX_INT_STR_DIGITS, PyError, int_limit_error};

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

// Unicode 15.0 decimal zeroes, excluding ASCII. Each begins a run of
// ten code points in numeric order. This is the database used by Python 3.12.
const DECIMAL_ZEROES: &[u32] = &[
    0x660, 0x6f0, 0x7c0, 0x966, 0x9e6, 0xa66, 0xae6, 0xb66, 0xbe6, 0xc66, 0xce6, 0xd66, 0xde6,
    0xe50, 0xed0, 0xf20, 0x1040, 0x1090, 0x17e0, 0x1810, 0x1946, 0x19d0, 0x1a80, 0x1a90, 0x1b50,
    0x1bb0, 0x1c40, 0x1c50, 0xa620, 0xa8d0, 0xa900, 0xa9d0, 0xa9f0, 0xaa50, 0xabf0, 0xff10,
    0x104a0, 0x10d30, 0x11066, 0x110f0, 0x11136, 0x111d0, 0x112f0, 0x11450, 0x114d0, 0x11650,
    0x116c0, 0x11730, 0x118e0, 0x11950, 0x11c50, 0x11d50, 0x11da0, 0x11f50, 0x16a60, 0x16ac0,
    0x16b50, 0x1d7ce, 0x1d7d8, 0x1d7e2, 0x1d7ec, 0x1d7f6, 0x1e140, 0x1e2f0, 0x1e4f0, 0x1e950,
    0x1fbf0,
];

fn decimal_digit(c: char) -> Option<u32> {
    if c.is_ascii() {
        return c.to_digit(10);
    }
    let code = u32::from(c);
    let position = DECIMAL_ZEROES.partition_point(|&zero| zero <= code);
    let digit = code - DECIMAL_ZEROES[position.checked_sub(1)?];
    (digit < 10).then_some(digit)
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
