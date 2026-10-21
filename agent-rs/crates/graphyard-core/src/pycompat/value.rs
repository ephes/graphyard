//! `str()`, `repr()`, `type().__name__` and `==` of decoded JSON values, and
//! `spec_config.py:_describe`.

use num_bigint::BigInt;
use num_traits::{FromPrimitive, Zero};
use serde_json::Value;

use super::PyError;
use super::num::{PyNumber, int_limit_error, py_float_repr, py_number};
use super::text::py_str_repr;

/// CPython's default `sys.get_int_max_str_digits()`.
pub const MAX_INT_STR_DIGITS: usize = 4300;

/// `type(value).__name__` of a decoded JSON value.
pub fn py_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Number(number) => match py_number(number) {
            PyNumber::Int(_) => "int",
            PyNumber::Float(_) => "float",
        },
        Value::String(_) => "str",
        Value::Array(_) => "list",
        Value::Object(_) => "dict",
    }
}

/// `repr(value)`: `None`, `True`, `12`, `1e+16`, `'text'`, `[1, 2.0]`,
/// `{'key': None}`.
///
/// Integers are rendered with all their digits. CPython raises `ValueError`
/// for an `int` of more than 4300 digits instead; such values never reach
/// the agent, because `json.loads` rejects them while loading (see
/// [`int_digit_limit_error`], D-11), and [`py_describe`] reproduces the
/// `ValueError` path of `_describe`.
pub fn py_repr(value: &Value) -> String {
    let mut out = String::new();
    write_repr(&mut out, value);
    out
}

fn write_repr(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("None"),
        Value::Bool(true) => out.push_str("True"),
        Value::Bool(false) => out.push_str("False"),
        Value::Number(number) => match py_number(number) {
            PyNumber::Int(int) => out.push_str(&int.to_string()),
            PyNumber::Float(float) => out.push_str(&py_float_repr(float)),
        },
        Value::String(text) => out.push_str(&py_str_repr(text)),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                write_repr(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (index, (key, item)) in map.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(&py_str_repr(key));
                out.push_str(": ");
                write_repr(out, item);
            }
            out.push('}');
        }
    }
}

/// `str(value)`: the text of a string, [`py_repr`] of anything else.
pub fn py_str(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => py_repr(other),
    }
}

/// Whether `repr(value)` would raise `ValueError` in CPython (an `int` of
/// more than 4300 digits somewhere in the value).
fn repr_exceeds_int_limit(value: &Value) -> bool {
    match value {
        Value::Number(number) => {
            int_token_digits(number.as_str()).is_some_and(|digits| digits > MAX_INT_STR_DIGITS)
        }
        Value::Array(items) => items.iter().any(repr_exceeds_int_limit),
        Value::Object(map) => map.values().any(repr_exceeds_int_limit),
        _ => false,
    }
}

/// The digit count of an integer token, `None` for a float token.
fn int_token_digits(token: &str) -> Option<usize> {
    if token.contains(['.', 'e', 'E']) {
        return None;
    }
    Some(token.bytes().filter(u8::is_ascii_digit).count())
}

/// `spec_config.py:_describe`: `repr(raw)`, cut to 61 characters plus `...`
/// when longer than 64, or `<int>`/`<list>`/`<dict>` when the repr raises
/// `ValueError` (an integer beyond 4300 digits).
pub fn py_describe(value: &Value) -> String {
    if repr_exceeds_int_limit(value) {
        return format!("<{}>", py_type_name(value));
    }
    describe_text(py_repr(value))
}

/// The truncation of `_describe` applied to an already rendered repr (for
/// values that are not JSON, such as a non-finite default float).
pub fn describe_text(text: String) -> String {
    if text.chars().count() <= 64 {
        return text;
    }
    let mut cut: String = text.chars().take(61).collect();
    cut.push_str("...");
    cut
}

/// The `ValueError` that `json.loads(text)` raises for the first integer
/// token of more than 4300 digits, in document order (D-11): `Exceeds the
/// limit (4300 digits) for integer string conversion: value has <n> digits;
/// use sys.set_int_max_str_digits() to increase the limit`. `None` when every
/// integer token is short enough. `text` is a JSON document that
/// `serde_json` accepted; strings are skipped, so digits inside them do not
/// count.
pub fn int_digit_limit_error(text: &str) -> Option<PyError> {
    let bytes = text.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'"' {
                    index += if bytes[index] == b'\\' { 2 } else { 1 };
                }
                index += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = index;
                while index < bytes.len()
                    && matches!(bytes[index], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    index += 1;
                }
                if let Some(digits) = int_token_digits(&text[start..index])
                    && digits > MAX_INT_STR_DIGITS
                {
                    return Some(int_limit_error(digits));
                }
            }
            _ => index += 1,
        }
    }
    None
}

/// A number operand of Python `==` (`bool` counts as `int`).
enum Operand {
    Int(BigInt),
    Float(f64),
}

fn operand(value: &Value) -> Option<Operand> {
    match value {
        Value::Bool(flag) => Some(Operand::Int(BigInt::from(u8::from(*flag)))),
        Value::Number(number) => Some(match py_number(number) {
            PyNumber::Int(int) => Operand::Int(int),
            PyNumber::Float(float) => Operand::Float(float),
        }),
        _ => None,
    }
}

/// Python's exact `int == float` (no rounding of the integer).
fn int_eq_float(int: &BigInt, float: f64) -> bool {
    if !float.is_finite() || float.fract() != 0.0 {
        return false;
    }
    match BigInt::from_f64(float) {
        Some(exact) => exact == *int,
        None => float == 0.0 && int.is_zero(),
    }
}

/// Python `a == b` for decoded JSON values: numbers compare by value across
/// `bool`/`int`/`float` (exactly, `True == 1 == 1.0`), strings by text,
/// lists item by item, dicts by key set and values (order ignored); values
/// of different kinds are unequal.
pub fn py_eq(a: &Value, b: &Value) -> bool {
    if let (Some(left), Some(right)) = (operand(a), operand(b)) {
        return match (left, right) {
            (Operand::Int(x), Operand::Int(y)) => x == y,
            (Operand::Float(x), Operand::Float(y)) => x == y,
            (Operand::Int(x), Operand::Float(y)) | (Operand::Float(y), Operand::Int(x)) => {
                int_eq_float(&x, y)
            }
        };
    }
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(left, right)| py_eq(left, right))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(key, left)| y.get(key).is_some_and(|right| py_eq(left, right)))
        }
        _ => false,
    }
}
