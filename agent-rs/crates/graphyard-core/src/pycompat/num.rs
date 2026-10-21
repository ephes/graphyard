//! Python numeric built-ins: `repr(float)`, `format(x, "g")`, `int()` and
//! `float()`, and the `int`/`float` reading of JSON number tokens.
//!
//! Rust's float formatting is exact (correctly rounded on the exact binary
//! value) like CPython's `dtoa`, so the decimal digits are taken from
//! `format!` and laid out with Python's rules.

use num_bigint::{BigInt, Sign};
use num_traits::ToPrimitive;
use serde_json::{Number, Value};

use super::text::py_str_repr;
use super::value::{MAX_INT_STR_DIGITS, py_type_name};
use super::{PyError, PyErrorKind};

/// The whitespace `int()` and `float()` strip from a `str` argument: ASCII
/// `Py_ISSPACE` (` \t\n\v\f\r`, not U+001C..U+001F) plus every non-ASCII
/// Unicode whitespace character, which
/// `_PyUnicode_TransformDecimalAndSpaceToASCII` turns into a space first.
fn strip_numeric(text: &str) -> &str {
    text.trim_matches(|c: char| {
        if c.is_ascii() {
            matches!(c, ' ' | '\t' | '\n' | '\u{b}' | '\u{c}' | '\r')
        } else {
            c.is_whitespace()
        }
    })
}

/// Split Rust's `{:e}` output into (negative, digits, decimal exponent).
fn split_sci(sci: &str) -> (bool, String, i32) {
    let (negative, rest) = match sci.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, sci),
    };
    let (mantissa, exponent) = rest.split_once('e').unwrap_or((rest, "0"));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    (negative, digits, exponent.parse().unwrap_or(0))
}

/// The shortest digits that round-trip, choosing the candidate nearest to
/// `x` (ties to even) like CPython's `dtoa` mode 0. Rust's shortest
/// formatting finds the length but may break a tie the other way, so the
/// correctly rounded decimal of that length is preferred when it round-trips.
fn shortest_digits(x: f64) -> (bool, String, i32) {
    let (negative, digits, exponent) = split_sci(&format!("{x:e}"));
    let nearest = format!("{:.*e}", digits.len() - 1, x);
    if nearest.parse::<f64>().ok() == Some(x) {
        return split_sci(&nearest);
    }
    (negative, digits, exponent)
}

fn push_exponent(out: &mut String, exponent: i32) {
    out.push('e');
    out.push(if exponent < 0 { '-' } else { '+' });
    out.push_str(&format!("{:02}", exponent.unsigned_abs()));
}

/// `repr(x)` (and `str(x)`) of a float: the shortest round-trip digits,
/// positional for exponents -4..16, else scientific (`1e+16`, `5e-324`).
pub fn py_float_repr(x: f64) -> String {
    if x.is_nan() {
        return "nan".to_owned();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    if x == 0.0 {
        return if x.is_sign_negative() { "-0.0" } else { "0.0" }.to_owned();
    }
    let (negative, digits, exponent) = shortest_digits(x);
    let decpt = exponent + 1;
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    if decpt > -4 && decpt <= 16 {
        if decpt <= 0 {
            out.push_str("0.");
            out.extend(std::iter::repeat_n('0', decpt.unsigned_abs() as usize));
            out.push_str(&digits);
        } else {
            let point = decpt.unsigned_abs() as usize;
            if point >= digits.len() {
                out.push_str(&digits);
                out.extend(std::iter::repeat_n('0', point - digits.len()));
                out.push_str(".0");
            } else {
                out.push_str(&digits[..point]);
                out.push('.');
                out.push_str(&digits[point..]);
            }
        }
    } else {
        out.push_str(&digits[..1]);
        if digits.len() > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        push_exponent(&mut out, exponent);
    }
    out
}

fn strip_fraction_zeros(text: &str) -> &str {
    if text.contains('.') {
        text.trim_end_matches('0').trim_end_matches('.')
    } else {
        text
    }
}

/// `format(x, "g")`: six significant digits, trailing zeros removed,
/// scientific below 1e-4 and from 1e6 on (`total_timeout_seconds={x:g}`).
pub fn py_format_g(x: f64) -> String {
    const PRECISION: i32 = 6;
    if x.is_nan() {
        return "nan".to_owned();
    }
    if x.is_infinite() {
        return if x > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    let sci = format!("{:.*e}", (PRECISION - 1).unsigned_abs() as usize, x);
    let (negative, digits, exponent) = split_sci(&sci);
    if (-4..PRECISION).contains(&exponent) {
        let decimals = (PRECISION - 1 - exponent).unsigned_abs() as usize;
        let fixed = format!("{x:.decimals$}");
        return strip_fraction_zeros(&fixed).to_owned();
    }
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    let mut mantissa = String::from(&digits[..1]);
    mantissa.push('.');
    mantissa.push_str(&digits[1..]);
    out.push_str(strip_fraction_zeros(&mantissa));
    push_exponent(&mut out, exponent);
    out
}

fn invalid_int(text: &str) -> PyError {
    // CPython formats the literal with `%.200R`: its repr cut to 200
    // characters (the closing quote included in the count).
    let literal: String = py_str_repr(text).chars().take(200).collect();
    PyError::value_error(format!("invalid literal for int() with base 10: {literal}"))
}

/// Digits with single underscores between them (PEP 515), ASCII only (D-6).
fn valid_digit_run(body: &str) -> bool {
    let bytes = body.as_bytes();
    !bytes.is_empty()
        && bytes[0].is_ascii_digit()
        && bytes[bytes.len() - 1].is_ascii_digit()
        && bytes.iter().all(|b| b.is_ascii_digit() || *b == b'_')
        && !body.contains("__")
}

/// The `ValueError` of a decimal integer longer than CPython's
/// `sys.get_int_max_str_digits()` (4300).
pub(super) fn int_limit_error(digits: usize) -> PyError {
    PyError::value_error(format!(
        "Exceeds the limit ({MAX_INT_STR_DIGITS} digits) for integer string conversion: \
         value has {digits} digits; use sys.set_int_max_str_digits() to increase the limit"
    ))
}

/// `int(text)` for a `str` (base 10): surrounding whitespace, a sign and
/// single underscores between digits are accepted.
///
/// # Errors
/// `ValueError` for anything Python's `int()` rejects, including more than
/// 4300 digits; non-ASCII digits are rejected too (D-6).
pub fn py_int(text: &str) -> Result<BigInt, PyError> {
    let stripped = strip_numeric(text);
    let (negative, body) = match stripped.as_bytes().first() {
        Some(b'-') => (true, &stripped[1..]),
        Some(b'+') => (false, &stripped[1..]),
        _ => (false, stripped),
    };
    if !valid_digit_run(body) {
        return Err(invalid_int(text));
    }
    let digits: String = body.chars().filter(|c| *c != '_').collect();
    if digits.len() > MAX_INT_STR_DIGITS {
        return Err(int_limit_error(digits.len()));
    }
    let magnitude: BigInt = digits.parse().map_err(|_| invalid_int(text))?;
    Ok(if negative { -magnitude } else { magnitude })
}

fn invalid_float(text: &str) -> PyError {
    PyError::value_error(format!(
        "could not convert string to float: {}",
        py_str_repr(text)
    ))
}

fn valid_decimal(text: &str) -> bool {
    let body = text.strip_prefix(['+', '-']).unwrap_or(text);
    let lower = body.to_ascii_lowercase();
    if matches!(lower.as_str(), "inf" | "infinity" | "nan") {
        return true;
    }
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(at) => (&body[..at], Some(&body[at + 1..])),
        None => (body, None),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    if !all_digits(whole) || !all_digits(fraction) || whole.len() + fraction.len() == 0 {
        return false;
    }
    match exponent {
        None => true,
        Some(exp) => {
            let digits = exp.strip_prefix(['+', '-']).unwrap_or(exp);
            !digits.is_empty() && all_digits(digits)
        }
    }
}

/// `float(text)` for a `str`: surrounding whitespace, underscores between
/// digits and the `inf`/`infinity`/`nan` spellings (any case, signed) are
/// accepted.
///
/// # Errors
/// `ValueError` for anything Python's `float()` rejects (non-ASCII digits
/// too, D-6). Overflowing values become `inf` as in Python.
pub fn py_float(text: &str) -> Result<f64, PyError> {
    let stripped = strip_numeric(text);
    let bytes = stripped.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'_' {
            let before = index > 0 && bytes[index - 1].is_ascii_digit();
            let after = bytes.get(index + 1).is_some_and(u8::is_ascii_digit);
            if !(before && after) {
                return Err(invalid_float(text));
            }
        }
    }
    let cleaned: String = stripped.chars().filter(|c| *c != '_').collect();
    if !valid_decimal(&cleaned) {
        return Err(invalid_float(text));
    }
    cleaned.parse().map_err(|_| invalid_float(text))
}

/// A JSON number as Python's `json.loads` decodes it.
#[derive(Clone, Debug, PartialEq)]
pub enum PyNumber {
    /// A token without fraction and exponent: an exact `int`.
    Int(BigInt),
    /// Any other token: `float(token)` (`inf` on overflow, like `json.loads`).
    Float(f64),
}

/// Decode a number token like `json.loads`.
pub fn py_number(number: &Number) -> PyNumber {
    let token = number.as_str();
    if !token.contains(['.', 'e', 'E'])
        && let Ok(value) = token.parse::<BigInt>()
    {
        return PyNumber::Int(value);
    }
    PyNumber::Float(token.parse().unwrap_or(f64::NAN))
}

/// `float(n)` for a Python `int`: correctly rounded (half to even).
///
/// # Errors
/// `OverflowError` ("int too large to convert to float") when the rounded
/// value does not fit in a double, like CPython.
fn float_of_int(value: &BigInt) -> Result<f64, PyError> {
    let converted = match value.to_i64() {
        // Exact below 2**53 and correctly rounded above, like the decimal
        // parse below.
        Some(small) => small as f64,
        None => value.to_string().parse::<f64>().unwrap_or(f64::INFINITY),
    };
    if converted.is_finite() {
        Ok(converted)
    } else {
        Err(PyError::overflow_error("int too large to convert to float"))
    }
}

/// `float(value)` for a decoded JSON value: bools are 0.0/1.0, integers are
/// rounded (`OverflowError` beyond the double range), strings parse with
/// [`py_float`].
///
/// # Errors
/// `TypeError` for `None`, lists and dicts, `ValueError` for strings
/// `float()` rejects, `OverflowError` for huge integers.
pub fn py_float_of(value: &Value) -> Result<f64, PyError> {
    match value {
        Value::Bool(flag) => Ok(if *flag { 1.0 } else { 0.0 }),
        Value::Number(number) => match py_number(number) {
            PyNumber::Int(int) => float_of_int(&int),
            PyNumber::Float(float) => Ok(float),
        },
        Value::String(text) => py_float(text),
        other => Err(PyError::new(
            PyErrorKind::TypeError,
            format!(
                "float() argument must be a string or a real number, not '{}'",
                py_type_name(other)
            ),
        )),
    }
}

/// Clamp an arbitrary integer into `i64`.
pub fn bigint_to_i64_saturating(value: &BigInt) -> i64 {
    value.to_i64().unwrap_or(if value.sign() == Sign::Minus {
        i64::MIN
    } else {
        i64::MAX
    })
}

/// `numerator / denominator` for Python `int`s (`long_true_divide`): the
/// exact quotient correctly rounded to a float (round half to even).
///
/// # Errors
/// `OverflowError: integer division result too large for a float` when the
/// quotient exceeds the double range.
///
/// # Panics
/// When `denominator` is zero (no caller divides by zero).
pub fn py_int_true_divide(numerator: &BigInt, denominator: u32) -> Result<f64, PyError> {
    assert!(denominator != 0, "division by zero");
    let negative = numerator.sign() == Sign::Minus;
    let n = numerator.magnitude();
    let d = num_bigint::BigUint::from(denominator);
    if n.bits() <= 53 {
        // Both operands are exact doubles; IEEE division rounds correctly.
        let quotient = n.to_f64().unwrap_or(0.0) / f64::from(denominator);
        return Ok(if negative { -quotient } else { quotient });
    }
    // Scale so the integer quotient has 55 or 56 significant bits, keep a
    // sticky bit for everything below, then round to 53 bits.
    let shift =
        i64::try_from(n.bits()).unwrap_or(i64::MAX) - i64::try_from(d.bits()).unwrap_or(0) - 55;
    let (scaled, mut sticky) = if shift >= 0 {
        let shift_u = u64::try_from(shift).unwrap_or(0);
        let scaled = n >> shift_u;
        let dropped = n - (&scaled << shift_u);
        (scaled, dropped.bits() != 0)
    } else {
        (n << u64::try_from(-shift).unwrap_or(0), false)
    };
    let quotient = &scaled / &d;
    sticky |= (&scaled - &quotient * &d).bits() != 0;
    let extra = quotient.bits() - 53;
    let mut mantissa = (&quotient >> extra).to_u64().unwrap_or(0);
    let rest = (&quotient - (num_bigint::BigUint::from(mantissa) << extra))
        .to_u64()
        .unwrap_or(0);
    let half = 1u64 << (extra - 1);
    if rest > half || (rest == half && (sticky || mantissa & 1 == 1)) {
        mantissa += 1;
    }
    let mut exponent = shift + i64::try_from(extra).unwrap_or(0);
    if mantissa == 1 << 53 {
        mantissa >>= 1;
        exponent += 1;
    }
    // mantissa has 53 bits: value = mantissa * 2^exponent < 2^(53 + exponent).
    if exponent + 53 > 1024 {
        return Err(PyError::overflow_error(
            "integer division result too large for a float",
        ));
    }
    let exponent = i32::try_from(exponent).expect("exponent within the double range");
    let value = mantissa as f64 * 2f64.powi(exponent);
    Ok(if negative { -value } else { value })
}
