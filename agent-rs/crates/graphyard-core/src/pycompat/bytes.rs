//! `bytes` helpers: `base64.b64decode(text)` (non-strict) and `repr(bytes)`.
//!
//! `influxdb_client` decodes `base64Binary` columns of a Flux result with
//! `base64.b64decode`, and a decoded value reaches graphyard text as
//! `str(bytes)`. Pinned by the `b64decode` cases of `fixtures/flux.json`.

use std::fmt::Write as _;

use super::{PyError, PyErrorKind};

/// The value of a base64 alphabet byte (`table_a2b_base64`), `None` for
/// any other byte.
fn sextet(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

fn binascii_error(message: impl Into<String>) -> PyError {
    PyError::new(PyErrorKind::BinasciiError, message)
}

/// `base64.b64decode(text)` with `validate=False`: CPython 3.12's
/// `binascii.a2b_base64(strict_mode=False)` (bytes outside the alphabet are
/// skipped, a complete pad sequence ends the input).
///
/// # Errors
/// `ValueError` for non-ASCII text; `binascii.Error` (`Incorrect padding`,
/// the "1 more than a multiple of 4" message) as in Python.
pub fn py_b64decode(text: &str) -> Result<Vec<u8>, PyError> {
    if !text.is_ascii() {
        return Err(PyError::value_error(
            "string argument should contain only ASCII characters",
        ));
    }
    let mut out = Vec::with_capacity(text.len() / 4 * 3 + 3);
    let mut quad_pos = 0;
    let mut left: u8 = 0;
    let mut pads = 0;
    for &byte in text.as_bytes() {
        if byte == b'=' {
            if quad_pos >= 2 {
                pads += 1;
                if quad_pos + pads >= 4 {
                    return Ok(out);
                }
            }
            continue;
        }
        let Some(value) = sextet(byte) else {
            continue;
        };
        pads = 0;
        match quad_pos {
            0 => {
                quad_pos = 1;
                left = value;
            }
            1 => {
                quad_pos = 2;
                out.push((left << 2) | (value >> 4));
                left = value & 0x0f;
            }
            2 => {
                quad_pos = 3;
                out.push((left << 4) | (value >> 2));
                left = value & 0x03;
            }
            _ => {
                quad_pos = 0;
                out.push((left << 6) | value);
                left = 0;
            }
        }
    }
    match quad_pos {
        0 => Ok(out),
        1 => Err(binascii_error(format!(
            "Invalid base64-encoded string: number of data characters ({}) cannot be 1 more \
             than a multiple of 4",
            out.len() / 3 * 4 + 1
        ))),
        _ => Err(binascii_error("Incorrect padding")),
    }
}

/// `repr(bytes)` (also `str(bytes)`): `b'...'`, double quotes only when the
/// value contains `'` and no `"`; `\\`, `\t`, `\n`, `\r` and the quote
/// escaped, other bytes outside `0x20..0x7e` as `\xNN`.
pub fn py_bytes_repr(value: &[u8]) -> String {
    let quote = if value.contains(&b'\'') && !value.contains(&b'"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(value.len() + 3);
    out.push('b');
    out.push(quote);
    for &byte in value {
        match byte {
            b'\\' => out.push_str("\\\\"),
            b'\t' => out.push_str("\\t"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b' '..=b'~' if char::from(byte) == quote => {
                out.push('\\');
                out.push(quote);
            }
            b' '..=b'~' => out.push(char::from(byte)),
            _ => {
                let _ = write!(out, "\\x{byte:02x}");
            }
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{Field, load};

    #[test]
    fn b64decode_and_bytes_repr_match_python() {
        let fixture = load(include_str!("../../../../fixtures/flux.json"));
        let cases = fixture.list_field("b64decode");
        assert!(cases.len() > 10);
        for case in cases {
            let input = case.str_field("input");
            match case.get("error") {
                Some(error) => {
                    let err = py_b64decode(input).expect_err(input);
                    assert_eq!(err.python_class(), error.str_field("class"), "{input:?}");
                    assert_eq!(err.message, error.str_field("message"), "{input:?}");
                }
                None => {
                    let decoded = py_b64decode(input).unwrap_or_else(|e| panic!("{input:?}: {e}"));
                    assert_eq!(py_bytes_repr(&decoded), case.str_field("str"), "{input:?}");
                }
            }
        }
    }

    #[test]
    fn bytes_repr_quote_selection() {
        assert_eq!(py_bytes_repr(b"it's"), "b\"it's\"");
        assert_eq!(py_bytes_repr(b"'\""), "b'\\'\"'");
        assert_eq!(py_bytes_repr(b""), "b''");
    }
}
