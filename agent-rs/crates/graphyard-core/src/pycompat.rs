//! CPython compatibility helpers (PLAN.md section 4.4).
//!
//! Small functions where no crate reproduces CPython and the agent's output
//! depends on the exact Python result: `repr(float)`, `format(x, "g")`,
//! `float()` and `int()` of strings, `str()`/`repr()` of decoded JSON values,
//! `spec_config.py:_describe`, `str.strip()`/`str.split()` and Python `==`
//! across `bool`/`int`/`float`. Pinned by `fixtures/pycompat.json`
//! (`fixtures/gen_pycompat.py`).
//!
//! JSON values are `serde_json::Value` parsed with `arbitrary_precision`: a
//! number keeps its token text, so it is an `int` exactly when Python's
//! `json.loads` makes it one (no fraction, no exponent), integers of any size
//! stay exact and float tokens are converted like `float(token)` (overflow
//! gives `inf`, as in `json.loads`). The functions here and in
//! [`crate::spec_config`] read numbers only through [`py_number`]. Documents
//! must be decoded with [`json_loads`], not `serde_json::from_str::<Value>`:
//! with `arbitrary_precision` the latter turns an object whose only key is
//! `serde_json`'s private number token into a number, where Python keeps the
//! `dict`.
//!
//! Inputs are ASCII where the plan asks for it (D-6): non-ASCII digits are
//! rejected by [`py_int`]/[`py_float`], and [`py_str_repr`] escapes only the
//! non-printable characters it can classify without CPython's Unicode
//! database (P2, section 2.3).
//!
//! The functions were adapted from the nyxmon agent's `pyjson` module
//! (nyxmon `agent-rs/crates/nyxmon-core/src/pyjson`) with their fixture
//! cases, as the plan asks, instead of depending on that repository.

mod json;
mod num;
mod text;
mod value;

use std::fmt;

pub use json::json_loads;
pub use num::{
    PyNumber, bigint_to_i64_saturating, py_float, py_float_of, py_float_repr, py_format_g, py_int,
    py_number,
};
pub use text::{py_isspace, py_split_whitespace, py_str_repr, py_strip};
pub use value::{
    MAX_INT_STR_DIGITS, describe_text, int_digit_limit_error, py_describe, py_eq, py_repr, py_str,
    py_type_name,
};

/// The class of a Python exception a helper reproduces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PyErrorKind {
    ValueError,
    TypeError,
    OverflowError,
    RecursionError,
}

impl PyErrorKind {
    /// The Python class name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValueError => "ValueError",
            Self::TypeError => "TypeError",
            Self::OverflowError => "OverflowError",
            Self::RecursionError => "RecursionError",
        }
    }
}

/// A Python exception: its class and `str(exc)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PyError {
    pub kind: PyErrorKind,
    pub message: String,
}

impl PyError {
    pub fn new(kind: PyErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn value_error(message: impl Into<String>) -> Self {
        Self::new(PyErrorKind::ValueError, message)
    }

    pub fn type_error(message: impl Into<String>) -> Self {
        Self::new(PyErrorKind::TypeError, message)
    }

    pub fn overflow_error(message: impl Into<String>) -> Self {
        Self::new(PyErrorKind::OverflowError, message)
    }

    /// The Python class name.
    pub const fn python_class(&self) -> &'static str {
        self.kind.as_str()
    }
}

impl fmt::Display for PyError {
    /// `str(exc)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PyError {}

#[cfg(test)]
mod tests;
