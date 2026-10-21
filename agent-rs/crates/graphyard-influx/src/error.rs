//! The errors of the query path, as the Python exceptions they reproduce.
//!
//! The conditions run stores `evaluation error: <str(err)>` (PLAN.md 3.5),
//! so every error carries Python's `str()` of the exception it stands for;
//! the class name is kept for tests and the auto-mode fallback.

use std::fmt;

use graphyard_core::pycompat::PyError;

/// An exception raised by `influx.query_range` / `query_condition_window`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryError {
    /// `influx.InfluxConfigurationError` (settings, SQL identifiers, the v3
    /// SQL errors, non-finite samples).
    Config(String),
    /// `influxdb_client.rest.ApiException`: a non-2xx response to the v2
    /// query. Only status 404 makes mode `auto` fall back to v3. The text is
    /// the client's rendering with the response headers and body (P2).
    Api { status: u16, message: String },
    /// Any other exception on the path, with its Python class name and
    /// `str()`: `ValueError`, `KeyError`, `IndexError`, `AttributeError`,
    /// `OverflowError`, `FluxQueryException`, urllib3 transport errors, ...
    Python {
        class: &'static str,
        message: String,
    },
}

impl QueryError {
    pub(crate) fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    pub(crate) fn python(class: &'static str, message: impl Into<String>) -> Self {
        Self::Python {
            class,
            message: message.into(),
        }
    }

    /// `IndexError: list index out of range`.
    pub(crate) fn index_error() -> Self {
        Self::python("IndexError", "list index out of range")
    }

    /// The Python class name of the exception.
    pub fn python_class(&self) -> &'static str {
        match self {
            Self::Config(_) => "InfluxConfigurationError",
            Self::Api { .. } => "ApiException",
            Self::Python { class, .. } => class,
        }
    }

    /// `influx._is_v2_query_missing`: an `ApiException` with status 404.
    pub fn is_v2_query_missing(&self) -> bool {
        matches!(self, Self::Api { status: 404, .. })
    }
}

impl fmt::Display for QueryError {
    /// `str(err)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) | Self::Api { message, .. } | Self::Python { message, .. } => {
                f.write_str(message)
            }
        }
    }
}

impl std::error::Error for QueryError {}

impl From<crate::write::WriteError> for QueryError {
    /// The v2 query shares the write client's urllib3 request path, so it
    /// raises the same exceptions.
    fn from(err: crate::write::WriteError) -> Self {
        use crate::write::WriteError;
        match err {
            WriteError::Configuration(err) => Self::Config(err.0),
            WriteError::Timeout(err) => err.into(),
            WriteError::Api { status, message } => Self::Api { status, message },
            WriteError::Transport { class, message } => Self::Python { class, message },
        }
    }
}

impl From<PyError> for QueryError {
    fn from(err: PyError) -> Self {
        Self::Python {
            class: err.python_class(),
            message: err.message,
        }
    }
}
