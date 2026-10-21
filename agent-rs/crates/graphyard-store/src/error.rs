//! Store errors.

use std::fmt;

use graphyard_core::pycompat::PyError;

use crate::datetime::DateTimeError;

/// Why a store operation failed. `Display` is the text Python's `str(err)`
/// shows for the same failure where the store reproduces it (P1 for
/// [`StoreError::NotUpdated`], P2 otherwise; PLAN.md 2.3).
#[derive(Debug)]
pub enum StoreError {
    /// An SQLite error (busy timeout, constraint, I/O, a value of an
    /// unexpected storage class, ...). An open transaction was rolled back.
    Sqlite(rusqlite::Error),
    /// A stored datetime that Django's loader rejects, or a value that
    /// cannot be written back (PLAN.md 5.3).
    DateTime(DateTimeError),
    /// `save(update_fields=...)` matched no row (the row was deleted after
    /// it was loaded): Django's `NotUpdated`.
    NotUpdated,
    /// The DB thread is gone (a panicking operation or [`crate::Store::close`]).
    ThreadGone,
    /// Spawning the DB thread failed.
    Io(std::io::Error),
    /// A metric point failed `normalize_metric_point` before the registry
    /// touch read anything (the `ValueError` text is P1).
    Point(PyError),
}

/// Django's message for a `save(update_fields=...)` that matched no row
/// (P1, PLAN.md 5.4).
pub const NOT_UPDATED_MESSAGE: &str = "Save with update_fields did not affect any rows.";

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StoreError::Sqlite(err) => write!(f, "{err}"),
            StoreError::DateTime(err) => write!(f, "{err}"),
            StoreError::NotUpdated => f.write_str(NOT_UPDATED_MESSAGE),
            StoreError::ThreadGone => f.write_str("the database thread is gone"),
            StoreError::Io(err) => write!(f, "{err}"),
            StoreError::Point(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StoreError::Sqlite(err) => Some(err),
            StoreError::DateTime(err) => Some(err),
            StoreError::Io(err) => Some(err),
            StoreError::NotUpdated | StoreError::ThreadGone | StoreError::Point(_) => None,
        }
    }
}

impl From<rusqlite::Error> for StoreError {
    fn from(err: rusqlite::Error) -> Self {
        StoreError::Sqlite(err)
    }
}

impl From<DateTimeError> for StoreError {
    fn from(err: DateTimeError) -> Self {
        StoreError::DateTime(err)
    }
}

impl StoreError {
    /// `type(err).__name__` of the exception Python raises for the same
    /// failure: Django's `IntegrityError` for a constraint violation and
    /// `OperationalError` for other SQLite errors, Django's `NotUpdated`,
    /// the class of a conversion error. The agent reports it where an
    /// executor's sink failure escapes (`collector raised <class>: ...`).
    pub fn python_class(&self) -> &'static str {
        match self {
            StoreError::Sqlite(rusqlite::Error::SqliteFailure(failure, _))
                if failure.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                "IntegrityError"
            }
            StoreError::Sqlite(_) | StoreError::ThreadGone => "OperationalError",
            StoreError::DateTime(err) => err.class,
            StoreError::NotUpdated => "NotUpdated",
            StoreError::Io(_) => "OSError",
            StoreError::Point(err) => err.python_class(),
        }
    }

    /// `record_heartbeat`'s retry test: an SQLite error (Python's
    /// `OperationalError`) whose text contains `database is locked`, ignoring
    /// case. SQLite reports a busy timeout with exactly that text.
    pub fn is_database_locked(&self) -> bool {
        match self {
            StoreError::Sqlite(err @ rusqlite::Error::SqliteFailure(..)) => err
                .to_string()
                .to_lowercase()
                .contains("database is locked"),
            _ => false,
        }
    }
}
