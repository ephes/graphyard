//! Column conversions shared by the row loaders (PLAN.md 5.3).
//!
//! Django's SQLite backend reads with `detect_types=PARSE_DECLTYPES`, so
//! the columns declared `bool` and `datetime` go through the converters it
//! registers, which receive the value's text form whatever its storage
//! class. The loaders therefore select those columns as `CAST(col AS TEXT)`
//! (SQLite renders INTEGER and REAL values exactly as `sqlite3_column_blob`
//! hands them to Python's converter).

use chrono::{DateTime, Utc};

use crate::datetime::{DbDateTime, parse_column};
use crate::error::StoreError;

/// The `bool` converter `b"1".__eq__`: true only for the text `1` (so a
/// stored `2` loads as false, although `WHERE enabled` selects it); NULL is
/// `None`, which Python treats as false.
pub(crate) fn bool_column(text: Option<&str>) -> bool {
    text == Some("1")
}

/// The `datetime` converter followed by `convert_datetimefield_value`.
pub(crate) fn datetime_column(text: Option<&str>) -> Result<Option<DbDateTime>, StoreError> {
    Ok(parse_column(text)?)
}

/// A clock value as Django writes it (`str()` of the naive UTC datetime,
/// microsecond precision).
pub(crate) fn now_text(now: DateTime<Utc>) -> Result<String, StoreError> {
    Ok(DbDateTime::from_utc(now).to_db_text()?)
}
