//! Python `datetime`/`timedelta` arithmetic on a microsecond timeline.
//!
//! Python datetimes have microsecond resolution and the range
//! `0001-01-01 00:00:00` to `9999-12-31 23:59:59.999999`; `timedelta`
//! allows at most 999 999 999 days. The condition evaluation computes with
//! microseconds since the Unix epoch (`DateTime::timestamp_micros`) and
//! these helpers reproduce where CPython raises and how
//! `timedelta.total_seconds()` rounds.

use super::PyError;

/// `datetime.min` (`0001-01-01T00:00:00Z`) in microseconds since the epoch.
pub const PY_DATETIME_MIN_MICROS: i64 = -62_135_596_800_000_000;
/// `datetime.max` (`9999-12-31T23:59:59.999999Z`) in microseconds since the
/// epoch.
pub const PY_DATETIME_MAX_MICROS: i64 = 253_402_300_799_999_999;

const MICROS_PER_SECOND: u64 = 1_000_000;
const MICROS_PER_MINUTE: i128 = 60_000_000;
const MINUTES_PER_DAY: i64 = 1440;
const TIMEDELTA_MAX_DAYS: i64 = 999_999_999;

/// `timedelta(minutes=minutes)` in microseconds, or the `OverflowError` of
/// the constructor: days outside a C `int` cannot be converted, days beyond
/// `timedelta.max.days` are out of range.
pub fn py_timedelta_minutes(minutes: i64) -> Result<i128, PyError> {
    let days = minutes.div_euclid(MINUTES_PER_DAY);
    if i32::try_from(days).is_err() {
        return Err(PyError::overflow_error(
            "Python int too large to convert to C int",
        ));
    }
    if days.abs() > TIMEDELTA_MAX_DAYS {
        return Err(PyError::overflow_error(format!(
            "days={days}; must have magnitude <= {TIMEDELTA_MAX_DAYS}"
        )));
    }
    Ok(i128::from(minutes) * MICROS_PER_MINUTE)
}

/// `datetime + timedelta` on microseconds, or `OverflowError("date value out
/// of range")` when the result leaves Python's datetime range.
pub fn py_datetime_add(micros: i64, delta_micros: i128) -> Result<i64, PyError> {
    let result = i128::from(micros) + delta_micros;
    if result < i128::from(PY_DATETIME_MIN_MICROS) || result > i128::from(PY_DATETIME_MAX_MICROS) {
        return Err(PyError::overflow_error("date value out of range"));
    }
    // In range, so it fits.
    Ok(i64::try_from(result).unwrap_or_default())
}

/// `timedelta(microseconds=micros).total_seconds()`: the microsecond count
/// divided by 10**6 as CPython's `int / int` does, correctly rounded (ties to
/// even). Up to 2**53 both operands are exact doubles and one IEEE division
/// rounds correctly (CPython's fast path); beyond that (differences of more
/// than about 285 years) the exact decimal quotient is parsed, which Rust
/// also rounds correctly.
pub fn py_total_seconds(micros: i64) -> f64 {
    let magnitude = micros.unsigned_abs();
    let seconds = if magnitude <= 1 << 53 {
        magnitude as f64 / MICROS_PER_SECOND as f64
    } else {
        format!(
            "{}.{:06}",
            magnitude / MICROS_PER_SECOND,
            magnitude % MICROS_PER_SECOND
        )
        .parse()
        .unwrap_or(f64::NAN)
    };
    if micros < 0 { -seconds } else { seconds }
}
