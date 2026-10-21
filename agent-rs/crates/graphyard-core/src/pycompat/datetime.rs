//! Python `datetime`/`timedelta` arithmetic on a microsecond timeline.
//!
//! Python datetimes have microsecond resolution and the range
//! `0001-01-01 00:00:00` to `9999-12-31 23:59:59.999999`; `timedelta`
//! allows at most 999 999 999 days. The condition evaluation computes with
//! microseconds since the Unix epoch (`DateTime::timestamp_micros`) and
//! these helpers reproduce where CPython raises and how
//! `timedelta.total_seconds()` rounds.
//!
//! The second part (S4b) is CPython 3.12's `datetime.datetime.fromisoformat`
//! with the `datetime` renderings the agent needs (`str()`, `isoformat()`,
//! the UTC conversion of `influx._ensure_utc`).
//!
//! The parser is a port of the C implementation (`Modules/_datetimemodule.c`
//! 3.12: `_find_isoformat_datetime_separator`, `parse_isoformat_date`,
//! `parse_hh_mm_ss_ff`, `parse_isoformat_time`, `datetime_fromisoformat`),
//! which the agent's Python dependencies use for InfluxDB timestamps
//! (`influxdb_client`'s date helper without `ciso8601`, `influx.py:
//! _parse_influx_ts`) and for Home Assistant's `last_updated`. It works on
//! the UTF-8 bytes like the C code, reading a NUL past the end, so embedded
//! NULs and multi-byte separators behave as in CPython. Pinned by
//! `fixtures/isoformat.json`.

use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc, Weekday};

use super::{PyError, py_str_repr};

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

// ---------------------------------------------------------------------------
// datetime.fromisoformat and renderings (S4b)
// ---------------------------------------------------------------------------

use std::fmt;

const MIN_YEAR: i32 = 1;
const MAX_YEAR: i32 = 9999;
const US_PER_SECOND: i64 = 1_000_000;
const US_PER_DAY: i64 = 86_400 * US_PER_SECOND;

/// A `datetime.datetime` as `fromisoformat` builds it: naive, or aware with a
/// fixed UTC offset (`datetime.timezone`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PyDateTime {
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub microsecond: u32,
    /// `utcoffset()` in microseconds; `None` for a naive datetime.
    pub offset_us: Option<i64>,
}

impl PyDateTime {
    /// The naive UTC wall time of the fields (no offset applied).
    fn naive(&self) -> chrono::NaiveDateTime {
        NaiveDate::from_ymd_opt(self.year, self.month, self.day)
            .and_then(|date| {
                date.and_hms_micro_opt(self.hour, self.minute, self.second, self.microsecond)
            })
            .expect("PyDateTime fields are validated on construction")
    }

    /// `influx._ensure_utc`: a naive datetime is taken as UTC
    /// (`replace(tzinfo=UTC)`), an aware one converted (`astimezone(UTC)`).
    ///
    /// # Errors
    /// `OverflowError: date value out of range` when the conversion leaves
    /// Python's year range 1..9999.
    pub fn to_utc(&self) -> Result<DateTime<Utc>, PyError> {
        let naive = self.naive();
        let utc = match self.offset_us {
            None => naive,
            Some(offset) => naive - Duration::microseconds(offset),
        };
        if !(MIN_YEAR..=MAX_YEAR).contains(&utc.year()) {
            return Err(date_out_of_range());
        }
        Ok(utc.and_utc())
    }
}

/// `OverflowError` of datetime arithmetic leaving years 1..9999.
pub fn date_out_of_range() -> PyError {
    PyError::overflow_error("date value out of range")
}

impl fmt::Display for PyDateTime {
    /// `str(datetime)`: `isoformat(sep=" ")`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )?;
        if self.microsecond != 0 {
            write!(f, ".{:06}", self.microsecond)?;
        }
        if let Some(offset) = self.offset_us {
            f.write_str(&format_offset(offset))?;
        }
        Ok(())
    }
}

/// `datetime._format_offset` with `sep=":"`: `+HH:MM`, plus `:SS` and
/// `.ffffff` when not zero.
fn format_offset(offset_us: i64) -> String {
    let sign = if offset_us < 0 { '-' } else { '+' };
    let total = offset_us.unsigned_abs();
    let micro = total % 1_000_000;
    let seconds = total / 1_000_000;
    let (hh, mm, ss) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    let mut out = format!("{sign}{hh:02}:{mm:02}");
    if ss != 0 || micro != 0 {
        out.push_str(&format!(":{ss:02}"));
        if micro != 0 {
            out.push_str(&format!(".{micro:06}"));
        }
    }
    out
}

/// `datetime.isoformat()` of an aware UTC datetime
/// (`YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00`).
pub fn py_isoformat_utc(dt: &DateTime<Utc>) -> String {
    let mut out = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );
    let micro = dt.timestamp_subsec_micros();
    if micro != 0 {
        out.push_str(&format!(".{micro:06}"));
    }
    out.push_str("+00:00");
    out
}

/// `repr(timedelta(microseconds=us))`, e.g. `datetime.timedelta(days=-1,
/// seconds=60)`.
pub fn py_timedelta_repr(us: i64) -> String {
    let days = us.div_euclid(US_PER_DAY);
    let rest = us.rem_euclid(US_PER_DAY);
    let (seconds, micro) = (rest / US_PER_SECOND, rest % US_PER_SECOND);
    let mut parts = Vec::new();
    if days != 0 {
        parts.push(format!("days={days}"));
    }
    if seconds != 0 {
        parts.push(format!("seconds={seconds}"));
    }
    if micro != 0 {
        parts.push(format!("microseconds={micro}"));
    }
    if parts.is_empty() {
        "datetime.timedelta(0)".to_owned()
    } else {
        format!("datetime.timedelta({})", parts.join(", "))
    }
}

/// The C string view: the UTF-8 bytes followed by NUL.
struct CStr<'a>(&'a [u8]);

impl CStr<'_> {
    fn at(&self, index: usize) -> u8 {
        self.0.get(index).copied().unwrap_or(0)
    }

    fn is_digit(&self, index: usize) -> bool {
        self.at(index).is_ascii_digit()
    }

    /// `parse_digits`: the position after `count` digits and their value.
    fn digits(&self, start: usize, count: usize) -> Option<(usize, i64)> {
        let mut value = 0;
        for index in start..start + count {
            if !self.is_digit(index) {
                return None;
            }
            value = value * 10 + i64::from(self.at(index) - b'0');
        }
        Some((start + count, value))
    }
}

/// `_find_isoformat_datetime_separator`; `None` for its `-1`.
fn find_separator(s: &CStr<'_>, len: usize) -> Option<usize> {
    if len == 7 {
        return Some(7);
    }
    if s.at(4) == b'-' {
        if s.at(5) == b'W' {
            if len < 8 {
                return None;
            }
            if len > 8 && s.at(8) == b'-' {
                if len == 9 {
                    return None;
                }
                if len > 10 && s.is_digit(10) {
                    return Some(8);
                }
                return Some(10);
            }
            return Some(8);
        }
        return Some(10);
    }
    if s.at(4) == b'W' {
        let mut index = 7;
        while index < len && s.is_digit(index) {
            index += 1;
        }
        if index < 9 {
            return Some(index);
        }
        return Some(if index % 2 == 0 { 7 } else { 8 });
    }
    Some(8)
}

/// `iso_to_ymd`; `Err` for an invalid ISO year, week or weekday.
fn iso_to_ymd(iso_year: i64, iso_week: i64, iso_day: i64) -> Result<(i64, i64, i64), ()> {
    if !(i64::from(MIN_YEAR)..=i64::from(MAX_YEAR)).contains(&iso_year) {
        return Err(());
    }
    let year = i32::try_from(iso_year).map_err(|_| ())?;
    if !(1..53).contains(&iso_week) {
        let first = NaiveDate::from_ymd_opt(year, 1, 1).ok_or(())?.weekday();
        let leap = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
        let long_year = first == Weekday::Thu || (first == Weekday::Wed && leap);
        if iso_week != 53 || !long_year {
            return Err(());
        }
    }
    if !(1..8).contains(&iso_day) {
        return Err(());
    }
    let monday = NaiveDate::from_isoywd_opt(year, 1, Weekday::Mon).ok_or(())?;
    let date = monday + Duration::days((iso_week - 1) * 7 + iso_day - 1);
    Ok((
        i64::from(date.year()),
        i64::from(date.month()),
        i64::from(date.day()),
    ))
}

/// `parse_isoformat_date`: 0 or a negative failure code, and the fields.
fn parse_date(s: &CStr<'_>, len: Option<usize>) -> (i32, [i64; 3]) {
    let len = len.unwrap_or(usize::MAX);
    let mut ymd = [0i64; 3];
    let Some((mut p, year)) = s.digits(0, 4) else {
        return (-1, ymd);
    };
    ymd[0] = year;
    let uses_separator = s.at(p) == b'-';
    if uses_separator {
        p += 1;
    }
    if s.at(p) == b'W' {
        p += 1;
        let Some((after_week, week)) = s.digits(p, 2) else {
            return (-3, ymd);
        };
        p = after_week;
        let day = if p < len {
            if uses_separator {
                let separator = s.at(p);
                p += 1;
                if separator != b'-' {
                    return (-2, ymd);
                }
            }
            match s.digits(p, 1) {
                Some((_, day)) => day,
                None => return (-4, ymd),
            }
        } else {
            1
        };
        return match iso_to_ymd(year, week, day) {
            Ok((y, m, d)) => (0, [y, m, d]),
            Err(()) => (-5, ymd),
        };
    }
    let Some((after_month, month)) = s.digits(p, 2) else {
        return (-1, ymd);
    };
    ymd[1] = month;
    p = after_month;
    if uses_separator {
        let separator = s.at(p);
        p += 1;
        if separator != b'-' {
            return (-2, ymd);
        }
    }
    let Some((_, day)) = s.digits(p, 2) else {
        return (-1, ymd);
    };
    ymd[2] = day;
    (0, ymd)
}

/// `parse_hh_mm_ss_ff` over `[start, end)`: 0, 1 (not at the end of the
/// string) or a negative failure code, and `[hour, minute, second, us]`.
#[allow(clippy::needless_range_loop)]
fn parse_hms(s: &CStr<'_>, start: usize, end: usize) -> (i32, [i64; 4]) {
    let mut vals = [0i64; 4];
    let mut p = start;
    let mut has_separator = true;
    for i in 0..3 {
        let Some((after, value)) = s.digits(p, 2) else {
            return (-3, vals);
        };
        vals[i] = value;
        p = after;
        let c = s.at(p);
        p += 1;
        if i == 0 {
            has_separator = c == b':';
        }
        if p >= end {
            return (i32::from(c != 0), vals);
        } else if has_separator && c == b':' {
            continue;
        } else if c == b'.' || c == b',' {
            break;
        } else if !has_separator {
            p -= 1;
        } else {
            return (-4, vals);
        }
    }
    // `p < end` here, so at least one fraction digit is parsed.
    let to_parse = (end - p).min(6);
    let Some((after, micro)) = s.digits(p, to_parse) else {
        return (-3, vals);
    };
    const CORRECTION: [i64; 6] = [0, 100_000, 10_000, 1_000, 100, 10];
    vals[3] = match CORRECTION.get(to_parse) {
        Some(factor) => micro * factor,
        None => micro,
    };
    p = after;
    while s.is_digit(p) {
        p += 1;
    }
    (i32::from(s.at(p) != 0), vals)
}

/// The parsed time: fields and, for code 1, the offset in seconds and its
/// microseconds (same sign).
struct TimePart {
    code: i32,
    hms: [i64; 4],
    offset_seconds: i64,
    offset_us: i64,
}

/// `parse_isoformat_time` over `[start, start + len)`.
fn parse_time(s: &CStr<'_>, start: usize, len: usize) -> TimePart {
    let end = start + len;
    let mut tz = start;
    loop {
        if matches!(s.at(tz), b'Z' | b'+' | b'-') {
            break;
        }
        tz += 1;
        if tz >= end {
            break;
        }
    }
    let (code, hms) = parse_hms(s, start, tz);
    let mut part = TimePart {
        code,
        hms,
        offset_seconds: 0,
        offset_us: 0,
    };
    if code < 0 {
        return part;
    }
    if tz == end {
        part.code = if code == 1 { -5 } else { 0 };
        return part;
    }
    if s.at(tz) == b'Z' {
        part.code = if s.at(tz + 1) != 0 { -5 } else { 1 };
        return part;
    }
    let sign = if s.at(tz) == b'-' { -1 } else { 1 };
    let (tz_code, tz_hms) = parse_hms(s, tz + 1, end);
    part.offset_seconds = sign * (tz_hms[0] * 3600 + tz_hms[1] * 60 + tz_hms[2]);
    part.offset_us = sign * tz_hms[3];
    part.code = if tz_code != 0 { -5 } else { 1 };
    part
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// `datetime.datetime.fromisoformat(text)` (CPython 3.12, C implementation).
///
/// # Errors
/// `ValueError` with Python's message: `Invalid isoformat string: <repr>`,
/// the offset range message, or the field range messages of the datetime
/// constructor (`month must be in 1..12`, ...).
pub fn py_fromisoformat(text: &str) -> Result<PyDateTime, PyError> {
    let invalid =
        || PyError::value_error(format!("Invalid isoformat string: {}", py_str_repr(text)));
    if text.chars().count() < 7 {
        return Err(invalid());
    }
    let s = CStr(text.as_bytes());
    let len = text.len();
    let separator = find_separator(&s, len);
    let (mut code, ymd) = parse_date(&s, separator);
    let mut time = TimePart {
        code: 0,
        hms: [0; 4],
        offset_seconds: 0,
        offset_us: 0,
    };
    if code == 0 {
        // A `-1` separator always fails the date (C: `p += -1` is never
        // reached with a successful date).
        let Some(separator) = separator else {
            return Err(invalid());
        };
        if len > separator {
            let lead = s.at(separator);
            let width = if lead & 0x80 == 0 {
                1
            } else {
                match lead & 0xf0 {
                    0xe0 => 3,
                    0xf0 => 4,
                    _ => 2,
                }
            };
            let start = separator + width;
            time = parse_time(&s, start, len.saturating_sub(start));
            code = time.code;
        }
    }
    if code < 0 {
        return Err(invalid());
    }
    // `tzinfo_from_isoformat_results` runs before the datetime constructor.
    let offset_us = if code == 1 {
        if time.offset_seconds == 0 {
            Some(0)
        } else {
            let total = time.offset_seconds * US_PER_SECOND + time.offset_us;
            if total <= -US_PER_DAY || total >= US_PER_DAY {
                return Err(PyError::value_error(format!(
                    "offset must be a timedelta strictly between -timedelta(hours=24) and \
                     timedelta(hours=24), not {}.",
                    py_timedelta_repr(total)
                )));
            }
            Some(total)
        }
    } else {
        None
    };
    let [year, month, day] = ymd;
    let [hour, minute, second, microsecond] = time.hms;
    if !(i64::from(MIN_YEAR)..=i64::from(MAX_YEAR)).contains(&year) {
        return Err(PyError::value_error(format!("year {year} is out of range")));
    }
    if !(1..=12).contains(&month) {
        return Err(PyError::value_error("month must be in 1..12"));
    }
    if day < 1 || day > days_in_month(year, month) {
        return Err(PyError::value_error("day is out of range for month"));
    }
    let checks = [
        (hour, 23, "hour must be in 0..23"),
        (minute, 59, "minute must be in 0..59"),
        (second, 59, "second must be in 0..59"),
        (microsecond, 999_999, "microsecond must be in 0..999999"),
    ];
    for (value, max, message) in checks {
        if !(0..=max).contains(&value) {
            return Err(PyError::value_error(message));
        }
    }
    // Every field is range-checked above, so the narrowing casts are exact.
    let field = |value: i64| u32::try_from(value).expect("checked field");
    Ok(PyDateTime {
        year: i32::try_from(year).expect("checked year"),
        month: field(month),
        day: field(day),
        hour: field(hour),
        minute: field(minute),
        second: field(second),
        microsecond: field(microsecond),
        offset_us,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;
    use crate::fixture::{Field, load};

    fn cases() -> Vec<Value> {
        load(include_str!("../../../../fixtures/isoformat.json"))
            .as_array()
            .expect("isoformat.json is a list")
            .clone()
    }

    #[test]
    fn fromisoformat_matches_cpython() {
        let cases = cases();
        assert!(cases.len() > 100);
        for case in &cases {
            let input = case.str_field("input");
            let actual = py_fromisoformat(input);
            if let Some(error) = case.get("error") {
                let err = actual.expect_err(input);
                assert_eq!(err.python_class(), "ValueError", "{input:?}");
                assert_eq!(err.message, error.as_str().expect("message"), "{input:?}");
                continue;
            }
            let dt = actual.unwrap_or_else(|err| panic!("{input:?}: {err}"));
            let fields: Vec<i64> = case
                .list_field("fields")
                .iter()
                .map(|v| v.as_i64().expect("field"))
                .collect();
            assert_eq!(
                fields,
                [
                    i64::from(dt.year),
                    i64::from(dt.month),
                    i64::from(dt.day),
                    i64::from(dt.hour),
                    i64::from(dt.minute),
                    i64::from(dt.second),
                    i64::from(dt.microsecond),
                ],
                "{input:?}"
            );
            assert_eq!(dt.offset_us, case.field("offset_us").as_i64(), "{input:?}");
            assert_eq!(dt.to_string(), case.str_field("str"), "{input:?}");
            match case.get("utc") {
                Some(utc) => assert_eq!(
                    py_isoformat_utc(&dt.to_utc().expect(input)),
                    utc.as_str().expect("utc"),
                    "{input:?}"
                ),
                None => {
                    let err = dt.to_utc().expect_err(input);
                    let expected = case.field("utc_error");
                    assert_eq!(err.python_class(), expected.str_field("class"));
                    assert_eq!(err.message, expected.str_field("message"));
                }
            }
        }
    }

    #[test]
    fn timedelta_repr_matches_python() {
        assert_eq!(py_timedelta_repr(0), "datetime.timedelta(0)");
        assert_eq!(py_timedelta_repr(US_PER_DAY), "datetime.timedelta(days=1)");
        assert_eq!(
            py_timedelta_repr(-US_PER_DAY + 60 * US_PER_SECOND),
            "datetime.timedelta(days=-1, seconds=60)"
        );
        assert_eq!(
            py_timedelta_repr(-1),
            "datetime.timedelta(days=-1, seconds=86399, microseconds=999999)"
        );
    }
}
