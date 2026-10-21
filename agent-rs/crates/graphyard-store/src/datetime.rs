//! Django's datetime storage on SQLite (PLAN.md 5.3).
//!
//! Writes: Django stores an aware datetime as its naive UTC text,
//! `str(datetime)`: `YYYY-MM-DD HH:MM:SS.ffffff`, without the fraction when
//! the microsecond is 0 (`operations.py:adapt_datetimefield_value`).
//!
//! Reads: Django's SQLite backend registers `parse_datetime` as the
//! converter of the `datetime` declared type (`base.py`), so a stored value
//! goes through Python's `sqlite3` converter (the value's text form, also for
//! INTEGER/REAL storage), `django.utils.dateparse.parse_datetime`
//! (CPython 3.12 `datetime.fromisoformat`, then Django's `datetime_re`) and
//! `convert_datetimefield_value` (naive means UTC). [`parse_column`] ports
//! that chain:
//!
//! - text that neither parser accepts is NULL (`parse_datetime` returns
//!   `None`);
//! - text that `datetime_re` matches with out-of-range fields raises
//!   `ValueError` (Django calls `datetime(...)` outside its `try`), which
//!   fails the whole load, as in Python;
//! - writing a loaded aware value back converts it to UTC, which raises
//!   `OverflowError` outside Python's year range 1..=9999.
//!
//! `fromisoformat` is ported from CPython 3.12's C implementation
//! (`Modules/_datetimemodule.c`, which runs in production), byte for byte
//! over the UTF-8 text. `datetime_re` accepts ASCII digits only where Python's
//! `\d` matches every Unicode decimal digit (D-6). Pinned by the
//! `datetime_codec.json` fixture.

use std::fmt;
use std::sync::LazyLock;

use chrono::{
    DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc, Weekday,
};
use regex::Regex;

/// A datetime column value as Django holds it after loading: an aware
/// datetime, kept as its UTC instant with microsecond precision. The instant
/// may lie outside Python's year range when the stored text carried an
/// offset; [`DbDateTime::to_db_text`] then fails like Python's conversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DbDateTime(NaiveDateTime);

impl DbDateTime {
    /// An instant from the clock, truncated to microseconds (Python's
    /// `datetime` resolution).
    pub fn from_utc(value: DateTime<Utc>) -> Self {
        Self::from_naive_utc(value.naive_utc())
    }

    /// A naive UTC datetime, truncated to microseconds.
    pub fn from_naive_utc(value: NaiveDateTime) -> Self {
        let micros = (value.nanosecond() / 1000).min(999_999);
        let truncated = value
            .with_nanosecond(micros * 1000)
            .expect("a whole number of microseconds below one second is valid");
        Self(truncated)
    }

    /// The UTC instant.
    pub fn naive_utc(self) -> NaiveDateTime {
        self.0
    }

    /// The UTC instant as an aware chrono value.
    pub fn to_utc(self) -> DateTime<Utc> {
        self.0.and_utc()
    }

    /// The stored text (`adapt_datetimefield_value`: naive UTC
    /// `str(datetime)`).
    ///
    /// # Errors
    /// `OverflowError: date value out of range` when the UTC instant is
    /// outside Python's year range 1..=9999.
    pub fn to_db_text(self) -> Result<String, DateTimeError> {
        let value = self.0;
        if !(1..=9999).contains(&value.year()) {
            return Err(DateTimeError::overflow("date value out of range"));
        }
        let mut text = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            value.year(),
            value.month(),
            value.day(),
            value.hour(),
            value.minute(),
            value.second()
        );
        let micros = value.nanosecond() / 1000;
        if micros != 0 {
            text.push_str(&format!(".{micros:06}"));
        }
        Ok(text)
    }
}

/// A Python exception raised while converting a datetime value: its class
/// name and `str()` (P2 text, PLAN.md 2.3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DateTimeError {
    pub class: &'static str,
    pub message: String,
}

impl DateTimeError {
    fn value(message: impl Into<String>) -> Self {
        Self {
            class: "ValueError",
            message: message.into(),
        }
    }

    fn overflow(message: impl Into<String>) -> Self {
        Self {
            class: "OverflowError",
            message: message.into(),
        }
    }
}

impl fmt::Display for DateTimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DateTimeError {}

/// A datetime as `parse_datetime` returns it: the wall time and, when the
/// text carried one, the UTC offset in microseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParsedDateTime {
    pub local: NaiveDateTime,
    pub offset_micros: Option<i64>,
}

impl ParsedDateTime {
    /// `convert_datetimefield_value`: an aware value keeps its instant, a
    /// naive one is UTC.
    pub fn to_db_datetime(self) -> DbDateTime {
        let offset = Duration::microseconds(self.offset_micros.unwrap_or(0));
        DbDateTime(self.local - offset)
    }
}

/// Read a stored datetime column (its text form, `CAST(col AS TEXT)`; NULL
/// stays `None`) the way Django's loader does.
///
/// # Errors
/// The `ValueError` Django raises for text that `datetime_re` matches with
/// out-of-range fields.
pub fn parse_column(value: Option<&str>) -> Result<Option<DbDateTime>, DateTimeError> {
    match value {
        None => Ok(None),
        Some(text) => Ok(parse_datetime(text)?.map(ParsedDateTime::to_db_datetime)),
    }
}

/// `django.utils.dateparse.parse_datetime`.
///
/// # Errors
/// `ValueError` when `datetime_re` matches but the fields are out of range.
pub fn parse_datetime(value: &str) -> Result<Option<ParsedDateTime>, DateTimeError> {
    if let Some(parsed) = fromisoformat(value) {
        return Ok(Some(parsed));
    }
    parse_datetime_re(value)
}

// ---------------------------------------------------------------------------
// CPython 3.12 `datetime.fromisoformat` (`_datetimemodule.c`). `None` is the
// `ValueError` that Django catches.

const MICROS_PER_DAY: i64 = 86_400_000_000;

/// The byte at `index` of the NUL-terminated C buffer (0 past the end).
fn at(bytes: &[u8], index: usize) -> u8 {
    bytes.get(index).copied().unwrap_or(0)
}

fn is_digit(byte: u8) -> bool {
    byte.is_ascii_digit()
}

/// `parse_digits`: `count` ASCII digits starting at `*pos`.
fn parse_digits(bytes: &[u8], pos: &mut usize, count: usize, value: &mut i64) -> Option<()> {
    for _ in 0..count {
        let byte = at(bytes, *pos);
        *pos += 1;
        if !is_digit(byte) {
            return None;
        }
        *value = *value * 10 + i64::from(byte - b'0');
    }
    Some(())
}

/// `_find_isoformat_datetime_separator`; `None` for its `-1`.
fn find_separator(bytes: &[u8]) -> Option<usize> {
    let len = bytes.len();
    if len == 7 {
        return Some(7);
    }
    if at(bytes, 4) == b'-' {
        if at(bytes, 5) == b'W' {
            if len < 8 {
                return None;
            }
            if len > 8 && at(bytes, 8) == b'-' {
                if len == 9 {
                    return None;
                }
                if len > 10 && is_digit(at(bytes, 10)) {
                    return Some(8);
                }
                return Some(10);
            }
            return Some(8);
        }
        return Some(10);
    }
    if at(bytes, 4) == b'W' {
        let mut idx = 7;
        while idx < len && is_digit(at(bytes, idx)) {
            idx += 1;
        }
        if idx < 9 {
            return Some(idx);
        }
        return Some(if idx % 2 == 0 { 7 } else { 8 });
    }
    Some(8)
}

/// `parse_isoformat_date`: `(year, month, day)`; the week-date form is
/// resolved like `iso_to_ymd` (which may yield year 0 or 10000; the range
/// check comes later).
fn parse_date(bytes: &[u8], len: usize) -> Option<(i64, i64, i64)> {
    let mut pos = 0;
    let mut year = 0;
    parse_digits(bytes, &mut pos, 4, &mut year)?;
    let uses_separator = at(bytes, pos) == b'-';
    if uses_separator {
        pos += 1;
    }
    if at(bytes, pos) == b'W' {
        pos += 1;
        let mut week = 0;
        parse_digits(bytes, &mut pos, 2, &mut week)?;
        let mut weekday = 0;
        if pos < len {
            if uses_separator {
                let byte = at(bytes, pos);
                pos += 1;
                if byte != b'-' {
                    return None;
                }
            }
            parse_digits(bytes, &mut pos, 1, &mut weekday)?;
        } else {
            weekday = 1;
        }
        return iso_to_ymd(year, week, weekday);
    }
    let mut month = 0;
    parse_digits(bytes, &mut pos, 2, &mut month)?;
    if uses_separator {
        let byte = at(bytes, pos);
        pos += 1;
        if byte != b'-' {
            return None;
        }
    }
    let mut day = 0;
    parse_digits(bytes, &mut pos, 2, &mut day)?;
    Some((year, month, day))
}

/// `iso_to_ymd`.
fn iso_to_ymd(year: i64, week: i64, weekday: i64) -> Option<(i64, i64, i64)> {
    if !(1..=9999).contains(&year) {
        return None;
    }
    let year = i32::try_from(year).ok()?;
    if !(1..=53).contains(&week) {
        return None;
    }
    if week == 53 {
        // 53 weeks in years starting on a Thursday and leap years starting
        // on a Wednesday.
        let first = NaiveDate::from_ymd_opt(year, 1, 1)?.weekday();
        let leap = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
        if !(first == Weekday::Thu || (first == Weekday::Wed && leap)) {
            return None;
        }
    }
    if !(1..=7).contains(&weekday) {
        return None;
    }
    // Monday of ISO week 1: the Monday on or before January 4th.
    let jan4 = NaiveDate::from_ymd_opt(year, 1, 4)?;
    let week1_monday = jan4 - Duration::days(i64::from(jan4.weekday().num_days_from_monday()));
    let date = week1_monday + Duration::days((week - 1) * 7 + weekday - 1);
    Some((
        i64::from(date.year()),
        i64::from(date.month()),
        i64::from(date.day()),
    ))
}

/// `parse_hh_mm_ss_ff` over `bytes[start..end]`: `(rv, [h, m, s, us])` with
/// `rv` 1 when something follows the parsed part; `None` for its negative
/// return codes.
fn parse_hh_mm_ss_ff(bytes: &[u8], start: usize, end: usize) -> Option<(bool, [i64; 4])> {
    let mut values = [0i64; 4];
    let mut pos = start;
    let mut has_separator = true;
    for index in 0..3 {
        parse_digits(bytes, &mut pos, 2, &mut values[index])?;
        let byte = at(bytes, pos);
        pos += 1;
        if index == 0 {
            has_separator = byte == b':';
        }
        if pos >= end {
            return Some((byte != 0, values));
        } else if has_separator && byte == b':' {
            continue;
        } else if byte == b'.' || byte == b',' {
            break;
        } else if !has_separator {
            pos -= 1;
        } else {
            return None;
        }
    }
    let to_parse = end.saturating_sub(pos).min(6);
    parse_digits(bytes, &mut pos, to_parse, &mut values[3])?;
    if to_parse < 6 {
        values[3] *= 10i64.pow(u32::try_from(6 - to_parse).ok()?);
    }
    while is_digit(at(bytes, pos)) {
        pos += 1;
    }
    Some((at(bytes, pos) != 0, values))
}

/// The parsed time: `[h, m, s, us]` and the offset `(seconds, micros)` when
/// present.
type ParsedTime = ([i64; 4], Option<(i64, i64)>);

/// `parse_isoformat_time` over `bytes[start..]`.
fn parse_time(bytes: &[u8], start: usize) -> Option<ParsedTime> {
    let end = bytes.len();
    let mut tz_pos = start;
    loop {
        let byte = at(bytes, tz_pos);
        if byte == b'Z' || byte == b'+' || byte == b'-' {
            break;
        }
        tz_pos += 1;
        if tz_pos >= end {
            break;
        }
    }
    let (trailing, time) = parse_hh_mm_ss_ff(bytes, start, tz_pos)?;
    if tz_pos == end {
        return (!trailing).then_some((time, None));
    }
    if at(bytes, tz_pos) == b'Z' {
        return (at(bytes, tz_pos + 1) == 0).then_some((time, Some((0, 0))));
    }
    let sign = if at(bytes, tz_pos) == b'-' { -1 } else { 1 };
    let (trailing, tz) = parse_hh_mm_ss_ff(bytes, tz_pos + 1, end)?;
    if trailing {
        return None;
    }
    let offset_seconds = sign * (tz[0] * 3600 + tz[1] * 60 + tz[2]);
    Some((time, Some((offset_seconds, sign * tz[3]))))
}

/// `datetime.fromisoformat` (CPython 3.12, C implementation).
pub fn fromisoformat(value: &str) -> Option<ParsedDateTime> {
    // `_sanitize_isoformat_str`: fewer than 7 characters is invalid. (Lone
    // surrogates, which it replaces, cannot occur in a Rust string.)
    if value.chars().count() < 7 {
        return None;
    }
    let bytes = value.as_bytes();
    let separator = find_separator(bytes)?;
    let (year, month, day) = parse_date(bytes, separator)?;
    let mut time = [0i64; 4];
    let mut offset = None;
    if bytes.len() > separator {
        // Skip the separator character (any UTF-8 character).
        let lead = at(bytes, separator);
        let width = if lead & 0x80 == 0 {
            1
        } else {
            match lead & 0xf0 {
                0xe0 => 3,
                0xf0 => 4,
                _ => 2,
            }
        };
        (time, offset) = parse_time(bytes, separator + width)?;
    }
    // `tzinfo_from_isoformat_results`: a zero second offset is UTC whatever
    // its microseconds; otherwise `timezone()` requires |offset| < 24 h.
    let offset_micros = match offset {
        None => None,
        Some((0, _)) => Some(0),
        Some((seconds, micros)) => {
            let total = seconds * 1_000_000 + micros;
            if total.abs() >= MICROS_PER_DAY {
                return None;
            }
            Some(total)
        }
    };
    let local = checked_datetime(year, month, day, time).ok()?;
    Some(ParsedDateTime {
        local,
        offset_micros,
    })
}

/// `datetime(year, month, day, hour, minute, second, microsecond)` with
/// CPython's range checks and messages.
fn checked_datetime(
    year: i64,
    month: i64,
    day: i64,
    [hour, minute, second, micros]: [i64; 4],
) -> Result<NaiveDateTime, DateTimeError> {
    if !(1..=9999).contains(&year) {
        return Err(DateTimeError::value(format!("year {year} is out of range")));
    }
    if !(1..=12).contains(&month) {
        return Err(DateTimeError::value("month must be in 1..12"));
    }
    let date = u32::try_from(day)
        .ok()
        .and_then(|day| NaiveDate::from_ymd_opt(year as i32, month as u32, day))
        .ok_or_else(|| DateTimeError::value("day is out of range for month"))?;
    if !(0..=23).contains(&hour) {
        return Err(DateTimeError::value("hour must be in 0..23"));
    }
    if !(0..=59).contains(&minute) {
        return Err(DateTimeError::value("minute must be in 0..59"));
    }
    if !(0..=59).contains(&second) {
        return Err(DateTimeError::value("second must be in 0..59"));
    }
    if !(0..=999_999).contains(&micros) {
        return Err(DateTimeError::value("microsecond must be in 0..999999"));
    }
    let time =
        NaiveTime::from_hms_micro_opt(hour as u32, minute as u32, second as u32, micros as u32)
            .expect("fields are in range");
    Ok(date.and_time(time))
}

// ---------------------------------------------------------------------------
// Django's fallback regex (`dateparse.py:datetime_re`). Python's `$` also
// matches before a trailing newline (`\n?\z`), its `\s` is `str.isspace`
// (Unicode White_Space plus U+001C..U+001F).

static DATETIME_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"\A(?P<year>[0-9]{4})-(?P<month>[0-9]{1,2})-(?P<day>[0-9]{1,2})",
        r"[T ](?P<hour>[0-9]{1,2}):(?P<minute>[0-9]{1,2})",
        r"(?::(?P<second>[0-9]{1,2})(?:[.,](?P<microsecond>[0-9]{1,6})[0-9]{0,6})?)?",
        r"[\s\x1c-\x1f]*(?P<tzinfo>Z|[+-][0-9]{2}(?::?[0-9]{2})?)?\n?\z",
    ))
    .expect("datetime_re compiles")
});

fn parse_datetime_re(value: &str) -> Result<Option<ParsedDateTime>, DateTimeError> {
    let Some(captures) = DATETIME_RE.captures(value) else {
        return Ok(None);
    };
    let number = |name: &str| -> i64 {
        captures
            .name(name)
            .map_or(0, |m| m.as_str().parse().expect("ASCII digits"))
    };
    let micros = captures.name("microsecond").map_or(0, |m| {
        format!("{:0<6}", m.as_str())
            .parse::<i64>()
            .expect("ASCII digits")
    });
    // `get_fixed_timezone(offset)` runs before `datetime(...)`.
    let offset_micros = match captures.name("tzinfo").map(|m| m.as_str()) {
        None => None,
        Some("Z") => Some(0),
        Some(tz) => {
            let minutes_part = if tz.len() > 3 {
                tz[tz.len() - 2..].parse::<i64>().expect("ASCII digits")
            } else {
                0
            };
            let mut minutes = 60 * tz[1..3].parse::<i64>().expect("ASCII digits") + minutes_part;
            if tz.starts_with('-') {
                minutes = -minutes;
            }
            let micros = minutes * 60_000_000;
            if micros.abs() >= MICROS_PER_DAY {
                return Err(DateTimeError::value(format!(
                    "offset must be a timedelta strictly between -timedelta(hours=24) and \
                     timedelta(hours=24), not {}.",
                    timedelta_repr(micros)
                )));
            }
            Some(micros)
        }
    };
    let local = checked_datetime(
        number("year"),
        number("month"),
        number("day"),
        [number("hour"), number("minute"), number("second"), micros],
    )?;
    Ok(Some(ParsedDateTime {
        local,
        offset_micros,
    }))
}

/// `repr(timedelta(microseconds=micros))`.
fn timedelta_repr(micros: i64) -> String {
    let days = micros.div_euclid(MICROS_PER_DAY);
    let rest = micros.rem_euclid(MICROS_PER_DAY);
    let seconds = rest / 1_000_000;
    let microseconds = rest % 1_000_000;
    let mut parts = Vec::new();
    if days != 0 {
        parts.push(format!("days={days}"));
    }
    if seconds != 0 {
        parts.push(format!("seconds={seconds}"));
    }
    if microseconds != 0 {
        parts.push(format!("microseconds={microseconds}"));
    }
    if parts.is_empty() {
        parts.push("0".to_owned());
    }
    format!("datetime.timedelta({})", parts.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_drop_a_zero_fraction() {
        let base = NaiveDate::from_ymd_opt(2026, 10, 21)
            .unwrap()
            .and_hms_opt(8, 15, 30)
            .unwrap();
        assert_eq!(
            DbDateTime::from_naive_utc(base).to_db_text().unwrap(),
            "2026-10-21 08:15:30"
        );
        let fraction = base + Duration::nanoseconds(123_456_789);
        assert_eq!(
            DbDateTime::from_naive_utc(fraction).to_db_text().unwrap(),
            "2026-10-21 08:15:30.123456"
        );
    }

    #[test]
    fn writes_outside_the_python_range_overflow() {
        let parsed = parse_datetime("0001-01-01T00:00:00+01:00")
            .unwrap()
            .unwrap();
        let error = parsed.to_db_datetime().to_db_text().unwrap_err();
        assert_eq!(error.class, "OverflowError");
        assert_eq!(error.to_string(), "date value out of range");
    }

    #[test]
    fn timedelta_repr_matches_python() {
        assert_eq!(timedelta_repr(0), "datetime.timedelta(0)");
        assert_eq!(
            timedelta_repr(86_460 * 1_000_000),
            "datetime.timedelta(days=1, seconds=60)"
        );
        assert_eq!(
            timedelta_repr(-86_460 * 1_000_000),
            "datetime.timedelta(days=-2, seconds=86340)"
        );
    }
}
