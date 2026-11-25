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
//! over the UTF-8 text. Django's `datetime_re` fallback uses the decimal
//! digits from Python 3.12's Unicode database. Pinned by the
//! `datetime_codec.json` fixture.

use std::fmt::{self, Write as _};
use std::sync::LazyLock;

use graphyard_core::pycompat::{
    DECIMAL_ZEROES, PY_DATETIME_MAX_MICROS, PY_DATETIME_MIN_MICROS, decimal_digit, py_iso_to_ymd,
};
use graphyard_core::utc::UtcDateTime;
use regex::Regex;

/// A datetime column value as Django holds it after loading: an aware
/// datetime, kept as its UTC instant with microsecond precision. The instant
/// may lie up to a day outside Python's year range when the stored text
/// carried an offset; [`DbDateTime::to_db_text`] then fails like Python's
/// conversion.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DbDateTime(UtcDateTime);

impl DbDateTime {
    /// An instant from the clock (microsecond resolution, like Python's
    /// `datetime`).
    pub fn from_utc(value: UtcDateTime) -> Self {
        Self(value)
    }

    /// The UTC instant.
    pub fn to_utc(self) -> UtcDateTime {
        self.0
    }

    /// The stored text (`adapt_datetimefield_value`: naive UTC
    /// `str(datetime)`).
    ///
    /// # Errors
    /// `OverflowError: date value out of range` when the UTC instant is
    /// outside Python's year range 1..=9999.
    pub fn to_db_text(self) -> Result<String, DateTimeError> {
        let out_of_range = || DateTimeError::overflow("date value out of range");
        if !(PY_DATETIME_MIN_MICROS..=PY_DATETIME_MAX_MICROS).contains(&self.0.micros()) {
            return Err(out_of_range());
        }
        // Python's range lies within the civil calendar.
        let value = self.0.civil().ok_or_else(out_of_range)?;
        let mut text = format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            value.year(),
            value.month(),
            value.day(),
            value.hour(),
            value.minute(),
            value.second()
        );
        let micros = self.0.subsec_micros();
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
    /// The wall time (years 1..=9999), placed on the UTC timeline as if it
    /// carried no offset.
    pub local: UtcDateTime,
    pub offset_micros: Option<i64>,
}

impl ParsedDateTime {
    /// `convert_datetimefield_value`: an aware value keeps its instant, a
    /// naive one is UTC.
    pub fn to_db_datetime(self) -> DbDateTime {
        // The offset is below 24 hours: no overflow.
        let offset = self.offset_micros.unwrap_or(0);
        DbDateTime(self.local.saturating_add_micros(-offset))
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
/// resolved like `iso_to_ymd` (which may yield year 10000; the range check
/// comes later).
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
        return py_iso_to_ymd(year, week, weekday);
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
    // `_sanitize_isoformat_str` requires seven Unicode scalars. Stop at
    // the seventh so an invalid prefix does not incur a scan of its tail.
    value.chars().nth(6)?;
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
) -> Result<UtcDateTime, DateTimeError> {
    if !(1..=9999).contains(&year) {
        return Err(DateTimeError::value(format!("year {year} is out of range")));
    }
    if !(1..=12).contains(&month) {
        return Err(DateTimeError::value("month must be in 1..12"));
    }
    // Year and month are valid, so only the day can fail here.
    let day = i8::try_from(day)
        .ok()
        .filter(|&day| UtcDateTime::from_ymd_hms(year as i16, month as i8, day, 0, 0, 0).is_some())
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
    Ok(UtcDateTime::from_ymd_hms_micro(
        year as i16,
        month as i8,
        day,
        hour as i8,
        minute as i8,
        second as i8,
        micros as i32,
    )
    .expect("fields are in range"))
}

// ---------------------------------------------------------------------------
// Django's fallback regex (`dateparse.py:datetime_re`). Python's `$` also
// matches before a trailing newline (`\n?\z`), its `\s` is `str.isspace`
// (Unicode White_Space plus U+001C..U+001F).

static DATETIME_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Use the same Unicode version for regex admission and digit conversion.
    let mut decimal_class = String::from("[0-9");
    for zero in DECIMAL_ZEROES {
        write!(decimal_class, r"\x{{{zero:x}}}-\x{{{:x}}}", zero + 9)
            .expect("writing to a String succeeds");
    }
    decimal_class.push(']');
    let pattern = concat!(
        r"\A(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})",
        r"[T ](?P<hour>\d{1,2}):(?P<minute>\d{1,2})",
        r"(?::(?P<second>\d{1,2})(?:[.,](?P<microsecond>\d{1,6})\d{0,6})?)?",
        r"[\s\x1c-\x1f]*(?P<tzinfo>Z|[+-](?P<tz_hour>\d{2})(?::?(?P<tz_minute>\d{2}))?)?\n?\z",
    );
    Regex::new(&pattern.replace(r"\d", &decimal_class)).expect("datetime_re compiles")
});

fn fallback_number(digits: &str) -> i64 {
    digits.chars().fold(0, |value, c| {
        value * 10 + i64::from(decimal_digit(c).expect("matched a decimal digit"))
    })
}

fn parse_datetime_re(value: &str) -> Result<Option<ParsedDateTime>, DateTimeError> {
    let Some(captures) = DATETIME_RE.captures(value) else {
        return Ok(None);
    };
    let number = |name: &str| -> i64 {
        captures
            .name(name)
            .map_or(0, |m| fallback_number(m.as_str()))
    };
    let micros = captures.name("microsecond").map_or(0, |m| {
        let digits = m.as_str();
        let width = digits.chars().count() as u32;
        fallback_number(digits) * 10_i64.pow(6 - width)
    });
    // `get_fixed_timezone(offset)` runs before `datetime(...)`.
    let offset_micros = match captures.name("tzinfo").map(|m| m.as_str()) {
        None => None,
        Some("Z") => Some(0),
        Some(tz) => {
            let mut minutes = 60 * number("tz_hour") + number("tz_minute");
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
    #[ignore = "timing regression probe; run explicitly on a local machine"]
    fn rejects_invalid_iso_prefix_without_scanning_the_tail() {
        use std::hint::black_box;
        use std::time::Instant;

        let tail_sizes = [32, 4 * 1024, 4 * 1024 * 1024];
        let timings = tail_sizes.map(|size| {
            let value = format!("x{}", "é".repeat(size));
            (0..3)
                .map(|_| {
                    let start = Instant::now();
                    for _ in 0..128 {
                        assert!(fromisoformat(black_box(&value)).is_none());
                    }
                    start.elapsed()
                })
                .min()
                .unwrap()
        });
        eprintln!("128 rejected parses for tails of {tail_sizes:?} scalars: {timings:?}");
        let budget = timings[0] * 32 + std::time::Duration::from_millis(5);
        assert!(
            timings[1..].iter().all(|&elapsed| elapsed <= budget),
            "invalid prefix rejection exceeded {budget:?}: {timings:?}"
        );
    }

    #[test]
    fn writes_drop_a_zero_fraction() {
        let base = UtcDateTime::from_ymd_hms(2026, 10, 21, 8, 15, 30).unwrap();
        assert_eq!(
            DbDateTime::from_utc(base).to_db_text().unwrap(),
            "2026-10-21 08:15:30"
        );
        let fraction = base + std::time::Duration::from_nanos(123_456_789);
        assert_eq!(
            DbDateTime::from_utc(fraction).to_db_text().unwrap(),
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
