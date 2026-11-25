//! The agent's UTC instant: microseconds since the Unix epoch.
//!
//! Python's aware `datetime` values are what the agent ports: microsecond
//! resolution, years 1..=9999. [`UtcDateTime`] is that timeline as a plain
//! `i64` count, so every Python value fits, and so do the instants up to a
//! day outside the range that a stored offset can produce
//! (`9999-12-31T23:59:59-01:00`). Arithmetic and comparisons are integer
//! operations and never fail.
//!
//! jiff supplies the clock and the calendar. Its own instant type is not
//! used as the agent's type because `jiff::Timestamp` ends at
//! `9999-12-30T22:00:00Z` (it reserves room for every UTC offset), which is
//! 26 hours short of Python's `datetime.max`. `jiff::civil::DateTime` reaches
//! `9999-12-31T23:59:59.999999999`, so the calendar fields come from adding
//! the count to the civil epoch ([`UtcDateTime::civil`]).

use std::fmt;
use std::time::Duration;

use jiff::civil;
use jiff::fmt::temporal::Pieces;
use jiff::{SignedDuration, Timestamp};

const MICROS_PER_SECOND: i64 = 1_000_000;
const CIVIL_EPOCH: civil::DateTime = civil::DateTime::constant(1970, 1, 1, 0, 0, 0, 0);

/// A UTC instant with microsecond resolution (a Python aware `datetime` in
/// UTC), as microseconds since `1970-01-01T00:00:00Z`.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UtcDateTime(i64);

impl UtcDateTime {
    /// `1970-01-01T00:00:00Z`.
    pub const UNIX_EPOCH: Self = Self(0);
    /// The earliest value of the type (far outside every calendar).
    pub const MIN: Self = Self(i64::MIN);
    /// The latest value of the type (far outside every calendar).
    pub const MAX: Self = Self(i64::MAX);

    /// The instant `micros` microseconds after the Unix epoch.
    pub const fn from_micros(micros: i64) -> Self {
        Self(micros)
    }

    /// The instant `seconds` seconds after the Unix epoch (saturating).
    pub const fn from_seconds(seconds: i64) -> Self {
        Self(seconds.saturating_mul(MICROS_PER_SECOND))
    }

    /// The system clock, truncated to microseconds (Python's resolution).
    pub fn now() -> Self {
        Self::from(Timestamp::now())
    }

    /// Microseconds since the Unix epoch.
    pub const fn micros(self) -> i64 {
        self.0
    }

    /// Whole seconds since the Unix epoch, rounded down.
    pub const fn timestamp(self) -> i64 {
        self.0.div_euclid(MICROS_PER_SECOND)
    }

    /// The microsecond within the second (0..=999999).
    pub const fn subsec_micros(self) -> u32 {
        // The remainder is in 0..10**6.
        self.0.rem_euclid(MICROS_PER_SECOND) as u32
    }

    /// The UTC wall time of a civil datetime, truncated to microseconds.
    pub fn from_civil(datetime: civil::DateTime) -> Self {
        let nanos = datetime.duration_since(CIVIL_EPOCH).as_nanos();
        // jiff's civil range spans about 6.3e17 microseconds.
        Self(i64::try_from(nanos.div_euclid(1000)).expect("civil datetimes fit microseconds"))
    }

    /// The UTC instant of calendar fields, `None` when they name no
    /// datetime in jiff's civil range (years -9999..=9999).
    pub fn from_ymd_hms_micro(
        year: i16,
        month: i8,
        day: i8,
        hour: i8,
        minute: i8,
        second: i8,
        microsecond: i32,
    ) -> Option<Self> {
        let nanosecond = microsecond.checked_mul(1000)?;
        civil::DateTime::new(year, month, day, hour, minute, second, nanosecond)
            .ok()
            .map(Self::from_civil)
    }

    /// [`UtcDateTime::from_ymd_hms_micro`] at a whole second.
    pub fn from_ymd_hms(
        year: i16,
        month: i8,
        day: i8,
        hour: i8,
        minute: i8,
        second: i8,
    ) -> Option<Self> {
        Self::from_ymd_hms_micro(year, month, day, hour, minute, second, 0)
    }

    /// An RFC 3339 timestamp with a time and a numeric offset or `Z`
    /// (`2026-10-21T12:00:00.5Z`), converted to UTC and truncated to
    /// microseconds.
    ///
    /// jiff's Temporal parser reads the date, time and offset; the instant
    /// is computed here, so the whole civil range is accepted (parsing into
    /// a `jiff::Timestamp` would reject `9999-12-31T00:00:00Z`). Python's
    /// own formats go through `pycompat::py_fromisoformat` instead.
    ///
    /// # Errors
    /// jiff's parse error, or a missing time or offset.
    pub fn parse_rfc3339(text: &str) -> Result<Self, ParseUtcError> {
        let pieces = Pieces::parse(text).map_err(|err| ParseUtcError(err.to_string()))?;
        let time = pieces
            .time()
            .ok_or_else(|| ParseUtcError(format!("no time of day in {text:?}")))?;
        let offset = pieces
            .to_numeric_offset()
            .ok_or_else(|| ParseUtcError(format!("no UTC offset in {text:?}")))?;
        let local = Self::from_civil(pieces.date().to_datetime(time));
        Ok(local.saturating_add_micros(-i64::from(offset.seconds()) * MICROS_PER_SECOND))
    }

    /// The UTC calendar fields, `None` outside jiff's civil range
    /// (`-9999-01-01T00:00:00` to `9999-12-31T23:59:59.999999`).
    pub fn civil(self) -> Option<civil::DateTime> {
        CIVIL_EPOCH
            .checked_add(SignedDuration::from_micros(self.0))
            .ok()
    }

    /// The UTC calendar fields, saturated at the ends of jiff's civil range.
    /// Python has no datetime there; renderings of such an instant show the
    /// nearest representable one.
    pub fn civil_saturating(self) -> civil::DateTime {
        self.civil().unwrap_or(if self.0 < 0 {
            civil::DateTime::MIN
        } else {
            civil::DateTime::MAX
        })
    }

    /// The instant `micros` microseconds later (earlier when negative),
    /// saturating at the ends of the type.
    pub const fn saturating_add_micros(self, micros: i64) -> Self {
        Self(self.0.saturating_add(micros))
    }

    /// `self - earlier` in microseconds, saturating.
    pub const fn micros_since(self, earlier: Self) -> i64 {
        self.0.saturating_sub(earlier.0)
    }

    /// `%Y-%m-%dT%H:%M:%S.%fZ`: RFC 3339 in UTC with all six fraction
    /// digits.
    pub fn rfc3339_micros(self) -> String {
        let civil = self.civil_saturating();
        format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
            civil.year(),
            civil.month(),
            civil.day(),
            civil.hour(),
            civil.minute(),
            civil.second(),
            civil.subsec_nanosecond() / 1000
        )
    }
}

/// Why [`UtcDateTime::parse_rfc3339`] rejected a text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParseUtcError(String);

impl fmt::Display for ParseUtcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ParseUtcError {}

/// `instant + sign * duration`, the duration truncated to microseconds; the
/// sum is computed exactly and then clamped to the type.
fn offset_by(instant: UtcDateTime, duration: Duration, sign: i128) -> UtcDateTime {
    // At most 2**64 * 10**6 microseconds: the sum fits an i128.
    let micros = i128::try_from(duration.as_micros()).unwrap_or(i128::MAX / 2);
    let sum = i128::from(instant.0) + sign * micros;
    UtcDateTime(sum.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64)
}

impl From<Timestamp> for UtcDateTime {
    /// Truncated to microseconds (rounded down, also before 1970).
    fn from(timestamp: Timestamp) -> Self {
        let micros = timestamp.as_nanosecond().div_euclid(1000);
        // jiff's timestamp range spans about 6.3e17 microseconds.
        Self(i64::try_from(micros).expect("jiff timestamps fit microseconds"))
    }
}

impl std::str::FromStr for UtcDateTime {
    type Err = ParseUtcError;

    /// [`UtcDateTime::parse_rfc3339`].
    fn from_str(text: &str) -> Result<Self, ParseUtcError> {
        Self::parse_rfc3339(text)
    }
}

impl std::ops::Add<Duration> for UtcDateTime {
    type Output = Self;

    /// Saturating; the duration is truncated to microseconds.
    fn add(self, duration: Duration) -> Self {
        offset_by(self, duration, 1)
    }
}

impl std::ops::Sub<Duration> for UtcDateTime {
    type Output = Self;

    /// Saturating; the duration is truncated to microseconds.
    fn sub(self, duration: Duration) -> Self {
        offset_by(self, duration, -1)
    }
}

impl fmt::Debug for UtcDateTime {
    /// `2026-10-21T08:15:30Z`, with three or six fraction digits when the
    /// microsecond is not zero; the raw count outside jiff's civil range.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(civil) = self.civil() else {
            return write!(f, "UtcDateTime({}us)", self.0);
        };
        write!(
            f,
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            civil.year(),
            civil.month(),
            civil.day(),
            civil.hour(),
            civil.minute(),
            civil.second()
        )?;
        let micros = self.subsec_micros();
        if micros != 0 && micros.is_multiple_of(1000) {
            write!(f, ".{:03}", micros / 1000)?;
        } else if micros != 0 {
            write!(f, ".{micros:06}")?;
        }
        f.write_str("Z")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pycompat::{PY_DATETIME_MAX_MICROS, PY_DATETIME_MIN_MICROS};

    #[test]
    fn python_range_ends_are_calendar_values() {
        // `datetime.max` lies after `jiff::Timestamp::MAX`
        // (9999-12-30T22:00:00Z) and is still a civil datetime.
        let max = UtcDateTime::from_micros(PY_DATETIME_MAX_MICROS);
        assert!(PY_DATETIME_MAX_MICROS > UtcDateTime::from(Timestamp::MAX).micros());
        assert_eq!(
            max.civil(),
            Some(civil::DateTime::constant(
                9999,
                12,
                31,
                23,
                59,
                59,
                999_999_000
            ))
        );
        assert_eq!(UtcDateTime::from_civil(max.civil().unwrap()), max);
        assert_eq!(max.rfc3339_micros(), "9999-12-31T23:59:59.999999Z");
        assert_eq!(format!("{max:?}"), "9999-12-31T23:59:59.999999Z");
        assert_eq!(
            UtcDateTime::from_ymd_hms_micro(9999, 12, 31, 23, 59, 59, 999_999),
            Some(max)
        );

        let min = UtcDateTime::from_micros(PY_DATETIME_MIN_MICROS);
        assert_eq!(UtcDateTime::from_ymd_hms(1, 1, 1, 0, 0, 0), Some(min));
        assert_eq!(format!("{min:?}"), "0001-01-01T00:00:00Z");
    }

    #[test]
    fn instants_past_the_calendar_keep_their_order() {
        // 9999-12-31T23:59:59-01:00 is one hour into year 10000.
        let stored = UtcDateTime::from_micros(PY_DATETIME_MAX_MICROS - 999_999 + 3_600_000_000);
        assert_eq!(stored.civil(), None);
        assert_eq!(stored.civil_saturating(), civil::DateTime::MAX);
        assert!(stored > UtcDateTime::from_micros(PY_DATETIME_MAX_MICROS));
        assert_eq!(format!("{stored:?}"), "UtcDateTime(253402304399000000us)");
        assert_eq!(UtcDateTime::MIN.civil(), None);
        assert_eq!(UtcDateTime::MIN.civil_saturating(), civil::DateTime::MIN);
        assert_eq!(
            UtcDateTime::MAX.micros_since(UtcDateTime::MIN),
            i64::MAX,
            "saturates"
        );
        assert_eq!(UtcDateTime::MAX + Duration::from_secs(1), UtcDateTime::MAX);
        assert_eq!(UtcDateTime::MIN - Duration::MAX, UtcDateTime::MIN);
        // The whole duration is applied before clamping: a span longer than
        // an `i64` of microseconds crosses the type from end to end.
        let longest = Duration::from_micros(u64::MAX);
        assert_eq!(UtcDateTime::MIN + longest, UtcDateTime::MAX);
        assert_eq!(UtcDateTime::MAX - longest, UtcDateTime::MIN);
        assert_eq!(
            UtcDateTime::MIN + Duration::from_micros(1 << 63),
            UtcDateTime::from_micros(0)
        );
        assert_eq!(
            UtcDateTime::from_micros(-1) - Duration::from_micros(i64::MAX as u64),
            UtcDateTime::MIN
        );
        assert_eq!(UtcDateTime::MAX + Duration::MAX, UtcDateTime::MAX);
        assert_eq!(UtcDateTime::UNIX_EPOCH - Duration::MAX, UtcDateTime::MIN);
    }

    #[test]
    fn seconds_and_fractions_round_down_before_the_epoch() {
        let before = UtcDateTime::from_micros(-1);
        assert_eq!(before.timestamp(), -1);
        assert_eq!(before.subsec_micros(), 999_999);
        assert_eq!(format!("{before:?}"), "1969-12-31T23:59:59.999999Z");
        let nanos = Timestamp::from_nanosecond(-1).unwrap();
        assert_eq!(UtcDateTime::from(nanos), before);
        let civil = civil::DateTime::constant(1969, 12, 31, 23, 59, 59, 999_999_999);
        assert_eq!(UtcDateTime::from_civil(civil), before);
    }

    #[test]
    fn parses_and_renders_rfc3339() {
        let parsed = UtcDateTime::parse_rfc3339("2026-10-21T12:00:00.5+02:00").unwrap();
        assert_eq!(
            parsed,
            UtcDateTime::from_ymd_hms_micro(2026, 10, 21, 10, 0, 0, 500_000).unwrap()
        );
        assert_eq!(parsed.rfc3339_micros(), "2026-10-21T10:00:00.500000Z");
        assert_eq!(format!("{parsed:?}"), "2026-10-21T10:00:00.500Z");
        assert_eq!(
            UtcDateTime::parse_rfc3339(&parsed.rfc3339_micros()).unwrap(),
            parsed
        );
        assert_eq!(
            UtcDateTime::parse_rfc3339("2026-10-21T12:00:00.123456789Z")
                .unwrap()
                .subsec_micros(),
            123_456
        );
        for missing in ["2026-10-21T12:00:00", "2026-10-21", "2026-10-21Z", "now"] {
            assert!(UtcDateTime::parse_rfc3339(missing).is_err(), "{missing}");
        }
        // The last 26 hours of year 9999 are no `jiff::Timestamp`.
        assert!("9999-12-31T23:59:30+00:00".parse::<Timestamp>().is_err());
        assert_eq!(
            UtcDateTime::parse_rfc3339("9999-12-31T23:59:30+00:00").unwrap(),
            UtcDateTime::from_ymd_hms(9999, 12, 31, 23, 59, 30).unwrap()
        );
        assert_eq!(
            UtcDateTime::parse_rfc3339("9999-12-31T23:59:59.999999-01:00")
                .unwrap()
                .micros(),
            PY_DATETIME_MAX_MICROS + 3_600_000_000
        );
        assert_eq!(
            UtcDateTime::from_seconds(1_792_584_000) + Duration::from_nanos(1_999),
            UtcDateTime::from_micros(1_792_584_000_000_001)
        );
    }
}
