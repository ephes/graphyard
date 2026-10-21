//! The client configuration of `influx.py:_build_client`, checked lazily per
//! write or query (PLAN.md 3.3), and the request timeout of the Python
//! client (`influxdb_client` `rest.py`, `urllib3.Timeout`).

use std::fmt;
use std::time::Duration;

use graphyard_core::pycompat::{PyError, py_float_repr};
use graphyard_core::settings::{InfluxApiMode, Settings};
use num_bigint::BigInt;
use num_traits::Zero;

/// `influx.py:InfluxConfigurationError`: a missing setting, raised at the
/// first write or query that needs it, never at startup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InfluxConfigurationError(pub String);

impl InfluxConfigurationError {
    /// The Python class name.
    pub const PYTHON_CLASS: &'static str = "InfluxConfigurationError";
}

impl fmt::Display for InfluxConfigurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for InfluxConfigurationError {}

/// The values `_build_client` passes to `InfluxDBClient`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InfluxClientConfig {
    /// `INFLUX_URL` with one trailing `/` removed (the client's
    /// `conf.host`); request paths are appended to it as text.
    pub host: String,
    /// `INFLUX_TOKEN`; in v3 mode `dev-influxdb3-token` when empty.
    pub token: String,
    /// The client's default org: `INFLUX_ORG`; in v3 mode `INFLUX_BUCKET or
    /// "graphyard"` when empty. Writes pass `INFLUX_ORG` explicitly instead.
    pub org: String,
    /// `INFLUX_TIMEOUT_MS`.
    pub timeout_ms: BigInt,
}

impl InfluxClientConfig {
    /// `_build_client()`.
    ///
    /// # Errors
    /// `Influx setting missing: INFLUX_URL is required` without a URL; in
    /// any mode but v3 `Influx settings missing: INFLUX_URL, INFLUX_TOKEN,
    /// INFLUX_ORG are required` without a token or org.
    pub fn from_settings(settings: &Settings) -> Result<Self, InfluxConfigurationError> {
        if settings.influx_url.is_empty() {
            return Err(InfluxConfigurationError(
                "Influx setting missing: INFLUX_URL is required".to_owned(),
            ));
        }
        let mut token = settings.influx_token.clone();
        let mut org = settings.influx_org.clone();
        if settings.influx_api_mode() == InfluxApiMode::V3 {
            if token.is_empty() {
                "dev-influxdb3-token".clone_into(&mut token);
            }
            if org.is_empty() {
                org = if settings.influx_bucket.is_empty() {
                    "graphyard".to_owned()
                } else {
                    settings.influx_bucket.clone()
                };
            }
        } else if token.is_empty() || org.is_empty() {
            return Err(InfluxConfigurationError(
                "Influx settings missing: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG are required"
                    .to_owned(),
            ));
        }
        let url = &settings.influx_url;
        let host = url.strip_suffix('/').unwrap_or(url).to_owned();
        Ok(Self {
            host,
            token,
            org,
            timeout_ms: settings.influx_timeout_ms.clone(),
        })
    }

    /// The per-request timeout of the Python client: none for `0`, else
    /// `urllib3.Timeout(total=timeout_ms / 1_000)`.
    ///
    /// # Errors
    /// What Python raises when the request is made: `OverflowError`
    /// (`integer division result too large for a float`) when the division
    /// overflows a float, `ValueError` from urllib3 for a timeout `<= 0`,
    /// `OverflowError` (`timestamp out of range for platform time_t`) from
    /// `socket.settimeout` when the timeout does not fit CPython's
    /// nanosecond clock (more than about 292 years).
    pub fn request_timeout(&self) -> Result<Option<RequestTimeout>, PyError> {
        request_timeout(&self.timeout_ms)
    }
}

/// A validated total request timeout.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RequestTimeout {
    /// The value Python passes to urllib3, in seconds (used in messages).
    pub seconds: f64,
    /// The same as a `Duration`.
    pub duration: Duration,
}

/// See [`InfluxClientConfig::request_timeout`].
///
/// # Errors
/// See [`InfluxClientConfig::request_timeout`].
pub fn request_timeout(timeout_ms: &BigInt) -> Result<Option<RequestTimeout>, PyError> {
    if timeout_ms.is_zero() {
        // `if _configured_timeout:` is false: urllib3's default (no timeout).
        return Ok(None);
    }
    // Python's int / int is correctly rounded; so is parsing the exact
    // decimal quotient.
    let seconds: f64 = format!("{timeout_ms}e-3")
        .parse()
        .expect("an integer with an exponent is a float literal");
    if !seconds.is_finite() {
        return Err(PyError::overflow_error(
            "integer division result too large for a float",
        ));
    }
    if seconds <= 0.0 {
        return Err(PyError::value_error(format!(
            "Attempted to set total timeout to {}, but the timeout cannot be set to a value less than or equal to 0.",
            py_float_repr(seconds)
        )));
    }
    // CPython's `_PyTime_FromSecondsObject` (`_PyTime_ROUND_TIMEOUT`, a
    // ceiling) into signed 64-bit nanoseconds.
    let nanos = (seconds * 1e9).ceil();
    if nanos >= 9_223_372_036_854_775_808.0 {
        return Err(PyError::overflow_error(
            "timestamp out of range for platform time_t",
        ));
    }
    Ok(Some(RequestTimeout {
        seconds,
        duration: Duration::from_secs_f64(seconds),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(vars: &[(&str, &str)]) -> Settings {
        Settings::from_lookup(|name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
        })
        .expect("valid settings")
    }

    #[test]
    fn v3_defaults_token_and_org() {
        let config = InfluxClientConfig::from_settings(&settings(&[
            ("INFLUX_URL", "http://h/"),
            ("INFLUX_API_MODE", "V3"),
            ("INFLUX_BUCKET", ""),
        ]))
        .unwrap();
        assert_eq!(config.host, "http://h");
        assert_eq!(config.token, "dev-influxdb3-token");
        assert_eq!(config.org, "graphyard");
        let config = InfluxClientConfig::from_settings(&settings(&[
            ("INFLUX_URL", "http://h//"),
            ("INFLUX_API_MODE", "v3"),
            ("INFLUX_BUCKET", "b"),
        ]))
        .unwrap();
        assert_eq!(config.host, "http://h/");
        assert_eq!(config.org, "b");
    }

    #[test]
    fn missing_settings_are_reported_lazily() {
        let err = InfluxClientConfig::from_settings(&settings(&[])).unwrap_err();
        assert_eq!(err.0, "Influx setting missing: INFLUX_URL is required");
        let err = InfluxClientConfig::from_settings(&settings(&[
            ("INFLUX_URL", "http://h"),
            ("INFLUX_TOKEN", "t"),
        ]))
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "Influx settings missing: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG are required"
        );
    }

    #[test]
    fn request_timeout_follows_the_python_client() {
        assert_eq!(request_timeout(&BigInt::from(0)), Ok(None));
        let timeout = request_timeout(&BigInt::from(10_000)).unwrap().unwrap();
        assert_eq!(timeout.seconds, 10.0);
        assert_eq!(timeout.duration, Duration::from_secs(10));
        let err = request_timeout(&BigInt::from(-5)).unwrap_err();
        assert_eq!(err.python_class(), "ValueError");
        assert_eq!(
            err.message,
            "Attempted to set total timeout to -0.005, but the timeout cannot be set to a value less than or equal to 0."
        );
        let huge: BigInt = format!("1{}", "0".repeat(400)).parse().unwrap();
        let err = request_timeout(&huge).unwrap_err();
        assert_eq!(err.python_class(), "OverflowError");
        // Finite, but beyond CPython's nanosecond clock.
        for ms in [
            format!("1{}", "0".repeat(300)),
            "9223372036854775807".to_owned(),
            "100000000000000000".to_owned(),
        ] {
            let err = request_timeout(&ms.parse().unwrap()).unwrap_err();
            assert_eq!(err.python_class(), "OverflowError", "{ms}");
            assert_eq!(err.message, "timestamp out of range for platform time_t");
        }
        let timeout = request_timeout(&BigInt::from(1_000_000_000_000_i64))
            .unwrap()
            .unwrap();
        assert_eq!(timeout.duration, Duration::from_secs(1_000_000_000));
    }
}
