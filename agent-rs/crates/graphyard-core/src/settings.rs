//! The agent's settings from the process environment (PLAN.md section 3.3).
//!
//! Python source: `config/settings/base.py` read through django-environ
//! 0.13.0 (`Env.get_value`/`Env.parse_value`), and the SQLite pragma
//! normalisation of `apps.py:_configure_sqlite_connection`. Pinned by
//! `fixtures/settings.json` (`fixtures/gen_settings.py`), which executes the
//! real settings module per case.
//!
//! Rules reproduced (P1):
//!
//! - every variable is read once, in the order of `base.py`; the first
//!   invalid one is the startup error;
//! - a missing variable takes its default; a value starting with `$` is a
//!   django-environ proxy: the leading `$` are stripped and the named
//!   variable is read with the same default and cast, recursively (a cycle
//!   is Python's `RecursionError`);
//! - integers are Python `int()` of the text (surrounding whitespace, a sign
//!   and `_` between digits accepted, any size; an empty or invalid value is
//!   the `ValueError` of `int()`);
//! - strings are taken as they are, except the SQLite modes (`strip()`) and
//!   `INFLUX_API_MODE` (`lower()`);
//! - `GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS` defaults to
//!   `GRAPHYARD_HA_POLL_INTERVAL_SECONDS` (default 60), which is read and
//!   validated even when the outer variable is set.
//!
//! Only the variables the agent uses are read: settings that only the web
//! reads are not validated (D-7), and `src/django/.env` is not read (D-9).
//! Environment entries that are not valid UTF-8 are read lossily (Python
//! decodes them with `surrogateescape`; P2).

use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt;

use num_bigint::BigInt;
use num_traits::Zero;

use crate::pycompat::{PyError, PyErrorKind, py_int, py_strip};

/// Python's default recursion limit, as the bound of a proxy chain.
const MAX_PROXY_DEPTH: usize = 1000;

/// `INFLUX_API_MODE` as `influx.py` dispatches on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InfluxApiMode {
    /// Any value other than `v3` and `auto`.
    V2,
    V3,
    Auto,
}

/// The settings the agent reads, with Python's values. Integers are Python
/// `int`s of any size; callers convert them where they are used.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub sqlite_timeout_seconds: BigInt,
    pub sqlite_busy_timeout_ms: BigInt,
    /// `GRAPHYARD_SQLITE_JOURNAL_MODE`, stripped (not yet validated).
    pub sqlite_journal_mode: String,
    /// `GRAPHYARD_SQLITE_SYNCHRONOUS`, stripped (not yet validated).
    pub sqlite_synchronous: String,
    pub influx_url: String,
    pub influx_token: String,
    pub influx_org: String,
    pub influx_bucket: String,
    pub influx_measurement: String,
    pub influx_timeout_ms: BigInt,
    /// `INFLUX_API_MODE`, lower-cased.
    pub influx_api_mode: String,
    /// `GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS` (default of
    /// `--metrics-interval`).
    pub metric_collection_interval_seconds: BigInt,
    /// `GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS` (default of
    /// `--condition-interval`).
    pub condition_eval_interval_seconds: BigInt,
    /// `GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS`
    /// (`settings.CONDITION_DATA_STALE_WARNING_SECONDS`).
    pub condition_data_stale_warning_seconds: BigInt,
}

/// An environment variable whose value Python rejects while importing the
/// settings (the agent exits 1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SettingsError {
    /// The variable being read (the outer one for a proxied value).
    pub variable: &'static str,
    /// The Python exception.
    pub error: PyError,
}

impl fmt::Display for SettingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid environment variable {}: {}: {}",
            self.variable,
            self.error.python_class(),
            self.error
        )
    }
}

impl std::error::Error for SettingsError {}

/// django-environ's `Env` over a variable lookup.
struct Env<F> {
    lookup: F,
}

/// The outcome of `Env.get_value` before casting.
enum Raw {
    Default,
    Text(String),
}

impl<F: Fn(&str) -> Option<String>> Env<F> {
    /// `get_value` up to the cast: the default for a missing variable, the
    /// proxy chain resolved.
    fn raw(&self, var: &str) -> Result<Raw, PyError> {
        let mut name = var.to_owned();
        for _ in 0..MAX_PROXY_DEPTH {
            let Some(value) = (self.lookup)(&name) else {
                return Ok(Raw::Default);
            };
            match value.strip_prefix('$') {
                Some(_) => name = value.trim_start_matches('$').to_owned(),
                None => return Ok(Raw::Text(value)),
            }
        }
        Err(PyError::new(
            PyErrorKind::RecursionError,
            "maximum recursion depth exceeded",
        ))
    }

    /// `env.int(var, default=default)`.
    fn int(&self, var: &'static str, default: BigInt) -> Result<BigInt, SettingsError> {
        let fail = |error| SettingsError {
            variable: var,
            error,
        };
        match self.raw(var).map_err(fail)? {
            Raw::Default => Ok(default),
            Raw::Text(text) => py_int(&text).map_err(fail),
        }
    }

    /// `env(var, default=default)` (smart cast to `str`).
    fn str(&self, var: &'static str, default: &str) -> Result<String, SettingsError> {
        match self.raw(var) {
            Ok(Raw::Default) => Ok(default.to_owned()),
            Ok(Raw::Text(text)) => Ok(text),
            Err(error) => Err(SettingsError {
                variable: var,
                error,
            }),
        }
    }
}

impl Settings {
    /// Read the settings through `lookup` (variable name to value).
    ///
    /// # Errors
    /// The first variable, in `base.py` order, whose value Python rejects.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, SettingsError> {
        let env = Env { lookup };
        let sqlite_timeout_seconds = env.int("GRAPHYARD_SQLITE_TIMEOUT_SECONDS", 30.into())?;
        let sqlite_busy_timeout_ms = env.int("GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS", 30000.into())?;
        let sqlite_journal_mode =
            py_strip(&env.str("GRAPHYARD_SQLITE_JOURNAL_MODE", "WAL")?).to_owned();
        let sqlite_synchronous =
            py_strip(&env.str("GRAPHYARD_SQLITE_SYNCHRONOUS", "NORMAL")?).to_owned();
        let influx_url = env.str("INFLUX_URL", "")?;
        let influx_token = env.str("INFLUX_TOKEN", "")?;
        let influx_org = env.str("INFLUX_ORG", "")?;
        let influx_bucket = env.str("INFLUX_BUCKET", "graphyard")?;
        let influx_measurement = env.str("INFLUX_MEASUREMENT", "graphyard_metrics")?;
        let influx_timeout_ms = env.int("INFLUX_TIMEOUT_MS", 10000.into())?;
        let influx_api_mode = env.str("INFLUX_API_MODE", "auto")?.to_lowercase();
        let ha_poll_interval_seconds = env.int("GRAPHYARD_HA_POLL_INTERVAL_SECONDS", 60.into())?;
        let metric_collection_interval_seconds = env.int(
            "GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS",
            ha_poll_interval_seconds,
        )?;
        let condition_eval_interval_seconds =
            env.int("GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS", 60.into())?;
        let condition_data_stale_warning_seconds =
            env.int("GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS", 600.into())?;
        Ok(Self {
            sqlite_timeout_seconds,
            sqlite_busy_timeout_ms,
            sqlite_journal_mode,
            sqlite_synchronous,
            influx_url,
            influx_token,
            influx_org,
            influx_bucket,
            influx_measurement,
            influx_timeout_ms,
            influx_api_mode,
            metric_collection_interval_seconds,
            condition_eval_interval_seconds,
            condition_data_stale_warning_seconds,
        })
    }

    /// Read the settings from a snapshot of the process environment (like
    /// `os.environ`, taken once).
    ///
    /// # Errors
    /// See [`Settings::from_lookup`].
    pub fn from_process_env() -> Result<Self, SettingsError> {
        Self::from_vars(std::env::vars_os())
    }

    /// Read the settings from environment entries (`std::env::vars_os`);
    /// names and values that are not UTF-8 are converted lossily.
    ///
    /// # Errors
    /// See [`Settings::from_lookup`].
    pub fn from_vars(
        vars: impl IntoIterator<Item = (OsString, OsString)>,
    ) -> Result<Self, SettingsError> {
        let snapshot: HashMap<String, String> = vars
            .into_iter()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.to_string_lossy().into_owned(),
                )
            })
            .collect();
        Self::from_lookup(|name| snapshot.get(name).cloned())
    }

    /// The query/write mode of `influx.py` (`== "v3"`, `== "auto"`, else v2).
    pub fn influx_api_mode(&self) -> InfluxApiMode {
        match self.influx_api_mode.as_str() {
            "v3" => InfluxApiMode::V3,
            "auto" => InfluxApiMode::Auto,
            _ => InfluxApiMode::V2,
        }
    }

    /// The pragmas `apps.py:_configure_sqlite_connection` applies to every
    /// connection, with its warnings for invalid modes.
    pub fn sqlite_pragmas(&self) -> SqlitePragmas {
        SqlitePragmas::new(
            &self.sqlite_busy_timeout_ms,
            &self.sqlite_journal_mode,
            &self.sqlite_synchronous,
        )
    }
}

const VALID_JOURNAL_MODES: [&str; 6] = ["DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"];
const VALID_SYNCHRONOUS_MODES: [&str; 4] = ["OFF", "NORMAL", "FULL", "EXTRA"];

/// The three graphyard pragmas (busy timeout, journal mode, synchronous) as
/// `apps.py` computes them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlitePragmas {
    /// `int(max(0, GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS))`.
    pub busy_timeout_ms: BigInt,
    /// `strip().upper()` of the setting, `WAL` when not a valid mode.
    pub journal_mode: String,
    /// `strip().upper()` of the setting, `NORMAL` when not a valid mode.
    pub synchronous: String,
    /// The warning lines for invalid modes, in the order Python logs them.
    pub warnings: Vec<String>,
}

impl SqlitePragmas {
    fn new(busy_timeout_ms: &BigInt, journal_mode: &str, synchronous: &str) -> Self {
        let busy_timeout_ms = if busy_timeout_ms < &BigInt::zero() {
            BigInt::zero()
        } else {
            busy_timeout_ms.clone()
        };
        let mut warnings = Vec::new();
        let mut journal_mode = py_strip(journal_mode).to_uppercase();
        let mut synchronous = py_strip(synchronous).to_uppercase();
        if !VALID_JOURNAL_MODES.contains(&journal_mode.as_str()) {
            warnings.push(format!(
                "Invalid GRAPHYARD_SQLITE_JOURNAL_MODE={journal_mode}, falling back to WAL"
            ));
            journal_mode = "WAL".to_owned();
        }
        if !VALID_SYNCHRONOUS_MODES.contains(&synchronous.as_str()) {
            warnings.push(format!(
                "Invalid GRAPHYARD_SQLITE_SYNCHRONOUS={synchronous}, falling back to NORMAL"
            ));
            synchronous = "NORMAL".to_owned();
        }
        Self {
            busy_timeout_ms,
            journal_mode,
            synchronous,
            warnings,
        }
    }

    /// The statements in the order Python executes them.
    pub fn statements(&self) -> [String; 3] {
        [
            format!("PRAGMA busy_timeout = {}", self.busy_timeout_ms),
            format!("PRAGMA journal_mode = {}", self.journal_mode),
            format!("PRAGMA synchronous = {}", self.synchronous),
        ]
    }
}

#[cfg(test)]
mod tests;
