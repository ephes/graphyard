//! The metric collection executors (PLAN.md 6.3): one async function per
//! spec type, returning the `(status, ingested, skipped, message)` tuple of
//! `services.py` as a [`SpecOutcome`].
//!
//! - [`page_probe`]: `http_page_probe` (S7b);
//! - [`unifi`]: `unifi_device_traffic` (S7b).
//!
//! An executor gets the process-wide [`HttpContext`], a [`MetricSink`] for
//! `influx.write_points` and `touch_registry_from_points`, the spec's name
//! and its decoded `config`. What Python lets escape the executor is an
//! [`ExecutorError`]: a `SpecConfigError` of the typed knobs (the runner
//! stores its message, `critical`) or another exception (the runner stores
//! `collector raised <class>: <message>` and logs it, PLAN.md 3.4).

pub mod page_probe;
pub mod unifi;

use std::fmt;
use std::future::Future;

use chrono::{DateTime, SubsecRound, Utc};
use serde_json::Value;

use graphyard_core::model::{MetricPoint, SpecOutcome};
use graphyard_core::pycompat::py_str;
use graphyard_core::spec_config::{Config, SpecConfigError};

pub use page_probe::execute_http_page_probe;
pub use unifi::execute_unifi_device_traffic;

/// Where the executors deliver their points: `influx.write_points(points)`
/// and `touch_registry_from_points(points)` (PLAN.md 6.4, 5.5). The agent
/// implements it with the InfluxDB writer and the store; tests record the
/// calls.
pub trait MetricSink {
    /// `influx.write_points(points)`: the number of points written.
    fn write_points(&self, points: &[MetricPoint]) -> impl Future<Output = Result<u64, SinkError>>;

    /// `touch_registry_from_points(points)`.
    fn touch_registry(&self, points: &[MetricPoint])
    -> impl Future<Output = Result<(), SinkError>>;
}

/// A failure of the sink in Python's terms: the exception class and
/// `str(err)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SinkError {
    pub class: String,
    pub message: String,
}

impl SinkError {
    pub fn new(class: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            class: class.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for SinkError {
    /// `str(err)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SinkError {}

/// An exception that leaves an executor
/// (`_run_single_metric_collection_spec` handles it).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecutorError {
    /// `SpecConfigError` of a typed knob: `critical`, its message.
    Config(SpecConfigError),
    /// Any other exception: `critical`, `collector raised <class>:
    /// <message>`, plus the runner's error log line.
    Raised { class: String, message: String },
}

impl ExecutorError {
    /// The Python class name (`type(err).__name__`).
    pub fn python_class(&self) -> &str {
        match self {
            Self::Config(_) => "SpecConfigError",
            Self::Raised { class, .. } => class,
        }
    }
}

impl fmt::Display for ExecutorError {
    /// `str(err)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(err) => err.fmt(f),
            Self::Raised { message, .. } => f.write_str(message),
        }
    }
}

impl std::error::Error for ExecutorError {}

impl From<SpecConfigError> for ExecutorError {
    fn from(err: SpecConfigError) -> Self {
        Self::Config(err)
    }
}

impl From<SinkError> for ExecutorError {
    fn from(err: SinkError) -> Self {
        Self::Raised {
            class: err.class,
            message: err.message,
        }
    }
}

/// What an executor returns: its outcome or the exception it raised.
pub type ExecutorResult = Result<SpecOutcome, ExecutorError>;

/// `isinstance(spec.config, dict)`: the config object, or `None` for the
/// `config must be an object` outcome.
pub(crate) fn config_object(config: &Value) -> Option<&Config> {
    config.as_object()
}

/// `str(config.get(key, default))`.
pub(crate) fn config_text(config: &Config, key: &str, default: &str) -> String {
    config.get(key).map_or_else(|| default.to_owned(), py_str)
}

/// `datetime.now(UTC)`: the wall clock at Python's microsecond precision.
pub(crate) fn now_utc() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

#[cfg(test)]
mod tests;
