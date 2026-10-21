//! The metric collection executors (PLAN.md 6.3): one async function per
//! spec type, returning the `(status, ingested, skipped, message)` tuple of
//! `services.py` as a [`SpecOutcome`].
//!
//! - [`home_assistant`]: `home_assistant_sensor`, `home_assistant_env_scan`
//!   (S7a);
//! - [`http_json`]: `http_json_metric` (S7a);
//! - [`page_probe`]: `http_page_probe` (S7b);
//! - [`unifi`]: `unifi_device_traffic` (S7b).
//!
//! An executor gets the process-wide [`HttpContext`], a [`MetricSink`] for
//! `influx.write_points` and `touch_registry_from_points`, the spec's name
//! and its decoded `config`; the HA executors also get the metrics run's
//! [`RunState`] (the mappings' warn-once keys and compiled regexes). What Python lets escape the executor is an
//! [`ExecutorError`]: a `SpecConfigError` of the typed knobs (the runner
//! stores its message, `critical`) or another exception (the runner stores
//! `collector raised <class>: <message>` and logs it, PLAN.md 3.4).

pub mod home_assistant;
pub mod http_json;
pub mod page_probe;
pub mod unifi;

use std::fmt;
use std::future::Future;

use chrono::{DateTime, SubsecRound, Utc};
use serde_json::Value;

use graphyard_core::home_assistant::MappingState;
use graphyard_core::logline::{LogEvent, LogLevel};
use graphyard_core::model::{MetricPoint, SpecOutcome, StatusLevel};
use graphyard_core::pycompat::py_str;
use graphyard_core::spec_config::{
    Config, DEFAULT_PAGE_PROBE_MAX_BODY_BYTES, SpecConfigError, bool_config,
    page_probe_total_timeout_config, positive_int_config, request_timeout_config,
};

use crate::http::{HttpClient, HttpError, read_http_json, run_with_deadline};

pub use home_assistant::{execute_home_assistant_env_scan, execute_home_assistant_sensor};
pub use http_json::execute_http_json_metric;
pub use page_probe::execute_http_page_probe;
pub use unifi::execute_unifi_device_traffic;

/// Where the executors deliver their points: `influx.write_points(points)`
/// and `touch_registry_from_points(points)` (PLAN.md 6.4, 5.5). The agent
/// implements it with the InfluxDB writer and the store; tests record the
/// calls.
pub trait MetricSink {
    /// `datetime.now(UTC)` as the S7a executors read it (the HA fallback
    /// timestamp, the `http_json_metric` point); tests pin it.
    fn now(&self) -> DateTime<Utc> {
        now_utc()
    }

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

/// The state of one metrics run: the HA mappings' warn-once keys and
/// compiled regexes (`_subject_mapping_warning_keys`, cleared at the start
/// of every run, so the run creates a new one).
#[derive(Debug, Default)]
pub struct RunState {
    pub mappings: MappingState,
}

impl RunState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Hand the mapping warnings recorded so far to the logger.
    pub(crate) fn flush_logs(&mut self) {
        for event in self.mappings.take_events() {
            emit(&event);
        }
    }
}

/// Hand a record of the pure code to the `log` facade.
pub(crate) fn emit(event: &LogEvent) {
    match event.level {
        LogLevel::Info => log::info!("{}", event.message),
        LogLevel::Warning => log::warn!("{}", event.message),
        LogLevel::Error => log::error!("{}", event.message),
    }
}

/// The typed HTTP knobs of a spec config, read in Python's order:
/// `request_timeout_seconds`, `total_timeout_seconds`, `max_body_bytes`,
/// `verify_tls`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HttpKnobs {
    pub timeout_seconds: f64,
    pub total_timeout_seconds: f64,
    pub max_body_bytes: u64,
    pub verify_tls: bool,
}

impl HttpKnobs {
    /// # Errors
    /// The first [`SpecConfigError`] in Python's order.
    pub fn from_config(config: &Config) -> Result<Self, SpecConfigError> {
        let timeout_seconds = request_timeout_config(config)?;
        let total_timeout_seconds = page_probe_total_timeout_config(config, timeout_seconds)?;
        let max_body_bytes =
            positive_int_config(config, "max_body_bytes", DEFAULT_PAGE_PROBE_MAX_BODY_BYTES)?;
        let verify_tls = bool_config(config, "verify_tls", true)?;
        Ok(Self {
            timeout_seconds,
            total_timeout_seconds,
            max_body_bytes,
            verify_tls,
        })
    }
}

/// `_run_http_with_deadline(client, url=url, operation=lambda c:
/// _read_http_json(client, url, max_body_bytes, c), total_timeout_seconds=…)`.
///
/// # Errors
/// The errors of [`read_http_json`] and [`run_with_deadline`].
pub(crate) async fn read_json_with_deadline(
    client: &HttpClient,
    url: &str,
    knobs: HttpKnobs,
) -> Result<Value, HttpError> {
    run_with_deadline(client, url, knobs.total_timeout_seconds, |deadline| async move {
        read_http_json(client, url, knobs.max_body_bytes, &deadline).await
    })
    .await
}

/// `StatusLevel.CRITICAL, 0, 0, message`.
pub(crate) fn critical(message: impl Into<String>) -> SpecOutcome {
    SpecOutcome::new(StatusLevel::Critical, 0, 0, message)
}

/// `StatusLevel.WARNING, 0, skipped, message`.
pub(crate) fn warning(skipped: u64, message: impl Into<String>) -> SpecOutcome {
    SpecOutcome::new(StatusLevel::Warning, 0, skipped, message)
}

/// `influx.write_points(points)` then `touch_registry_from_points(points)`;
/// `ok, written, skipped, ""`, or `critical, 0, 0, str(err)` for the first
/// failure (points already written stay written, as in Python).
pub(crate) async fn write_and_touch<S: MetricSink>(
    sink: &S,
    points: &[MetricPoint],
    skipped: u64,
) -> SpecOutcome {
    let written = match sink.write_points(points).await {
        Ok(written) => written,
        Err(err) => return critical(err.message),
    };
    if let Err(err) = sink.touch_registry(points).await {
        return critical(err.message);
    }
    SpecOutcome::new(StatusLevel::Ok, written, skipped, "")
}

#[cfg(test)]
mod tests;
