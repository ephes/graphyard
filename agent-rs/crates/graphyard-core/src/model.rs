//! Shared domain types of the agent.
//!
//! Python sources: `models.py` (`StatusLevel`, `SubjectType`,
//! `MetricCollectionSpecType`, `ComparisonOperator`), `influx.py`
//! (`MetricPoint`, `MetricSample`) and the result dataclasses of
//! `services.py` (`ConditionEvaluation`, `ConditionEvaluationRun`,
//! `MetricCollectionSpecRun`, the executors' `(status, ingested, skipped,
//! message)` tuple). The value tables are pinned by `fixtures/model.json`.
//!
//! The enums parse their stored text exactly, like the Python dictionary
//! lookups (`_STATUS_SEVERITY[...]`, `_SPEC_EXECUTORS.get(...)`,
//! `_operator_fn(...)`); normalisation such as `strip().lower()` of a subject
//! type belongs to the callers (PLAN.md 6.4).

use std::fmt;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;

/// Custom tags of a point or sample: a Python `dict[str, str]` (insertion
/// order kept, re-assigning a key keeps its position).
pub type Tags = IndexMap<String, String>;

/// `models.py:StatusLevel`: spec, condition and heartbeat status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StatusLevel {
    Ok,
    Warning,
    Critical,
}

impl StatusLevel {
    /// In the order of `StatusLevel.CHOICES`.
    pub const ALL: [Self; 3] = [Self::Ok, Self::Warning, Self::Critical];

    /// The stored text.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }

    /// Exact lookup of the stored text.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|level| level.as_str() == value)
    }

    /// `services.py:_STATUS_SEVERITY`: ok 0, warning 1, critical 2.
    pub const fn severity(self) -> u8 {
        match self {
            Self::Ok => 0,
            Self::Warning => 1,
            Self::Critical => 2,
        }
    }
}

impl fmt::Display for StatusLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `models.py:SubjectType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SubjectType {
    Host,
    NetworkDevice,
    EnvironmentSensor,
    Service,
}

impl SubjectType {
    /// In the order of `SubjectType.CHOICES`.
    pub const ALL: [Self; 4] = [
        Self::Host,
        Self::NetworkDevice,
        Self::EnvironmentSensor,
        Self::Service,
    ];

    /// `sorted(SubjectType.ALL)`, as listed in the `Unknown subject_type`
    /// message of `influx.py:normalize_metric_point`.
    pub const SORTED_NAMES: [&'static str; 4] =
        ["environment_sensor", "host", "network_device", "service"];

    /// The stored text.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::NetworkDevice => "network_device",
            Self::EnvironmentSensor => "environment_sensor",
            Self::Service => "service",
        }
    }

    /// Exact lookup of the stored text (`value in SubjectType.ALL`).
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

impl fmt::Display for SubjectType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `models.py:MetricCollectionSpecType`: the spec types that have an
/// executor (`services.py:_SPEC_EXECUTORS`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SpecType {
    HomeAssistantSensor,
    HomeAssistantEnvScan,
    HttpJsonMetric,
    HttpPageProbe,
    UnifiDeviceTraffic,
}

impl SpecType {
    /// In the order of `MetricCollectionSpecType.CHOICES`.
    pub const ALL: [Self; 5] = [
        Self::HomeAssistantSensor,
        Self::HomeAssistantEnvScan,
        Self::HttpJsonMetric,
        Self::HttpPageProbe,
        Self::UnifiDeviceTraffic,
    ];

    /// The stored text.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HomeAssistantSensor => "home_assistant_sensor",
            Self::HomeAssistantEnvScan => "home_assistant_env_scan",
            Self::HttpJsonMetric => "http_json_metric",
            Self::HttpPageProbe => "http_page_probe",
            Self::UnifiDeviceTraffic => "unifi_device_traffic",
        }
    }

    /// Exact lookup (`_SPEC_EXECUTORS.get(spec.spec_type)`); `None` is the
    /// `unsupported spec_type: <t>` path.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

impl fmt::Display for SpecType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `models.py:ComparisonOperator` with `services.py:_operator_fn`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Gt,
    Gte,
    Lt,
    Lte,
}

impl ComparisonOperator {
    /// In the order of `ComparisonOperator.CHOICES`.
    pub const ALL: [Self; 4] = [Self::Gt, Self::Gte, Self::Lt, Self::Lte];

    /// The stored text (also the text condition messages interpolate).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gt => "gt",
            Self::Gte => "gte",
            Self::Lt => "lt",
            Self::Lte => "lte",
        }
    }

    /// Exact lookup; `None` is the `KeyError` of `_operator_fn`.
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|operator| operator.as_str() == value)
    }

    /// `_operator_fn(operator)(left, right)`.
    pub fn compare(self, left: f64, right: f64) -> bool {
        match self {
            Self::Gt => left > right,
            Self::Gte => left >= right,
            Self::Lt => left < right,
            Self::Lte => left <= right,
        }
    }
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// `influx.py:MetricPoint`: a point as an executor builds it, before
/// `normalize_metric_point`. The dimension fields hold the raw text, so the
/// normalisation can report what it rejected.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricPoint {
    pub ts: DateTime<Utc>,
    pub metric: String,
    pub value: f64,
    pub subject_type: String,
    pub subject_id: String,
    pub source_system: String,
    pub collector_service: String,
    pub collector_host: String,
    pub source_instance: String,
    pub source_entity_id: Option<String>,
    pub host: Option<String>,
    pub service: Option<String>,
    pub tags: Option<Tags>,
}

impl MetricPoint {
    /// The dataclass default of `source_instance`.
    pub const DEFAULT_SOURCE_INSTANCE: &'static str = "default";

    /// A point with the required fields and the dataclass defaults
    /// (`source_instance="default"`, every optional field `None`).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ts: DateTime<Utc>,
        metric: impl Into<String>,
        value: f64,
        subject_type: impl Into<String>,
        subject_id: impl Into<String>,
        source_system: impl Into<String>,
        collector_service: impl Into<String>,
        collector_host: impl Into<String>,
    ) -> Self {
        Self {
            ts,
            metric: metric.into(),
            value,
            subject_type: subject_type.into(),
            subject_id: subject_id.into(),
            source_system: source_system.into(),
            collector_service: collector_service.into(),
            collector_host: collector_host.into(),
            source_instance: Self::DEFAULT_SOURCE_INSTANCE.to_owned(),
            source_entity_id: None,
            host: None,
            service: None,
            tags: None,
        }
    }
}

/// `influx.py:MetricSample`: one sample read back for condition evaluation.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricSample {
    pub ts: DateTime<Utc>,
    pub value: f64,
    pub host: String,
    pub metric: String,
    pub service: Option<String>,
    pub subject_type: Option<String>,
    pub subject_id: Option<String>,
    pub source_system: Option<String>,
    pub source_instance: Option<String>,
    pub source_entity_id: Option<String>,
    pub collector_service: Option<String>,
    pub collector_host: Option<String>,
    pub tags: Tags,
}

impl MetricSample {
    /// A sample with the required fields and the dataclass defaults (every
    /// optional dimension `None`, no custom tags).
    pub fn new(
        ts: DateTime<Utc>,
        value: f64,
        host: impl Into<String>,
        metric: impl Into<String>,
        service: Option<String>,
    ) -> Self {
        Self {
            ts,
            value,
            host: host.into(),
            metric: metric.into(),
            service,
            subject_type: None,
            subject_id: None,
            source_system: None,
            source_instance: None,
            source_entity_id: None,
            collector_service: None,
            collector_host: None,
            tags: Tags::new(),
        }
    }
}

/// `services.py:ConditionEvaluation`: the result of `evaluate_condition`.
#[derive(Clone, Debug, PartialEq)]
pub struct ConditionEvaluation {
    pub status: StatusLevel,
    pub message: String,
    pub last_value: Option<f64>,
    pub evaluated_at: DateTime<Utc>,
}

/// `services.py:ConditionEvaluationRun`: counters of one conditions run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConditionEvaluationRun {
    pub total: u64,
    pub failed: u64,
}

/// `services.py:MetricCollectionSpecRun`: counters of one metrics run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetricCollectionSpecRun {
    pub total: u64,
    pub failed: u64,
    pub warning: u64,
    pub ingested: u64,
    pub skipped: u64,
}

/// The `(status, ingested, skipped, message)` tuple every executor and
/// `_run_single_metric_collection_spec` return.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpecOutcome {
    pub status: StatusLevel,
    pub ingested: u64,
    pub skipped: u64,
    pub message: String,
}

impl SpecOutcome {
    /// `(status, ingested, skipped, message)`.
    pub fn new(
        status: StatusLevel,
        ingested: u64,
        skipped: u64,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            ingested,
            skipped,
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests;
