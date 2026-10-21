//! The real executors, metric sink and condition samples of the runs.
//!
//! The sink is `influx.write_points(points)` (or the shadow file of
//! `--influx-write-file`) followed by `touch_registry_from_points(points)`
//! on the store (PLAN.md 6.4, 5.5); the samples are the InfluxDB condition
//! query (6.5).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;

use graphyard_collect::executors::{
    ExecutorResult, MetricSink, RunState, SinkError, execute_home_assistant_env_scan,
    execute_home_assistant_sensor, execute_http_json_metric, execute_http_page_probe,
    execute_unifi_device_traffic,
};
use graphyard_collect::http::HttpContext;
use graphyard_core::settings::Settings;
use graphyard_core::{MetricPoint, MetricSample, SpecType};
use graphyard_influx::{ConditionQuery, InfluxWriter, QueryClient, QueryError};
use graphyard_store::Store;

use crate::runs::{Clock, ConditionSamples, SpecExecutors, SystemClock};

/// `influx.write_points` then `touch_registry_from_points`.
pub struct AgentSink {
    pub settings: Arc<Settings>,
    pub writer: Arc<InfluxWriter>,
    pub store: Store,
}

impl MetricSink for AgentSink {
    async fn write_points(&self, points: &[MetricPoint]) -> Result<u64, SinkError> {
        self.writer
            .write_points(&self.settings, points)
            .await
            .map_err(|err| SinkError::new(err.python_class(), err.to_string()))
    }

    async fn touch_registry(&self, points: &[MetricPoint]) -> Result<(), SinkError> {
        // `timezone.now()` inside the call.
        self.store
            .touch_registry(points.to_vec(), SystemClock.now())
            .await
            .map_err(|err| SinkError::new(err.python_class(), err.to_string()))
    }
}

/// `_SPEC_EXECUTORS`: the five executors on the process-wide HTTP context.
pub struct Collectors {
    pub http: HttpContext,
    pub sink: AgentSink,
}

impl SpecExecutors for Collectors {
    async fn execute(
        &self,
        spec_type: SpecType,
        name: &str,
        config: &Value,
        run: &mut RunState,
    ) -> ExecutorResult {
        let (http, sink) = (&self.http, &self.sink);
        match spec_type {
            SpecType::HomeAssistantSensor => {
                execute_home_assistant_sensor(http, sink, run, name, config).await
            }
            SpecType::HomeAssistantEnvScan => {
                execute_home_assistant_env_scan(http, sink, run, name, config).await
            }
            SpecType::HttpJsonMetric => execute_http_json_metric(http, sink, name, config).await,
            SpecType::HttpPageProbe => execute_http_page_probe(http, sink, name, config).await,
            SpecType::UnifiDeviceTraffic => {
                execute_unifi_device_traffic(http, sink, name, config).await
            }
        }
    }
}

/// The InfluxDB condition query.
pub struct InfluxSamples(pub QueryClient);

impl ConditionSamples for InfluxSamples {
    async fn query_condition_window(
        &self,
        condition: &ConditionQuery<'_>,
        now: DateTime<Utc>,
    ) -> Result<Vec<MetricSample>, QueryError> {
        self.0.query_condition_window(condition, now).await
    }
}
