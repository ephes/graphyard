//! `http_page_probe` (`services._execute_http_page_probe_spec`, PLAN.md
//! 6.3): one GET under the total deadline with redirects followed by hand
//! ([`run_page_probe`]); five timing and status points on a response, two
//! failure points on the warning path.
//!
//! Pinned by `fixtures/page_probe.json` (`gen_page_probe_unifi.py`) and the
//! deadline tests in `executors/tests`.

use chrono::{DateTime, Utc};
use serde_json::Value;

use graphyard_core::model::{MetricPoint, SpecOutcome, StatusLevel, SubjectType, Tags};
use graphyard_core::pycompat::{py_str, py_strip};
use graphyard_core::spec_config::{
    Config, DEFAULT_PAGE_PROBE_MAX_BODY_BYTES, bool_config, page_probe_total_timeout_config,
    positive_int_config, request_timeout_config,
};

use super::{ExecutorResult, MetricSink, config_object, config_text, now_utc};
use crate::http::{ClientOptions, HttpClient, HttpContext, run_page_probe, run_with_deadline};

/// The point metrics, in the order Python builds them.
pub const TTFB_METRIC: &str = "service.http_page_ttfb_seconds";
pub const TOTAL_METRIC: &str = "service.http_page_total_seconds";
pub const STATUS_CODE_METRIC: &str = "service.http_page_status_code";
pub const SUCCESS_METRIC: &str = "service.http_page_success";
pub const REDIRECT_COUNT_METRIC: &str = "service.http_page_redirect_count";

/// The dimensions every point of one probe shares (`_point` /
/// `_build_http_page_probe_point`).
struct ProbePoints {
    subject_id: String,
    service_id: String,
    source_system: String,
    source_instance: String,
    collector_service: String,
    collector_host: String,
    url: String,
    tags: Tags,
}

impl ProbePoints {
    fn point(&self, ts: DateTime<Utc>, metric: &str, value: f64) -> MetricPoint {
        let mut point = MetricPoint::new(
            ts,
            metric,
            value,
            SubjectType::Service.as_str(),
            self.subject_id.clone(),
            self.source_system.clone(),
            self.collector_service.clone(),
            self.collector_host.clone(),
        );
        point.source_instance.clone_from(&self.source_instance);
        point.source_entity_id = Some(self.url.clone());
        point.service = Some(self.service_id.clone());
        point.tags = Some(self.tags.clone());
        point
    }
}

/// `str(config.get(key, default)).strip()`.
fn stripped(config: &Config, key: &str, default: &str) -> String {
    py_strip(&config_text(config, key, default)).to_owned()
}

/// `value or fallback`.
fn or_default(value: String, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_owned()
    } else {
        value
    }
}

/// `_execute_http_page_probe_spec(spec)`.
///
/// # Errors
/// [`super::ExecutorError::Config`] for an invalid typed knob (checked
/// before the required fields, like Python); a sink failure on the warning
/// path is raised ([`super::ExecutorError::Raised`]), as Python raises it
/// out of its `except` block.
pub async fn execute_http_page_probe<S: MetricSink>(
    http: &HttpContext,
    sink: &S,
    name: &str,
    config: &Value,
) -> ExecutorResult {
    let Some(config) = config_object(config) else {
        return Ok(critical("config must be an object"));
    };
    let url = stripped(config, "url", "");
    let subject_id = stripped(config, "subject_id", "");
    let service_id = stripped(config, "service_id", &subject_id);
    let source_system = stripped(config, "source_system", "http_probe");
    let source_instance = stripped(config, "source_instance", "public_web");
    let collector_service = stripped(config, "collector_service", "graphyard-agent");
    let collector_host = stripped(config, "collector_host", "external");
    let timeout_seconds = request_timeout_config(config)?;
    let total_timeout_seconds = page_probe_total_timeout_config(config, timeout_seconds)?;
    let max_body_bytes =
        positive_int_config(config, "max_body_bytes", DEFAULT_PAGE_PROBE_MAX_BODY_BYTES)?;
    let verify_tls = bool_config(config, "verify_tls", true)?;
    let follow_redirects = bool_config(config, "follow_redirects", true)?;

    if url.is_empty() {
        return Ok(critical("config.url is required"));
    }
    if subject_id.is_empty() {
        return Ok(critical("config.subject_id is required"));
    }

    let mut tags = Tags::new();
    tags.insert("spec_name".to_owned(), name.to_owned());
    tags.insert("http_method".to_owned(), "get".to_owned());
    if let Some(Value::Object(configured)) = config.get("tags") {
        for (key, value) in configured {
            tags.insert(key.clone(), py_str(value));
        }
    }
    let shared = ProbePoints {
        subject_id,
        service_id,
        source_system: or_default(source_system, "http_probe"),
        source_instance: or_default(source_instance, "public_web"),
        collector_service: or_default(collector_service, "graphyard-agent"),
        collector_host: or_default(collector_host, "external"),
        url,
        tags,
    };

    let client = HttpClient::new(http, &ClientOptions::new(timeout_seconds, verify_tls));
    let url = shared.url.as_str();
    let probe = run_with_deadline(&client, url, total_timeout_seconds, |deadline| {
        let client = &client;
        async move {
            run_page_probe(client, url, max_body_bytes, follow_redirects, &deadline).await
        }
    })
    .await;

    match probe {
        Ok(probe) => {
            let now = now_utc();
            let success = (200..400).contains(&probe.status_code);
            let points = [
                shared.point(now, TTFB_METRIC, probe.ttfb_seconds),
                shared.point(now, TOTAL_METRIC, probe.total_seconds),
                shared.point(now, STATUS_CODE_METRIC, f64::from(probe.status_code)),
                shared.point(now, SUCCESS_METRIC, if success { 1.0 } else { 0.0 }),
                shared.point(now, REDIRECT_COUNT_METRIC, f64::from(probe.redirect_count)),
            ];
            // `except Exception`: a sink failure after a response is a
            // critical outcome, not an escaping exception.
            let written = match sink.write_points(&points).await {
                Ok(written) => written,
                Err(err) => return Ok(critical(err.message)),
            };
            if let Err(err) = sink.touch_registry(&points).await {
                return Ok(critical(err.message));
            }
            if success {
                Ok(SpecOutcome::new(StatusLevel::Ok, written, 0, ""))
            } else {
                Ok(SpecOutcome::new(
                    StatusLevel::Warning,
                    written,
                    0,
                    format!("unexpected status code: {}", probe.status_code),
                ))
            }
        }
        Err(err) if err.kind.is_page_probe_warning() => {
            let now = now_utc();
            let points = [
                shared.point(now, STATUS_CODE_METRIC, 0.0),
                shared.point(now, SUCCESS_METRIC, 0.0),
            ];
            // Raised out of the `except` block (PLAN.md 13.1 R3).
            let written = sink.write_points(&points).await?;
            sink.touch_registry(&points).await?;
            Ok(SpecOutcome::new(
                StatusLevel::Warning,
                written,
                0,
                err.message,
            ))
        }
        Err(err) => Ok(critical(err.message)),
    }
}

fn critical(message: impl Into<String>) -> SpecOutcome {
    SpecOutcome::new(StatusLevel::Critical, 0, 0, message)
}
