//! The `http_json_metric` executor (PLAN.md 6.3,
//! `services._execute_http_json_metric_spec`): GET a JSON document (no
//! redirects, body cap, total deadline), pick one value with the metric
//! path and write it as one point.

use serde_json::Value;

use graphyard_core::json_path::resolve_json_path;
use graphyard_core::model::{MetricPoint, SubjectType, Tags};
use graphyard_core::normalize::parse_finite_float;
use graphyard_core::pycompat::{py_str, py_strip};
use graphyard_core::spec_config::Config;

use super::{
    ExecutorResult, HttpKnobs, MetricSink, config_text, critical, read_json_with_deadline, warning,
    write_and_touch,
};
use crate::http::{ClientOptions, HttpClient, HttpContext};

/// `str(config.get(key, default)).strip()`.
fn stripped(config: &Config, key: &str, default: &str) -> String {
    py_strip(&config_text(config, key, default)).to_owned()
}

/// `a or b`.
fn or<'a>(a: &'a str, b: &'a str) -> &'a str {
    if a.is_empty() { b } else { a }
}

/// `_execute_http_json_metric_spec(spec)`.
///
/// # Errors
/// The `SpecConfigError` of a typed knob (Python raises it out of the
/// executor; the knobs are read before the required fields are checked).
pub async fn execute_http_json_metric<S: MetricSink>(
    http: &HttpContext,
    sink: &S,
    name: &str,
    config: &Value,
) -> ExecutorResult {
    let Value::Object(config) = config else {
        return Ok(critical("config must be an object"));
    };
    let url = stripped(config, "url", "");
    let metric_path = stripped(config, "metric_path", "");
    let metric_name = stripped(config, "metric_name", "");
    let host_id = config_text(config, "host_id", "external");
    let service_id = stripped(config, "service_id", "");
    let subject_type = stripped(config, "subject_type", SubjectType::Service.as_str());
    let subject_id = stripped(
        config,
        "subject_id",
        or(or(&service_id, &host_id), "external"),
    );
    let source_system = stripped(config, "source_system", "http");
    let source_instance = stripped(config, "source_instance", "default");
    let source_entity_id = stripped(config, "source_entity_id", "");
    let collector_service = stripped(
        config,
        "collector_service",
        or(&service_id, "graphyard-agent"),
    );
    let collector_host = stripped(config, "collector_host", or(&host_id, "external"));
    let knobs = HttpKnobs::from_config(config)?;

    if url.is_empty() {
        return Ok(critical("config.url is required"));
    }
    if metric_path.is_empty() {
        return Ok(critical("config.metric_path is required"));
    }
    if metric_name.is_empty() {
        return Ok(critical("config.metric_name is required"));
    }

    let mut tags = Tags::new();
    tags.insert("spec_name".to_owned(), name.to_owned());
    if let Some(Value::Object(configured)) = config.get("tags") {
        for (key, value) in configured {
            tags.insert(key.clone(), py_str(value));
        }
    }

    let mut options = ClientOptions::new(knobs.timeout_seconds, knobs.verify_tls)
        .header("Accept", "application/json");
    let bearer_token = stripped(config, "bearer_token", "");
    if !bearer_token.is_empty() {
        options = options.header("Authorization", &format!("Bearer {bearer_token}"));
    }
    let basic_username = stripped(config, "basic_username", "");
    let basic_password = stripped(config, "basic_password", "");
    if !basic_username.is_empty() && !basic_password.is_empty() {
        options = options.basic_auth(&basic_username, &basic_password);
    }
    let client = HttpClient::new(http, &options);
    let payload = match read_json_with_deadline(&client, &url, knobs).await {
        Ok(payload) => payload,
        Err(err) => return Ok(critical(err.message)),
    };
    drop(client);

    let raw_value = match resolve_json_path(&payload, &metric_path) {
        Ok(Some(raw_value)) => raw_value,
        Ok(None) => return Ok(warning(1, format!("metric_path not found: {metric_path}"))),
        Err(err) => return Ok(critical(err.message)),
    };
    let Ok(metric_value) = parse_finite_float(raw_value) else {
        return Ok(warning(1, "metric value is not a finite number"));
    };

    let is_host = py_strip(&subject_type).to_lowercase() == SubjectType::Host.as_str();
    let mut point = MetricPoint::new(
        sink.now(),
        metric_name,
        metric_value,
        subject_type,
        subject_id,
        source_system,
        or(&collector_service, "graphyard-agent"),
        or(or(&collector_host, &host_id), "external"),
    );
    point.source_instance = or(&source_instance, "default").to_owned();
    point.source_entity_id = (!source_entity_id.is_empty()).then_some(source_entity_id);
    point.host = is_host.then(|| host_id.clone());
    point.service = (!service_id.is_empty()).then_some(service_id);
    point.tags = Some(tags);
    Ok(write_and_touch(sink, &[point], 0).await)
}
