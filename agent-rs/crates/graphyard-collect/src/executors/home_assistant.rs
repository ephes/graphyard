//! The Home Assistant executors (PLAN.md 6.3): `home_assistant_sensor`
//! (`services._execute_home_assistant_sensor_spec`) and
//! `home_assistant_env_scan` (`services._execute_home_assistant_env_scan_spec`).
//!
//! Both GET the HA REST API with `Authorization: Bearer <access_token>`,
//! no redirects, the body cap and the total deadline, and normalise states
//! with [`graphyard_core::home_assistant`].

use std::collections::HashSet;

use serde_json::Value;

use graphyard_core::home_assistant::{HaSpec, normalize_state, regex_search};
use graphyard_core::model::{SpecOutcome, Tags};
use graphyard_core::pycompat::{py_str, py_strip};
use graphyard_core::spec_config::Config;

use super::{
    ExecutorResult, HttpKnobs, MetricSink, RunState, config_text, critical,
    read_json_with_deadline, warning, write_and_touch,
};
use crate::http::{ClientOptions, HttpClient, HttpContext};

/// The default `include_device_classes`.
const DEFAULT_DEVICE_CLASSES: [&str; 2] = ["temperature", "humidity"];
/// The default `entity_id_regex`.
const DEFAULT_ENTITY_ID_REGEX: &str = "(temperature|humidity)";

/// `base_url` without trailing `/` and `access_token`, or the
/// required-field outcome.
fn base_url_and_token(config: &Config) -> Result<(String, String), SpecOutcome> {
    let base_url = config_text(config, "base_url", "")
        .trim_end_matches('/')
        .to_owned();
    let access_token = config_text(config, "access_token", "");
    if base_url.is_empty() {
        return Err(critical("config.base_url is required"));
    }
    if access_token.is_empty() {
        return Err(critical("config.access_token is required"));
    }
    Ok((base_url, access_token))
}

/// `httpx.Client(timeout=…, headers={"Authorization": "Bearer …",
/// "Accept": "application/json"}, verify=…)`.
fn ha_client(http: &HttpContext, knobs: HttpKnobs, access_token: &str) -> HttpClient {
    let options = ClientOptions::new(knobs.timeout_seconds, knobs.verify_tls)
        .header("Authorization", &format!("Bearer {access_token}"))
        .header("Accept", "application/json");
    HttpClient::new(http, &options)
}

/// `{"spec_name": spec.name}`.
fn spec_name_tag(name: &str) -> Tags {
    let mut tags = Tags::new();
    tags.insert("spec_name".to_owned(), name.to_owned());
    tags
}

/// `_execute_home_assistant_sensor_spec(spec)`: one entity's state from
/// `/api/states/<entity_id>`, one point.
///
/// # Errors
/// The `SpecConfigError` of a typed knob (Python raises it out of the
/// executor).
pub async fn execute_home_assistant_sensor<S: MetricSink>(
    http: &HttpContext,
    sink: &S,
    run: &mut RunState,
    name: &str,
    config: &Value,
) -> ExecutorResult {
    let Value::Object(config) = config else {
        return Ok(critical("config must be an object"));
    };
    let (base_url, access_token) = match base_url_and_token(config) {
        Ok(fields) => fields,
        Err(outcome) => return Ok(outcome),
    };
    let entity_id = py_strip(&config_text(config, "entity_id", "")).to_owned();
    if entity_id.is_empty() {
        return Ok(critical("config.entity_id is required"));
    }
    let metric_name = config_text(config, "metric_name", "");
    let knobs = HttpKnobs::from_config(config)?;

    let client = ha_client(http, knobs, &access_token);
    let url = format!("{base_url}/api/states/{entity_id}");
    let payload = match read_json_with_deadline(&client, &url, knobs).await {
        Ok(payload) => payload,
        Err(err) => return Ok(critical(err.message)),
    };
    drop(client);
    let Value::Object(state) = &payload else {
        return Ok(warning(1, "invalid JSON payload"));
    };
    let point = normalize_state(
        state,
        HaSpec { name, config },
        &metric_name,
        &spec_name_tag(name),
        sink.now(),
        &mut run.mappings,
    );
    run.flush_logs();
    match point {
        Ok(Some(point)) => Ok(write_and_touch(sink, &[point], 0).await),
        Ok(None) => Ok(warning(1, "state payload not numeric or mapping failed")),
        Err(err) => Ok(critical(err.message)),
    }
}

/// `include_device_classes` (a list, else the default), each
/// `str(item).strip().lower()`.
fn device_classes(config: &Config) -> HashSet<String> {
    match config.get("include_device_classes") {
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| py_strip(&py_str(item)).to_lowercase())
            .collect(),
        _ => DEFAULT_DEVICE_CLASSES
            .iter()
            .map(|class| (*class).to_owned())
            .collect(),
    }
}

/// `_execute_home_assistant_env_scan_spec(spec)`: every `sensor.` and
/// `binary_sensor.` state from `/api/states` whose device class is listed
/// or whose entity id matches `entity_id_regex` (case-insensitive search).
///
/// # Errors
/// The `SpecConfigError` of a typed knob (Python raises it out of the
/// executor).
pub async fn execute_home_assistant_env_scan<S: MetricSink>(
    http: &HttpContext,
    sink: &S,
    run: &mut RunState,
    name: &str,
    config: &Value,
) -> ExecutorResult {
    let Value::Object(config) = config else {
        return Ok(critical("config must be an object"));
    };
    let (base_url, access_token) = match base_url_and_token(config) {
        Ok(fields) => fields,
        Err(outcome) => return Ok(outcome),
    };
    let metric_prefix = config_text(config, "metric_prefix", "ha.");
    let knobs = HttpKnobs::from_config(config)?;
    let include_device_classes = device_classes(config);
    let entity_regex_value = config_text(config, "entity_id_regex", DEFAULT_ENTITY_ID_REGEX);
    let entity_pattern = match run.mappings.compile(&entity_regex_value, true) {
        Ok(pattern) => pattern,
        Err(err) => return Ok(critical(format!("invalid entity_id_regex: {err}"))),
    };

    let client = ha_client(http, knobs, &access_token);
    let url = format!("{base_url}/api/states");
    let payload = match read_json_with_deadline(&client, &url, knobs).await {
        Ok(payload) => payload,
        Err(err) => return Ok(critical(err.message)),
    };
    drop(client);
    let Value::Array(items) = &payload else {
        return Ok(critical("states response is not a list"));
    };

    let ha_spec = HaSpec { name, config };
    let extra_tags = spec_name_tag(name);
    let mut points = Vec::new();
    let mut skipped: u64 = 0;
    for item in items {
        let Value::Object(item) = item else {
            continue;
        };
        let Some(Value::String(entity_id)) = item.get("entity_id") else {
            continue;
        };
        if !(entity_id.starts_with("sensor.") || entity_id.starts_with("binary_sensor.")) {
            continue;
        }
        let device_class = match item.get("attributes") {
            Some(Value::Object(attributes)) => attributes.get("device_class").map(py_str),
            _ => None,
        }
        .unwrap_or_default()
        .to_lowercase();
        let include = if !device_class.is_empty() && include_device_classes.contains(&device_class)
        {
            true
        } else {
            match regex_search(&entity_pattern, entity_id) {
                Ok(found) => found,
                Err(err) => return Ok(critical(err.message)),
            }
        };
        if !include {
            continue;
        }
        let point = normalize_state(
            item,
            ha_spec,
            &format!("{metric_prefix}{entity_id}"),
            &extra_tags,
            sink.now(),
            &mut run.mappings,
        );
        run.flush_logs();
        match point {
            Ok(Some(point)) => points.push(point),
            Ok(None) => skipped += 1,
            Err(err) => return Ok(critical(err.message)),
        }
    }
    if points.is_empty() {
        return Ok(warning(skipped, "no matching numeric states"));
    }
    Ok(write_and_touch(sink, &points, skipped).await)
}
