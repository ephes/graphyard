//! The Home Assistant executors (PLAN.md 6.3): `home_assistant_sensor`
//! (`services._execute_home_assistant_sensor_spec`) and
//! `home_assistant_env_scan` (`services._execute_home_assistant_env_scan_spec`).
//!
//! Both GET the HA REST API with `Authorization: Bearer <access_token>`,
//! no redirects, the body cap and the total deadline, and normalise states
//! with [`graphyard_core::home_assistant`].

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use graphyard_core::diagnostics::collector_failure;
use serde_json::Value;

use graphyard_core::home_assistant::{
    HaSpec, admit_output, admit_work, entity_name_slug, normalize_state, prepare_mappings,
    regex_search,
};
use graphyard_core::model::{MetricPoint, SpecOutcome, Tags};
use graphyard_core::pycompat::{py_str, py_strip};
use graphyard_core::spec_config::{Config, bool_config, positive_int_config};

use super::{
    ExecutorResult, HttpKnobs, MetricSink, RunState, config_text, critical,
    post_json_with_deadline, read_json_with_deadline, warning, write_and_touch,
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
        Err(err) => return Ok(critical(err.safe_message())),
    };
    drop(client);
    let Value::Object(state) = &payload else {
        return Ok(warning(1, "invalid JSON payload"));
    };
    if let Err(message) = admit_work(config, std::slice::from_ref(&payload)) {
        return Ok(critical(message));
    }
    if let Err(message) = admit_output(config, name, std::slice::from_ref(&payload)) {
        return Ok(critical(message));
    }
    prepare_mappings(config, &mut run.mappings);
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
        Err(err) => Ok(critical(collector_failure(err.python_class()))),
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

/// Rust-only env scan knobs (the retired Python collector ignores them):
/// `include_entity_metadata` tags every point with the entity's
/// `friendly_name`, its Home Assistant area (`area`) and
/// `device_role=thermostat` when its device has a `climate.*` entity;
/// `climate_attributes` writes those numeric attributes of every
/// `climate.*` entity as their own points.
#[derive(Clone, Debug, Default)]
struct ScanOptions {
    include_metadata: bool,
    metadata_refresh: Duration,
    climate_attributes: Vec<String>,
}

/// The default `entity_metadata_refresh_seconds`.
const DEFAULT_METADATA_REFRESH_SECONDS: u64 = 600;
/// At most this many `climate_attributes`.
const MAX_CLIMATE_ATTRIBUTES: usize = 4;
/// Tag values longer than this many characters are left out (the writer
/// rejects tag values over 256 characters).
const MAX_METADATA_TAG_CHARS: usize = 256;

/// One row per `sensor.`, `binary_sensor.` and `climate.` entity:
/// `[entity_id, area name or "", device has a climate entity]`, rendered
/// by Home Assistant's `/api/template` as JSON text. `area_name` resolves
/// the entity's own area, else its device's.
const METADATA_TEMPLATE: &str = concat!(
    "[{%- for s in states.sensor | list + states.binary_sensor | list + states.climate | list -%}",
    "{%- set d = device_id(s.entity_id) -%}",
    "{%- if not loop.first %},{% endif -%}",
    "[{{ s.entity_id | tojson }},{{ (area_name(s.entity_id) or '') | tojson }},",
    "{{ (d is not none and (device_entities(d) | select('match', 'climate\\\\.') | list | count) > 0) | tojson }}]",
    "{%- endfor -%}]",
);

impl ScanOptions {
    /// The options, or the critical outcome of a malformed
    /// `climate_attributes` (lower-case names, at most four).
    ///
    /// # Errors
    /// The `SpecConfigError` of a typed knob.
    fn from_config(
        config: &Config,
    ) -> Result<Result<Self, SpecOutcome>, graphyard_core::spec_config::SpecConfigError> {
        let include_metadata = bool_config(config, "include_entity_metadata", false)?;
        let refresh = positive_int_config(
            config,
            "entity_metadata_refresh_seconds",
            DEFAULT_METADATA_REFRESH_SECONDS,
        )?;
        let climate_attributes = match config.get("climate_attributes") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(items)) => {
                let mut names = Vec::new();
                for item in items {
                    let Value::String(name) = item else {
                        return Ok(Err(critical("invalid climate_attributes")));
                    };
                    let valid = !name.is_empty()
                        && name.len() <= 64
                        && name.bytes().all(|b| b.is_ascii_lowercase() || b == b'_');
                    if !valid {
                        return Ok(Err(critical("invalid climate_attributes")));
                    }
                    if !names.contains(name) {
                        names.push(name.clone());
                    }
                }
                if names.len() > MAX_CLIMATE_ATTRIBUTES {
                    return Ok(Err(critical("invalid climate_attributes")));
                }
                names
            }
            Some(_) => return Ok(Err(critical("invalid climate_attributes"))),
        };
        Ok(Ok(Self {
            include_metadata,
            metadata_refresh: Duration::from_secs(refresh),
            climate_attributes,
        }))
    }
}

/// What the metadata template says about one entity.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct EntityMetadata {
    area: String,
    thermostat: bool,
}

type MetadataMap = Arc<HashMap<String, EntityMetadata>>;

struct MetadataCacheEntry {
    fetched: Instant,
    entities: MetadataMap,
}

/// Per spec name and base URL; lives as long as the agent process.
static METADATA_CACHE: LazyLock<Mutex<HashMap<String, MetadataCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Parse the template's rows; malformed rows are ignored.
///
/// # Errors
/// The payload is not a list or has more than [`HA_MAX_ENTITIES`] rows.
fn parse_metadata(payload: &Value) -> Result<HashMap<String, EntityMetadata>, &'static str> {
    let Value::Array(rows) = payload else {
        return Err("entity metadata is not a list");
    };
    if rows.len() > graphyard_core::home_assistant::HA_MAX_ENTITIES {
        return Err("entity metadata exceeds the entity limit");
    }
    let mut entities = HashMap::with_capacity(rows.len());
    for row in rows {
        let Some(
            [
                Value::String(entity_id),
                Value::String(area),
                Value::Bool(thermostat),
            ],
        ) = row.as_array().map(Vec::as_slice)
        else {
            continue;
        };
        entities.insert(
            entity_id.clone(),
            EntityMetadata {
                area: py_strip(area).to_owned(),
                thermostat: *thermostat,
            },
        );
    }
    Ok(entities)
}

/// The cached entity metadata of this spec, refreshed through
/// `/api/template` once it is older than the refresh interval. A failed
/// refresh keeps the previous metadata (or none) and is retried on the
/// next run.
async fn entity_metadata(
    http: &HttpContext,
    knobs: HttpKnobs,
    base_url: &str,
    access_token: &str,
    name: &str,
    refresh: Duration,
) -> MetadataMap {
    let key = format!("{name}\u{1f}{base_url}");
    let cached = {
        let cache = METADATA_CACHE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cache.get(&key).map(|entry| {
            (
                entry.fetched.elapsed() < refresh,
                Arc::clone(&entry.entities),
            )
        })
    };
    if let Some((true, entities)) = &cached {
        return Arc::clone(entities);
    }
    let previous = cached.map(|(_, entities)| entities);
    let body =
        serde_json::to_vec(&serde_json::json!({"template": METADATA_TEMPLATE})).unwrap_or_default();
    let client = ha_client(http, knobs, access_token);
    let url = format!("{base_url}/api/template");
    let result = post_json_with_deadline(&client, &url, body, knobs).await;
    drop(client);
    let parsed = match result {
        Ok(payload) => parse_metadata(&payload).map_err(str::to_owned),
        Err(err) => Err(err.safe_message()),
    };
    match parsed {
        Ok(entities) => {
            let entities = Arc::new(entities);
            let mut cache = METADATA_CACHE
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            cache.insert(
                key,
                MetadataCacheEntry {
                    fetched: Instant::now(),
                    entities: Arc::clone(&entities),
                },
            );
            entities
        }
        Err(message) => {
            log::warn!(
                "Spec {} entity metadata refresh failed: {}",
                graphyard_core::logline::escape_log_value(name),
                graphyard_core::logline::escape_log_value(&message)
            );
            previous.unwrap_or_default()
        }
    }
}

/// A tag value from Home Assistant text: stripped, `None` when empty, too
/// long or containing control characters.
fn metadata_tag_value(raw: &str) -> Option<String> {
    let value = py_strip(raw);
    let valid = !value.is_empty()
        && value.chars().take(MAX_METADATA_TAG_CHARS + 1).count() <= MAX_METADATA_TAG_CHARS
        && !value.chars().any(char::is_control);
    valid.then(|| value.to_owned())
}

/// The writer's tag limit (`metric exceeds 32 tags`).
const MAX_POINT_TAGS: usize = 32;

/// Add `friendly_name`, `area` and `device_role` to a point's tags, in that
/// order, never replacing a tag the point already has and only while the
/// point stays within the writer's tag limit, so metadata never costs a
/// reading.
fn add_metadata_tags(
    point: &mut MetricPoint,
    entity_id: &str,
    item: &serde_json::Map<String, Value>,
    metadata: &HashMap<String, EntityMetadata>,
) {
    let friendly_name = item
        .get("attributes")
        .and_then(Value::as_object)
        .and_then(|attributes| attributes.get("friendly_name"))
        .and_then(Value::as_str)
        .and_then(metadata_tag_value);
    let entry = metadata.get(entity_id);
    let area = entry.and_then(|entry| metadata_tag_value(&entry.area));
    let role = entry
        .filter(|entry| entry.thermostat)
        .map(|_| "thermostat".to_owned());
    let tags = point.tags.get_or_insert_with(Tags::new);
    for (key, value) in [
        ("friendly_name", friendly_name),
        ("area", area),
        ("device_role", role),
    ] {
        let Some(value) = value else { continue };
        if tags.len() >= MAX_POINT_TAGS || tags.contains_key(key) {
            continue;
        }
        tags.insert(key.to_owned(), value);
    }
}

/// The envelope of the Rust-only knobs, checked before any point is built,
/// without copying rows. Climate attribute points count as rows of their
/// own (entity id `<entity_id>.<attribute>`): the work ceilings
/// ([`admit_work_rows`]) and the output budget hold for rows plus climate
/// points together. Metadata tags (friendly name as received and capped
/// like the tag, the longest area, the role and the keys) are charged per
/// point to the same [`HA_MAX_OUTPUT_BYTES`] budget as the rows.
///
/// # Errors
/// The collection does not fit; no partial batch is allowed.
fn admit_extensions(
    config: &Config,
    name: &str,
    items: &[Value],
    options: &ScanOptions,
    metadata: Option<&HashMap<String, EntityMetadata>>,
) -> Result<(), &'static str> {
    use graphyard_core::home_assistant::{HA_MAX_OUTPUT_BYTES, OutputReservation, admit_work_rows};
    const EXCEEDED: &str = "collector failure: _HomeAssistantLimitExceeded";
    if options.climate_attributes.is_empty() && metadata.is_none() {
        return Ok(());
    }
    let reservation = OutputReservation::new(config, name);
    let area = metadata.map_or(0, |metadata| {
        metadata
            .values()
            .map(|entry| entry.area.len().min(MAX_METADATA_TAG_CHARS * 4))
            .max()
            .unwrap_or(0)
    });
    let mut rows = 0usize;
    let mut entity_bytes = 0usize;
    let mut total = 0usize;
    for item in items {
        let entity_id = item.get("entity_id").and_then(Value::as_str).unwrap_or("");
        // As admit_work: a non-string id counts with its Python text.
        let id_len = item.get("entity_id").map_or(0, |id| py_str(id).len());
        let metadata_bytes = if metadata.is_some() {
            let name = item
                .get("attributes")
                .and_then(|attributes| attributes.get("friendly_name"))
                .and_then(Value::as_str)
                .map_or(0, |name| name.len().min(MAX_METADATA_TAG_CHARS * 4));
            name.saturating_add(area).saturating_add(64)
        } else {
            0
        };
        rows = rows.saturating_add(1);
        entity_bytes = entity_bytes.saturating_add(id_len);
        total = total
            .saturating_add(reservation.row(item))
            .saturating_add(metadata_bytes);
        if entity_id.starts_with("climate.") {
            for attribute in &options.climate_attributes {
                let id_bytes = entity_id
                    .len()
                    .saturating_add(1)
                    .saturating_add(attribute.len());
                rows = rows.saturating_add(1);
                entity_bytes = entity_bytes.saturating_add(id_bytes);
                total = total
                    .saturating_add(reservation.entity(id_bytes))
                    .saturating_add(metadata_bytes);
            }
        }
        if total > HA_MAX_OUTPUT_BYTES {
            return Err(EXCEEDED);
        }
    }
    admit_work_rows(config, rows, entity_bytes)
}

/// `measurement_kind` of a climate attribute point.
fn climate_measurement_kind(attribute: &str) -> String {
    if attribute == "temperature" {
        "setpoint".to_owned()
    } else {
        format!("climate_{attribute}")
    }
}

/// The state object of one numeric climate attribute, shaped like a sensor
/// state so [`normalize_state`] maps it: the attribute value as `state`,
/// the climate entity's `last_updated`, no unit or device class.
fn climate_attribute_state(
    item: &serde_json::Map<String, Value>,
    attribute: &str,
) -> Option<serde_json::Map<String, Value>> {
    let value = item
        .get("attributes")
        .and_then(Value::as_object)
        .and_then(|attributes| attributes.get(attribute))?;
    if !value.is_number() {
        return None;
    }
    let mut state = serde_json::Map::new();
    state.insert("entity_id".to_owned(), item.get("entity_id")?.clone());
    state.insert("state".to_owned(), value.clone());
    if let Some(updated) = item.get("last_updated") {
        state.insert("last_updated".to_owned(), updated.clone());
    }
    Some(state)
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
    let options = match ScanOptions::from_config(config)? {
        Ok(options) => options,
        Err(outcome) => return Ok(outcome),
    };

    if let Err(message) = admit_work(config, &[]) {
        return Ok(critical(message));
    }
    let entity_pattern = match run.mappings.compile(&entity_regex_value, true) {
        Ok(pattern) => pattern,
        Err(err) => return Ok(critical(format!("invalid entity_id_regex: {err}"))),
    };

    let client = ha_client(http, knobs, &access_token);
    let url = format!("{base_url}/api/states");
    let payload = match read_json_with_deadline(&client, &url, knobs).await {
        Ok(payload) => payload,
        Err(err) => return Ok(critical(err.safe_message())),
    };
    drop(client);
    let Value::Array(items) = &payload else {
        return Ok(critical("states response is not a list"));
    };

    if let Err(message) = admit_work(config, items) {
        return Ok(critical(message));
    }
    if let Err(message) = admit_output(config, name, items) {
        return Ok(critical(message));
    }
    prepare_mappings(config, &mut run.mappings);
    let metadata = if options.include_metadata {
        Some(
            entity_metadata(
                http,
                knobs,
                &base_url,
                &access_token,
                name,
                options.metadata_refresh,
            )
            .await,
        )
    } else {
        None
    };
    if let Err(message) = admit_extensions(config, name, items, &options, metadata.as_deref()) {
        return Ok(critical(message));
    }

    let ha_spec = HaSpec { name, config };
    let extra_tags = spec_name_tag(name);
    let mut points = Vec::new();
    let mut skipped: u64 = 0;
    for item in items {
        let Value::Object(item) = item else {
            skipped += 1;
            continue;
        };
        let Some(Value::String(entity_id)) = item.get("entity_id") else {
            skipped += 1;
            continue;
        };
        if entity_id.starts_with("climate.") && !options.climate_attributes.is_empty() {
            for attribute in &options.climate_attributes {
                let Some(state) = climate_attribute_state(item, attribute) else {
                    continue;
                };
                let mut tags = extra_tags.clone();
                tags.insert(
                    "measurement_kind".to_owned(),
                    climate_measurement_kind(attribute),
                );
                tags.insert("climate_attribute".to_owned(), attribute.clone());
                let point = normalize_state(
                    &state,
                    ha_spec,
                    &format!("{metric_prefix}{entity_id}.{attribute}"),
                    &tags,
                    sink.now(),
                    &mut run.mappings,
                );
                run.flush_logs();
                match point {
                    Ok(Some(mut point)) => {
                        if let Some(metadata) = &metadata {
                            add_metadata_tags(&mut point, entity_id, item, metadata);
                        }
                        points.push(point);
                    }
                    Ok(None) => skipped += 1,
                    Err(err) => return Ok(critical(collector_failure(err.python_class()))),
                }
            }
            continue;
        }
        if !(entity_id.starts_with("sensor.") || entity_id.starts_with("binary_sensor.")) {
            continue;
        }
        // Invalid upstream identities are row failures, independent of whether
        // a static subject mapping could otherwise hide the malformed source.
        if entity_name_slug(entity_id).is_err() {
            skipped += 1;
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
                Err(err) => return Ok(critical(collector_failure(err.python_class()))),
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
            Ok(Some(mut point)) => {
                if let Some(metadata) = &metadata {
                    add_metadata_tags(&mut point, entity_id, item, metadata);
                }
                points.push(point);
            }
            Ok(None) => skipped += 1,
            Err(err) => return Ok(critical(collector_failure(err.python_class()))),
        }
    }
    if points.is_empty() {
        return Ok(warning(skipped, "no matching numeric states"));
    }
    Ok(write_and_touch(sink, &points, skipped).await)
}
