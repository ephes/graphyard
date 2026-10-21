//! Home Assistant state normalisation and the subject and metric mappings
//! of the HA executors (PLAN.md 6.3).
//!
//! Python sources: `services.py:_normalize_home_assistant_sensor_state`,
//! `_resolve_subject_mapping`, `_resolve_home_assistant_metric_mapping`,
//! `_entity_name_slug`, `_warn_subject_mapping_once`. Pinned by
//! `fixtures/ha_mapping.json` (`fixtures/gen_ha_mapping.py`), including the
//! production env scan config of macmini.
//!
//! The functions are pure: the warning records go into [`MappingState`]
//! (with Python's warn-once keys) and the caller hands them to the logger;
//! the clock value for a state without a usable `last_updated` is passed in.
//! User regexes run on `fancy-regex` (D-5).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use fancy_regex::{Regex, RegexBuilder};
use serde_json::{Map, Value};

use crate::logline::{LogEvent, escape_log_value};
use crate::model::{MetricPoint, SubjectType, Tags};
use crate::normalize::{normalize_subject_id, parse_finite_float};
use crate::pycompat::{
    PyError, PyErrorKind, py_float_of, py_fromisoformat, py_repr, py_str, py_strip, py_type_name,
};

/// The spec as the mapping functions see it: its name and config object.
#[derive(Clone, Copy, Debug)]
pub struct HaSpec<'a> {
    pub name: &'a str,
    pub config: &'a Map<String, Value>,
}

type RegexKey = (String, bool);

/// Per-run state of the mappings: Python's module-level
/// `_subject_mapping_warning_keys` (cleared at the start of every metrics
/// run, so S8 creates one state per run), the warning records not yet
/// logged and the compiled regexes (Python's `re` module cache).
#[derive(Debug, Default)]
pub struct MappingState {
    warned: HashSet<String>,
    events: Vec<LogEvent>,
    regexes: HashMap<RegexKey, Result<Arc<Regex>, String>>,
}

impl MappingState {
    pub fn new() -> Self {
        Self::default()
    }

    /// The warning records since the last call, in order.
    pub fn take_events(&mut self) -> Vec<LogEvent> {
        std::mem::take(&mut self.events)
    }

    /// The warn-once keys recorded so far (tests).
    pub fn warned_keys(&self) -> impl Iterator<Item = &str> {
        self.warned.iter().map(String::as_str)
    }

    fn warn(&mut self, message: String) {
        self.events.push(LogEvent::warning(message));
    }

    /// `_warn_subject_mapping_once(key, message, ...)`.
    fn warn_once(&mut self, key: String, message: impl FnOnce() -> String) {
        if self.warned.insert(key) {
            self.warn(message());
        }
    }

    /// `re.compile(pattern, re.IGNORECASE if case_insensitive else 0)`;
    /// the error is the compile error text (P2).
    ///
    /// # Errors
    /// The compile error text.
    pub fn compile(&mut self, pattern: &str, case_insensitive: bool) -> Result<Arc<Regex>, String> {
        self.regexes
            .entry((pattern.to_owned(), case_insensitive))
            .or_insert_with(|| {
                RegexBuilder::new(pattern)
                    .case_insensitive(case_insensitive)
                    .build()
                    .map(Arc::new)
                    .map_err(|err| err.to_string())
            })
            .clone()
    }

    /// `re.compile(pattern).fullmatch(text) is not None`: the pattern
    /// anchored at both ends, compiled after the pattern itself compiled.
    fn fullmatch(&mut self, pattern: &str, text: &str) -> Result<bool, PyError> {
        // A pattern with a leading verbose flag (`(?x)`) may end in a
        // comment; the line break ends it before the anchor.
        let line_break = if verbose(pattern) { "\n" } else { "" };
        let anchored = format!("\\A(?:{pattern}{line_break})\\z");
        let regex = self
            .compile(&anchored, false)
            .map_err(|err| regex_runtime_error(&err))?;
        regex
            .is_match(text)
            .map_err(|err| regex_runtime_error(&err.to_string()))
    }
}

/// Whether the global inline flag groups at the start of the pattern (Python
/// accepts global flags only there, one or more consecutive groups such as
/// `(?i)(?x)`) set `x`.
fn verbose(pattern: &str) -> bool {
    let mut rest = pattern;
    while let Some((flags, tail)) = rest
        .strip_prefix("(?")
        .and_then(|inner| inner.split_once(')'))
    {
        if flags.is_empty() || !flags.chars().all(|c| c.is_ascii_alphabetic()) {
            return false;
        }
        if flags.contains('x') {
            return true;
        }
        rest = tail;
    }
    false
}

/// A regex that fails while matching (`fancy-regex`'s backtrack limit;
/// Python's `re` keeps backtracking instead, D-5).
fn regex_runtime_error(err: &str) -> PyError {
    PyError::value_error(format!("regex match failed: {err}"))
}

/// `re.search(...) is not None` with a compiled regex.
///
/// # Errors
/// A regex runtime failure (D-5).
pub fn regex_search(regex: &Regex, text: &str) -> Result<bool, PyError> {
    regex
        .is_match(text)
        .map_err(|err| regex_runtime_error(&err.to_string()))
}

/// `str(mapping.get(key, default))`.
fn get_str(map: &Map<String, Value>, key: &str, default: &str) -> String {
    map.get(key).map_or_else(|| default.to_owned(), py_str)
}

/// `str(...).strip().lower()`.
fn stripped_lower(text: &str) -> String {
    py_strip(text).to_lowercase()
}

/// `_entity_name_slug(entity_id)`: lower-cased, a `sensor.` or
/// `binary_sensor.` prefix removed, then `normalize_subject_id`.
///
/// # Errors
/// The `ValueError` of `normalize_subject_id` (nothing left).
pub fn entity_name_slug(entity_id: &str) -> Result<String, PyError> {
    let normalized = stripped_lower(entity_id);
    let slug = ["sensor.", "binary_sensor."]
        .iter()
        .find_map(|prefix| normalized.strip_prefix(prefix))
        .unwrap_or(&normalized);
    normalize_subject_id(slug)
}

/// `_resolve_subject_mapping(spec=..., entity_id=...)`: the first rule
/// whose `match_entity_id_regex` matches the whole entity id, else the
/// default mapping; `None` when a matching rule cannot produce a subject id.
///
/// # Errors
/// The `ValueError` of [`entity_name_slug`] for the default mapping, a
/// regex runtime failure.
pub fn resolve_subject_mapping(
    spec: HaSpec<'_>,
    entity_id: &str,
    state: &mut MappingState,
) -> Result<Option<(String, String)>, PyError> {
    let name = escape_log_value(spec.name);
    let environment_sensor = SubjectType::EnvironmentSensor.as_str();
    let subject_mapping = match spec.config.get("subject_mapping") {
        None | Some(Value::Null) => {
            state.warn_once(format!("{}:missing", spec.name), || {
                format!(
                    "Spec {name} missing config.subject_mapping; using default entity_name_slug mapping"
                )
            });
            return Ok(Some((
                environment_sensor.to_owned(),
                entity_name_slug(entity_id)?,
            )));
        }
        Some(Value::Object(mapping)) => mapping,
        Some(other) => {
            state.warn_once(format!("{}:invalid_type", spec.name), || {
                format!(
                    "Spec {name} has invalid config.subject_mapping type={}; using default mapping",
                    py_type_name(other)
                )
            });
            return Ok(Some((
                environment_sensor.to_owned(),
                entity_name_slug(entity_id)?,
            )));
        }
    };

    if let Some(Value::Array(rules)) = subject_mapping.get("rules") {
        for (index, rule) in rules.iter().enumerate() {
            let Value::Object(rule) = rule else {
                state.warn(format!(
                    "Spec {name} subject_mapping.rules[{index}] is not an object; skipping"
                ));
                continue;
            };
            let pattern_raw = py_strip(&get_str(rule, "match_entity_id_regex", "")).to_owned();
            if pattern_raw.is_empty() {
                continue;
            }
            if let Err(err) = state.compile(&pattern_raw, false) {
                state.warn(format!(
                    "Spec {name} has invalid subject mapping regex at rules[{index}]: {}",
                    escape_log_value(&err)
                ));
                continue;
            }
            if !state.fullmatch(&pattern_raw, entity_id)? {
                continue;
            }
            let subject_type = stripped_lower(&get_str(rule, "subject_type", ""));
            if SubjectType::parse(&subject_type).is_none() {
                let key = format!(
                    "{}:rule:{index}:unknown_subject_type:{subject_type}",
                    spec.name
                );
                state.warn_once(key, || {
                    format!(
                        "Spec {name} subject mapping rules[{index}] has unknown subject_type={}",
                        escape_log_value(&subject_type)
                    )
                });
                continue;
            }
            let template = match rule.get("subject_id_template") {
                Some(Value::String(template)) if !py_strip(template).is_empty() => template,
                _ => {
                    state.warn(format!(
                        "Spec {name} subject mapping rules[{index}] matched {} but has no static subject_id_template",
                        escape_log_value(entity_id)
                    ));
                    return Ok(None);
                }
            };
            return match normalize_subject_id(template) {
                Ok(subject_id) => Ok(Some((subject_type, subject_id))),
                Err(err) => {
                    state.warn(format!(
                        "Spec {name} subject mapping rules[{index}] produced invalid subject_id for {}: {}",
                        escape_log_value(entity_id),
                        escape_log_value(&err.message)
                    ));
                    Ok(None)
                }
            };
        }
    }

    let empty = Map::new();
    let default_mapping = match subject_mapping.get("default") {
        None => &empty,
        Some(Value::Object(mapping)) => mapping,
        Some(_) => {
            state.warn_once(format!("{}:invalid_default", spec.name), || {
                format!("Spec {name} has invalid subject_mapping.default; using fallback")
            });
            &empty
        }
    };
    let mut subject_type = stripped_lower(&get_str(
        default_mapping,
        "subject_type",
        environment_sensor,
    ));
    if SubjectType::parse(&subject_type).is_none() {
        let key = format!("{}:unknown_default_subject_type:{subject_type}", spec.name);
        state.warn_once(key, || {
            format!(
                "Spec {name} has unknown default subject_type={}; using {environment_sensor}",
                escape_log_value(&subject_type)
            )
        });
        subject_type = environment_sensor.to_owned();
    }
    let subject_id_from = get_str(default_mapping, "subject_id_from", "entity_name_slug");
    if subject_id_from != "entity_name_slug" {
        let key = format!(
            "{}:unsupported_subject_id_from:{subject_id_from}",
            spec.name
        );
        state.warn_once(key, || {
            format!(
                "Spec {name} has unsupported subject_id_from={}; using entity_name_slug",
                escape_log_value(&subject_id_from)
            )
        });
    }
    Ok(Some((subject_type, entity_name_slug(entity_id)?)))
}

/// `float(multiplier_raw)` for `value_multiplier`, booleans rejected:
/// `Ok(None)` for Python's `TypeError`/`ValueError` (caller warns and uses
/// 1).
///
/// # Errors
/// The `OverflowError` of `float()` for a huge integer, which Python does
/// not catch.
fn multiplier_of(raw: &Value) -> Result<Option<f64>, PyError> {
    if raw.is_boolean() {
        return Ok(None);
    }
    match py_float_of(raw) {
        Ok(value) => Ok(Some(value)),
        Err(err) if err.kind == PyErrorKind::OverflowError => Err(err),
        Err(_) => Ok(None),
    }
}

/// `_resolve_home_assistant_metric_mapping(spec=..., entity_id=...,
/// metric_name=..., value=...)`: the first rule whose regex is found in the
/// entity id rewrites the metric name, multiplies the value and adds its
/// `extra_tags`; without a match everything stays.
///
/// # Errors
/// The `OverflowError` of a huge integer `value_multiplier`, a regex
/// runtime failure.
pub fn resolve_metric_mapping(
    spec: HaSpec<'_>,
    entity_id: &str,
    metric_name: &str,
    value: f64,
    state: &mut MappingState,
) -> Result<(String, f64, Tags), PyError> {
    let unchanged = || (metric_name.to_owned(), value, Tags::new());
    let Some(Value::Object(metric_mapping)) = spec.config.get("metric_mapping") else {
        return Ok(unchanged());
    };
    let name = escape_log_value(spec.name);
    let rules = match metric_mapping.get("rules") {
        None => return Ok(unchanged()),
        Some(Value::Array(rules)) => rules,
        Some(other) => {
            state.warn_once(format!("{}:invalid_metric_mapping_rules", spec.name), || {
                format!(
                    "Spec {name} has invalid config.metric_mapping.rules type={}; ignoring metric mapping",
                    py_type_name(other)
                )
            });
            return Ok(unchanged());
        }
    };

    for (index, rule) in rules.iter().enumerate() {
        let Value::Object(rule) = rule else {
            state.warn_once(
                format!("{}:invalid_metric_mapping_rule:{index}", spec.name),
                || format!("Spec {name} metric_mapping.rules[{index}] is not an object; skipping"),
            );
            continue;
        };
        let pattern_raw = py_strip(&get_str(rule, "match_entity_id_regex", "")).to_owned();
        if pattern_raw.is_empty() {
            state.warn_once(
                format!("{}:missing_metric_mapping_regex:{index}", spec.name),
                || {
                    format!(
                        "Spec {name} metric_mapping.rules[{index}] missing match_entity_id_regex; skipping"
                    )
                },
            );
            continue;
        }
        let regex = match state.compile(&pattern_raw, false) {
            Ok(regex) => regex,
            Err(err) => {
                state.warn_once(
                    format!("{}:invalid_metric_mapping_regex:{index}", spec.name),
                    || {
                        format!(
                            "Spec {name} metric_mapping.rules[{index}] invalid regex {}: {}",
                            escape_log_value(&pattern_raw),
                            escape_log_value(&err)
                        )
                    },
                );
                continue;
            }
        };
        if !regex_search(&regex, entity_id)? {
            continue;
        }

        // `str(rule.get("metric_name", metric_name)).strip()`: the default
        // is stripped too.
        let mut mapped_metric_name =
            py_strip(&get_str(rule, "metric_name", metric_name)).to_owned();
        if mapped_metric_name.is_empty() {
            mapped_metric_name = metric_name.to_owned();
        }

        let one = Value::from(1);
        let multiplier_raw = rule.get("value_multiplier").unwrap_or(&one);
        let multiplier = match multiplier_of(multiplier_raw)? {
            Some(multiplier) => multiplier,
            None => {
                state.warn_once(
                    format!("{}:invalid_metric_mapping_multiplier:{index}", spec.name),
                    || {
                        format!(
                            "Spec {name} metric_mapping.rules[{index}] has invalid value_multiplier={}; using 1",
                            escape_log_value(&py_repr(multiplier_raw))
                        )
                    },
                );
                1.0
            }
        };

        let extra_tags = match rule.get("extra_tags") {
            None => Tags::new(),
            Some(Value::Object(tags)) => tags
                .iter()
                .map(|(key, item)| (key.clone(), py_str(item)))
                .collect(),
            Some(other) => {
                state.warn_once(
                    format!("{}:invalid_metric_mapping_tags:{index}", spec.name),
                    || {
                        format!(
                            "Spec {name} metric_mapping.rules[{index}] has invalid extra_tags type={}; ignoring",
                            py_type_name(other)
                        )
                    },
                );
                Tags::new()
            }
        };
        return Ok((mapped_metric_name, value * multiplier, extra_tags));
    }
    Ok(unchanged())
}

/// Python truthiness of a decoded JSON value (`x or default`).
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        // A huge integer (`OverflowError` as a float) is not zero.
        Value::Number(_) => !matches!(py_float_of(value), Ok(number) if number == 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

/// The point timestamp from `last_updated`: `Z` replaced by `+00:00`,
/// `datetime.fromisoformat`, naive taken as UTC, converted to UTC; `now`
/// when it is missing, falsy or not ISO text.
///
/// # Errors
/// The `OverflowError` of the UTC conversion (an aware time whose UTC value
/// leaves years 1..9999), which Python does not catch.
fn state_timestamp(
    payload: &Map<String, Value>,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, PyError> {
    let updated_raw = match payload.get("last_updated") {
        Some(value) if truthy(value) => py_str(value),
        _ => return Ok(now),
    };
    match py_fromisoformat(&updated_raw.replace('Z', "+00:00")) {
        Ok(parsed) => parsed.to_utc(),
        Err(_) => Ok(now),
    }
}

/// `_normalize_home_assistant_sensor_state(payload, spec=...,
/// metric_name_override=..., extra_tags=...)`: one metric point from a HA
/// state object, `None` when the state is not numeric, the subject mapping
/// fails or the mapped value is not finite.
///
/// # Errors
/// What Python lets escape to the executor's `except`: the `ValueError` of
/// the default subject slug, the `OverflowError` of a huge multiplier or of
/// the timestamp conversion, a regex runtime failure.
pub fn normalize_state(
    payload: &Map<String, Value>,
    spec: HaSpec<'_>,
    metric_name_override: &str,
    extra_tags: &Tags,
    now: DateTime<Utc>,
    state: &mut MappingState,
) -> Result<Option<MetricPoint>, PyError> {
    let entity_id = py_strip(&get_str(payload, "entity_id", "")).to_owned();
    if entity_id.is_empty() {
        return Ok(None);
    }
    let value = match payload.get("state") {
        None | Some(Value::Null) => return Ok(None),
        Some(raw) => match parse_finite_float(raw) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        },
    };
    let ts = state_timestamp(payload, now)?;
    let empty = Map::new();
    let attributes = match payload.get("attributes") {
        Some(Value::Object(attributes)) => attributes,
        _ => &empty,
    };

    let Some((subject_type, subject_id)) = resolve_subject_mapping(spec, &entity_id, state)? else {
        state.warn(format!(
            "Spec {} failed subject resolution for entity_id={}",
            escape_log_value(spec.name),
            escape_log_value(&entity_id)
        ));
        return Ok(None);
    };

    let config = spec.config;
    let source_system = stripped_lower(&get_str(config, "source_system", "homeassistant"));
    let source_instance = non_empty(
        py_strip(&get_str(config, "source_instance", "default")),
        "default",
    );
    // Legacy defaults, overridable by the canonical collector fields.
    let legacy_host_id = non_empty(
        py_strip(&get_str(config, "host_id", "homeassistant")),
        "homeassistant",
    );
    let legacy_service_id = non_empty(
        py_strip(&get_str(config, "service_id", "homeassistant")),
        "homeassistant",
    );
    let collector_service = non_empty(
        py_strip(&get_str(config, "collector_service", "graphyard-agent")),
        "graphyard-agent",
    );
    let collector_host = non_empty(
        py_strip(&get_str(config, "collector_host", &legacy_host_id)),
        &legacy_host_id,
    );

    let metric_name = if metric_name_override.is_empty() {
        format!("ha.{}", entity_id.replace(' ', "_"))
    } else {
        metric_name_override.to_owned()
    };
    let (metric_name, value, mapped_tags) =
        resolve_metric_mapping(spec, &entity_id, &metric_name, value, state)?;
    if !value.is_finite() {
        // A finite reading times value_multiplier can overflow to inf/NaN.
        return Ok(None);
    }

    let mut tags = Tags::new();
    tags.insert("entity_id".to_owned(), entity_id.clone());
    for (key, item) in extra_tags.iter().chain(mapped_tags.iter()) {
        tags.insert(key.clone(), item.clone());
    }
    for (attribute, tag) in [
        ("unit_of_measurement", "unit"),
        ("device_class", "device_class"),
    ] {
        match attributes.get(attribute) {
            None | Some(Value::Null) => {}
            Some(raw) => {
                tags.insert(tag.to_owned(), py_str(raw));
            }
        }
    }

    let normalized_legacy_host = normalize_subject_id(&legacy_host_id).unwrap_or_default();
    let host = (subject_type == SubjectType::Host.as_str() && subject_id == normalized_legacy_host)
        .then_some(normalized_legacy_host);

    let mut point = MetricPoint::new(
        ts,
        metric_name,
        value,
        subject_type,
        subject_id,
        source_system,
        collector_service,
        collector_host,
    );
    point.source_instance = source_instance;
    point.source_entity_id = Some(entity_id);
    point.host = host;
    point.service = Some(legacy_service_id);
    point.tags = Some(tags);
    Ok(Some(point))
}

/// `text or fallback` for a stripped text.
fn non_empty(text: &str, fallback: &str) -> String {
    if text.is_empty() { fallback } else { text }.to_owned()
}

#[cfg(test)]
mod tests;
