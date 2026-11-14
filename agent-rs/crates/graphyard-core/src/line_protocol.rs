//! InfluxDB line protocol as `influxdb-client` 1.50.0 writes it, and the
//! pure part of `influx.py:write_points` (PLAN.md section 6.4).
//!
//! Python sources: `influxdb_client/client/write/point.py`
//! (`Point.to_line_protocol`, `_append_tags`, `_append_fields`,
//! `_escape_key`, `_escape_tag_value`, `_convert_timestamp`) and the point
//! building loop of `write_points`. Pinned by `fixtures/line_protocol.json`
//! (`fixtures/gen_line_protocol.py`): the series identity (measurement,
//! tag set, field, timestamp) must not drift (R4).
//!
//! The rules:
//!
//! - the measurement escapes `,`, space, `\n`, `\t`, `\r`; tag keys, tag
//!   values and field keys also escape `=`; backslashes are not escaped;
//! - tags are sorted by key; a tag whose escaped key or value is empty is
//!   dropped; an escaped tag value ending in `\` gets a trailing space;
//! - the field is `value=<str(float)>` without a trailing `.0`; a non-finite
//!   value leaves the point without fields, which renders as an empty line;
//! - the timestamp is the UTC time in nanoseconds (microseconds × 1000,
//!   like Python's `datetime`).

use chrono::{DateTime, Utc};

use crate::logline::{LogEvent, escape_log_value};
use crate::model::{MetricPoint, Tags};
use crate::normalize::{NormalizedPoint, is_reserved_tag_key, normalize_metric_point};
use crate::pycompat::py_float_repr;

/// `str.translate(table)` with the escapes of `point.py`.
fn escape(text: &str, equals: bool) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            ',' => out.push_str("\\,"),
            ' ' => out.push_str("\\ "),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '=' if equals => out.push_str("\\="),
            c => out.push(c),
        }
    }
    out
}

/// `_escape_key(name, _ESCAPE_MEASUREMENT)`.
pub fn escape_measurement(measurement: &str) -> String {
    escape(measurement, false)
}

/// `_escape_key(key)` for tag and field keys.
pub fn escape_key(key: &str) -> String {
    escape(key, true)
}

/// `_escape_tag_value(value)`: the key escapes plus a space after a
/// trailing backslash.
pub fn escape_tag_value(value: &str) -> String {
    let mut escaped = escape(value, true);
    if escaped.ends_with('\\') {
        escaped.push(' ');
    }
    escaped
}

/// A float field value as `_append_fields` renders it (`str(value)` without
/// a trailing `.0`); `None` for a non-finite value, which the client skips.
pub fn format_float_field(value: f64) -> Option<String> {
    if !value.is_finite() {
        return None;
    }
    let text = py_float_repr(value);
    Some(match text.strip_suffix(".0") {
        Some(trimmed) => trimmed.to_owned(),
        None => text,
    })
}

/// `_convert_timestamp(ts, WritePrecision.NS)`: nanoseconds since the epoch
/// from the microseconds of a Python `datetime` (sub-microsecond parts of a
/// Rust timestamp are dropped). `i128`, because Python's `datetime` range
/// (years 1 to 9999) exceeds `i64` nanoseconds.
pub fn timestamp_ns(ts: &DateTime<Utc>) -> i128 {
    i128::from(ts.timestamp_micros()) * 1000
}

/// A `Point(measurement)` with tags, the field `value` and a time, as
/// `write_points` builds it.
#[derive(Clone, Debug, PartialEq)]
pub struct LinePoint {
    pub measurement: String,
    /// `Point._tags`: setting a key again replaces its value.
    pub tags: Tags,
    pub value: f64,
    pub ts: DateTime<Utc>,
}

impl LinePoint {
    pub fn new(measurement: impl Into<String>, value: f64, ts: DateTime<Utc>) -> Self {
        Self {
            measurement: measurement.into(),
            tags: Tags::new(),
            value,
            ts,
        }
    }

    /// `Point.tag(key, value)`.
    pub fn tag(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// `Point.to_line_protocol()`.
    pub fn to_line_protocol(&self) -> String {
        let Some(value) = format_float_field(self.value) else {
            return String::new();
        };
        let mut tags: Vec<(&String, &String)> = self.tags.iter().collect();
        tags.sort();
        let mut line = escape_measurement(&self.measurement);
        for (key, tag_value) in tags {
            let key = escape_key(key);
            let tag_value = escape_tag_value(tag_value);
            if !key.is_empty() && !tag_value.is_empty() {
                line.push(',');
                line.push_str(&key);
                line.push('=');
                line.push_str(&tag_value);
            }
        }
        line.push(' ');
        line.push_str(&escape_key("value"));
        line.push('=');
        line.push_str(&value);
        line.push(' ');
        line.push_str(&timestamp_ns(&self.ts).to_string());
        line
    }
}

/// The point `write_points` builds for a normalised point: the seven fixed
/// dimensions, `source_entity_id`, `host` and `service` when set, then the
/// custom tags except reserved keys (one warning per reserved key, appended
/// to `events`).
pub fn line_point(
    measurement: &str,
    point: &NormalizedPoint,
    events: &mut Vec<LogEvent>,
) -> LinePoint {
    let mut line = LinePoint::new(measurement, point.value, point.ts);
    line.tag("metric", point.metric.as_str())
        .tag("subject_type", point.subject_type.as_str())
        .tag("subject_id", point.subject_id.as_str())
        .tag("source_system", point.source_system.as_str())
        .tag("source_instance", point.source_instance.as_str())
        .tag("collector_service", point.collector_service.as_str())
        .tag("collector_host", point.collector_host.as_str());
    for (key, value) in [
        ("source_entity_id", &point.source_entity_id),
        ("host", &point.host),
        ("service", &point.service),
    ] {
        if let Some(value) = value.as_deref().filter(|value| !value.is_empty()) {
            line.tag(key, value);
        }
    }
    for (key, value) in &point.tags {
        if is_reserved_tag_key(key) {
            events.push(LogEvent::warning(format!(
                "Ignoring custom tag override for reserved key {}",
                escape_log_value(key)
            )));
            continue;
        }
        line.tag(key.as_str(), value.as_str());
    }
    line
}

/// The batch `write_points` would send, with the log records of the
/// normalisation in their order.
#[derive(Clone, Debug, PartialEq)]
pub struct PreparedWrite {
    /// Whole batch refused before network or shadow-file effects.
    pub request_too_large: bool,
    /// One line per valid point, in point order.
    pub lines: Vec<String>,
    /// `len(points)`.
    pub total: usize,
    /// Points that failed `normalize_metric_point` (not counted as written).
    pub rejected: usize,
    /// Validation errors and reserved-key warnings, in point order.
    pub events: Vec<LogEvent>,
}

impl PreparedWrite {
    /// The number of points to write (`len(influx_points)`).
    pub fn written(&self) -> usize {
        self.lines.len()
    }

    /// Whether there is anything to send (`influx_points` not empty).
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// The request body: the lines joined with `\n`.
    pub fn body(&self) -> String {
        self.lines.join("\n")
    }

    /// The `metrics_write_rejected` warning `write_points` logs when points
    /// were rejected: after a successful write, or instead of the write when
    /// every point was rejected (`written_points=0`).
    pub fn rejected_event(&self) -> Option<LogEvent> {
        (self.rejected > 0).then(|| {
            LogEvent::warning(format!(
                "metrics_write_rejected category=normalization rejected_points={} written_points={} total_points={}",
                self.rejected,
                self.written(),
                self.total
            ))
        })
    }
}

/// Maximum UTF-8 request body, including newline separators (8 MiB).
pub const WRITE_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
/// Bounded diagnostics even for many invalid points or reserved tag overrides.
const MAX_WRITE_EVENTS: usize = 32;

/// The pure part of `write_points`: admit dimensions, skip invalid points and
/// build the valid lines in order, refusing a request above the total byte cap.
pub fn prepare_write(measurement: &str, points: &[MetricPoint]) -> PreparedWrite {
    prepare_write_with_limit(measurement, points, WRITE_MAX_REQUEST_BYTES)
}

/// Prepare one atomic request; overflow discards the complete batch.
pub fn prepare_write_with_limit(
    measurement: &str,
    points: &[MetricPoint],
    max_bytes: usize,
) -> PreparedWrite {
    let mut prepared = PreparedWrite {
        request_too_large: false,
        lines: Vec::new(),
        total: points.len(),
        rejected: 0,
        events: Vec::new(),
    };
    let mut bytes = 0usize;
    // Count measurement escaping before allocating a possibly oversized line.
    let measurement_bytes = measurement.len().saturating_add(
        measurement
            .chars()
            .filter(|c| matches!(c, ',' | ' ' | '\n' | '\r' | '\t'))
            .count(),
    );
    if measurement_bytes > max_bytes {
        prepared.request_too_large = true;
    }
    for item in points {
        if prepared.request_too_large {
            break;
        }
        match normalize_metric_point(item) {
            Ok(normalized) => {
                let line = line_point(measurement, &normalized, &mut prepared.events);
                let encoded = line.to_line_protocol();
                let size = encoded.len() + usize::from(!prepared.lines.is_empty());
                if size > max_bytes.saturating_sub(bytes) {
                    prepared.request_too_large = true;
                    break;
                }
                bytes += size;
                prepared.lines.push(encoded);
            }
            Err(err) => {
                if prepared.events.len() < MAX_WRITE_EVENTS {
                    prepared.events.push(LogEvent::error(format!(
                    "Metric point validation failed metric={} subject_type={} subject_id={}: {}",
                    escape_log_value(&item.metric.chars().take(256).collect::<String>()),
                    escape_log_value(&item.subject_type.chars().take(256).collect::<String>()),
                    escape_log_value(&item.subject_id.chars().take(128).collect::<String>()),
                    escape_log_value(&err.message.chars().take(512).collect::<String>())
                )));
                }
                prepared.rejected += 1;
            }
        }
        prepared.events.truncate(MAX_WRITE_EVENTS);
    }
    if prepared.request_too_large {
        prepared.lines.clear();
        prepared.rejected = prepared.total;
        prepared.events.clear();
        prepared.events.push(LogEvent::error(
            "metrics_write_rejected category=request_bytes",
        ));
    }
    prepared
}

#[cfg(test)]
mod tests;
