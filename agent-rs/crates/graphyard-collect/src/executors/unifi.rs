//! `unifi_device_traffic` (`services._execute_unifi_device_traffic_spec`,
//! PLAN.md 6.3): log in to the UniFi controller (legacy `/api/login`, then
//! UniFi OS `/api/auth/login`), fetch the site's device list and write the
//! receive and transmit rates of one interface of one device. Redirects are
//! followed automatically (`follow_redirects=True`); there is no total
//! deadline.
//!
//! Python sources: `_execute_unifi_device_traffic_spec`, `_login_to_unifi`,
//! `_unifi_login_response_ok`, `_unifi_response_error`,
//! `_unifi_response_summary`, `_find_unifi_device`,
//! `_resolve_unifi_interface_stats`. Pinned by `fixtures/unifi.json`
//! (`gen_page_probe_unifi.py`).

use std::sync::OnceLock;

use num_traits::Zero as _;
use regex::Regex;
use serde_json::{Map, Value};

use graphyard_core::model::{MetricPoint, SpecOutcome, StatusLevel, SubjectType, Tags};
use graphyard_core::normalize::parse_finite_float;
use graphyard_core::pycompat::{
    PyError, PyNumber, int_digit_limit_error, json_loads, py_eq, py_int, py_number,
    py_split_whitespace, py_str, py_str_repr, py_strip,
};
use graphyard_core::spec_config::{Config, bool_config, request_timeout_config};

use super::{ExecutorResult, MetricSink, SinkError, config_object, config_text, now_utc};
use crate::http::{
    ClientOptions, HttpClient, HttpContext, HttpError, Method, decode_json,
    send_following_redirects,
};

/// The default metric names.
pub const RECEIVE_METRIC: &str = "network_device.network_receive_bytes_per_second";
pub const TRANSMIT_METRIC: &str = "network_device.network_transmit_bytes_per_second";

/// `_unifi_response_summary` keeps this many characters.
const SUMMARY_CHARS: usize = 240;

/// A failure inside the executor's `try` block: `str(err)` becomes the
/// `critical` message.
struct Failure(String);

impl From<HttpError> for Failure {
    fn from(err: HttpError) -> Self {
        Self(err.message)
    }
}

impl From<PyError> for Failure {
    fn from(err: PyError) -> Self {
        Self(err.message)
    }
}

impl From<SinkError> for Failure {
    fn from(err: SinkError) -> Self {
        Self(err.message)
    }
}

/// The spec config after `_execute_unifi_device_traffic_spec`'s parsing.
struct UnifiSpec<'a> {
    name: &'a str,
    base_url: String,
    username: String,
    password: String,
    site_id: String,
    device_name: String,
    device_mac: String,
    interface_selector: String,
    subject_type: String,
    subject_id: String,
    source_system: String,
    source_instance: String,
    service_id: String,
    collector_service: String,
    collector_host: String,
    timeout_seconds: f64,
    verify_tls: bool,
    auth_mode: String,
    receive_metric_name: String,
    transmit_metric_name: String,
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

/// `_execute_unifi_device_traffic_spec(spec)`.
///
/// # Errors
/// [`super::ExecutorError::Config`] for an invalid `request_timeout_seconds`
/// or `verify_tls` (checked before the required fields, like Python). Every
/// failure after the required fields is a `critical` outcome.
pub async fn execute_unifi_device_traffic<S: MetricSink>(
    http: &HttpContext,
    sink: &S,
    name: &str,
    config: &Value,
) -> ExecutorResult {
    let Some(config) = config_object(config) else {
        return Ok(critical("config must be an object"));
    };
    let site_id = or_default(stripped(config, "site_id", "default"), "default");
    let spec = UnifiSpec {
        name,
        base_url: config_text(config, "base_url", "")
            .trim_end_matches('/')
            .to_owned(),
        username: stripped(config, "username", ""),
        password: stripped(config, "password", ""),
        device_name: stripped(config, "device_name", ""),
        device_mac: stripped(config, "device_mac", ""),
        interface_selector: stripped(config, "interface_selector", "uplink"),
        subject_type: or_default(
            stripped(config, "subject_type", SubjectType::NetworkDevice.as_str()),
            SubjectType::NetworkDevice.as_str(),
        ),
        subject_id: stripped(config, "subject_id", ""),
        source_system: or_default(stripped(config, "source_system", "unifi"), "unifi"),
        source_instance: or_default(stripped(config, "source_instance", &site_id), &site_id),
        service_id: or_default(stripped(config, "service_id", "unifi"), "unifi"),
        collector_service: or_default(
            stripped(config, "collector_service", "graphyard-agent"),
            "graphyard-agent",
        ),
        collector_host: stripped(config, "collector_host", "macmini"),
        timeout_seconds: request_timeout_config(config)?,
        verify_tls: bool_config(config, "verify_tls", true)?,
        auth_mode: or_default(stripped(config, "auth_mode", "auto").to_lowercase(), "auto"),
        receive_metric_name: stripped(config, "receive_metric_name", RECEIVE_METRIC),
        transmit_metric_name: stripped(config, "transmit_metric_name", TRANSMIT_METRIC),
        site_id,
    };

    let required = [
        (spec.base_url.is_empty(), "config.base_url is required"),
        (spec.username.is_empty(), "config.username is required"),
        (spec.password.is_empty(), "config.password is required"),
        (
            spec.device_name.is_empty() && spec.device_mac.is_empty(),
            "config.device_name or config.device_mac is required",
        ),
        (spec.subject_id.is_empty(), "config.subject_id is required"),
    ];
    if let Some((_, message)) = required.iter().find(|(missing, _)| *missing) {
        return Ok(critical(*message));
    }

    // The `try` block: any exception is `critical, 0, 0, str(err)`.
    Ok(collect(http, sink, &spec)
        .await
        .unwrap_or_else(|Failure(message)| critical(message)))
}

async fn collect<S: MetricSink>(
    http: &HttpContext,
    sink: &S,
    spec: &UnifiSpec<'_>,
) -> Result<SpecOutcome, Failure> {
    let redactions = [spec.username.as_str(), spec.password.as_str()];
    let payload = {
        // `with httpx.Client(..., follow_redirects=True) as client:`
        let client = HttpClient::new(
            http,
            &ClientOptions::new(spec.timeout_seconds, spec.verify_tls),
        );
        let (api_prefix, headers) = login(&client, spec).await?;
        let endpoint = format!("{api_prefix}/s/{}/stat/device", spec.site_id);
        let headers: Vec<(&str, &str)> = headers
            .iter()
            .map(|(name, value)| (*name, value.as_str()))
            .collect();
        let request = client.build_request(
            Method::Get,
            &format!("{}{endpoint}", spec.base_url),
            &headers,
            None,
        )?;
        let mut response = send_following_redirects(&client, request).await?;
        let body = response.read_to_end().await?;
        if !(200..300).contains(&response.status) {
            return Err(Failure(response_error(
                response.status,
                &body,
                &endpoint,
                &redactions,
            )));
        }
        response_json(&body)?
    };

    let Some(payload) = payload.as_object() else {
        return Ok(critical("device response is not an object"));
    };
    let Some(Value::Array(devices)) = payload.get("data") else {
        return Ok(critical("device response missing data list"));
    };
    let Some(device) = find_device(devices, &spec.device_name, &spec.device_mac) else {
        return Ok(SpecOutcome::new(
            StatusLevel::Warning,
            0,
            1,
            "configured UniFi device not found",
        ));
    };
    let (stats, interface_tags) = resolve_interface_stats(device, &spec.interface_selector)?;

    let device_mac = py_strip(&config_text(device, "mac", "")).to_lowercase();
    let mut base_tags = Tags::new();
    base_tags.insert("device_class".to_owned(), "data_rate".to_owned());
    base_tags.insert("spec_name".to_owned(), spec.name.to_owned());
    base_tags.extend(interface_tags);
    if !device_mac.is_empty() {
        base_tags.insert("device_mac".to_owned(), device_mac.clone());
    }

    let now = now_utc();
    let mut points = Vec::new();
    let mut skipped: u64 = 0;
    let metrics = [
        (&spec.receive_metric_name, "rx_bytes-r", "receive"),
        (&spec.transmit_metric_name, "tx_bytes-r", "transmit"),
    ];
    for (metric, field, direction) in metrics {
        let Ok(value) = parse_finite_float(stats.get(field).unwrap_or(&Value::Null)) else {
            skipped += 1;
            continue;
        };
        let mut tags = base_tags.clone();
        tags.insert("traffic_direction".to_owned(), direction.to_owned());
        let mut point = MetricPoint::new(
            now,
            metric.clone(),
            value,
            spec.subject_type.clone(),
            spec.subject_id.clone(),
            spec.source_system.clone(),
            spec.collector_service.clone(),
            or_default(spec.collector_host.clone(), "macmini"),
        );
        point.source_instance.clone_from(&spec.source_instance);
        point.service = Some(spec.service_id.clone());
        point.source_entity_id = (!device_mac.is_empty()).then(|| device_mac.clone());
        point.tags = Some(tags);
        points.push(point);
    }

    if points.is_empty() {
        return Ok(SpecOutcome::new(
            StatusLevel::Warning,
            0,
            skipped,
            "no numeric traffic values found",
        ));
    }
    let written = sink.write_points(&points).await?;
    sink.touch_registry(&points).await?;
    Ok(SpecOutcome::new(StatusLevel::Ok, written, skipped, ""))
}

/// `_login_to_unifi(client, ...)`: the API prefix and the headers of the
/// device request.
async fn login(
    client: &HttpClient,
    spec: &UnifiSpec<'_>,
) -> Result<(&'static str, Vec<(&'static str, String)>), Failure> {
    let mut attempts: Vec<(&str, &str, &'static str)> = Vec::new();
    if matches!(spec.auth_mode.as_str(), "auto" | "legacy") {
        attempts.push(("legacy", "/api/login", "/api"));
    }
    if matches!(spec.auth_mode.as_str(), "auto" | "unifi_os") {
        attempts.push(("unifi_os", "/api/auth/login", "/proxy/network/api"));
    }
    if attempts.is_empty() {
        return Err(Failure(format!(
            "unsupported UniFi auth_mode: {}",
            spec.auth_mode
        )));
    }

    let body = login_body(&spec.username, &spec.password);
    let content_length = body.len().to_string();
    let redactions = [spec.username.as_str(), spec.password.as_str()];
    let mut errors = Vec::new();
    for (mode, login_path, api_prefix) in attempts {
        // `client.post(url, json=payload, headers=headers)`: the explicit
        // headers, then httpx's `Content-Length` for the JSON body.
        let request = client.build_request(
            Method::Post,
            &format!("{}{login_path}", spec.base_url),
            &[
                ("Accept", "application/json"),
                ("Content-Type", "application/json"),
                ("Content-Length", &content_length),
            ],
            Some(body.clone()),
        )?;
        let mut response = send_following_redirects(client, request).await?;
        let response_body = response.read_to_end().await?;
        if login_response_ok(response.status, &response_body) {
            let csrf = response.header("x-csrf-token").unwrap_or_default();
            let csrf = py_strip(&csrf);
            let mut headers = vec![("Accept", "application/json".to_owned())];
            if !csrf.is_empty() {
                headers.push(("x-csrf-token", csrf.to_owned()));
            }
            return Ok((api_prefix, headers));
        }
        errors.push(format!(
            "{mode} {}",
            response_error(response.status, &response_body, login_path, &redactions)
        ));
    }
    Err(Failure(format!(
        "UniFi login failed; attempts: {}",
        errors.join("; ")
    )))
}

/// The JSON body of `client.post(..., json={"username": …, "password": …,
/// "remember": True})`: httpx's compact `json.dumps(ensure_ascii=False)`.
pub fn login_body(username: &str, password: &str) -> Vec<u8> {
    let mut payload = Map::new();
    payload.insert("username".to_owned(), Value::String(username.to_owned()));
    payload.insert("password".to_owned(), Value::String(password.to_owned()));
    payload.insert("remember".to_owned(), Value::Bool(true));
    serde_json::to_vec(&Value::Object(payload)).unwrap_or_default()
}

/// `resp.json()`: [`decode_json`] plus the integer digit limit of
/// `json.loads` (D-11). Error texts are P2 except the digit limit.
fn response_json(body: &[u8]) -> Result<Value, Failure> {
    let value = decode_json(body)?;
    let text = std::str::from_utf8(body.strip_prefix(b"\xef\xbb\xbf").unwrap_or(body))
        .map_err(|err| Failure(err.to_string()))?;
    match int_digit_limit_error(text) {
        Some(err) => Err(err.into()),
        None => Ok(value),
    }
}

/// `_unifi_login_response_ok(resp)`: a 2xx status whose body, if it is a
/// JSON object with a `meta` object, has `meta.rc` (default `"ok"`) equal to
/// `ok` in any case. Unparseable and non-object bodies count as success.
pub fn login_response_ok(status: u16, body: &[u8]) -> bool {
    if !(200..300).contains(&status) {
        return false;
    }
    let Ok(payload) = response_json(body) else {
        return true;
    };
    let Some(Value::Object(meta)) = payload.as_object().and_then(|map| map.get("meta")) else {
        return true;
    };
    meta.get("rc")
        .map_or_else(|| "ok".to_owned(), py_str)
        .to_lowercase()
        == "ok"
}

/// `resp.text` of a body: UTF-8 with replacement characters (httpx's
/// default encoding). A body in another charset declared by the server is
/// P2 (PLAN.md 6.3).
fn response_text(body: &[u8]) -> String {
    String::from_utf8_lossy(body).into_owned()
}

fn password_field() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        // Python's `\s` is `str.isspace()`, which adds U+001C..U+001F to
        // Unicode `White_Space`.
        Regex::new(r#"(?i)("password"[\s\x1c-\x1f]*:[\s\x1c-\x1f]*")[^"]*(")"#)
            .expect("valid password pattern")
    })
}

/// `_unifi_response_summary(resp, redactions=…)`: the body text with every
/// redaction value of at least 4 characters and every JSON `"password"`
/// value replaced by `<redacted>`, whitespace collapsed, cut to 240
/// characters.
pub fn response_summary(body: &[u8], redactions: &[&str]) -> String {
    let mut text = response_text(body);
    if text.is_empty() {
        return text;
    }
    for value in redactions {
        if value.chars().count() >= 4 {
            text = text.replace(value, "<redacted>");
        }
    }
    let text = password_field().replace_all(&text, "${1}<redacted>${2}");
    py_split_whitespace(&text)
        .join(" ")
        .chars()
        .take(SUMMARY_CHARS)
        .collect()
}

/// `_unifi_response_error(resp, endpoint=…, redactions=…)`.
pub fn response_error(status: u16, body: &[u8], endpoint: &str, redactions: &[&str]) -> String {
    let summary = response_summary(body, redactions);
    if summary.is_empty() {
        format!("{endpoint} returned HTTP {status}")
    } else {
        format!("{endpoint} returned HTTP {status}: {summary}")
    }
}

/// `_find_unifi_device(devices, device_name=…, device_mac=…)`: the first
/// object whose stripped `name` equals the name or whose stripped,
/// lower-cased `mac` equals the MAC (name checked first per item).
pub fn find_device<'a>(
    devices: &'a [Value],
    device_name: &str,
    device_mac: &str,
) -> Option<&'a Map<String, Value>> {
    let mac = py_strip(device_mac).to_lowercase();
    let name = py_strip(device_name);
    devices.iter().filter_map(Value::as_object).find(|item| {
        (!name.is_empty() && py_strip(&config_text(item, "name", "")) == name)
            || (!mac.is_empty() && py_strip(&config_text(item, "mac", "")).to_lowercase() == mac)
    })
}

/// Python truthiness of a decoded JSON value.
fn py_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => match py_number(number) {
            PyNumber::Int(int) => !int.is_zero(),
            PyNumber::Float(float) => float != 0.0,
        },
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

/// `dict.get(key)` with JSON `null` as `None`.
fn get_some<'a>(map: &'a Map<String, Value>, key: &str) -> Option<&'a Value> {
    map.get(key).filter(|value| !value.is_null())
}

fn insert_speed(tags: &mut Tags, stats: &Map<String, Value>) {
    if let Some(speed) = get_some(stats, "speed") {
        tags.insert("speed_mbps".to_owned(), py_str(speed));
    }
}

fn insert_port_name(tags: &mut Tags, port: &Map<String, Value>) {
    let name = py_strip(&config_text(port, "name", "")).to_owned();
    if !name.is_empty() {
        tags.insert("port_name".to_owned(), name);
    }
}

/// The text after the first `:` of a selector, stripped.
fn selector_argument(selector: &str) -> &str {
    py_strip(selector.split_once(':').map_or("", |(_, rest)| rest))
}

/// `_resolve_unifi_interface_stats(device, interface_selector=…)`: the
/// stats object of the selected interface and its tags. Selectors: `uplink`
/// (default), `port_idx:<n>`, `port_name:<name>` (prefixes in any case).
///
/// # Errors
/// The `ValueError` messages of the Python function, and `int()`'s digit
/// limit for a `port_idx` beyond 4300 digits. Non-ASCII digits in a
/// `port_idx` selector are invalid (D-6).
pub fn resolve_interface_stats<'a>(
    device: &'a Map<String, Value>,
    interface_selector: &str,
) -> Result<(&'a Map<String, Value>, Tags), PyError> {
    let selector = match py_strip(interface_selector) {
        "" => "uplink",
        stripped => stripped,
    };
    let selector_lower = selector.to_lowercase();
    let port_table: &[Value] = match device.get("port_table") {
        Some(Value::Array(items)) => items,
        _ => &[],
    };
    let ports = || port_table.iter().filter_map(Value::as_object);
    let mut tags = Tags::new();

    if selector_lower == "uplink" {
        let Some(Value::Object(uplink)) = device.get("uplink") else {
            return Err(PyError::value_error("device uplink stats missing"));
        };
        tags.insert("traffic_scope".to_owned(), "uplink".to_owned());
        let interface = py_strip(&config_text(uplink, "name", "")).to_owned();
        if !interface.is_empty() {
            tags.insert("interface".to_owned(), interface);
        }
        // `uplink.get("port_idx") or uplink.get("num_port")`
        let port_idx = match uplink.get("port_idx") {
            Some(value) if py_truthy(value) => Some(value),
            _ => get_some(uplink, "num_port"),
        };
        if let Some(port_idx) = port_idx {
            tags.insert("port_idx".to_owned(), py_str(port_idx));
            if let Some(port) = ports().find(|port| {
                port.get("port_idx")
                    .is_some_and(|value| py_eq(value, port_idx))
            }) {
                insert_port_name(&mut tags, port);
            }
        }
        insert_speed(&mut tags, uplink);
        return Ok((uplink, tags));
    }

    if selector_lower.starts_with("port_idx:") {
        let raw = selector_argument(selector);
        if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(PyError::value_error(format!(
                "invalid port_idx selector: {selector}"
            )));
        }
        let target = py_int(raw)?;
        let target_value =
            json_loads(&target.to_string()).map_err(|err| PyError::value_error(err.to_string()))?;
        let Some(port) = ports().find(|port| {
            port.get("port_idx")
                .is_some_and(|value| py_eq(value, &target_value))
        }) else {
            return Err(PyError::value_error(format!(
                "port_idx {target} not found on device"
            )));
        };
        tags.insert("traffic_scope".to_owned(), "port".to_owned());
        tags.insert("port_idx".to_owned(), target.to_string());
        insert_port_name(&mut tags, port);
        insert_speed(&mut tags, port);
        return Ok((port, tags));
    }

    if selector_lower.starts_with("port_name:") {
        let target = selector_argument(selector);
        if target.is_empty() {
            return Err(PyError::value_error(format!(
                "invalid port_name selector: {selector}"
            )));
        }
        let Some(port) = ports().find(|port| py_strip(&config_text(port, "name", "")) == target)
        else {
            return Err(PyError::value_error(format!(
                "port_name {} not found on device",
                py_str_repr(target)
            )));
        };
        tags.insert("traffic_scope".to_owned(), "port".to_owned());
        tags.insert("port_name".to_owned(), target.to_owned());
        if let Some(port_idx) = get_some(port, "port_idx") {
            tags.insert("port_idx".to_owned(), py_str(port_idx));
        }
        insert_speed(&mut tags, port);
        return Ok((port, tags));
    }

    Err(PyError::value_error(format!(
        "unsupported interface_selector: {selector}"
    )))
}

fn critical(message: impl Into<String>) -> SpecOutcome {
    SpecOutcome::new(StatusLevel::Critical, 0, 0, message)
}
