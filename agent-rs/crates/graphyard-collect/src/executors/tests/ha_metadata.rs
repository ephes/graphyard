//! Rust-only env scan knobs with real sockets: `include_entity_metadata`
//! (friendly name, Home Assistant area and thermostat role as tags, the
//! area map cached per spec) and `climate_attributes` (setpoints and other
//! numeric `climate.*` attributes as points). The retired Python collector
//! has no counterpart, so there is no fixture.

use serde_json::{Value, json};

use graphyard_core::model::{MetricPoint, SpecOutcome, StatusLevel};

use super::ha_json::{ClockedSink, run_executor};
use crate::executors::{RunState, now_utc};
use crate::http::HttpContext;
use crate::http::tests::server::{Recorded, Reply, Server, response};

fn states() -> Value {
    json!([
        {
            "entity_id": "sensor.kitchen_temperature",
            "state": "21.5",
            "last_updated": "2026-11-24T12:00:00+00:00",
            "attributes": {
                "device_class": "temperature",
                "unit_of_measurement": "°C",
                "friendly_name": "Küche Sensor"
            }
        },
        {
            "entity_id": "sensor.eve_valve_position",
            "state": "12",
            "last_updated": "2026-11-24T12:00:00+00:00",
            "attributes": {"unit_of_measurement": "%", "friendly_name": "Eve Küche Ventil"}
        },
        {
            "entity_id": "sensor.attic_humidity",
            "state": "55",
            "last_updated": "2026-11-24T12:00:00+00:00",
            "attributes": {"device_class": "humidity", "friendly_name": "Dachboden\nFeuchte"}
        },
        {
            "entity_id": "climate.eve",
            "state": "heat",
            "last_updated": "2026-11-24T12:01:00+00:00",
            "attributes": {
                "temperature": 21.0,
                "current_temperature": 22.5,
                "hvac_action": "heating",
                "friendly_name": "Eve Küche"
            }
        },
        {
            "entity_id": "climate.no_setpoint",
            "state": "off",
            "attributes": {"temperature": null, "friendly_name": "Aus"}
        }
    ])
}

fn metadata() -> Value {
    json!([
        ["sensor.kitchen_temperature", "Küche", false],
        ["sensor.eve_valve_position", "Küche", true],
        ["sensor.attic_humidity", "", false],
        ["climate.eve", "Küche", true],
        ["malformed"]
    ])
}

fn json_reply(value: &Value) -> Reply {
    response(
        "200 OK",
        &[("Content-Type", "application/json")],
        value.to_string().as_bytes(),
    )
}

/// `/api/states` and, when `template` is given, `/api/template`.
async fn server(template: Option<Reply>) -> Server {
    Server::start(move |request: &Recorded| match request.target.as_str() {
        "/api/states" => json_reply(&states()),
        "/api/template" => template
            .clone()
            .unwrap_or_else(|| response("404 Not Found", &[], b"")),
        _ => response("404 Not Found", &[], b""),
    })
    .await
}

fn config(server: &Server, extra: Value) -> Value {
    let mut config = json!({
        "base_url": server.origin(),
        "access_token": "test-ha",
        "verify_tls": false,
        "entity_id_regex": "_valve_position$",
    });
    for (key, value) in extra.as_object().expect("extra knobs") {
        config[key] = value.clone();
    }
    config
}

async fn scan(name: &str, config: &Value) -> (SpecOutcome, Vec<MetricPoint>) {
    let sink = ClockedSink::new(now_utc());
    let outcome = run_executor(
        "home_assistant_env_scan",
        &HttpContext::new(),
        &sink,
        &mut RunState::new(),
        name,
        config,
    )
    .await
    .expect("no config error");
    let written = sink.written.borrow().clone().unwrap_or_default();
    (outcome, written)
}

fn point<'a>(points: &'a [MetricPoint], metric: &str) -> &'a MetricPoint {
    points
        .iter()
        .find(|point| point.metric == metric)
        .unwrap_or_else(|| panic!("no point {metric} in {points:?}"))
}

fn tag<'a>(point: &'a MetricPoint, key: &str) -> Option<&'a str> {
    point.tags.as_ref()?.get(key).map(String::as_str)
}

fn template_requests(server: &Server) -> Vec<Recorded> {
    server
        .requests()
        .into_iter()
        .filter(|request| request.target == "/api/template")
        .collect()
}

#[tokio::test]
async fn without_the_knobs_nothing_changes() {
    let server = server(Some(json_reply(&metadata()))).await;
    let (outcome, points) = scan("plain", &config(&server, json!({}))).await;
    assert_eq!(outcome.status, StatusLevel::Ok);
    assert_eq!(points.len(), 3, "{points:?}");
    for point in &points {
        assert!(!point.metric.starts_with("ha.climate."));
        for key in ["friendly_name", "area", "device_role", "measurement_kind"] {
            assert_eq!(tag(point, key), None, "{key} on {point:?}");
        }
    }
    assert!(template_requests(&server).is_empty());
}

#[tokio::test]
async fn metadata_tags_and_climate_attributes() {
    let server = server(Some(json_reply(&metadata()))).await;
    let config = config(
        &server,
        json!({
            "include_entity_metadata": true,
            "climate_attributes": ["temperature", "current_temperature", "hvac_action"],
        }),
    );
    let (outcome, points) = scan("metadata", &config).await;
    assert_eq!(outcome.status, StatusLevel::Ok, "{outcome:?}");

    let kitchen = point(&points, "ha.sensor.kitchen_temperature");
    assert_eq!(tag(kitchen, "friendly_name"), Some("Küche Sensor"));
    assert_eq!(tag(kitchen, "area"), Some("Küche"));
    assert_eq!(tag(kitchen, "device_role"), None);

    let valve = point(&points, "ha.sensor.eve_valve_position");
    assert_eq!(tag(valve, "area"), Some("Küche"));
    assert_eq!(tag(valve, "device_role"), Some("thermostat"));

    // No area, and a friendly name with a control character is left out.
    let attic = point(&points, "ha.sensor.attic_humidity");
    assert_eq!(tag(attic, "area"), None);
    assert_eq!(tag(attic, "friendly_name"), None);

    let setpoint = point(&points, "ha.climate.eve.temperature");
    assert!((setpoint.value - 21.0).abs() < f64::EPSILON);
    assert_eq!(tag(setpoint, "measurement_kind"), Some("setpoint"));
    assert_eq!(tag(setpoint, "climate_attribute"), Some("temperature"));
    assert_eq!(tag(setpoint, "entity_id"), Some("climate.eve"));
    assert_eq!(tag(setpoint, "friendly_name"), Some("Eve Küche"));
    assert_eq!(tag(setpoint, "area"), Some("Küche"));
    assert_eq!(tag(setpoint, "device_role"), Some("thermostat"));
    assert_eq!(setpoint.subject_type, "environment_sensor");
    assert_eq!(
        setpoint.ts.to_rfc3339(),
        "2026-11-24T12:01:00+00:00",
        "the climate entity's last_updated"
    );

    let current = point(&points, "ha.climate.eve.current_temperature");
    assert!((current.value - 22.5).abs() < f64::EPSILON);
    assert_eq!(
        tag(current, "measurement_kind"),
        Some("climate_current_temperature")
    );

    // A text attribute (hvac_action) and a null setpoint write nothing.
    assert!(
        points
            .iter()
            .all(|point| !point.metric.ends_with(".hvac_action")
                && !point.metric.starts_with("ha.climate.no_setpoint"))
    );
    assert_eq!(points.len(), 5, "{points:?}");

    let requests = template_requests(&server);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "POST");
    let body: Value = serde_json::from_slice(&requests[0].body).expect("JSON body");
    assert!(
        body["template"]
            .as_str()
            .expect("template")
            .contains("area_name(s.entity_id)")
    );
}

#[tokio::test]
async fn the_area_map_is_cached_per_spec() {
    let server = server(Some(json_reply(&metadata()))).await;
    let config = config(&server, json!({"include_entity_metadata": true}));
    for _ in 0..3 {
        let (outcome, points) = scan("cached", &config).await;
        assert_eq!(outcome.status, StatusLevel::Ok);
        assert_eq!(
            tag(point(&points, "ha.sensor.kitchen_temperature"), "area"),
            Some("Küche")
        );
    }
    assert_eq!(template_requests(&server).len(), 1);

    // A refresh interval of one second fetches again once it passed.
    let short = config_with(&config, "entity_metadata_refresh_seconds", json!(1));
    scan("short", &short).await;
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    scan("short", &short).await;
    assert_eq!(template_requests(&server).len(), 3);
}

fn config_with(config: &Value, key: &str, value: Value) -> Value {
    let mut config = config.clone();
    config[key] = value;
    config
}

#[tokio::test]
async fn a_failed_template_keeps_the_points() {
    let server = server(Some(response("500 Internal Server Error", &[], b"boom"))).await;
    let config = config(&server, json!({"include_entity_metadata": true}));
    let (outcome, points) = scan("failing", &config).await;
    assert_eq!(outcome.status, StatusLevel::Ok);
    let kitchen = point(&points, "ha.sensor.kitchen_temperature");
    assert_eq!(tag(kitchen, "area"), None);
    assert_eq!(tag(kitchen, "friendly_name"), Some("Küche Sensor"));

    // Not a list: the same.
    let server = server_with_template(json_reply(&json!({"not": "a list"}))).await;
    let config = config_with(&config, "base_url", json!(server.origin()));
    let (outcome, points) = scan("not-a-list", &config).await;
    assert_eq!(outcome.status, StatusLevel::Ok);
    assert_eq!(
        tag(point(&points, "ha.sensor.kitchen_temperature"), "area"),
        None
    );
}

async fn server_with_template(reply: Reply) -> Server {
    server(Some(reply)).await
}

#[tokio::test]
async fn malformed_climate_attributes_are_critical() {
    let server = server(None).await;
    for bad in [
        json!("temperature"),
        json!([1]),
        json!(["Temperature"]),
        json!(["a", "b", "c", "d", "e"]),
        json!([""]),
    ] {
        let config = config(&server, json!({"climate_attributes": bad}));
        let (outcome, points) = scan("bad", &config).await;
        assert_eq!(outcome.status, StatusLevel::Critical, "{bad}");
        assert_eq!(outcome.message, "invalid climate_attributes");
        assert!(points.is_empty());
    }
}

#[tokio::test]
async fn climate_points_stay_inside_the_envelope() {
    let rows: Vec<Value> = (0..1100)
        .map(|index| {
            json!({
                "entity_id": format!("climate.room_{index}"),
                "state": "heat",
                "attributes": {"temperature": 20.0, "current_temperature": 21.0}
            })
        })
        .collect();
    let body = Value::Array(rows).to_string();
    let server = Server::start(move |_| {
        response(
            "200 OK",
            &[("Content-Type", "application/json")],
            body.as_bytes(),
        )
    })
    .await;
    let config = config(
        &server,
        json!({"climate_attributes": ["temperature", "current_temperature", "target_temp_low", "target_temp_high"]}),
    );
    let (outcome, points) = scan("envelope", &config).await;
    assert_eq!(outcome.status, StatusLevel::Critical);
    assert_eq!(
        outcome.message,
        "collector failure: _HomeAssistantLimitExceeded"
    );
    assert!(points.is_empty());
}

#[tokio::test]
async fn metadata_never_pushes_a_point_past_the_tag_limit() {
    let server = server(Some(json_reply(&metadata()))).await;
    let mut extra = serde_json::Map::new();
    for index in 0..27 {
        extra.insert(format!("mapped_{index}"), json!("x"));
    }
    // entity_id, spec_name, 27 mapped, unit, device_class: 31 tags.
    let config = config(
        &server,
        json!({
            "include_entity_metadata": true,
            "metric_mapping": {"rules": [{
                "match_entity_id_regex": "^sensor\\.kitchen_temperature$",
                "extra_tags": Value::Object(extra),
            }]},
        }),
    );
    let (outcome, points) = scan("tag-limit", &config).await;
    assert_eq!(outcome.status, StatusLevel::Ok);
    let kitchen = point(&points, "ha.sensor.kitchen_temperature");
    let tags = kitchen.tags.as_ref().expect("tags");
    assert_eq!(tags.len(), 32);
    assert_eq!(tag(kitchen, "friendly_name"), Some("Küche Sensor"));
    assert_eq!(tag(kitchen, "area"), None, "no room left for the area");
    // Other points keep all metadata.
    assert_eq!(
        tag(point(&points, "ha.sensor.eve_valve_position"), "area"),
        Some("Küche")
    );
}

#[tokio::test]
async fn rows_and_metadata_share_one_output_budget() {
    // About 6.9 MB of row reservation and 3.2 MB of friendly names: each fits
    // the 8 MiB envelope alone, together they do not.
    let name = "a".repeat(1000);
    let rows: Vec<Value> = (0..3000)
        .map(|index| {
            json!({
                "entity_id": format!("sensor.room_{index}_temperature"),
                "state": "20",
                "attributes": {"friendly_name": name, "device_class": "temperature"},
            })
        })
        .collect();
    let body = Value::Array(rows).to_string();
    let server = Server::start(move |request: &Recorded| {
        if request.target == "/api/template" {
            response("200 OK", &[], b"[]")
        } else {
            response(
                "200 OK",
                &[("Content-Type", "application/json")],
                body.as_bytes(),
            )
        }
    })
    .await;
    let plain = config(&server, json!({"max_body_bytes": 20_000_000}));
    let (outcome, points) = scan("budget-plain", &plain).await;
    assert_eq!(outcome.status, StatusLevel::Ok, "{outcome:?}");
    assert_eq!(points.len(), 3000);

    let with_metadata = config_with(&plain, "include_entity_metadata", json!(true));
    let (outcome, points) = scan("budget-metadata", &with_metadata).await;
    assert_eq!(outcome.status, StatusLevel::Critical);
    assert_eq!(
        outcome.message,
        "collector failure: _HomeAssistantLimitExceeded"
    );
    assert!(points.is_empty());
}
