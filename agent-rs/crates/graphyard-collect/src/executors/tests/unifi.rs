//! `unifi_device_traffic`: the `unifi.json` helper corpora and scenarios,
//! and the counterparts of the UniFi tests of
//! `test_metric_collection_specs.py`.

use std::sync::{Arc, OnceLock};

use serde_json::{Value, json};

use graphyard_core::model::StatusLevel;
use graphyard_core::pycompat::py_strip;

use super::support::{RecordingSink, Run, hex_bytes, replay, text};
use crate::executors::execute_unifi_device_traffic;
use crate::executors::unifi::{
    find_device, login_body, login_response_ok, resolve_interface_stats, response_error,
    response_summary,
};
use crate::http::tests::server::{Server, StubLookup, response};
use crate::http::tests::transport::tls_material;
use crate::http::{HttpContext, Lookup};

const FIXTURE: &str = include_str!("../../../../../fixtures/unifi.json");

fn fixture() -> &'static Value {
    static FIXTURE_VALUE: OnceLock<Value> = OnceLock::new();
    FIXTURE_VALUE.get_or_init(|| serde_json::from_str(FIXTURE).expect("unifi.json"))
}

fn section(name: &str) -> &'static [Value] {
    fixture()[name]
        .as_array()
        .unwrap_or_else(|| panic!("section {name}"))
}

fn tag_pairs(value: &Value) -> Vec<(String, String)> {
    value
        .as_array()
        .expect("tag pairs")
        .iter()
        .map(|pair| {
            (
                pair[0].as_str().expect("key").to_owned(),
                pair[1].as_str().expect("value").to_owned(),
            )
        })
        .collect()
}

const RUN: Run =
    |http, sink, name, config| Box::pin(execute_unifi_device_traffic(http, sink, name, config));

#[tokio::test]
async fn unifi_scenarios_match_python() {
    let fixture = fixture();
    let cases = section("executor");
    assert!(cases.len() >= 30);
    for case in cases {
        replay(case, text(fixture, "origin"), RUN).await;
    }
}

#[test]
fn find_device_matches_python() {
    let devices = fixture()["devices"].as_array().expect("devices");
    for case in section("find_device") {
        let found = find_device(devices, text(case, "device_name"), text(case, "device_mac"));
        let index = found.map(|device| {
            devices
                .iter()
                .position(|item| item.as_object() == Some(device))
                .expect("device from the list")
        });
        assert_eq!(
            index.map(|index| index as u64),
            case["index"].as_u64(),
            "{case}"
        );
    }
}

#[test]
fn interface_stats_match_python() {
    let devices = fixture()["interface_devices"]
        .as_object()
        .expect("interface devices");
    for case in section("interface_stats") {
        let device = devices[text(case, "device")].as_object().expect("device");
        let result = resolve_interface_stats(device, text(case, "selector"));
        match (result, case.get("error")) {
            (Ok((stats, tags)), None) => {
                assert_eq!(&Value::Object(stats.clone()), &case["stats"], "{case}");
                let tags: Vec<(String, String)> = tags.into_iter().collect();
                assert_eq!(tags, tag_pairs(&case["tags"]), "{case}");
            }
            (Err(err), Some(expected)) => {
                assert_eq!(err.kind.as_str(), text(expected, "class"), "{case}");
                assert_eq!(err.message, text(expected, "message"), "{case}");
            }
            (actual, _) => panic!("{case}: got {actual:?}"),
        }
    }
}

#[test]
fn response_summaries_match_python() {
    for case in section("response_summary") {
        let body = hex_bytes(text(case, "body_hex"));
        let redactions: Vec<&str> = case["redactions"]
            .as_array()
            .expect("redactions")
            .iter()
            .map(|value| value.as_str().expect("redaction"))
            .collect();
        assert_eq!(
            response_summary(&body, &redactions),
            text(case, "summary"),
            "{case}"
        );
        let status = u16::try_from(case["status"].as_u64().expect("status")).expect("u16");
        assert_eq!(
            response_error(status, &body, "/api/login", &redactions),
            text(case, "error"),
            "{case}"
        );
    }
}

#[test]
fn login_response_ok_matches_python() {
    for case in section("login_ok") {
        let status = u16::try_from(case["status"].as_u64().expect("status")).expect("u16");
        let body = hex_bytes(text(case, "body_hex"));
        assert_eq!(
            login_response_ok(status, &body),
            case["ok"].as_bool().expect("ok"),
            "{case}"
        );
    }
}

#[test]
fn login_body_is_httpx_json() {
    // The bodies the fixture's scenarios recorded, from httpx's `json=`.
    let mut checked = 0;
    for case in section("executor") {
        let Some(config) = case["config"].as_object() else {
            continue;
        };
        let (Some(username), Some(password)) = (
            config.get("username").and_then(Value::as_str),
            config.get("password").and_then(Value::as_str),
        ) else {
            continue;
        };
        for request in case["requests"].as_array().expect("requests") {
            if text(request, "method") == "POST" && text(request, "target") == "/api/login" {
                let body = login_body(py_strip(username), py_strip(password));
                assert_eq!(
                    String::from_utf8(body).expect("UTF-8"),
                    text(request, "body"),
                    "{}",
                    text(case, "name")
                );
                checked += 1;
            }
        }
    }
    assert!(checked > 10, "{checked}");
}

#[tokio::test]
async fn self_signed_controller_with_verify_tls_false() {
    // The production spec: `https://127.0.0.1:8443`, `verify_tls: false`.
    let (_root, acceptor) = tls_material();
    let server = Server::start_with(
        |request| match request.target.as_str() {
            "/api/login" => response("200 OK", &[], br#"{"meta":{"rc":"ok"}}"#),
            "/api/s/default/stat/device" => response(
                "200 OK",
                &[],
                br#"{"data":[{"name":"USW","uplink":{"rx_bytes-r":1.5,"tx_bytes-r":2}}]}"#,
            ),
            _ => response("404 Not Found", &[], b""),
        },
        Some(acceptor),
    )
    .await;
    let lookup: Arc<dyn Lookup> = Arc::new(StubLookup::loopback());
    let context = HttpContext::new().with_lookup(lookup);
    let config = |verify: bool| {
        json!({
            "base_url": format!("https://unifi.test:{}", server.port()),
            "username": "homeassistant",
            "password": "secret",
            "device_name": "USW",
            "subject_id": "usw",
            "verify_tls": verify,
        })
    };

    let sink = RecordingSink::default();
    let outcome = execute_unifi_device_traffic(&context, &sink, "unifi", &config(false))
        .await
        .expect("outcome");
    assert_eq!(outcome.status, StatusLevel::Ok, "{outcome:?}");
    assert_eq!(outcome.ingested, 2);

    let sink = RecordingSink::default();
    let outcome = execute_unifi_device_traffic(&context, &sink, "unifi", &config(true))
        .await
        .expect("outcome");
    assert_eq!(outcome.status, StatusLevel::Critical, "{outcome:?}");
    assert!(sink.calls.borrow().is_empty());
}

#[tokio::test]
async fn transport_failures_are_critical() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("address").port();
    drop(listener);
    let config = json!({
        "base_url": format!("http://127.0.0.1:{port}"),
        "username": "homeassistant",
        "password": "secret",
        "device_mac": "aa:bb:cc:dd:ee:ff",
        "subject_id": "usw",
    });
    let sink = RecordingSink::default();
    let outcome = execute_unifi_device_traffic(&HttpContext::new(), &sink, "unifi", &config)
        .await
        .expect("outcome");
    assert_eq!(outcome.status, StatusLevel::Critical);
    assert!(outcome.message.starts_with("[Errno "), "{outcome:?}");
    assert!(sink.calls.borrow().is_empty());
}
