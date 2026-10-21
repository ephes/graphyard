//! The `page_probe` and `read_json` request sequences of the fixture,
//! replayed against a fake server: same requests, same `HTTP Request:` log
//! lines, same result or error.

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::Value;

use graphyard_core::pycompat::json_loads;

use super::fixture::{cases, hex_bytes, number, text};
use super::logs;
use super::server::{Reply, Server};
use crate::http::client::{ClientOptions, HttpClient, HttpContext};
use crate::http::deadline::Deadline;
use crate::http::error::HttpError;
use crate::http::probe::{read_http_json, run_page_probe};
use crate::http::status::standard_reason_phrase;

/// The origin the Python scenarios ran against.
const ORIGIN: &str = "http://probe.test";

fn encode_reply(reply: &Value) -> Reply {
    let status = number(reply, "status");
    let code = u16::try_from(status).expect("status");
    let body = hex_bytes(text(reply, "body_hex"));
    let mut head = format!("HTTP/1.1 {status} {}\r\n", standard_reason_phrase(code));
    for pair in reply["headers"].as_array().expect("headers") {
        let name = pair[0].as_str().expect("header name");
        let value = pair[1].as_str().expect("header value");
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str(&format!(
        "Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    ));
    let mut bytes = head.into_bytes();
    bytes.extend_from_slice(&body);
    Reply::Raw(bytes)
}

/// A server answering each route's replies in order (the last repeats),
/// 404 for anything else, like the Python `MockTransport` handler.
async fn scripted_server(case: &Value) -> Server {
    let mut routes: HashMap<String, Vec<Reply>> = HashMap::new();
    for (target, replies) in case["routes"].as_object().expect("routes") {
        let replies = replies
            .as_array()
            .expect("reply list")
            .iter()
            .map(encode_reply)
            .collect();
        routes.insert(target.clone(), replies);
    }
    let counters: Mutex<HashMap<String, usize>> = Mutex::new(HashMap::new());
    Server::start(move |request| {
        let Some(replies) = routes.get(&request.target) else {
            return Reply::Raw(
                b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    .to_vec(),
            );
        };
        let mut counters = counters.lock().expect("counters");
        let index = counters.entry(request.target.clone()).or_insert(0);
        let reply = replies[(*index).min(replies.len() - 1)].clone();
        *index += 1;
        reply
    })
    .await
}

fn localise(text: &str, origin: &str) -> String {
    text.replace(ORIGIN, origin)
}

/// Compare requests, log lines and the error (if any) with the case.
fn check_common(case: &Value, server: &Server, error: Option<&HttpError>) {
    let name = text(case, "name");
    let origin = server.origin();
    let requests: Vec<String> = server.requests().iter().map(|r| r.line()).collect();
    let expected_requests: Vec<String> = case["requests"]
        .as_array()
        .expect("requests")
        .iter()
        .map(|line| line.as_str().expect("request line").to_owned())
        .collect();
    assert_eq!(requests, expected_requests, "requests of {name}");
    let expected_log: Vec<String> = case["log"]
        .as_array()
        .expect("log")
        .iter()
        .map(|line| localise(line.as_str().expect("log line"), &origin))
        .collect();
    assert_eq!(logs::lines(), expected_log, "log of {name}");
    match (case.get("error"), error) {
        (Some(expected), Some(err)) => {
            assert_eq!(err.kind.python_class(), text(expected, "class"), "{name}");
            if expected.get("p2").is_none() {
                assert_eq!(
                    err.message,
                    localise(text(expected, "message"), &origin),
                    "{name}"
                );
            }
        }
        (None, None) => {}
        (expected, actual) => panic!("{name}: expected error {expected:?}, got {actual:?}"),
    }
}

fn client() -> HttpClient {
    HttpClient::new(&HttpContext::new(), &ClientOptions::new(10.0, true))
}

#[tokio::test]
async fn page_probe_scenarios_match_python() {
    for case in cases("page_probe") {
        let server = scripted_server(case).await;
        let url = format!("{}{}", server.origin(), text(case, "path"));
        logs::capture();
        let result = run_page_probe(
            &client(),
            &url,
            number(case, "max_body_bytes"),
            case["follow_redirects"].as_bool().expect("flag"),
            &Deadline::none(),
        )
        .await;
        check_common(case, &server, result.as_ref().err());
        if let Ok(probe) = result {
            let expected = &case["result"];
            let name = text(case, "name");
            assert_eq!(
                u64::from(probe.status_code),
                number(expected, "status_code"),
                "{name}"
            );
            assert_eq!(
                u64::from(probe.redirect_count),
                number(expected, "redirect_count"),
                "{name}"
            );
            assert!(probe.ttfb_seconds <= probe.total_seconds, "{name}");
        }
    }
}

#[tokio::test]
async fn read_json_scenarios_match_python() {
    for case in cases("read_json") {
        let server = scripted_server(case).await;
        let url = format!("{}{}", server.origin(), text(case, "path"));
        logs::capture();
        let result = read_http_json(
            &client(),
            &url,
            number(case, "max_body_bytes"),
            &Deadline::none(),
        )
        .await;
        check_common(case, &server, result.as_ref().err());
        if let Ok(payload) = result {
            let expected = json_loads(text(case, "result")).expect("json result");
            assert_eq!(payload, expected, "{}", text(case, "name"));
        }
    }
}
