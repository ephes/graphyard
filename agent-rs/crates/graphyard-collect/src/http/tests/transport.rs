//! Client rules of PLAN.md 6.1 against fake servers: request headers,
//! authentication, decoding, cookies, redirects, TLS, timeouts and the
//! classes of transport failures.

use std::io::Write as _;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rustls::pki_types::CertificateDer;

use super::logs;
use super::server::{Reply, Server, StubLookup, response};
use crate::http::client::{ClientOptions, HttpClient, HttpContext, Method};
use crate::http::deadline::Deadline;
use crate::http::error::ErrorKind;
use crate::http::probe::{read_http_json, run_page_probe, send_following_redirects};
use crate::http::resolve::{Lookup, SystemLookup};

fn client(context: &HttpContext, options: &ClientOptions) -> HttpClient {
    HttpClient::new(context, options)
}

fn default_client() -> HttpClient {
    client(&HttpContext::new(), &ClientOptions::new(10.0, true))
}

fn gzip(data: &[u8]) -> Vec<u8> {
    // A stored (uncompressed) deflate block in a gzip member is valid gzip
    // and needs no compressor in the tests.
    let mut out = vec![0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff];
    let length = u16::try_from(data.len()).expect("small test body");
    out.push(1);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&(!length).to_le_bytes());
    out.extend_from_slice(data);
    out.extend_from_slice(&crc32(data).to_le_bytes());
    out.extend_from_slice(&u32::try_from(data.len()).expect("size").to_le_bytes());
    out
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xffff_ffff_u32;
    for byte in data {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[tokio::test]
async fn default_request_headers_are_httpx_ones() {
    let server = Server::start(|_| response("200 OK", &[], b"{}")).await;
    read_http_json(
        &default_client(),
        &format!("{}/", server.origin()),
        100,
        &Deadline::none(),
    )
    .await
    .expect("json");
    let request = &server.requests()[0];
    assert_eq!(request.header_values("accept"), ["*/*"]);
    assert_eq!(request.header_values("accept-encoding"), ["gzip, deflate"]);
    assert_eq!(request.header_values("user-agent"), ["python-httpx/0.28.1"]);
    assert!(request.header_values("authorization").is_empty());
    assert_eq!(
        request.header_values("host"),
        [format!("127.0.0.1:{}", server.port())]
    );
}

#[tokio::test]
async fn client_headers_and_authentication_are_sent() {
    // test_json_stream_preserves_authentication_and_tls (headers part).
    let server = Server::start(|_| response("200 OK", &[], br#"{"value": 7}"#)).await;
    let url = format!("{}/", server.origin());
    let bearer = ClientOptions::new(10.0, false)
        .header("Accept", "application/json")
        .header("Authorization", "Bearer test-bearer");
    let basic = ClientOptions::new(10.0, false)
        .header("Accept", "application/json")
        .basic_auth("user", "pass");
    for options in [bearer, basic] {
        read_http_json(
            &client(&HttpContext::new(), &options),
            &url,
            100,
            &Deadline::none(),
        )
        .await
        .expect("json");
    }
    let requests = server.requests();
    let authorizations: Vec<&str> = requests
        .iter()
        .flat_map(|request| request.header_values("authorization"))
        .collect();
    assert_eq!(authorizations, ["Bearer test-bearer", "Basic dXNlcjpwYXNz"]);
    for request in &requests {
        assert_eq!(request.header_values("accept"), ["application/json"]);
    }
}

#[tokio::test]
async fn gzip_bodies_are_decoded_and_counted_decoded() {
    // test_streaming_json_byte_budget with compression.
    let small = gzip(br#"{"value": 7}"#);
    let mut padded = br#"{"value": 7}"#.to_vec();
    padded.extend(std::iter::repeat_n(b' ', 4096));
    let large = gzip(&padded);
    let server = Server::start(move |request| {
        let body = if request.target == "/small" {
            small.clone()
        } else {
            large.clone()
        };
        response("200 OK", &[("Content-Encoding", "gzip")], &body)
    })
    .await;
    let client = default_client();
    let value = read_http_json(
        &client,
        &format!("{}/small", server.origin()),
        64,
        &Deadline::none(),
    )
    .await
    .expect("decoded JSON");
    assert_eq!(value["value"], 7);
    let err = read_http_json(
        &client,
        &format!("{}/large", server.origin()),
        64,
        &Deadline::none(),
    )
    .await
    .expect_err("decoded size beyond the cap");
    assert_eq!(err.kind, ErrorKind::LimitExceeded);
    assert_eq!(err.message, "response body exceeded max_body_bytes=64");
}

#[tokio::test]
async fn corrupt_gzip_is_a_decoding_error() {
    let server = Server::start(|_| {
        response(
            "200 OK",
            &[("Content-Encoding", "gzip")],
            b"not gzip at all",
        )
    })
    .await;
    let err = run_page_probe(
        &default_client(),
        &format!("{}/", server.origin()),
        1000,
        true,
        &Deadline::none(),
    )
    .await
    .expect_err("decoding error");
    assert_eq!(err.kind, ErrorKind::DecodingError);
    assert!(err.kind.is_page_probe_warning());
}

#[tokio::test]
async fn cookies_follow_manual_redirects() {
    let server = Server::start(|request| match request.target.as_str() {
        "/" => response(
            "302 Found",
            &[
                ("Location", "/final"),
                ("Set-Cookie", "session=abc; Path=/"),
            ],
            b"",
        ),
        _ => response("200 OK", &[], b"ok"),
    })
    .await;
    let result = run_page_probe(
        &default_client(),
        &format!("{}/", server.origin()),
        1000,
        true,
        &Deadline::none(),
    )
    .await
    .expect("probe");
    assert_eq!(result.redirect_count, 1);
    let requests = server.requests();
    assert!(requests[0].header_values("cookie").is_empty());
    assert_eq!(requests[1].header_values("cookie"), ["session=abc"]);
}

#[tokio::test]
async fn automatic_redirects_follow_httpx_rules() {
    let other = Server::start(|_| response("200 OK", &[], b"other")).await;
    let other_origin = other.origin();
    let server = Server::start(move |request| match request.target.as_str() {
        "/post-302" => response("302 Found", &[("Location", "/landing")], b"redirect body"),
        "/post-307" => response("307 Temporary Redirect", &[("Location", "/landing")], b""),
        "/cross" => response(
            "302 Found",
            &[("Location", &format!("{other_origin}/x"))],
            b"",
        ),
        target if target.starts_with("/loop") => {
            response("302 Found", &[("Location", "/loop")], b"")
        }
        _ => response("200 OK", &[], b"landed"),
    })
    .await;
    let client = client(
        &HttpContext::new(),
        &ClientOptions::new(10.0, true).header("Authorization", "Bearer t"),
    );
    let origin = server.origin();
    let body = Some(b"{\"a\":1}".to_vec());
    let mut response = send_following_redirects(
        &client,
        client
            .build_request(
                Method::Post,
                &format!("{origin}/post-302"),
                &[],
                body.clone(),
            )
            .expect("request"),
    )
    .await
    .expect("302");
    assert_eq!(response.status, 200);
    assert_eq!(response.read_to_end().await.expect("body"), b"landed");
    send_following_redirects(
        &client,
        client
            .build_request(Method::Post, &format!("{origin}/post-307"), &[], body)
            .expect("request"),
    )
    .await
    .expect("307");
    let requests = server.requests();
    let lines: Vec<String> = requests.iter().map(|r| r.line()).collect();
    assert_eq!(
        lines,
        [
            "POST /post-302",
            "GET /landing",
            "POST /post-307",
            "POST /landing"
        ]
    );
    assert!(
        requests[1].body.is_empty(),
        "302 turns POST into a bodyless GET"
    );
    assert_eq!(requests[3].body, b"{\"a\":1}", "307 keeps method and body");
    assert_eq!(requests[3].header_values("authorization"), ["Bearer t"]);

    send_following_redirects(
        &client,
        client
            .build_request(Method::Get, &format!("{origin}/cross"), &[], None)
            .expect("request"),
    )
    .await
    .expect("cross-origin");
    assert!(
        other.requests()[0]
            .header_values("authorization")
            .is_empty(),
        "Authorization is dropped on a cross-origin redirect"
    );

    logs::capture();
    let err = send_following_redirects(
        &client,
        client
            .build_request(Method::Get, &format!("{origin}/loop0"), &[], None)
            .expect("request"),
    )
    .await
    .expect_err("too many redirects");
    assert_eq!(err.kind, ErrorKind::TooManyRedirects);
    assert_eq!(err.message, "Exceeded maximum allowed redirects.");
    assert_eq!(
        logs::lines().len(),
        21,
        "21 requests, the 21st redirect is refused"
    );
}

#[tokio::test]
async fn redirect_to_another_scheme_is_unsupported_protocol() {
    let server =
        Server::start(|_| response("302 Found", &[("Location", "ftp://files.test/file")], b""))
            .await;
    logs::capture();
    let err = run_page_probe(
        &default_client(),
        &format!("{}/", server.origin()),
        1000,
        true,
        &Deadline::none(),
    )
    .await
    .expect_err("ftp");
    assert_eq!(err.kind, ErrorKind::UnsupportedProtocol);
    assert_eq!(
        err.message,
        "Request URL has an unsupported protocol 'ftp://'."
    );
    assert!(err.kind.is_page_probe_warning());
    assert_eq!(logs::lines().len(), 1);
}

#[tokio::test]
async fn ws_urls_are_plain_http_requests() {
    let server = Server::start(|_| response("200 OK", &[], b"ok")).await;
    let url = format!("ws://127.0.0.1:{}/socket", server.port());
    logs::capture();
    let result = run_page_probe(&default_client(), &url, 1000, true, &Deadline::none())
        .await
        .expect("ws as http");
    assert_eq!(result.status_code, 200);
    assert_eq!(server.requests()[0].line(), "GET /socket");
    assert_eq!(
        logs::lines(),
        [format!("HTTP Request: GET {url} \"HTTP/1.1 200 OK\"")]
    );
}

#[tokio::test]
async fn status_line_details_reach_log_and_errors() {
    let server = Server::start(|request| match request.target.as_str() {
        "/custom" => Reply::Raw(
            b"HTTP/1.1 404 Nope\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec(),
        ),
        "/empty" => {
            Reply::Raw(b"HTTP/1.1 404 \r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec())
        }
        "/http10" => Reply::Raw(b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\n{}".to_vec()),
        _ => Reply::Raw(
            b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .to_vec(),
        ),
    })
    .await;
    let origin = server.origin();
    let client = default_client();
    logs::capture();
    let mut messages = Vec::new();
    for path in ["/custom", "/empty", "/other"] {
        let err = read_http_json(&client, &format!("{origin}{path}"), 100, &Deadline::none())
            .await
            .expect_err("status error");
        assert_eq!(err.kind, ErrorKind::HttpStatusError);
        messages.push(err.message.lines().next().expect("first line").to_owned());
    }
    read_http_json(&client, &format!("{origin}/http10"), 100, &Deadline::none())
        .await
        .expect("HTTP/1.0 response");
    assert_eq!(
        messages,
        [
            format!("Client error '404 Nope' for url '{origin}/custom'"),
            format!("Client error '404 ' for url '{origin}/empty'"),
            format!("Client error '413 Payload Too Large' for url '{origin}/other'"),
        ]
    );
    assert_eq!(
        logs::lines(),
        [
            format!("HTTP Request: GET {origin}/custom \"HTTP/1.1 404 Nope\""),
            format!("HTTP Request: GET {origin}/empty \"HTTP/1.1 404 \""),
            format!("HTTP Request: GET {origin}/other \"HTTP/1.1 413 Payload Too Large\""),
            format!("HTTP Request: GET {origin}/http10 \"HTTP/1.0 200 OK\""),
        ]
    );
}

#[tokio::test]
async fn transport_failures_have_httpx_classes() {
    let server = Server::start(|request| match request.target.as_str() {
        "/close" => Reply::Close,
        "/truncated" => Reply::Raw(
            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nabc".to_vec(),
        ),
        _ => Reply::Drip {
            prefix: b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n".to_vec(),
            drip: b"x".to_vec(),
            interval: Duration::from_secs(5),
            give_up: Duration::from_secs(6),
        },
    })
    .await;
    let origin = server.origin();
    let client = client(&HttpContext::new(), &ClientOptions::new(0.3, true));
    let probe = |path: &'static str| {
        let url = format!("{origin}{path}");
        let client = &client;
        async move { run_page_probe(client, &url, 1000, true, &Deadline::none()).await }
    };

    let err = probe("/close").await.expect_err("no response");
    assert_eq!(err.kind, ErrorKind::RemoteProtocolError);
    assert_eq!(
        err.message,
        "Server disconnected without sending a response."
    );

    let err = probe("/truncated").await.expect_err("short body");
    assert_eq!(err.kind, ErrorKind::RemoteProtocolError, "{err:?}");

    let started = Instant::now();
    let err = probe("/stall").await.expect_err("read timeout");
    assert_eq!(err.kind, ErrorKind::ReadTimeout, "{err:?}");
    assert_eq!(err.message, "timed out");
    assert!(started.elapsed() < Duration::from_secs(3));

    for err in [
        probe("/close").await.unwrap_err(),
        probe("/truncated").await.unwrap_err(),
    ] {
        assert!(err.kind.is_page_probe_warning());
    }
}

#[tokio::test]
async fn connection_refused_is_a_connect_error() {
    let port = {
        let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind");
        listener.local_addr().expect("address").port()
    };
    let err = run_page_probe(
        &default_client(),
        &format!("http://127.0.0.1:{port}/"),
        1000,
        true,
        &Deadline::none(),
    )
    .await
    .expect_err("refused");
    assert_eq!(err.kind, ErrorKind::ConnectError);
    assert!(err.message.starts_with("[Errno "), "{err:?}");
    assert!(err.message.ends_with("] Connection refused"), "{err:?}");
}

#[tokio::test]
async fn lookup_failures_are_connect_errors_with_the_gaierror_text() {
    let lookup = Arc::new(StubLookup {
        fail: vec!["missing.test".to_owned()],
        ..StubLookup::loopback()
    });
    let context = HttpContext::new().with_lookup(lookup);
    let err = run_page_probe(
        &client(&context, &ClientOptions::new(5.0, true)),
        "http://missing.test/",
        1000,
        true,
        &Deadline::none(),
    )
    .await
    .expect_err("lookup failure");
    assert_eq!(err.kind, ErrorKind::ConnectError);
    assert_eq!(err.message, "[Errno -2] Name or service not known");
}

#[tokio::test]
async fn hosts_and_ports_reach_getaddrinfo_as_httpx_writes_them() {
    // httpcore hands httpx's host text and the port to `getaddrinfo`, which
    // is where an escaped host or a port above 65535 fails (no network: the
    // stub refuses the escaped name, the numeric host needs no DNS).
    let lookup = Arc::new(StubLookup {
        fail: vec!["ho%41st.test".to_owned()],
        ..StubLookup::loopback()
    });
    let stubbed = HttpContext::new().with_lookup(lookup);
    let err = run_page_probe(
        &client(&stubbed, &ClientOptions::new(5.0, true)),
        "http://ho%41st.test/",
        1000,
        true,
        &Deadline::none(),
    )
    .await
    .expect_err("escaped host");
    assert_eq!(err.kind, ErrorKind::ConnectError);
    assert_eq!(err.message, "[Errno -2] Name or service not known");
    let err = run_page_probe(
        &default_client(),
        "http://127.0.0.1:99999/",
        1000,
        true,
        &Deadline::none(),
    )
    .await
    .expect_err("port beyond 65535");
    assert_eq!(err.kind, ErrorKind::ConnectError, "{err:?}");
    assert!(err.kind.is_page_probe_warning());
}

#[tokio::test]
async fn invalid_and_relative_urls_fail_before_any_request() {
    let client = default_client();
    let err = run_page_probe(&client, "http://host:abc/", 1000, true, &Deadline::none())
        .await
        .expect_err("invalid");
    assert_eq!(err.kind, ErrorKind::InvalidUrl);
    assert_eq!(err.message, "Invalid port: 'abc'");
    assert!(!err.kind.is_page_probe_warning());
    let err = run_page_probe(&client, "example.test", 1000, true, &Deadline::none())
        .await
        .expect_err("relative");
    assert_eq!(err.kind, ErrorKind::UnsupportedProtocol);
    assert!(err.kind.is_page_probe_warning());
}

#[test]
fn system_lookup_resolves_localhost_without_network() {
    let addresses = SystemLookup
        .lookup("localhost")
        .expect("localhost resolves");
    assert!(addresses.iter().any(IpAddr::is_loopback), "{addresses:?}");
}

/// A CA and a server certificate for `tls.test` (short validity and the
/// server-auth usage, which platform verifiers insist on).
fn tls_material() -> (CertificateDer<'static>, tokio_rustls::TlsAcceptor) {
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair,
    };
    let now = time::OffsetDateTime::now_utc();
    let mut ca_params = CertificateParams::new(Vec::new()).expect("CA params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.not_before = now - time::Duration::days(1);
    ca_params.not_after = now + time::Duration::days(30);
    let ca =
        CertifiedIssuer::self_signed(ca_params, KeyPair::generate().expect("CA key")).expect("CA");
    let mut params = CertificateParams::new(vec!["tls.test".to_owned()]).expect("params");
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.not_before = now - time::Duration::days(1);
    params.not_after = now + time::Duration::days(30);
    let key = KeyPair::generate().expect("key");
    let cert = params.signed_by(&key, &ca).expect("server certificate");
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            rustls::pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        )
        .expect("server config");
    (
        ca.der().clone(),
        tokio_rustls::TlsAcceptor::from(Arc::new(config)),
    )
}

#[tokio::test]
async fn tls_verification_follows_verify_tls() {
    let (root, acceptor) = tls_material();
    let server = Server::start_with(|_| response("200 OK", &[], b"{}"), Some(acceptor)).await;
    let lookup: Arc<dyn Lookup> = Arc::new(StubLookup::loopback());
    let trusted = HttpContext::new()
        .with_lookup(Arc::clone(&lookup))
        .with_extra_root(root);
    let untrusted = HttpContext::new().with_lookup(lookup);
    let port = server.port();
    let read = |context: HttpContext, host: &'static str, verify: bool| async move {
        let client = client(&context, &ClientOptions::new(5.0, verify));
        read_http_json(
            &client,
            &format!("https://{host}:{port}/"),
            100,
            &Deadline::none(),
        )
        .await
    };
    read(trusted.clone(), "tls.test", true)
        .await
        .expect("trusted certificate");
    let err = read(untrusted.clone(), "tls.test", true)
        .await
        .expect_err("unknown issuer");
    assert_eq!(err.kind, ErrorKind::ConnectError, "{err:?}");
    let err = read(trusted, "other.test", true)
        .await
        .expect_err("wrong host name");
    assert_eq!(err.kind, ErrorKind::ConnectError, "{err:?}");
    // verify_tls: false accepts the unknown issuer and the wrong name.
    read(untrusted.clone(), "tls.test", false)
        .await
        .expect("verify_tls false");
    read(untrusted, "other.test", false)
        .await
        .expect("verify_tls false ignores the host name");
}

#[test]
fn utf8_errors_and_bom_follow_json_loads() {
    use crate::http::probe::decode_json;
    assert_eq!(
        decode_json(b"\xef\xbb\xbf[1]").expect("BOM skipped"),
        serde_json::json!([1])
    );
    let err = decode_json(b"[\xff]").expect_err("invalid UTF-8");
    assert_eq!(err.kind, ErrorKind::UnicodeDecodeError);
    assert_eq!(
        err.message,
        "'utf-8' codec can't decode byte 0xff in position 1: invalid start byte"
    );
    let err = decode_json(b"{\"a\": NaN}").expect_err("D-4");
    assert_eq!(err.kind, ErrorKind::JsonDecodeError);
    assert!(!err.kind.is_page_probe_warning());
    let mut sink = Vec::new();
    write!(sink, "{err}").expect("display");
    assert!(!sink.is_empty());
}
