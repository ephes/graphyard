//! Transport admission regressions; synthetic evidence and loopback only.
use super::server::{Server, response};
use crate::http::client::{ClientOptions, HttpClient, HttpContext, Method};
use crate::http::probe::send_following_redirects;

#[tokio::test]
async fn explicit_host_is_refreshed_only_on_cross_origin_redirect() {
    let destination = Server::start(|_| response("200 OK", &[], b"ok")).await;
    let destination_origin = destination.origin();
    let source = Server::start(move |request| match request.target.as_str() {
        "/start" => response("302 Found", &[("Location", "/same")], b""),
        _ => response("302 Found", &[("Location", &destination_origin)], b""),
    })
    .await;
    let client = HttpClient::new(
        &HttpContext::new(),
        &ClientOptions::new(5.0, true)
            .header("hOsT", "custom.test")
            .header("Authorization", "Bearer synthetic"),
    );
    let request = client
        .build_request(
            Method::Get,
            &format!("{}/start", source.origin()),
            &[],
            None,
        )
        .unwrap();
    send_following_redirects(&client, request).await.unwrap();
    for request in source.requests() {
        assert_eq!(request.header_values("host"), ["custom.test"]);
        assert_eq!(request.header_values("authorization"), ["Bearer synthetic"]);
    }
    let requests = destination.requests();
    assert_eq!(
        requests[0].header_values("host"),
        [format!("127.0.0.1:{}", destination.port())]
    );
    assert!(requests[0].header_values("authorization").is_empty());
}

#[tokio::test]
async fn explicit_cookie_wins_until_redirect_rebuilds_from_jar() {
    let server = Server::start(|request| match request.target.as_str() {
        "/seed" => response("200 OK", &[("Set-Cookie", "session=jar; Path=/")], b""),
        "/explicit" => response("302 Found", &[("Location", "/landing")], b""),
        _ => response("200 OK", &[], b"ok"),
    })
    .await;
    let client = HttpClient::new(&HttpContext::new(), &ClientOptions::new(5.0, true));
    let seed = client
        .build_request(Method::Get, &format!("{}/seed", server.origin()), &[], None)
        .unwrap();
    client.send(&seed).await.unwrap();
    let request = client
        .build_request(
            Method::Get,
            &format!("{}/explicit", server.origin()),
            &[("cOoKiE", "session=explicit")],
            None,
        )
        .unwrap();
    send_following_redirects(&client, request).await.unwrap();
    let requests = server.requests();
    assert_eq!(requests[1].header_values("cookie"), ["session=explicit"]);
    assert_eq!(requests[2].header_values("cookie"), ["session=jar"]);
    // An explicitly empty credential also suppresses the jar.
    let empty = client
        .build_request(Method::Get, &server.origin(), &[("Cookie", "")], None)
        .unwrap();
    client.send(&empty).await.unwrap();
    assert_eq!(server.requests()[3].header_values("cookie"), [""]);
}
