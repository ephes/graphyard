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

#[tokio::test]
async fn single_label_cookie_domains_cannot_cross_sites() {
    use super::server::StubLookup;
    use std::sync::Arc;
    let server = Server::start(|request| match request.target.as_str() {
        "/seed" => response(
            "302 Found",
            &[
                ("Location", "/same"),
                ("Set-Cookie", "injected=bad; Domain=.com; Path=/"),
                ("Set-Cookie", "injected2=bad; Domain=com; Path=/"),
                ("Set-Cookie", "host=ok; Path=/"),
            ],
            b"",
        ),
        "/same" => response(
            "302 Found",
            &[(
                "Location",
                &format!(
                    "http://b.com{}/other",
                    request.header_values("host")[0]
                        .strip_prefix("a.com")
                        .unwrap()
                ),
            )],
            b"",
        ),
        "/domain" => response(
            "200 OK",
            &[("Set-Cookie", "shared=ok; Domain=example.com; Path=/")],
            b"",
        ),
        _ => response("200 OK", &[], b"ok"),
    })
    .await;
    let context = HttpContext::new().with_lookup(Arc::new(StubLookup::loopback()));
    let client = HttpClient::new(&context, &ClientOptions::new(5.0, true));
    let request = client
        .build_request(
            Method::Get,
            &format!("http://a.com:{}/seed", server.port()),
            &[],
            None,
        )
        .unwrap();
    send_following_redirects(&client, request).await.unwrap();
    let requests = server.requests();
    assert_eq!(requests[1].header_values("cookie"), ["host=ok"]);
    assert!(requests[2].header_values("cookie").is_empty());
    for (host, path) in [("a.example.com", "domain"), ("b.example.com", "other")] {
        let request = client
            .build_request(
                Method::Get,
                &format!("http://{host}:{}/{path}", server.port()),
                &[],
                None,
            )
            .unwrap();
        client.send(&request).await.unwrap();
    }
    assert_eq!(server.requests()[4].header_values("cookie"), ["shared=ok"]);
}
