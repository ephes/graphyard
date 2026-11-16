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

#[tokio::test]
async fn cookie_jar_count_is_bounded_across_redirects() {
    let server = Server::start(|request| {
        let hop: usize = request.target.trim_start_matches('/').parse().unwrap();
        let mut headers = vec![("Location".to_owned(), format!("/{}", hop + 1))];
        headers.extend((0..16).map(|i| {
            (
                "Set-Cookie".to_owned(),
                format!("c{}=x; Path=/", hop * 16 + i),
            )
        }));
        let headers: Vec<_> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        response("302 Found", &headers, b"")
    })
    .await;
    let client = HttpClient::new(&HttpContext::new(), &ClientOptions::new(5.0, true));
    let request = client
        .build_request(Method::Get, &format!("{}/0", server.origin()), &[], None)
        .unwrap();
    let err = send_following_redirects(&client, request.clone())
        .await
        .unwrap_err();
    assert_eq!(err.message, "collector cookie jar limit exceeded");
    assert_eq!(err.safe_message(), err.message);
    assert_eq!(server.requests().len(), 17);
    let outgoing = client.hyper_request(&request).unwrap();
    assert_eq!(
        outgoing.headers()["cookie"]
            .to_str()
            .unwrap()
            .split("; ")
            .count(),
        256
    );
    // A new spec client has its own empty jar.
    let independent = HttpClient::new(&HttpContext::new(), &ClientOptions::new(5.0, true));
    assert!(
        !independent
            .hyper_request(&request)
            .unwrap()
            .headers()
            .contains_key("cookie")
    );
}

#[tokio::test]
async fn cookie_jar_bytes_account_for_replacements_without_accumulating() {
    let server = Server::start(|request| {
        let value = match request.target.as_str() {
            "/exact" => format!("c={}; Path=/", "x".repeat(65535)),
            "/grow" => format!("c={}; Path=/", "x".repeat(65536)),
            "/shrink" => "c=x; Path=/".to_owned(),
            _ => format!("d={}; Path=/", "x".repeat(65533)),
        };
        response("200 OK", &[("Set-Cookie", &value)], b"")
    })
    .await;
    let client = HttpClient::new(&HttpContext::new(), &ClientOptions::new(5.0, true));
    for path in ["exact", "exact", "grow", "shrink", "extra"] {
        let request = client
            .build_request(
                Method::Get,
                &format!("{}/{path}", server.origin()),
                &[],
                None,
            )
            .unwrap();
        let result = client.send(&request).await;
        if path == "grow" {
            assert_eq!(
                result.unwrap_err().message,
                "collector cookie jar limit exceeded"
            );
            let outgoing = client.hyper_request(&request).unwrap();
            assert_eq!(outgoing.headers()["cookie"].as_bytes().len(), 65537);
        } else {
            result.unwrap();
        }
    }
}

#[tokio::test]
async fn excessive_content_encoding_is_refused_before_reading_body() {
    use super::server::Reply;
    use std::time::Duration;
    for encodings in ["gzip, identity,".repeat(6000), "identity, ".repeat(5)] {
        let server = Server::start(move |_| Reply::Drip {
            prefix: format!(
                "HTTP/1.1 200 OK\r\nContent-Encoding: {encodings}\r\nConnection: close\r\n\r\n"
            )
            .into_bytes(),
            drip: b"x".to_vec(),
            interval: Duration::from_secs(5),
            give_up: Duration::from_secs(5),
        })
        .await;
        let client = HttpClient::new(&HttpContext::new(), &ClientOptions::new(0.5, true));
        let request = client
            .build_request(Method::Get, &server.origin(), &[], None)
            .unwrap();
        let err = client.send(&request).await.unwrap_err();
        assert_eq!(
            err.message,
            "collector content encoding stage limit exceeded"
        );
    }
}

#[tokio::test]
async fn encoded_byte_overflow_aborts_a_chunked_connection_before_body_completion() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(stream.read_u8().await.unwrap());
        }
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n\r\n",
            )
            .await
            .unwrap();
        // Two individually valid gzip metadata fields emit no decoded bytes.
        let mut body = vec![0x1f, 0x8b, 8, 24, 0, 0, 0, 0, 0, 0xff];
        body.extend(std::iter::repeat_n(b'f', 40_000));
        body.push(0);
        body.extend(std::iter::repeat_n(b'c', 40_000));
        for chunk in body.chunks(1024) {
            let mut frame = format!("{:x}\r\n", chunk.len()).into_bytes();
            frame.extend_from_slice(chunk);
            frame.extend_from_slice(b"\r\n");
            if stream.write_all(&frame).await.is_err() {
                return;
            }
        }
        // Keep the response open: the client must abort, not await EOF/timeout.
        let mut byte = [0];
        assert!(matches!(stream.read(&mut byte).await, Ok(0) | Err(_)));
    });
    let client = HttpClient::new(&HttpContext::new(), &ClientOptions::new(0.5, true));
    let request = client
        .build_request(Method::Get, &format!("http://{address}/"), &[], None)
        .unwrap();
    let mut response = client.send(&request).await.unwrap();
    response.set_body_limit(4);
    let err = response.chunk().await.unwrap_err();
    assert_eq!(
        err.message,
        "collector encoded response byte limit exceeded"
    );
    assert_eq!(err.safe_message(), err.message);
    // Keep Response alive while observing that its connection was retired.
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status, 200);
}

#[tokio::test]
async fn encoded_byte_diagnostic_survives_page_probe_redirect_accounting() {
    use crate::http::deadline::Deadline;
    use crate::http::probe::run_page_probe;
    use std::io::Write as _;
    let mut encoder = flate2::GzBuilder::new()
        .filename(vec![b'f'; 40_000])
        .comment(vec![b'c'; 40_000])
        .write(Vec::new(), flate2::Compression::fast());
    encoder.write_all(b"x").unwrap();
    let body = encoder.finish().unwrap();
    let server = Server::start(move |request| {
        if request.target == "/first" {
            response("302 Found", &[("Location", "/final")], b"x")
        } else {
            response("200 OK", &[("Content-Encoding", "gzip")], &body)
        }
    })
    .await;
    let client = HttpClient::new(&HttpContext::new(), &ClientOptions::new(5.0, true));
    let err = run_page_probe(
        &client,
        &format!("{}/first", server.origin()),
        4,
        true,
        &Deadline::none(),
    )
    .await
    .unwrap_err();
    assert_eq!(
        err.message,
        "collector encoded response byte limit exceeded"
    );
    assert_eq!(server.requests().len(), 2);
}
