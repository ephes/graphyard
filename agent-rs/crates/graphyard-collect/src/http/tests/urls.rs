//! httpx's URL model against the `urls`, `long_urls`, `urljoin` and
//! `redirects` fixture cases.

use num_traits::ToPrimitive;

use super::fixture::{cases, number, opt_text, text};
use crate::http::client::{ClientOptions, HttpClient, HttpContext, Method, Request, check_scheme};
use crate::http::error::ErrorKind;
use crate::http::url::{HttpxUrl, urljoin};

#[test]
fn urls_parse_render_and_classify_like_httpx() {
    let client = HttpClient::new(&HttpContext::new(), &ClientOptions::new(10.0, true));
    for case in cases("urls") {
        let input = text(case, "url");
        let parsed = HttpxUrl::parse(input);
        if let Some(message) = opt_text(case, "invalid") {
            assert_eq!(
                parsed.as_ref().map_err(|err| err.0.as_str()),
                Err(message),
                "InvalidURL for {input:?}"
            );
            continue;
        }
        let url = parsed.unwrap_or_else(|err| panic!("{input:?} is valid in httpx: {err}"));
        assert_eq!(url.to_string(), text(case, "display"), "str() of {input:?}");
        assert_eq!(
            url.is_absolute(),
            case["absolute"].as_bool().expect("absolute flag"),
            "{input:?}"
        );
        let send = &case["send"];
        let tls = check_scheme(&url);
        if let Some(class) = opt_text(send, "class") {
            let err = tls.expect_err("httpx refuses before connecting");
            assert_eq!(err.kind.python_class(), class, "{input:?}");
            assert_eq!(err.message, text(send, "message"), "{input:?}");
            continue;
        }
        let tls = tls.expect("httpx connects");
        let connect = send["connect"].as_array().expect("connect target");
        assert_eq!(url.host(), connect[0].as_str().expect("host"), "{input:?}");
        let port = url
            .port()
            .and_then(ToPrimitive::to_u64)
            .unwrap_or(if tls { 443 } else { 80 });
        assert_eq!(port, connect[1].as_u64().expect("port"), "{input:?}");
        assert_eq!(url.target(), text(send, "target"), "{input:?}");
        assert_eq!(url.netloc(), text(send, "host_header"), "{input:?}");
        let request = client
            .build_request(Method::Get, input, &[], None)
            .expect("request");
        assert_eq!(
            client.authenticated(&request).header("Authorization"),
            opt_text(send, "authorization"),
            "{input:?}"
        );
    }
}

#[test]
fn url_length_limit_matches_httpx() {
    for case in cases("long_urls") {
        let length = usize::try_from(number(case, "length")).expect("length");
        let prefix = text(case, "prefix");
        let input = format!(
            "{prefix}{}",
            text(case, "fill").repeat(length - prefix.len())
        );
        let result = HttpxUrl::parse(&input).err().map(|err| err.0);
        assert_eq!(
            result.as_deref(),
            opt_text(case, "invalid"),
            "length {length}"
        );
    }
}

#[test]
fn urljoin_matches_cpython() {
    for case in cases("urljoin") {
        assert_eq!(
            urljoin(text(case, "base"), text(case, "url")),
            text(case, "joined"),
            "{case}"
        );
    }
}

fn method(name: &str) -> Method {
    match name {
        "GET" => Method::Get,
        "HEAD" => Method::Head,
        "POST" => Method::Post,
        other => panic!("unexpected method {other}"),
    }
}

#[test]
fn redirect_requests_match_httpx() {
    for case in cases("redirects") {
        let base = text(case, "base");
        let method_name = text(case, "method");
        let request = Request {
            method: method(method_name),
            url: HttpxUrl::parse(base).expect("valid base"),
            headers: vec![("Authorization".to_owned(), "Bearer secret".to_owned())],
            body: (method_name == "POST").then(|| b"body".to_vec()),
        };
        let status = u16::try_from(number(case, "status")).expect("status");
        let location = text(case, "location");
        let result = request.redirect(status, location);
        if let Some(error) = case.get("error") {
            let err = result.expect_err("httpx refuses the redirect");
            assert_eq!(
                err.kind.python_class(),
                text(error, "class"),
                "{base} -> {location:?}"
            );
            assert_eq!(
                err.message,
                text(error, "message"),
                "{base} -> {location:?}"
            );
            continue;
        }
        let redirect = result.unwrap_or_else(|err| panic!("{base} -> {location:?}: {err}"));
        let expected = &case["redirect"];
        assert_eq!(redirect.method.as_str(), text(expected, "method"), "{case}");
        assert_eq!(redirect.url.to_string(), text(expected, "url"), "{case}");
        assert_eq!(
            redirect.header("Authorization").is_some(),
            expected["authorization"].as_bool().expect("flag"),
            "{case}"
        );
        assert_eq!(
            redirect.body.is_some(),
            expected["body"].as_bool().expect("flag"),
            "{case}"
        );
    }
}

#[test]
fn invalid_url_is_not_an_http_error() {
    // A spec URL httpx rejects is a critical collector failure, not a probe
    // warning (`except (httpx.HTTPError, …)` does not catch InvalidURL).
    let kind = ErrorKind::InvalidUrl;
    assert!(!kind.is_http_error());
    assert!(!kind.is_page_probe_warning());
}

#[test]
fn every_httpx_request_target_goes_on_the_wire() {
    let client = HttpClient::new(&HttpContext::new(), &ClientOptions::new(10.0, true));
    for case in cases("urls") {
        let Some(target) = opt_text(&case["send"], "target") else {
            continue;
        };
        let request = client
            .build_request(Method::Get, text(case, "url"), &[], None)
            .expect("request");
        let outgoing = client.hyper_request(&request).unwrap_or_else(|err| {
            panic!("{target:?} cannot be sent: {err}");
        });
        assert_eq!(
            outgoing.uri().path_and_query().map(|pq| pq.as_str()),
            Some(target)
        );
    }
}
