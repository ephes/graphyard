//! Response status texts (PLAN.md 6.1): httpx's reason phrases and the
//! `raise_for_status()` message, a P1 template pinned by the
//! `reason_phrases` and `raise_for_status` cases of
//! `fixtures/http_errors.json`.

/// `httpx.codes.get_reason_phrase(code)`: the phrase httpx shows when the
/// server sends an empty reason; empty for an unknown code.
pub fn standard_reason_phrase(code: u16) -> &'static str {
    match code {
        100 => "Continue",
        101 => "Switching Protocols",
        102 => "Processing",
        103 => "Early Hints",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        203 => "Non-Authoritative Information",
        204 => "No Content",
        205 => "Reset Content",
        206 => "Partial Content",
        207 => "Multi-Status",
        208 => "Already Reported",
        226 => "IM Used",
        300 => "Multiple Choices",
        301 => "Moved Permanently",
        302 => "Found",
        303 => "See Other",
        304 => "Not Modified",
        305 => "Use Proxy",
        307 => "Temporary Redirect",
        308 => "Permanent Redirect",
        400 => "Bad Request",
        401 => "Unauthorized",
        402 => "Payment Required",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        406 => "Not Acceptable",
        407 => "Proxy Authentication Required",
        408 => "Request Timeout",
        409 => "Conflict",
        410 => "Gone",
        411 => "Length Required",
        412 => "Precondition Failed",
        413 => "Request Entity Too Large",
        414 => "Request-URI Too Long",
        415 => "Unsupported Media Type",
        416 => "Requested Range Not Satisfiable",
        417 => "Expectation Failed",
        418 => "I'm a teapot",
        421 => "Misdirected Request",
        422 => "Unprocessable Entity",
        423 => "Locked",
        424 => "Failed Dependency",
        425 => "Too Early",
        426 => "Upgrade Required",
        428 => "Precondition Required",
        429 => "Too Many Requests",
        431 => "Request Header Fields Too Large",
        451 => "Unavailable For Legal Reasons",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        505 => "HTTP Version Not Supported",
        506 => "Variant Also Negotiates",
        507 => "Insufficient Storage",
        508 => "Loop Detected",
        510 => "Not Extended",
        511 => "Network Authentication Required",
        _ => "",
    }
}

/// `Response.reason_phrase` of a real response: the server's reason with
/// non-ASCII bytes dropped (`decode("ascii", errors="ignore")`), empty when
/// the server sent none. ([`standard_reason_phrase`] applies only to
/// responses built without a status line, such as `MockTransport` ones.)
pub fn reason_phrase(server_reason: &[u8]) -> String {
    server_reason
        .iter()
        .filter(|byte| byte.is_ascii())
        .map(|byte| char::from(*byte))
        .collect()
}

/// The statuses `Response.has_redirect_location` accepts with a
/// `Location` header (301, 302, 303, 307, 308).
pub fn is_redirect_status(code: u16) -> bool {
    matches!(code, 301 | 302 | 303 | 307 | 308)
}

/// The text of the `HTTPStatusError` `raise_for_status()` raises, `None`
/// for a 2xx status. `location` is the `Location` header when the response
/// has a redirect location ([`is_redirect_status`] and a header present).
pub fn status_error_message(
    code: u16,
    reason_phrase: &str,
    url: &str,
    location: Option<&str>,
) -> Option<String> {
    if (200..300).contains(&code) {
        return None;
    }
    let error_type = match code / 100 {
        1 => "Informational response",
        3 => "Redirect response",
        4 => "Client error",
        5 => "Server error",
        _ => "Invalid status code",
    };
    let redirect = match location {
        Some(location) if is_redirect_status(code) => {
            format!("Redirect location: '{location}'\n")
        }
        _ => String::new(),
    };
    Some(format!(
        "{error_type} '{code} {reason_phrase}' for url '{url}'\n{redirect}For more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/{code}"
    ))
}
