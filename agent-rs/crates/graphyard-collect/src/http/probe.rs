//! The request sequences the executors run (PLAN.md 6.1, 6.2, 6.3):
//! `services._read_http_json`, `services._run_http_page_probe` and httpx's
//! automatic redirect following (`follow_redirects=True`, used by UniFi).
//! Pinned by the `read_json` and `page_probe` scenarios of
//! `fixtures/http_errors.json`.

use std::time::Instant;

use serde_json::Value;

use graphyard_core::pycompat::{int_digit_limit_error, json_loads};

use super::client::{HttpClient, MAX_REDIRECTS, Method, Request, Response};
use super::deadline::Deadline;
use super::error::{ErrorKind, HttpError};

/// `_read_http_json(client, url, max_body_bytes, cancellation)`: GET without
/// redirects, `raise_for_status()`, the decoded body capped at
/// `max_body_bytes`, `json.loads`, with the deadline checked before the
/// request, per chunk, before and after the parse.
///
/// # Errors
/// `InvalidURL`, the transport errors of [`HttpClient::send`],
/// `HTTPStatusError`, `_PageProbeLimitExceeded`, the JSON decode error, or
/// the deadline error.
pub async fn read_http_json(
    client: &HttpClient,
    url: &str,
    max_body_bytes: u64,
    deadline: &Deadline,
) -> Result<Value, HttpError> {
    let request = client.build_request(Method::Get, url, &[], None)?;
    deadline.check()?;
    let mut response = client.send(&request).await?;
    response.raise_for_status()?;
    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        deadline.check()?;
        if exceeds(body.len() + chunk.len(), max_body_bytes) {
            return Err(HttpError::body_limit(max_body_bytes));
        }
        body.extend_from_slice(&chunk);
    }
    finish_json(&body, deadline)
}

/// The end of `_read_http_json`: the deadline is checked before and after
/// `json.loads`, so a deadline that passes while a large body is parsed
/// still fails the operation.
///
/// # Errors
/// The deadline error or the decode error.
pub(crate) fn finish_json(body: &[u8], deadline: &Deadline) -> Result<Value, HttpError> {
    deadline.check()?;
    let payload = decode_json(body)?;
    deadline.check()?;
    Ok(payload)
}

fn exceeds(length: usize, max_body_bytes: u64) -> bool {
    u64::try_from(length).map_or(true, |length| length > max_body_bytes)
}

/// `json.loads(body)` of a response body: UTF-8 (a UTF-8 byte order mark is
/// skipped, as `json.detect_encoding` does); UTF-16/32 bodies are a decode
/// error (D-4). The error texts are P2, except CPython's integer digit
/// limit (P1).
///
/// # Errors
/// `UnicodeDecodeError` for invalid UTF-8, `JSONDecodeError` for invalid
/// JSON, `ValueError` for an integer token of more than 4300 digits (D-11;
/// Python raises it while parsing, so a document that is also invalid
/// after that token is a `JSONDecodeError` here, P2).
pub fn decode_json(body: &[u8]) -> Result<Value, HttpError> {
    let body = body.strip_prefix(b"\xef\xbb\xbf").unwrap_or(body);
    let text = std::str::from_utf8(body).map_err(|err| {
        let position = err.valid_up_to();
        let byte = body.get(position).copied().unwrap_or(0);
        let reason = match err.error_len() {
            None => "unexpected end of data",
            Some(_) if (0x80..0xc2).contains(&byte) || byte >= 0xf5 => "invalid start byte",
            Some(_) => "invalid continuation byte",
        };
        HttpError::new(
            ErrorKind::UnicodeDecodeError,
            format!(
                "'utf-8' codec can't decode byte 0x{byte:02x} in position {position}: {reason}"
            ),
        )
    })?;
    let payload = json_loads(text)
        .map_err(|err| HttpError::new(ErrorKind::JsonDecodeError, err.to_string()))?;
    if let Some(err) = int_digit_limit_error(text) {
        return Err(HttpError::new(ErrorKind::ValueError, err.message));
    }
    Ok(payload)
}

/// The timings and outcome of a page probe (`_PageProbeResult`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PageProbeResult {
    /// From the first request to the final response's headers (redirect
    /// round-trips included).
    pub ttfb_seconds: f64,
    /// From the first request to the end of the final body.
    pub total_seconds: f64,
    pub status_code: u16,
    pub redirect_count: u32,
}

/// `_run_http_page_probe(client, url=…, max_body_bytes=…,
/// follow_redirects=…, cancellation=…)`: GET with redirects followed by
/// hand, so every hop's decoded body is read and counts towards
/// `max_body_bytes`; at most [`MAX_REDIRECTS`] redirects are followed.
///
/// # Errors
/// `InvalidURL`, the errors of [`HttpClient::send`] (including invalid
/// `Location` headers), body read errors, `_PageProbeLimitExceeded`,
/// `TooManyRedirects`, or the deadline error.
pub async fn run_page_probe(
    client: &HttpClient,
    url: &str,
    max_body_bytes: u64,
    follow_redirects: bool,
    deadline: &Deadline,
) -> Result<PageProbeResult, HttpError> {
    let started_at = Instant::now();
    let mut body_bytes: u64 = 0;
    let mut redirect_count: u32 = 0;
    let mut request = client.build_request(Method::Get, url, &[], None)?;
    loop {
        deadline.check()?;
        let mut response = client.send(&request).await?;
        let headers_received_at = Instant::now();
        while let Some(chunk) = response.chunk().await? {
            deadline.check()?;
            body_bytes = body_bytes.saturating_add(chunk.len() as u64);
            if body_bytes > max_body_bytes {
                return Err(HttpError::body_limit(max_body_bytes));
            }
        }
        let status_code = response.status;
        let next = response.next_request.take();
        drop(response);
        match next {
            Some(next) if follow_redirects => {
                redirect_count += 1;
                if redirect_count > MAX_REDIRECTS {
                    return Err(HttpError::too_many_redirects());
                }
                request = next;
            }
            _ => {
                return Ok(PageProbeResult {
                    ttfb_seconds: (headers_received_at - started_at).as_secs_f64(),
                    total_seconds: started_at.elapsed().as_secs_f64(),
                    status_code,
                    redirect_count,
                });
            }
        }
    }
}

/// `client.send(request, follow_redirects=True)`: redirects followed with
/// each redirect body read in full (`response.read()`), at most
/// [`MAX_REDIRECTS`]; the final response is returned unread. The auth flow
/// runs once, on the first request (`_send_handling_auth`); the hops are
/// sent as `_build_redirect_request` made them, so an `Authorization`
/// stripped on a cross-origin redirect is not added back.
///
/// # Errors
/// The errors of [`HttpClient::send`], body read errors of the redirect
/// hops, or `TooManyRedirects` when a 21st redirect would be followed.
pub async fn send_following_redirects(
    client: &HttpClient,
    request: Request,
) -> Result<Response, HttpError> {
    let mut request = client.authenticated(&request);
    let mut history: u32 = 0;
    loop {
        if history > MAX_REDIRECTS {
            return Err(HttpError::too_many_redirects());
        }
        let mut response = client.send_single(&request).await?;
        let Some(next) = response.next_request.take() else {
            return Ok(response);
        };
        response.read_to_end().await?;
        history += 1;
        request = next;
    }
}
