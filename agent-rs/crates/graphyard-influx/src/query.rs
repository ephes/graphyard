//! The condition query client: `influx.py:query_range` in the modes `v2`
//! (Flux through the `influxdb_client` request), `v3` (`_read_v3_sql_rows`)
//! and `auto` (v2, then v3 after a v2 404 only), and
//! `query_condition_window` (PLAN.md 6.5).
//!
//! Like Python, every query uses a fresh HTTP client (no connection reuse),
//! HTTP/1.1 only, no redirects. The v2 request carries the headers the
//! Python client sends (`Accept-Encoding: identity`, no proxy from the
//! environment); the v3 request the ones httpx sends (`gzip, deflate`,
//! decoded; proxies from the environment like httpx's `trust_env`).

use std::time::Duration;

use chrono::{DateTime, Utc};
use graphyard_core::MetricSample;
use graphyard_core::pycompat::{
    int_digit_limit_error, json_loads, py_int_true_divide, py_str_repr,
};
use graphyard_core::settings::{InfluxApiMode, Settings};
use reqwest::header::{
    ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue, USER_AGENT,
};
use reqwest::{StatusCode, Url};
use serde_json::Value;
use tokio::time::Instant;

use crate::config::InfluxClientConfig;
use crate::error::QueryError;
use crate::flux::{flux_query, flux_request_body, flux_samples, parse_annotated_csv};
use crate::sql::{sql_query, sql_samples};
use crate::window::{ConditionQuery, QueryRange, condition_window};
use crate::write::{
    Target, USER_AGENT_VALUE, build_client, quote_plus, transport_error, urllib3_url,
};

/// `influx.SQL_QUERY_MAX_BODY_BYTES`: the decoded v3 response cap.
pub const SQL_QUERY_MAX_BODY_BYTES: usize = 10 * 1024 * 1024;
/// `influx.SQL_QUERY_TOTAL_TIMEOUT_SECONDS`: the whole v3 request.
pub const SQL_QUERY_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);

/// `User-Agent` of httpx 0.28.1.
const V3_USER_AGENT: &str = "python-httpx/0.28.1";

/// The query client: the InfluxDB settings of one agent process.
#[derive(Clone, Debug)]
pub struct QueryClient {
    settings: Settings,
    mode: InfluxApiMode,
    sql_total_timeout: Duration,
    sql_max_body_bytes: usize,
}

impl QueryClient {
    /// A client for the agent's settings (read once at startup).
    pub fn new(settings: &Settings) -> Self {
        Self {
            settings: settings.clone(),
            mode: settings.influx_api_mode(),
            sql_total_timeout: SQL_QUERY_TOTAL_TIMEOUT,
            sql_max_body_bytes: SQL_QUERY_MAX_BODY_BYTES,
        }
    }

    /// Replace the v3 total deadline and body cap (Python tests patch the
    /// module constants the same way). The deadline message keeps naming
    /// 30 seconds, as in Python.
    pub fn with_sql_limits(mut self, total_timeout: Duration, max_body_bytes: usize) -> Self {
        self.sql_total_timeout = total_timeout;
        self.sql_max_body_bytes = max_body_bytes;
        self
    }

    /// `influx.query_condition_window(condition, now=now)`.
    ///
    /// # Errors
    /// See [`condition_window`] and [`QueryClient::query_range`].
    pub async fn query_condition_window(
        &self,
        condition: &ConditionQuery<'_>,
        now: DateTime<Utc>,
    ) -> Result<Vec<MetricSample>, QueryError> {
        let range = condition_window(
            condition,
            &self.settings.condition_data_stale_warning_seconds,
            now,
        )?;
        self.query_range(&range).await
    }

    /// `influx.query_range` in the configured mode.
    ///
    /// # Errors
    /// The exception the Python query raises (configuration, HTTP, decoding,
    /// non-finite samples).
    pub async fn query_range(&self, range: &QueryRange) -> Result<Vec<MetricSample>, QueryError> {
        match self.mode {
            InfluxApiMode::V3 => self.query_v3(range).await,
            InfluxApiMode::Auto => match self.query_v2(range).await {
                Err(err) if err.is_v2_query_missing() => self.query_v3(range).await,
                other => other,
            },
            InfluxApiMode::V2 => self.query_v2(range).await,
        }
    }

    /// `_query_range_v2_flux` through `influx._build_client()` and
    /// `QueryApi.query`.
    async fn query_v2(&self, range: &QueryRange) -> Result<Vec<MetricSample>, QueryError> {
        let settings = &self.settings;
        let flux = flux_query(range, &settings.influx_bucket, &settings.influx_measurement);
        let config =
            InfluxClientConfig::from_settings(settings).map_err(|err| QueryError::Config(err.0))?;
        // The request path of the write client: urllib3 URL parsing, the
        // per-read timeout, transport errors by urllib3 class.
        let timeout = config.request_timeout()?;
        let url = urllib3_url(format!(
            "{}/api/v2/query?org={}",
            config.host,
            quote_plus(&settings.influx_org)
        ))?;
        let target = Target::of(&url);
        let authorization = latin1_header(&format!("Token {}", config.token))?;
        let client = build_client(timeout.map(|timeout| timeout.duration))
            .map_err(|err| client_error(&err))?;
        // The request asks for `identity`, but urllib3 decodes a gzip or
        // deflate response anyway; reqwest decodes too when the header is set
        // explicitly.
        let request = client
            .post(url)
            .header(ACCEPT_ENCODING, "identity")
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .header(AUTHORIZATION, authorization)
            .header(USER_AGENT, USER_AGENT_VALUE)
            .body(flux_request_body(&flux));
        let response = request
            .send()
            .await
            .map_err(|err| transport_error(&err, &target, timeout))?;
        let status = response.status();
        if !status.is_success() {
            let reason = latin1(&reason_phrase(&response));
            let headers = response.headers().clone();
            // `ApiException` reads the body; a broken read raises urllib3's
            // error instead (and is no 404 for the auto fallback).
            let body = response
                .bytes()
                .await
                .map_err(|err| transport_error(&err, &target, timeout))?;
            return Err(QueryError::Api {
                status: status.as_u16(),
                message: api_exception_text(status, &reason, &headers, &body),
            });
        }
        let body = response
            .bytes()
            .await
            .map_err(|err| transport_error(&err, &target, timeout))?;
        let records = parse_annotated_csv(&body)?;
        flux_samples(&records, &range.metric)
    }

    /// `_query_range_v3_sql`.
    async fn query_v3(&self, range: &QueryRange) -> Result<Vec<MetricSample>, QueryError> {
        let sql = sql_query(range, &self.settings.influx_measurement)?;
        let payload = serde_json::json!({"db": self.settings.influx_bucket, "q": sql}).to_string();
        let rows = self.read_v3_sql_rows(payload).await?;
        sql_samples(&rows, &range.metric)
    }

    /// `_read_v3_sql_rows`: the whole request under the total deadline.
    async fn read_v3_sql_rows(&self, payload: String) -> Result<Value, QueryError> {
        let deadline = Instant::now() + self.sql_total_timeout;
        let deadline_error =
            || QueryError::config("InfluxDB SQL response exceeded the 30-second total deadline");
        match tokio::time::timeout_at(deadline, self.v3_request(payload, deadline)).await {
            Err(_) | Ok(Err(V3Failure::Deadline)) => Err(deadline_error()),
            Ok(Err(V3Failure::Error(err))) => Err(err),
            Ok(Ok(rows)) => Ok(rows),
        }
    }

    async fn v3_request(&self, payload: String, deadline: Instant) -> Result<Value, V3Failure> {
        // `httpx.AsyncClient(timeout=max(1.0, INFLUX_TIMEOUT_MS / 1000))`;
        // the division's OverflowError is not caught by `_read_v3_sql_rows`.
        let per_operation = py_int_true_divide(&self.settings.influx_timeout_ms, 1000)
            .map_err(QueryError::from)?
            .max(1.0);
        let token = &self.settings.influx_token;
        let target = format!(
            "{}/api/v3/query_sql",
            self.settings.influx_url.trim_end_matches('/')
        );
        let url = match Url::parse(&target) {
            Ok(url) => url,
            Err(url::ParseError::RelativeUrlWithoutBase | url::ParseError::EmptyHost) => {
                return Err(sql_failed("UnsupportedProtocol").into());
            }
            // `httpx.InvalidURL` is not an `HTTPError`: it propagates as is.
            Err(err) => return Err(QueryError::python("InvalidURL", err.to_string()).into()),
        };
        // httpx encodes header values as ASCII before sending anything.
        if !token.is_ascii() {
            return Err(sql_failed("UnicodeEncodeError").into());
        }
        if !matches!(url.scheme(), "http" | "https") {
            return Err(sql_failed("UnsupportedProtocol").into());
        }
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        if !token.is_empty() {
            // h11 rejects control characters in header values (it does so
            // after connecting; Rust before, P2).
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| sql_failed("LocalProtocolError"))?;
            headers.insert(AUTHORIZATION, value);
        }
        // httpx's spelling; reqwest still decodes the response.
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip, deflate"));
        headers.insert(USER_AGENT, HeaderValue::from_static(V3_USER_AGENT));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let timeout = Duration::try_from_secs_f64(per_operation).ok();
        let client = v3_client(timeout).map_err(|err| client_error(&err))?;
        let mut response = client
            .post(url.clone())
            .headers(headers)
            .body(payload)
            .send()
            .await
            .map_err(|err| sql_failed(httpx_error_class(&err)))?;
        let status = response.status();
        log::info!(
            "HTTP Request: POST {url} \"{:?} {} {}\"",
            response.version(),
            status.as_u16(),
            String::from_utf8_lossy(&reason_phrase(&response))
        );
        if status == StatusCode::NOT_FOUND {
            return Err(QueryError::config(
                "InfluxDB v3 SQL endpoint not found. Set INFLUX_API_MODE=v2 for InfluxDB 2.",
            )
            .into());
        }
        if !status.is_success() {
            return Err(QueryError::config(format!(
                "InfluxDB SQL query failed: HTTP {}",
                status.as_u16()
            ))
            .into());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|err| sql_failed(httpx_error_class(&err)))?
        {
            if Instant::now() >= deadline {
                return Err(V3Failure::Deadline);
            }
            if body.len() + chunk.len() > self.sql_max_body_bytes {
                return Err(QueryError::config(
                    "InfluxDB SQL response exceeds the 10 MiB decoded byte limit",
                )
                .into());
            }
            body.extend_from_slice(&chunk);
        }
        let rows = decode_sql_body(&body)?;
        // JSON decoding is synchronous; its time counts too.
        if Instant::now() >= deadline {
            return Err(V3Failure::Deadline);
        }
        Ok(rows)
    }
}

/// How the v3 request ends when it fails: the total deadline, or an error.
enum V3Failure {
    Deadline,
    Error(QueryError),
}

impl From<QueryError> for V3Failure {
    fn from(err: QueryError) -> Self {
        Self::Error(err)
    }
}

/// `InfluxDB SQL query failed: <httpx class>`.
fn sql_failed(class: &str) -> QueryError {
    QueryError::config(format!("InfluxDB SQL query failed: {class}"))
}

/// `json.loads(body)` of the v3 response: UTF-8 (a BOM is skipped, as
/// `json.loads` detects `utf-8-sig`), any decoding error is "invalid JSON".
/// UTF-16/32 bodies and `NaN`/`Infinity` tokens are invalid here (D-4).
fn decode_sql_body(body: &[u8]) -> Result<Value, QueryError> {
    let invalid = || QueryError::config("InfluxDB SQL query failed: invalid JSON");
    let body = body.strip_prefix(b"\xef\xbb\xbf").unwrap_or(body);
    let text = std::str::from_utf8(body).map_err(|_| invalid())?;
    if int_digit_limit_error(text).is_some() {
        return Err(invalid());
    }
    json_loads(text).map_err(|_| invalid())
}

/// A fresh reqwest client for one v3 query, like httpx's `AsyncClient`:
/// HTTP/1.1, no redirects, no retries, no idle connections, gzip and deflate
/// responses decoded, proxies from the environment (`trust_env`).
fn v3_client(timeout: Option<Duration>) -> Result<reqwest::Client, reqwest::Error> {
    crate::install_crypto_provider();
    let mut builder = reqwest::Client::builder()
        .http1_only()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .pool_max_idle_per_host(0);
    if let Some(timeout) = timeout {
        builder = builder.connect_timeout(timeout).read_timeout(timeout);
    }
    builder.build()
}

fn client_error(err: &reqwest::Error) -> QueryError {
    QueryError::python("ValueError", err.to_string())
}

/// A header value as `http.client` sends it: Latin-1 encoded, without bare
/// line breaks.
fn latin1_header(value: &str) -> Result<HeaderValue, QueryError> {
    let mut bytes = Vec::with_capacity(value.len());
    for (position, c) in value.chars().enumerate() {
        let code = u32::from(c);
        let Ok(byte) = u8::try_from(code) else {
            return Err(QueryError::python(
                "UnicodeEncodeError",
                format!(
                    "'latin-1' codec can't encode character {} in position {position}: ordinal \
                     not in range(256)",
                    py_str_repr(&c.to_string())
                ),
            ));
        };
        bytes.push(byte);
    }
    HeaderValue::from_bytes(&bytes).map_err(|_| {
        QueryError::python(
            "ValueError",
            format!(
                "Invalid header value {}",
                graphyard_core::pycompat::py_bytes_repr(&bytes)
            ),
        )
    })
}

/// The reason phrase of the status line as received.
fn reason_phrase(response: &reqwest::Response) -> Vec<u8> {
    match response.extensions().get::<hyper::ext::ReasonPhrase>() {
        Some(reason) => reason.as_bytes().to_vec(),
        None => response
            .status()
            .canonical_reason()
            .unwrap_or("")
            .as_bytes()
            .to_vec(),
    }
}

fn latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| char::from(*byte)).collect()
}

/// `str(ApiException)` for a response: status, reason, the headers as
/// urllib3's `HTTPHeaderDict` repr and the body as `bytes` repr (header
/// names arrive lower-cased in Rust, P2).
fn api_exception_text(
    status: StatusCode,
    reason: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> String {
    let mut text = format!("({})\nReason: {reason}\n", status.as_u16());
    if !headers.is_empty() {
        let items: Vec<String> = headers
            .keys()
            .map(|name| {
                let values: Vec<String> = headers
                    .get_all(name)
                    .iter()
                    .map(|value| latin1(value.as_bytes()))
                    .collect();
                format!(
                    "{}: {}",
                    py_str_repr(name.as_str()),
                    py_str_repr(&values.join(", "))
                )
            })
            .collect();
        text.push_str(&format!(
            "HTTP response headers: HTTPHeaderDict({{{}}})\n",
            items.join(", ")
        ));
    }
    if !body.is_empty() {
        text.push_str(&format!(
            "HTTP response body: {}\n",
            graphyard_core::pycompat::py_bytes_repr(body)
        ));
    }
    text
}

/// Whether the chain holds an error of type `E`.
fn has_source<E: std::error::Error + 'static>(err: &(dyn std::error::Error + 'static)) -> bool {
    let mut source = Some(err);
    while let Some(current) = source {
        if current.is::<E>() {
            return true;
        }
        source = current.source();
    }
    false
}

/// Whether the chain holds an I/O error of one of `kinds`.
fn has_io_kind(err: &(dyn std::error::Error + 'static), kinds: &[std::io::ErrorKind]) -> bool {
    let mut source = Some(err);
    while let Some(current) = source {
        if let Some(io) = current.downcast_ref::<std::io::Error>()
            && kinds.contains(&io.kind())
        {
            return true;
        }
        source = current.source();
    }
    false
}

/// The httpx exception class of a transport failure (the category in
/// `InfluxDB SQL query failed: <class>`).
fn httpx_error_class(err: &reqwest::Error) -> &'static str {
    use std::io::ErrorKind;
    if err.is_connect() {
        return if err.is_timeout() {
            "ConnectTimeout"
        } else {
            "ConnectError"
        };
    }
    if err.is_timeout() {
        return "ReadTimeout";
    }
    // reqwest reports every failure while reading a decoded body as a
    // decode error; only one without a transport error inside is httpx's
    // `DecodingError` (invalid gzip/deflate data).
    if err.is_decode() && !has_source::<hyper::Error>(err) {
        return "DecodingError";
    }
    if err.is_builder() {
        return "LocalProtocolError";
    }
    if has_io_kind(
        err,
        &[
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::BrokenPipe,
        ],
    ) {
        return "ReadError";
    }
    "RemoteProtocolError"
}
