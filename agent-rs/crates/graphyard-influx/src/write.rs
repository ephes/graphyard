//! `influx.py:write_points` (PLAN.md section 6.4): normalise, build the line
//! protocol batch, `POST /api/v2/write` as `influxdb-client` 1.50.0 does
//! with `SYNCHRONOUS` write options.
//!
//! The request (pinned by `fixtures/line_protocol.json` from the Python
//! client talking to a recording server): `POST
//! <INFLUX_URL>/api/v2/write?org=<INFLUX_ORG>&bucket=<INFLUX_BUCKET>&precision=ns`
//! with the query encoded like `urllib.parse.urlencode`, the headers
//! `Accept-Encoding: identity`, `Content-Type: text/plain`, `Accept:
//! application/json`, `Authorization: Token <token>` and the client's
//! `User-Agent`, the lines joined with `\n` as body. One request per call,
//! a fresh connection (the Python client is closed after each call), no
//! retries, no redirects, no proxy from the environment. The timeout
//! `urllib3.Timeout(total=T)` bounds the connect by `T` and then every socket
//! read by what is left of `T` (an inactivity timeout, not a deadline for the
//! response); Rust uses `T` for the connect and for every read (the connect
//! time is not subtracted, P2). The response body is always read in full and
//! decoded as UTF-8 (`RESTResponse`), so a truncated or stalled body fails
//! the write even after a 2xx status. A 2xx answer is success, anything else
//! the client's `ApiException`; a TLS failure is the `ApiException` with
//! status 0 the client raises for urllib3's `SSLError`.
//!
//! The order of effects is Python's: validation and reserved-key log
//! records first, then the configuration check (only when something is to
//! be written), then the timeout and URL checks of the request, then the
//! request; the `metrics_write_rejected` warning only after a successful
//! write (or instead of it when every point was rejected).

use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use graphyard_core::line_protocol::{PreparedWrite, prepare_write};
use graphyard_core::logline::{LogEvent, LogLevel};
use graphyard_core::model::MetricPoint;
use graphyard_core::pycompat::{PyError, py_float_repr, py_str_repr};
use graphyard_core::settings::Settings;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::header::{ACCEPT, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use reqwest::{StatusCode, Url};

use crate::config::{InfluxClientConfig, InfluxConfigurationError, RequestTimeout};

/// The `User-Agent` of `influxdb-client` 1.50.0, kept for a zero-change
/// cutover like the collectors' `python-httpx/0.28.1` (PLAN.md 6.1).
pub const USER_AGENT_VALUE: &str = "influxdb-client-python/1.50.0";

/// The write path of the v2 API.
pub const WRITE_PATH: &str = "/api/v2/write";

/// `urllib.parse.quote_plus`: everything but ASCII letters, digits and
/// `_.-~` is percent-encoded (UTF-8), the space becomes `+`.
const QUOTE_PLUS: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'_')
    .remove(b'.')
    .remove(b'-')
    .remove(b'~')
    .remove(b' ');

fn quote_plus(text: &str) -> String {
    utf8_percent_encode(text, QUOTE_PLUS)
        .to_string()
        .replace(' ', "+")
}

/// A failed `write_points`: what Python raises, by class.
#[derive(Debug)]
pub enum WriteError {
    /// `InfluxConfigurationError` of `_build_client` (P1 text).
    Configuration(InfluxConfigurationError),
    /// `ValueError`/`OverflowError` of the request timeout (`urllib3`,
    /// CPython).
    Timeout(PyError),
    /// The client's `ApiException` for a non-2xx answer, or with status 0
    /// for a TLS failure (P2 text).
    Api { status: u16, message: String },
    /// A urllib3 exception before or during the exchange (P2 text):
    /// `URLSchemeUnknown`, `LocationParseError`, `NewConnectionError`,
    /// `NameResolutionError`, `ConnectTimeoutError`, `ReadTimeoutError`,
    /// `ProtocolError`, `UnicodeDecodeError` for a body that is not UTF-8,
    /// or `ValueError` for a header value http.client rejects.
    Transport {
        class: &'static str,
        message: String,
    },
}

impl WriteError {
    /// `type(err).__name__` of the Python exception.
    pub fn python_class(&self) -> &'static str {
        match self {
            Self::Configuration(_) => InfluxConfigurationError::PYTHON_CLASS,
            Self::Timeout(err) => err.python_class(),
            Self::Api { .. } => "ApiException",
            Self::Transport { class, .. } => class,
        }
    }
}

impl fmt::Display for WriteError {
    /// `str(err)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(err) => err.fmt(f),
            Self::Timeout(err) => err.fmt(f),
            Self::Api { message, .. } | Self::Transport { message, .. } => f.write_str(message),
        }
    }
}

impl std::error::Error for WriteError {}

impl From<InfluxConfigurationError> for WriteError {
    fn from(err: InfluxConfigurationError) -> Self {
        Self::Configuration(err)
    }
}

/// Hand a record of the pure code to the `log` facade.
fn emit(event: &LogEvent) {
    match event.level {
        LogLevel::Info => log::info!("{}", event.message),
        LogLevel::Warning => log::warn!("{}", event.message),
        LogLevel::Error => log::error!("{}", event.message),
    }
}

/// The Python client warns once per process (`warnings.warn`,
/// `SyntaxWarning`, default filter) about a measurement starting with `#`.
static HASH_MEASUREMENT_WARNED: AtomicBool = AtomicBool::new(false);

fn warn_hash_measurement(measurement: &str) {
    if measurement.starts_with('#') && !HASH_MEASUREMENT_WARNED.swap(true, Ordering::Relaxed) {
        log::warn!(
            "SyntaxWarning: The measurement name '{}' start with '#'. The output Line protocol will be interpret as a comment by InfluxDB.",
            graphyard_core::logline::escape_log_value(measurement)
        );
    }
}

/// Build an HTTP client: HTTP/1.1, no redirects, no retries, no proxy, no
/// idle connections, TLS with the system trust store (D-12); `timeout` is
/// the connect timeout and the per-read inactivity timeout.
fn build_client(timeout: Option<Duration>) -> Result<reqwest::Client, reqwest::Error> {
    // reqwest is built without a provider of its own and panics without one.
    crate::install_crypto_provider();
    let mut builder = reqwest::Client::builder()
        .http1_only()
        .http1_title_case_headers()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .pool_max_idle_per_host(0);
    if let Some(timeout) = timeout {
        builder = builder.connect_timeout(timeout).read_timeout(timeout);
    }
    builder.build()
}

/// The InfluxDB write client. One per process; every write uses a new
/// connection, like the Python client that is closed after each call.
/// reqwest sets read timeouts per client, so the client is built for the
/// timeout in use and kept until the timeout changes.
#[derive(Debug)]
pub struct InfluxWriter {
    clients: Mutex<Option<(Option<Duration>, reqwest::Client)>>,
}

impl InfluxWriter {
    /// A writer with a client for the default timeout (10 s); a write with
    /// another timeout builds its own client.
    ///
    /// # Errors
    /// When reqwest cannot build the client (TLS setup).
    pub fn new() -> Result<Self, reqwest::Error> {
        let timeout = Some(Duration::from_secs(10));
        let client = build_client(timeout)?;
        Ok(Self {
            clients: Mutex::new(Some((timeout, client))),
        })
    }

    /// The client for `timeout`, built when it differs from the cached one.
    fn client(&self, timeout: Option<Duration>) -> Result<reqwest::Client, reqwest::Error> {
        let mut cached = self
            .clients
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some((cached_timeout, client)) = cached.as_ref()
            && *cached_timeout == timeout
        {
            return Ok(client.clone());
        }
        let client = build_client(timeout)?;
        *cached = Some((timeout, client.clone()));
        Ok(client)
    }

    /// `write_points(points)`: the number of points written.
    ///
    /// Logs the validation and reserved-key records, then writes the valid
    /// points; `Ok(0)` without a request when there is nothing valid.
    ///
    /// # Errors
    /// See [`WriteError`]; the points were then not written (as far as the
    /// client knows).
    pub async fn write_points(
        &self,
        settings: &Settings,
        points: &[MetricPoint],
    ) -> Result<u64, WriteError> {
        if points.is_empty() {
            return Ok(0);
        }
        let prepared = prepare_write(&settings.influx_measurement, points);
        for event in &prepared.events {
            emit(event);
        }
        if prepared.is_empty() {
            if let Some(event) = prepared.rejected_event() {
                emit(&event);
            }
            return Ok(0);
        }
        let config = InfluxClientConfig::from_settings(settings)?;
        warn_hash_measurement(&settings.influx_measurement);
        self.post(&config, settings, &prepared).await?;
        if let Some(event) = prepared.rejected_event() {
            emit(&event);
        }
        Ok(prepared.written() as u64)
    }

    /// `write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=…)` for
    /// a prepared, non-empty batch.
    async fn post(
        &self,
        config: &InfluxClientConfig,
        settings: &Settings,
        prepared: &PreparedWrite,
    ) -> Result<(), WriteError> {
        let timeout = config.request_timeout().map_err(WriteError::Timeout)?;
        let url = write_url(&config.host, &settings.influx_org, &settings.influx_bucket)?;
        let target = Target::of(&url);
        let client = self
            .client(timeout.map(|timeout| timeout.duration))
            .map_err(|err| WriteError::Transport {
                class: "ValueError",
                message: format!("Invalid client configuration: {}", root_cause(&err)),
            })?;
        let request = client
            .post(url.clone())
            .header(ACCEPT_ENCODING, "identity")
            .header(CONTENT_TYPE, "text/plain")
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, format!("Token {}", config.token))
            .header(USER_AGENT, USER_AGENT_VALUE)
            .body(prepared.body());
        let response = request
            .send()
            .await
            .map_err(|err| transport_error(&err, &target, timeout))?;
        let status = response.status();
        let headers: Vec<String> = response
            .headers()
            .iter()
            .map(|(name, value)| {
                format!(
                    "{}: {}",
                    py_str_repr(name.as_str()),
                    py_str_repr(&String::from_utf8_lossy(value.as_bytes()))
                )
            })
            .collect();
        // `preload_content=True`: the whole body is read, then decoded.
        let body = response
            .bytes()
            .await
            .map_err(|err| transport_error(&err, &target, timeout))?;
        let body = String::from_utf8(body.to_vec()).map_err(|err| WriteError::Transport {
            class: "UnicodeDecodeError",
            message: format!("'utf-8' codec can't decode the response body: {err}"),
        })?;
        if status.is_success() {
            return Ok(());
        }
        Err(WriteError::Api {
            status: status.as_u16(),
            message: api_exception_text(status, &headers, &body),
        })
    }
}

/// `str(ApiException)` of the Python client: status, reason, headers (as
/// urllib3's `HTTPHeaderDict` repr; header names arrive lower-cased here)
/// and body.
fn api_exception_text(status: StatusCode, headers: &[String], body: &str) -> String {
    let mut text = format!(
        "({})\nReason: {}\n",
        status.as_u16(),
        status.canonical_reason().unwrap_or("")
    );
    if !headers.is_empty() {
        text.push_str(&format!(
            "HTTP response headers: HTTPHeaderDict({{{}}})\n",
            headers.join(", ")
        ));
    }
    if !body.is_empty() {
        text.push_str(&format!("HTTP response body: {body}\n"));
    }
    text
}

/// The request URL: `conf.host + "/api/v2/write"` and the urlencoded query,
/// parsed like urllib3 (`parse_url`): a URL without a scheme is `http`,
/// schemes other than `http`/`https` are refused.
///
/// # Errors
/// `URLSchemeUnknown` or `LocationParseError` (P2 texts).
pub fn write_url(host: &str, org: &str, bucket: &str) -> Result<Url, WriteError> {
    let text = format!(
        "{host}{WRITE_PATH}?org={}&bucket={}&precision=ns",
        quote_plus(org),
        quote_plus(bucket)
    );
    // urllib3 `_SCHEME_RE = ^(?:[a-zA-Z][a-zA-Z0-9+-]*:|/)`; anything else
    // is read as `//<text>`, a host without a scheme (http).
    let scheme_end = text
        .char_indices()
        .take_while(|(index, c)| {
            if *index == 0 {
                c.is_ascii_alphabetic()
            } else {
                c.is_ascii_alphanumeric() || *c == '+' || *c == '-'
            }
        })
        .count();
    let has_scheme = scheme_end > 0 && text[scheme_end..].starts_with(':');
    let text = if has_scheme || text.starts_with('/') {
        text
    } else {
        format!("http://{text}")
    };
    if has_scheme {
        let scheme = text[..scheme_end].to_ascii_lowercase();
        if scheme != "http" && scheme != "https" {
            return Err(WriteError::Transport {
                class: "URLSchemeUnknown",
                message: format!("Not supported URL scheme {scheme}"),
            });
        }
    }
    let url = Url::parse(&text).map_err(|err| WriteError::Transport {
        class: "LocationParseError",
        message: format!("Failed to parse: {text} ({err})"),
    })?;
    if url.host_str().is_none_or(str::is_empty) {
        return Err(WriteError::Transport {
            class: "LocationParseError",
            message: "No host specified.".to_owned(),
        });
    }
    Ok(url)
}

/// Host and port as urllib3 names them in its messages.
struct Target {
    https: bool,
    host: String,
    port: u16,
}

impl Target {
    fn of(url: &Url) -> Self {
        Self {
            https: url.scheme() == "https",
            host: url.host_str().unwrap_or_default().to_owned(),
            port: url.port_or_known_default().unwrap_or(80),
        }
    }

    fn connection(&self) -> String {
        format!(
            "{}(host={}, port={})",
            if self.https {
                "HTTPSConnection"
            } else {
                "HTTPConnection"
            },
            py_str_repr(&self.host),
            self.port
        )
    }

    fn pool(&self) -> String {
        format!(
            "{}(host={}, port={})",
            if self.https {
                "HTTPSConnectionPool"
            } else {
                "HTTPConnectionPool"
            },
            py_str_repr(&self.host),
            self.port
        )
    }
}

/// Every error of the chain, outermost first. `io::Error::source` skips
/// the error an `io::Error` wraps (its `get_ref`), so that one is visited
/// explicitly (hyper-rustls reports TLS failures that way).
fn chain<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> Vec<&'a (dyn std::error::Error + 'static)> {
    let mut errors = Vec::new();
    let mut pending = vec![err];
    while let Some(current) = pending.pop() {
        errors.push(current);
        if let Some(source) = current.source() {
            pending.push(source);
        }
        if let Some(inner) = current
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
        {
            pending.push(inner);
        }
    }
    errors
}

/// `[Errno N] <strerror>` of the first OS error in the chain.
fn os_error(err: &(dyn std::error::Error + 'static)) -> Option<String> {
    chain(err).into_iter().find_map(|current| {
        let code = current.downcast_ref::<std::io::Error>()?.raw_os_error()?;
        let text = std::io::Error::from_raw_os_error(code).to_string();
        let suffix = format!(" (os error {code})");
        let strerror = text.strip_suffix(&suffix).unwrap_or(&text);
        Some(format!("[Errno {code}] {strerror}"))
    })
}

/// The innermost message of the chain (hyper/reqwest wrap the cause).
fn root_cause(err: &(dyn std::error::Error + 'static)) -> String {
    chain(err)
        .last()
        .map_or_else(|| err.to_string(), ToString::to_string)
}

fn chain_has(err: &(dyn std::error::Error + 'static), text: &str) -> bool {
    chain(err).iter().any(|current| current.to_string() == text)
}

/// The text of a TLS failure, if the failure is one: a rustls error in the
/// chain (certificate, protocol), or the connection closing during the
/// handshake of an `https` connect, which tokio-rustls reports as an
/// `UnexpectedEof` I/O error without a rustls error (Python's ssl module:
/// `EOF occurred in violation of protocol`, an `SSLError` as well).
fn tls_error(err: &reqwest::Error, target: &Target) -> Option<String> {
    let errors = chain(err);
    if let Some(tls) = errors.iter().find_map(|current| {
        current
            .downcast_ref::<rustls::Error>()
            .map(ToString::to_string)
    }) {
        return Some(tls);
    }
    if !(target.https && err.is_connect()) {
        return None;
    }
    errors.into_iter().find_map(|current| {
        let io = current.downcast_ref::<std::io::Error>()?;
        (io.kind() == std::io::ErrorKind::UnexpectedEof && io.raw_os_error().is_none())
            .then(|| format!("EOF occurred in violation of protocol ({io})"))
    })
}

/// The urllib3 exception (class and P2 text) for a reqwest failure.
fn transport_error(
    err: &reqwest::Error,
    target: &Target,
    timeout: Option<RequestTimeout>,
) -> WriteError {
    let seconds = timeout.map_or_else(|| "None".to_owned(), |t| py_float_repr(t.seconds));
    let (class, message) = if err.is_builder() {
        (
            "ValueError",
            format!("Invalid request: {}", root_cause(err)),
        )
    } else if err.is_timeout() && err.is_connect() {
        (
            "ConnectTimeoutError",
            format!(
                "{}: Connection to {} timed out. (connect timeout={seconds})",
                target.connection(),
                target.host
            ),
        )
    } else if err.is_timeout() {
        (
            "ReadTimeoutError",
            format!(
                "{}: Read timed out. (read timeout={seconds})",
                target.pool()
            ),
        )
    } else if err.is_connect() && chain_has(err, "dns error") {
        (
            "NameResolutionError",
            format!(
                "{}: Failed to resolve {} ({})",
                target.connection(),
                py_str_repr(&target.host),
                os_error(err).unwrap_or_else(|| root_cause(err))
            ),
        )
    } else if let Some(tls) = tls_error(err, target) {
        // rest.py: `except urllib3.exceptions.SSLError as e: raise
        // ApiException(status=0, reason=f"{type(e).__name__}\n{e}")`.
        return WriteError::Api {
            status: 0,
            message: format!("(0)\nReason: SSLError\n{tls}\n"),
        };
    } else if err.is_connect() {
        (
            "NewConnectionError",
            format!(
                "{}: Failed to establish a new connection: {}",
                target.connection(),
                os_error(err).unwrap_or_else(|| root_cause(err))
            ),
        )
    } else {
        (
            "ProtocolError",
            format!(
                "('Connection aborted.', {})",
                os_error(err).unwrap_or_else(|| py_str_repr(&root_cause(err)))
            ),
        )
    };
    WriteError::Transport { class, message }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_plus_matches_urlencode() {
        assert_eq!(quote_plus("my org~*/\u{fc}+&"), "my+org~%2A%2F%C3%BC%2B%26");
        assert_eq!(quote_plus("b&c=d e%"), "b%26c%3Dd+e%25");
        assert_eq!(quote_plus("a_b.c-d"), "a_b.c-d");
        assert_eq!(quote_plus(""), "");
    }

    #[test]
    fn write_url_follows_urllib3_parsing() {
        let url = write_url("http://127.0.0.1:8086", "o", "b").unwrap();
        assert_eq!(
            url.as_str(),
            "http://127.0.0.1:8086/api/v2/write?org=o&bucket=b&precision=ns"
        );
        let url = write_url("127.0.0.1:8086", "o", "b").unwrap();
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.port(), Some(8086));
        let err = write_url("ftp://h", "o", "b").unwrap_err();
        assert_eq!(err.python_class(), "URLSchemeUnknown");
        assert_eq!(err.to_string(), "Not supported URL scheme ftp");
        let err = write_url("FTP://h", "o", "b").unwrap_err();
        assert_eq!(err.to_string(), "Not supported URL scheme ftp");
        assert_eq!(
            write_url("not a url", "o", "b").unwrap_err().python_class(),
            "LocationParseError"
        );
        assert_eq!(
            write_url("/relative", "o", "b").unwrap_err().python_class(),
            "LocationParseError"
        );
        assert_eq!(
            write_url("http://[::1", "o", "b")
                .unwrap_err()
                .python_class(),
            "LocationParseError"
        );
    }

    #[test]
    fn api_exception_text_has_the_python_layout() {
        let text = api_exception_text(
            StatusCode::BAD_REQUEST,
            &["'content-type': 'application/json'".to_owned()],
            "{\"code\":\"invalid\"}",
        );
        assert_eq!(
            text,
            "(400)\nReason: Bad Request\nHTTP response headers: HTTPHeaderDict({'content-type': 'application/json'})\nHTTP response body: {\"code\":\"invalid\"}\n"
        );
        assert_eq!(
            api_exception_text(StatusCode::UNAUTHORIZED, &[], ""),
            "(401)\nReason: Unauthorized\n"
        );
    }

    #[test]
    fn errors_report_python_classes() {
        let err = WriteError::from(InfluxConfigurationError("x".to_owned()));
        assert_eq!(err.python_class(), "InfluxConfigurationError");
        assert_eq!(err.to_string(), "x");
        let err = WriteError::Timeout(PyError::value_error("v"));
        assert_eq!(err.python_class(), "ValueError");
        let err = WriteError::Api {
            status: 500,
            message: "m".to_owned(),
        };
        assert_eq!(
            (err.python_class(), err.to_string().as_str()),
            ("ApiException", "m")
        );
    }
}
