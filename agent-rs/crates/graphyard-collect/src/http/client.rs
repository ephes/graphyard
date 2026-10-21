//! The httpx client as `services.py` configures it (PLAN.md 6.1), on
//! hyper's HTTP/1.1 connection API.
//!
//! One [`HttpClient`] per spec execution (`httpx.Client(...)`) with its own
//! cookie jar. Each request opens its own connection like httpcore
//! (`socket.create_connection`, then TLS), and the timeout bounds the
//! connect, the TLS handshake and every single socket read and write
//! separately ([`super::connect`]). No redirect is followed automatically:
//! each hop is one [`HttpClient::send`], which logs httpx's `HTTP Request:`
//! line and prepares the redirect request like
//! `Client.send(follow_redirects=False)`.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use cookie_store::{CookieStore, RawCookie};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use hyper_util::rt::TokioIo;
use num_bigint::BigInt;
use num_traits::ToPrimitive;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use tokio::task::JoinHandle;

use super::connect::{Stream, TimeoutIo, TlsConfigs, connect_tcp, start_tls, system_roots};
use super::decode::BodyDecoder;
use super::error::{ErrorKind, HttpError};
use super::resolve::{AbandonedLookups, Lookup, LookupScope, SystemLookup, resolve};
use super::status::{is_redirect_status, reason_phrase, status_error_message};
use super::url::HttpxUrl;

/// httpx's `User-Agent`, kept for a zero-change cutover (PLAN.md 6.1).
pub const USER_AGENT: &str = "python-httpx/0.28.1";
/// httpx's `Accept-Encoding` without brotli/zstandard installed.
pub const ACCEPT_ENCODING: &str = "gzip, deflate";
/// `httpx.Client().max_redirects`.
pub const MAX_REDIRECTS: u32 = 20;
/// `_PAGE_PROBE_CLOSE_JOIN_SECONDS`: how long a cancelled request's lookups
/// may keep running before they count as abandoned.
pub const CLOSE_JOIN: Duration = Duration::from_secs(1);
/// `_PAGE_PROBE_MAX_ABANDONED_WORKERS`.
pub const MAX_ABANDONED: usize = 4;

/// Process-wide state of the HTTP layer: the resolver, the trust store, the
/// abandoned-lookup set and the deadline limits. The agent creates one at
/// startup (so a CA update needs a restart, PLAN.md D-12); tests create
/// their own.
#[derive(Clone)]
pub struct HttpContext {
    pub(crate) lookup: Arc<dyn Lookup>,
    pub(crate) abandoned: Arc<AbandonedLookups>,
    pub(crate) tls: Arc<TlsConfigs>,
    extra_roots: Vec<CertificateDer<'static>>,
    pub(crate) close_join: Duration,
    pub(crate) max_abandoned: usize,
}

impl Default for HttpContext {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpContext {
    /// The production context: libc `getaddrinfo`, the system trust store,
    /// `CLOSE_JOIN` and `MAX_ABANDONED`.
    pub fn new() -> Self {
        Self {
            lookup: Arc::new(SystemLookup),
            abandoned: Arc::new(AbandonedLookups::default()),
            tls: Arc::new(TlsConfigs::new(system_roots())),
            extra_roots: Vec::new(),
            close_join: CLOSE_JOIN,
            max_abandoned: MAX_ABANDONED,
        }
    }

    /// Replace the resolver (tests: a stub that blocks or maps names).
    #[must_use]
    pub fn with_lookup(mut self, lookup: Arc<dyn Lookup>) -> Self {
        self.lookup = lookup;
        self
    }

    /// Trust an extra root certificate besides the system store (tests).
    #[must_use]
    pub fn with_extra_root(mut self, certificate: CertificateDer<'static>) -> Self {
        self.extra_roots.push(certificate);
        let mut roots: RootCertStore = system_roots();
        let (_added, _ignored) = roots.add_parsable_certificates(self.extra_roots.clone());
        self.tls = Arc::new(TlsConfigs::new(roots));
        self
    }

    /// Change the close wait and the abandoned cap (tests, like the Python
    /// test that patches `_PAGE_PROBE_CLOSE_JOIN_SECONDS` and
    /// `_PAGE_PROBE_MAX_ABANDONED_WORKERS`).
    #[must_use]
    pub fn with_limits(mut self, close_join: Duration, max_abandoned: usize) -> Self {
        self.close_join = close_join;
        self.max_abandoned = max_abandoned;
        self
    }

    /// `_live_abandoned_page_probe_workers()`.
    pub fn live_abandoned(&self) -> usize {
        self.abandoned.live()
    }
}

/// The `httpx.Client(...)` arguments the executors pass.
#[derive(Clone, Debug)]
pub struct ClientOptions {
    /// `timeout=`: seconds for connect, TLS and each read and write
    /// (validated by `spec_config`: finite, > 0, at most one day).
    pub timeout_seconds: f64,
    /// `verify=`: `false` disables certificate and host name checks.
    pub verify_tls: bool,
    /// `headers=`: client headers, overriding httpx's defaults by name.
    pub headers: Vec<(String, String)>,
    /// `auth=httpx.BasicAuth(username, password)`.
    pub basic_auth: Option<(String, String)>,
}

impl ClientOptions {
    pub fn new(timeout_seconds: f64, verify_tls: bool) -> Self {
        Self {
            timeout_seconds,
            verify_tls,
            headers: Vec::new(),
            basic_auth: None,
        }
    }

    #[must_use]
    pub fn header(mut self, name: &str, value: &str) -> Self {
        set_header(&mut self.headers, name, value);
        self
    }

    #[must_use]
    pub fn basic_auth(mut self, username: &str, password: &str) -> Self {
        self.basic_auth = Some((username.to_owned(), password.to_owned()));
        self
    }
}

/// Set a header, replacing one of the same name (case-insensitive).
fn set_header(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
    match headers
        .iter_mut()
        .find(|(existing, _)| existing.eq_ignore_ascii_case(name))
    {
        Some(entry) => entry.1 = value.to_owned(),
        None => headers.push((name.to_owned(), value.to_owned())),
    }
}

fn remove_header(headers: &mut Vec<(String, String)>, name: &str) {
    headers.retain(|(existing, _)| !existing.eq_ignore_ascii_case(name));
}

/// An HTTP method the collectors use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Get,
    Head,
    Post,
}

impl Method {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Head => "HEAD",
            Self::Post => "POST",
        }
    }
}

/// An outgoing request (`httpx.Request`); headers are the client's merged
/// with the request's own.
#[derive(Clone, Debug)]
pub struct Request {
    pub method: Method,
    pub url: HttpxUrl,
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
}

impl Request {
    /// The `Authorization` header, if any.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(existing, _)| existing.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// `Client._build_redirect_request(request, response)` for a redirect
    /// status with `location`.
    ///
    /// # Errors
    /// `RemoteProtocolError` (`Invalid URL in location header: …`) for an
    /// unparseable `Location`; `InvalidURL` when the joined URL is invalid.
    pub fn redirect(&self, status: u16, location: &str) -> Result<Self, HttpError> {
        let method = match (status, self.method) {
            (303 | 302, method) if method != Method::Head => Method::Get,
            (301, Method::Post) => Method::Get,
            (_, method) => method,
        };
        let url = self.redirect_url(location)?;
        let mut headers = self.headers.clone();
        if !same_origin(&url, &self.url) && !is_https_redirect(&self.url, &url) {
            remove_header(&mut headers, "Authorization");
        }
        let mut body = self.body.clone();
        if method != self.method && method == Method::Get {
            remove_header(&mut headers, "Content-Length");
            remove_header(&mut headers, "Transfer-Encoding");
            body = None;
        }
        remove_header(&mut headers, "Cookie");
        Ok(Self {
            method,
            url,
            headers,
            body,
        })
    }

    /// `Client._redirect_url`.
    fn redirect_url(&self, location: &str) -> Result<HttpxUrl, HttpError> {
        let mut url = HttpxUrl::parse(location).map_err(|err| {
            HttpError::new(
                ErrorKind::RemoteProtocolError,
                format!("Invalid URL in location header: {err}."),
            )
        })?;
        let invalid = |err: super::url::InvalidUrl| HttpError::new(ErrorKind::InvalidUrl, err.0);
        if !url.scheme().is_empty() && url.host().is_empty() {
            url = url.with_host(self.url.host()).map_err(invalid)?;
        }
        if !url.is_absolute() {
            url = self.url.join(&url).map_err(invalid)?;
        }
        if let Some(fragment) = self.url.fragment().filter(|fragment| !fragment.is_empty())
            && url.fragment().is_none_or(str::is_empty)
        {
            url = url.with_fragment(fragment);
        }
        Ok(url)
    }
}

/// `_same_origin`.
fn same_origin(url: &HttpxUrl, other: &HttpxUrl) -> bool {
    url.scheme() == other.scheme()
        && url.host() == other.host()
        && url.port_or_default() == other.port_or_default()
}

/// `_is_https_redirect`: `http://host[:80]` to `https://host[:443]`.
fn is_https_redirect(url: &HttpxUrl, location: &HttpxUrl) -> bool {
    url.host() == location.host()
        && url.scheme() == "http"
        && url.port_or_default() == Some(BigInt::from(80))
        && location.scheme() == "https"
        && location.port_or_default() == Some(BigInt::from(443))
}

/// A response whose body has not been read yet (`client.send(...,
/// stream=True)`). Dropping it closes the connection.
pub struct Response {
    body: Incoming,
    decoder: BodyDecoder,
    headers: HeaderMap,
    connection: JoinHandle<()>,
    pub status: u16,
    pub reason_phrase: String,
    pub http_version: &'static str,
    pub url: HttpxUrl,
    /// The redirect request httpx prepares for a redirect status with a
    /// `Location` header (`response.next_request`).
    pub next_request: Option<Request>,
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("url", &self.url.to_string())
            .finish_non_exhaustive()
    }
}

impl Drop for Response {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

impl Response {
    /// The response headers.
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// A header as httpx decodes it: the whole header block as ASCII, else
    /// UTF-8, else Latin-1 (`Headers.encoding`); repeated headers joined
    /// with `", "`.
    pub fn header(&self, name: &str) -> Option<String> {
        header_text(&self.headers, name)
    }

    /// `response.raise_for_status()`.
    ///
    /// # Errors
    /// `HTTPStatusError` with httpx's text for a non-2xx status.
    pub fn raise_for_status(&self) -> Result<(), HttpError> {
        let location = if is_redirect_status(self.status) {
            self.header("location")
        } else {
            None
        };
        match status_error_message(
            self.status,
            &self.reason_phrase,
            &self.url.to_string(),
            location.as_deref(),
        ) {
            Some(message) => Err(HttpError::new(ErrorKind::HttpStatusError, message)),
            None => Ok(()),
        }
    }

    /// The next chunk of the decoded body (`iter_bytes()`), `None` at the
    /// end.
    ///
    /// # Errors
    /// The read, timeout or decoding error.
    pub async fn chunk(&mut self) -> Result<Option<Bytes>, HttpError> {
        while let Some(frame) = self.body.frame().await {
            let frame = frame.map_err(|err| HttpError::transport(&err, true))?;
            if let Ok(data) = frame.into_data() {
                let decoded = self.decoder.decode(&data)?;
                if !decoded.is_empty() {
                    return Ok(Some(Bytes::from(decoded)));
                }
            }
        }
        Ok(None)
    }

    /// The whole decoded body (`response.read()`), without a cap.
    ///
    /// # Errors
    /// The read, timeout or decoding error.
    pub async fn read_to_end(&mut self) -> Result<Vec<u8>, HttpError> {
        let mut body = Vec::new();
        while let Some(chunk) = self.chunk().await? {
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

/// `Headers.__getitem__` with httpx's header decoding.
fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    let values: Vec<&HeaderValue> = headers.get_all(name).iter().collect();
    if values.is_empty() {
        return None;
    }
    let all = |check: fn(&[u8]) -> bool| headers.values().all(|value| check(value.as_bytes()));
    let utf8 = all(<[u8]>::is_ascii) || all(|bytes| std::str::from_utf8(bytes).is_ok());
    let decode = |value: &HeaderValue| -> String {
        let bytes = value.as_bytes();
        if utf8 {
            String::from_utf8_lossy(bytes).into_owned()
        } else {
            bytes.iter().map(|byte| char::from(*byte)).collect()
        }
    };
    Some(
        values
            .into_iter()
            .map(decode)
            .collect::<Vec<_>>()
            .join(", "),
    )
}

/// `httpx.Client` for one spec execution.
pub struct HttpClient {
    timeout: Duration,
    verify_tls: bool,
    headers: Vec<(String, String)>,
    /// The `Authorization` value of `auth=httpx.BasicAuth(...)`.
    auth: Option<String>,
    jar: Mutex<CookieStore>,
    pub(crate) context: HttpContext,
    pub(crate) scope: Arc<LookupScope>,
}

impl HttpClient {
    /// `httpx.Client(timeout=…, verify=…, headers=…, auth=…)`.
    pub fn new(context: &HttpContext, options: &ClientOptions) -> Self {
        // httpx's default client headers, then `headers=`.
        let mut headers = vec![
            ("Accept".to_owned(), "*/*".to_owned()),
            ("Accept-Encoding".to_owned(), ACCEPT_ENCODING.to_owned()),
            ("Connection".to_owned(), "keep-alive".to_owned()),
            ("User-Agent".to_owned(), USER_AGENT.to_owned()),
        ];
        for (name, value) in &options.headers {
            set_header(&mut headers, name, value);
        }
        let auth = options
            .basic_auth
            .as_ref()
            .map(|(username, password)| basic_authorization(username, password));
        Self {
            timeout: Duration::from_secs_f64(options.timeout_seconds),
            verify_tls: options.verify_tls,
            headers,
            auth,
            jar: Mutex::new(CookieStore::default()),
            context: context.clone(),
            scope: Arc::new(LookupScope::default()),
        }
    }

    /// `client.build_request(method, url, headers=…, content=…)`.
    ///
    /// # Errors
    /// `InvalidURL` for a URL httpx rejects.
    pub fn build_request(
        &self,
        method: Method,
        url: &str,
        headers: &[(&str, &str)],
        body: Option<Vec<u8>>,
    ) -> Result<Request, HttpError> {
        let url =
            HttpxUrl::parse(url).map_err(|err| HttpError::new(ErrorKind::InvalidUrl, err.0))?;
        let mut merged = self.headers.clone();
        for (name, value) in headers {
            set_header(&mut merged, name, value);
        }
        Ok(Request {
            method,
            url,
            headers: merged,
            body,
        })
    }

    /// `client.send(request, stream=True, follow_redirects=False)`: one
    /// request on a new connection; logs `HTTP Request: <METHOD> <url>
    /// "<version> <code> <reason>"` once the response headers arrived and
    /// prepares `next_request` for a redirect.
    ///
    /// # Errors
    /// `UnsupportedProtocol` for a URL without scheme or host or with a
    /// scheme other than http(s)/ws(s); connect, TLS, read and timeout
    /// errors; the redirect errors of [`Request::redirect`].
    pub async fn send(&self, request: &Request) -> Result<Response, HttpError> {
        let tls = check_scheme(&request.url)?;
        let request = &self.authenticated(request);
        let url = &request.url;
        let outgoing = self.hyper_request(request)?;
        let stream = self.connect(url, tls).await?;
        let io = TokioIo::new(TimeoutIo::new(stream, self.timeout));
        let (mut sender, connection) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|err| HttpError::transport(&err, false))?;
        let connection = tokio::spawn(async move {
            let _ = connection.await;
        });
        let response = match sender.send_request(outgoing).await {
            Ok(response) => response,
            Err(err) => {
                connection.abort();
                return Err(HttpError::transport(&err, false));
            }
        };
        let (parts, body) = response.into_parts();
        self.store_cookies(url, &parts.headers);
        let status = parts.status.as_u16();
        let server_reason = parts
            .extensions
            .get::<hyper::ext::ReasonPhrase>()
            .map_or_else(
                || parts.status.canonical_reason().unwrap_or("").as_bytes(),
                |reason| reason.as_bytes(),
            );
        let reason = reason_phrase(server_reason);
        let http_version = match parts.version {
            hyper::Version::HTTP_09 => "HTTP/0.9",
            hyper::Version::HTTP_10 => "HTTP/1.0",
            _ => "HTTP/1.1",
        };
        log::info!(
            "HTTP Request: {} {} \"{} {} {}\"",
            request.method.as_str(),
            escape_log(&url.to_string()),
            http_version,
            status,
            escape_log(&reason),
        );
        let encodings = header_text(&parts.headers, "content-encoding").unwrap_or_default();
        let mut response = Response {
            body,
            decoder: BodyDecoder::new(encodings.split(',')),
            headers: parts.headers,
            connection,
            status,
            reason_phrase: reason,
            http_version,
            url: url.clone(),
            next_request: None,
        };
        if is_redirect_status(status)
            && let Some(location) = response.header("location")
        {
            response.next_request = Some(request.redirect(status, &location)?);
        }
        Ok(response)
    }

    /// The request as httpx's auth flow sends it (`_build_request_auth`):
    /// the client's `auth=` sets `Authorization` on every request, else
    /// credentials in the URL become basic auth.
    pub(crate) fn authenticated(&self, request: &Request) -> Request {
        let mut request = request.clone();
        let header = self.auth.clone().or_else(|| {
            let (username, password) = request.url.credentials();
            (!username.is_empty() || !password.is_empty())
                .then(|| basic_authorization(&username, &password))
        });
        if let Some(header) = header {
            set_header(&mut request.headers, "Authorization", &header);
        }
        request
    }

    /// `socket.create_connection((host, port), timeout)` and, for https/wss,
    /// `start_tls`. The lookup is not bounded by the timeout (httpcore's
    /// `getaddrinfo` is not); the total deadline bounds it.
    async fn connect(&self, url: &HttpxUrl, tls: bool) -> Result<Stream, HttpError> {
        let port = url
            .port()
            .cloned()
            .unwrap_or_else(|| BigInt::from(if tls { 443 } else { 80 }));
        let host = url.host();
        let small_port = port.to_u16();
        let service = small_port.is_none().then(|| port.to_string());
        let addresses = resolve(&self.context.lookup, &self.scope, host, service)
            .await
            .map_err(|err| HttpError::new(ErrorKind::ConnectError, err.0))?;
        // A port beyond 65535 that the resolver accepted cannot be
        // connected to (`getsockaddrarg`, an `OverflowError` in Python).
        let port = small_port.ok_or_else(|| {
            HttpError::new(
                ErrorKind::ConnectError,
                "getsockaddrarg: port must be 0-65535.",
            )
        })?;
        let addresses: Vec<std::net::SocketAddr> = addresses
            .into_iter()
            .map(|ip| std::net::SocketAddr::new(ip, port))
            .collect();
        let tcp = connect_tcp(&addresses, self.timeout).await?;
        if !tls {
            return Ok(Stream::Plain(tcp));
        }
        let config = if self.verify_tls {
            Arc::clone(&self.context.tls.verify)
        } else {
            Arc::clone(&self.context.tls.no_verify)
        };
        start_tls(tcp, host, config, self.timeout).await
    }

    /// The request on the wire: httpx's request target (`raw_path`), `Host`
    /// from the URL's netloc, the merged headers and the jar's cookies.
    pub(crate) fn hyper_request(
        &self,
        request: &Request,
    ) -> Result<hyper::Request<Full<Bytes>>, HttpError> {
        let local = |err: &dyn std::fmt::Display| {
            HttpError::new(ErrorKind::LocalProtocolError, err.to_string())
        };
        let body = Full::new(Bytes::from(request.body.clone().unwrap_or_default()));
        let mut outgoing = hyper::Request::builder()
            .method(request.method.as_str())
            .uri(request.url.target())
            .body(body)
            .map_err(|err| local(&err))?;
        let headers = outgoing.headers_mut();
        let host = HeaderValue::from_str(&request.url.netloc()).map_err(|err| local(&err))?;
        headers.insert(hyper::header::HOST, host);
        for (name, value) in &request.headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(|err| local(&err))?;
            let value = HeaderValue::from_str(value).map_err(|err| local(&err))?;
            headers.insert(name, value);
        }
        if let Some(cookie) = self.cookie_header(&request.url) {
            headers.insert(hyper::header::COOKIE, cookie);
        }
        Ok(outgoing)
    }

    fn cookie_header(&self, url: &HttpxUrl) -> Option<HeaderValue> {
        let url = cookie_url(url)?;
        let jar = self.jar.lock().unwrap_or_else(PoisonError::into_inner);
        let mut cookies = jar.matches(&url);
        if cookies.is_empty() {
            return None;
        }
        // http.cookiejar: longer paths first, otherwise insertion order.
        cookies.sort_by_key(|cookie| std::cmp::Reverse(String::from(&cookie.path).len()));
        let text = cookies
            .iter()
            .map(|cookie| format!("{}={}", cookie.name(), cookie.value()))
            .collect::<Vec<_>>()
            .join("; ");
        HeaderValue::from_str(&text).ok()
    }

    fn store_cookies(&self, url: &HttpxUrl, headers: &HeaderMap) {
        let Some(url) = cookie_url(url) else {
            return;
        };
        let cookies: Vec<RawCookie<'static>> = headers
            .get_all(hyper::header::SET_COOKIE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .filter_map(|text| RawCookie::parse(text.to_owned()).ok())
            .collect();
        if !cookies.is_empty() {
            let mut jar = self.jar.lock().unwrap_or_else(PoisonError::into_inner);
            jar.store_response_cookies(cookies.into_iter(), &url);
        }
    }
}

/// `httpx.BasicAuth(username, password)`: the `Authorization` value.
fn basic_authorization(username: &str, password: &str) -> String {
    format!("Basic {}", BASE64.encode(format!("{username}:{password}")))
}

/// The URL the cookie jar matches against (ws/wss as http/https).
fn cookie_url(url: &HttpxUrl) -> Option<url::Url> {
    let scheme = match url.scheme() {
        "ws" => "http",
        "wss" => "https",
        other => other,
    };
    let target = url.target();
    let path = target.split('?').next().unwrap_or("/");
    url::Url::parse(&format!("{scheme}://{}{path}", url.netloc())).ok()
}

/// The checks httpx makes before connecting: `Client._merge_url` turns a
/// URL without scheme or host into a bare path, which httpcore rejects like
/// a scheme other than http(s)/ws(s). Returns whether the URL uses TLS.
///
/// # Errors
/// `UnsupportedProtocol`.
pub(crate) fn check_scheme(url: &HttpxUrl) -> Result<bool, HttpError> {
    if !url.is_absolute() {
        return Err(HttpError::new(
            ErrorKind::UnsupportedProtocol,
            "Request URL is missing an 'http://' or 'https://' protocol.",
        ));
    }
    match url.scheme() {
        "http" | "ws" => Ok(false),
        "https" | "wss" => Ok(true),
        other => Err(HttpError::new(
            ErrorKind::UnsupportedProtocol,
            format!("Request URL has an unsupported protocol '{other}://'."),
        )),
    }
}

/// Escape `\n` and `\r` in a value interpolated into a log line (D-10).
pub(crate) fn escape_log(value: &str) -> String {
    value.replace('\n', "\\n").replace('\r', "\\r")
}
