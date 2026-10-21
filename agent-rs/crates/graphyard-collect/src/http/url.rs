//! httpx's URL model (PLAN.md 6.1, D-14).
//!
//! The collectors log, compare and report URLs exactly as httpx 0.28.1 does:
//! the `HTTP Request:` log line, the `raise_for_status` text and the
//! `InvalidURL` / `UnsupportedProtocol` classification all use
//! `str(httpx.URL(...))`. [`HttpxUrl`] ports `httpx._urlparse.urlparse`
//! (validation and normalisation of a URL string) and the redirect join
//! (`urllib.parse.urljoin`, which `httpx.URL.join` calls), pinned by the
//! `urls`, `long_urls` and `redirects` cases of `fixtures/http_errors.json`.
//!
//! The request goes out exactly as httpcore sends it: `getaddrinfo` on
//! [`HttpxUrl::host`], [`HttpxUrl::target`] as the request target and
//! [`HttpxUrl::netloc`] as `Host`. Internationalised host names are encoded
//! with UTS 46 (the `idna` crate) where httpx uses IDNA 2008 (D-14).

use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr};

use num_bigint::BigInt;

use graphyard_core::pycompat::{py_int, py_str_repr};

/// `httpx._urlparse.MAX_URL_LENGTH`.
pub const MAX_URL_LENGTH: usize = 65536;

const UNRESERVED: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
const SUB_DELIMS: &str = "!$&'()*+,;=";
const WHATWG_SAFE: &str = "\"`{}%|\\";

/// Printable ASCII except the listed bytes (the `*_SAFE` sets of httpx).
const fn safe_set(excluded: &[u8]) -> [bool; 128] {
    let mut set = [false; 128];
    let mut byte = 0x20;
    while byte < 0x7f {
        let mut allowed = true;
        let mut index = 0;
        while index < excluded.len() {
            if excluded[index] == byte as u8 {
                allowed = false;
            }
            index += 1;
        }
        set[byte] = allowed;
        byte += 1;
    }
    set
}

const FRAG_SAFE: [bool; 128] = safe_set(&[0x20, 0x22, 0x3C, 0x3E, 0x60]);
const QUERY_SAFE: [bool; 128] = safe_set(&[0x20, 0x22, 0x23, 0x3C, 0x3E]);
const PATH_SAFE: [bool; 128] = safe_set(&[0x20, 0x22, 0x23, 0x3C, 0x3E, 0x3F, 0x60, 0x7B, 0x7D]);
const USERINFO_SAFE: [bool; 128] = safe_set(&[
    0x20, 0x22, 0x23, 0x3C, 0x3E, 0x3F, 0x60, 0x7B, 0x7D, 0x2F, 0x3B, 0x3D, 0x40, 0x5B, 0x5C, 0x5D,
    0x5E, 0x7C,
]);

/// `httpx.InvalidURL`: the URL text is not a valid URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidUrl(pub String);

impl fmt::Display for InvalidUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A URL in httpx's normalised form (`httpx._urlparse.ParseResult`): every
/// component is ASCII and percent-encoded as httpx encodes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpxUrl {
    scheme: String,
    userinfo: String,
    host: String,
    port: Option<BigInt>,
    path: String,
    query: Option<String>,
    fragment: Option<String>,
}

/// The raw components `urlparse` splits a URL string into.
struct RawParts<'a> {
    scheme: &'a str,
    userinfo: &'a str,
    host: &'a str,
    port: Option<&'a str>,
    path: &'a str,
    query: Option<&'a str>,
    fragment: Option<&'a str>,
}

impl HttpxUrl {
    /// `httpx.URL(text)`.
    ///
    /// # Errors
    /// The `InvalidURL` message httpx raises.
    pub fn parse(text: &str) -> Result<Self, InvalidUrl> {
        if text.chars().count() > MAX_URL_LENGTH {
            return Err(InvalidUrl("URL too long".to_owned()));
        }
        if let Some((index, ch)) = text
            .chars()
            .enumerate()
            .find(|(_, ch)| is_ascii_control(*ch))
        {
            return Err(InvalidUrl(format!(
                "Invalid non-printable ASCII character in URL, {} at position {index}.",
                py_str_repr(&ch.to_string())
            )));
        }
        Self::build(&split(text))
    }

    /// Normalise and validate the components (the second half of
    /// `urlparse`).
    fn build(raw: &RawParts<'_>) -> Result<Self, InvalidUrl> {
        let scheme = raw.scheme.to_ascii_lowercase();
        let userinfo = quote(raw.userinfo, &USERINFO_SAFE);
        let host = encode_host(raw.host)?;
        // `normalize_port(port, scheme)` compares with the default port of
        // the scheme as written, so `HTTPS://host:443` keeps its port.
        let port = normalize_port(raw.port, raw.scheme)?;
        let has_scheme = !scheme.is_empty();
        let has_authority = !userinfo.is_empty() || !host.is_empty() || port.is_some();
        validate_path(raw.path, has_scheme, has_authority)?;
        let path = if has_scheme || has_authority {
            normalize_path(raw.path)
        } else {
            raw.path.to_owned()
        };
        Ok(Self {
            scheme,
            userinfo,
            host,
            port,
            path: quote(&path, &PATH_SAFE),
            query: raw.query.map(|query| quote(query, &QUERY_SAFE)),
            fragment: raw.fragment.map(|fragment| quote(fragment, &FRAG_SAFE)),
        })
    }

    /// The scheme, lower-cased.
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// The host, IDNA-encoded (`URL.raw_host`), without IPv6 brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The explicit port, `None` for the scheme's default.
    pub fn port(&self) -> Option<&BigInt> {
        self.port.as_ref()
    }

    /// `(URL.username, URL.password)`: the userinfo split at the first ':'
    /// and percent-decoded.
    pub fn credentials(&self) -> (String, String) {
        let (username, password) = self
            .userinfo
            .split_once(':')
            .unwrap_or((self.userinfo.as_str(), ""));
        let unquote = |text: &str| {
            percent_encoding::percent_decode_str(text)
                .decode_utf8_lossy()
                .into_owned()
        };
        (unquote(username), unquote(password))
    }

    /// The fragment as httpx stores it (percent-encoded), if any.
    pub fn fragment(&self) -> Option<&str> {
        self.fragment.as_deref()
    }

    /// `URL.is_absolute_url`: a scheme and a host.
    pub fn is_absolute(&self) -> bool {
        !self.scheme.is_empty() && !self.host.is_empty()
    }

    /// The request target httpx sends (`URL.raw_path`).
    pub fn target(&self) -> String {
        let path = if self.path.is_empty() {
            "/"
        } else {
            &self.path
        };
        match &self.query {
            Some(query) => format!("{path}?{query}"),
            None => path.to_owned(),
        }
    }

    /// The port, or the default of `http`/`https` (`_port_or_default`).
    pub(crate) fn port_or_default(&self) -> Option<BigInt> {
        self.port.clone().or_else(|| match self.scheme.as_str() {
            "http" => Some(BigInt::from(80)),
            "https" => Some(BigInt::from(443)),
            _ => None,
        })
    }

    fn authority(&self) -> String {
        let mut authority = String::new();
        if !self.userinfo.is_empty() {
            authority.push_str(&self.userinfo);
            authority.push('@');
        }
        authority.push_str(&self.netloc());
        authority
    }

    /// `host[:port]`, IPv6 in brackets (`URL.netloc`).
    pub fn netloc(&self) -> String {
        let mut netloc = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        if let Some(port) = &self.port {
            netloc.push(':');
            netloc.push_str(&port.to_string());
        }
        netloc
    }

    /// `URL.copy_with(host=host)`: the components rebuilt with a new host,
    /// validated again (a host-less absolute `Location` gets the request's
    /// host this way).
    ///
    /// # Errors
    /// The `InvalidURL` of the rebuilt URL.
    pub(crate) fn with_host(&self, host: &str) -> Result<Self, InvalidUrl> {
        let host = if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
            format!("[{host}]")
        } else {
            host.to_owned()
        };
        let port = self.port.as_ref().map(ToString::to_string);
        Self::build(&RawParts {
            scheme: &self.scheme,
            userinfo: &self.userinfo,
            host: &host,
            port: port.as_deref(),
            path: &self.path,
            query: self.query.as_deref(),
            fragment: self.fragment.as_deref(),
        })
    }

    /// `URL.copy_with(fragment=fragment)`.
    pub(crate) fn with_fragment(&self, fragment: &str) -> Self {
        Self {
            fragment: Some(quote(fragment, &FRAG_SAFE)),
            ..self.clone()
        }
    }

    /// `URL.join(other)`: `urllib.parse.urljoin(str(self), str(other))`,
    /// parsed again.
    ///
    /// # Errors
    /// The `InvalidURL` of the joined URL.
    pub(crate) fn join(&self, other: &Self) -> Result<Self, InvalidUrl> {
        Self::parse(&urljoin(&self.to_string(), &other.to_string()))
    }
}

impl fmt::Display for HttpxUrl {
    /// `str(httpx.URL)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.scheme.is_empty() {
            write!(f, "{}:", self.scheme)?;
        }
        let authority = self.authority();
        if !authority.is_empty() {
            write!(f, "//{authority}")?;
        }
        f.write_str(&self.path)?;
        if let Some(query) = &self.query {
            write!(f, "?{query}")?;
        }
        if let Some(fragment) = &self.fragment {
            write!(f, "#{fragment}")?;
        }
        Ok(())
    }
}

/// `char.isascii() and not char.isprintable()`.
fn is_ascii_control(ch: char) -> bool {
    ch.is_ascii_control()
}

fn is_scheme_char(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-')
}

/// `URL_REGEX` and `AUTHORITY_REGEX` of `httpx._urlparse`.
fn split(text: &str) -> RawParts<'_> {
    let bytes = text.as_bytes();
    // `(?:(?P<scheme>([a-zA-Z][a-zA-Z0-9+.-]*)?):)?`: the leading run of
    // scheme characters when a ':' follows it; an empty scheme when the
    // text starts with ':'.
    let run = if bytes.first().is_some_and(u8::is_ascii_alphabetic) {
        bytes
            .iter()
            .take_while(|byte| is_scheme_char(**byte))
            .count()
    } else {
        0
    };
    let (scheme, rest) = if bytes.get(run) == Some(&b':') {
        (&text[..run], &text[run + 1..])
    } else {
        ("", text)
    };
    let (authority, rest) = match rest.strip_prefix("//") {
        Some(after) => {
            let end = after.find(['/', '?', '#']).unwrap_or(after.len());
            (Some(&after[..end]), &after[end..])
        }
        None => (None, rest),
    };
    let (rest, fragment) = match rest.find('#') {
        Some(index) => (&rest[..index], Some(&rest[index + 1..])),
        None => (rest, None),
    };
    let (path, query) = match rest.find('?') {
        Some(index) => (&rest[..index], Some(&rest[index + 1..])),
        None => (rest, None),
    };
    let authority = authority.unwrap_or("");
    // `(?:(?P<userinfo>.*)@)?`: up to the last '@'.
    let (userinfo, hostport) = match authority.rfind('@') {
        Some(index) => (&authority[..index], &authority[index + 1..]),
        None => ("", authority),
    };
    // `(?P<host>(\[.*\]|[^:@]*))`: up to the last ']' when bracketed, else
    // up to the first ':'; then `:?(?P<port>.*)?`.
    let host_end = if hostport.starts_with('[') {
        hostport.rfind(']').map_or_else(
            || hostport.find(':').unwrap_or(hostport.len()),
            |index| index + 1,
        )
    } else {
        hostport.find(':').unwrap_or(hostport.len())
    };
    let host = &hostport[..host_end];
    let after_host = &hostport[host_end..];
    let port_text = after_host.strip_prefix(':').unwrap_or(after_host);
    RawParts {
        scheme,
        userinfo,
        host,
        port: Some(port_text),
        path,
        query,
        fragment,
    }
}

/// `encode_host`.
fn encode_host(host: &str) -> Result<String, InvalidUrl> {
    if host.is_empty() {
        return Ok(String::new());
    }
    if is_ipv4_style(host) {
        return host
            .parse::<Ipv4Addr>()
            .map(|_| host.to_owned())
            .map_err(|_| InvalidUrl(format!("Invalid IPv4 address: {}", py_str_repr(host))));
    }
    if host.len() >= 2 && host.starts_with('[') && host.ends_with(']') {
        let inner = &host[1..host.len() - 1];
        if is_ipv6_address(inner) {
            return Ok(inner.to_owned());
        }
        return Err(InvalidUrl(format!(
            "Invalid IPv6 address: {}",
            py_str_repr(host)
        )));
    }
    if host.is_ascii() {
        let mut safe = PATH_SAFE;
        safe.fill(false);
        for byte in SUB_DELIMS.bytes().chain(WHATWG_SAFE.bytes()) {
            safe[usize::from(byte)] = true;
        }
        return Ok(quote(&host.to_ascii_lowercase(), &safe));
    }
    // `idna.encode(host.lower())` (IDNA 2008); the idna crate applies UTS 46,
    // which agrees for ordinary internationalised names (D-14).
    idna::domain_to_ascii_strict(&host.to_lowercase())
        .map_err(|_| InvalidUrl(format!("Invalid IDNA hostname: {}", py_str_repr(host))))
}

/// `IPv4_STYLE_HOSTNAME`: `^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$`.
fn is_ipv4_style(host: &str) -> bool {
    let parts: Vec<&str> = host.split('.').collect();
    parts.len() == 4
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

/// `ipaddress.IPv6Address(text)`, which accepts a `%scope` suffix.
fn is_ipv6_address(text: &str) -> bool {
    let (address, scope) = match text.split_once('%') {
        Some((address, scope)) => (address, Some(scope)),
        None => (text, None),
    };
    if scope.is_some_and(|scope| scope.is_empty() || scope.contains('%')) {
        return false;
    }
    address.parse::<Ipv6Addr>().is_ok()
}

/// `normalize_port`.
fn normalize_port(port: Option<&str>, scheme: &str) -> Result<Option<BigInt>, InvalidUrl> {
    let Some(port) = port.filter(|port| !port.is_empty()) else {
        return Ok(None);
    };
    let value =
        py_int(port).map_err(|_| InvalidUrl(format!("Invalid port: {}", py_str_repr(port))))?;
    let default = match scheme {
        "ftp" => Some(21),
        "http" | "ws" => Some(80),
        "https" | "wss" => Some(443),
        _ => None,
    };
    if default.is_some_and(|default| value == BigInt::from(default)) {
        return Ok(None);
    }
    Ok(Some(value))
}

/// `validate_path`.
fn validate_path(path: &str, has_scheme: bool, has_authority: bool) -> Result<(), InvalidUrl> {
    if has_authority && !path.is_empty() && !path.starts_with('/') {
        return Err(InvalidUrl(
            "For absolute URLs, path must be empty or begin with '/'".to_owned(),
        ));
    }
    if !has_scheme && !has_authority {
        if path.starts_with("//") {
            return Err(InvalidUrl(
                "Relative URLs cannot have a path starting with '//'".to_owned(),
            ));
        }
        if path.starts_with(':') {
            return Err(InvalidUrl(
                "Relative URLs cannot have a path starting with ':'".to_owned(),
            ));
        }
    }
    Ok(())
}

/// `normalize_path`: drop `.` and `..` segments.
fn normalize_path(path: &str) -> String {
    if !path.contains('.') {
        return path.to_owned();
    }
    let components: Vec<&str> = path.split('/').collect();
    if !components.iter().any(|part| *part == "." || *part == "..") {
        return path.to_owned();
    }
    let mut output: Vec<&str> = Vec::new();
    for component in components {
        match component {
            "." => {}
            ".." => {
                if !output.is_empty() && output != [""] {
                    output.pop();
                }
            }
            other => output.push(other),
        }
    }
    output.join("/")
}

/// `quote(string, safe)`: percent-encode everything but unreserved and
/// `safe` characters, keeping existing `%XX` escapes.
fn quote(text: &str, safe: &[bool; 128]) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%'
            && bytes.get(index + 1).is_some_and(u8::is_ascii_hexdigit)
            && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit)
        {
            out.push_str(&text[index..index + 3]);
            index += 3;
            continue;
        }
        let ch = text[index..].chars().next().unwrap_or('\u{fffd}');
        let width = ch.len_utf8();
        if ch.is_ascii() && (UNRESERVED.as_bytes().contains(&byte) || safe[usize::from(byte)]) {
            out.push(ch);
        } else {
            for encoded in &bytes[index..index + width] {
                out.push_str(&format!("%{encoded:02X}"));
            }
        }
        index += width;
    }
    out
}

// --- urllib.parse.urljoin (CPython 3.12) ------------------------------------

const USES_RELATIVE: &[&str] = &[
    "", "ftp", "http", "gopher", "nntp", "imap", "wais", "file", "https", "shttp", "mms",
    "prospero", "rtsp", "rtsps", "rtspu", "sftp", "svn", "svn+ssh", "ws", "wss",
];
const USES_NETLOC: &[&str] = &[
    "",
    "ftp",
    "http",
    "gopher",
    "nntp",
    "telnet",
    "imap",
    "wais",
    "file",
    "mms",
    "https",
    "shttp",
    "snews",
    "prospero",
    "rtsp",
    "rtsps",
    "rtspu",
    "rsync",
    "svn",
    "svn+ssh",
    "sftp",
    "nfs",
    "git",
    "git+ssh",
    "ws",
    "wss",
    "itms-services",
];
const USES_PARAMS: &[&str] = &[
    "", "ftp", "hdl", "prospero", "http", "imap", "https", "shttp", "rtsp", "rtsps", "rtspu",
    "sip", "sips", "mms", "sftp", "tel",
];

/// The six parts of `urllib.parse.urlparse` (absent parts are empty).
struct UrlParts {
    scheme: String,
    netloc: String,
    path: String,
    params: String,
    query: String,
    fragment: String,
}

/// `urlparse(url, scheme)` (`urlsplit` + `_splitparams`), for the ASCII
/// texts httpx produces: nothing for `urlsplit` to strip, brackets
/// balanced.
fn urlparse(url: &str, default_scheme: &str) -> UrlParts {
    let mut scheme = default_scheme.to_owned();
    let mut rest = url;
    if let Some(index) = url.find(':')
        && index > 0
        && url.as_bytes()[0].is_ascii_alphabetic()
        && url.as_bytes()[..index]
            .iter()
            .all(|byte| is_scheme_char(*byte))
    {
        scheme = url[..index].to_ascii_lowercase();
        rest = &url[index + 1..];
    }
    let mut netloc = "";
    if let Some(after) = rest.strip_prefix("//") {
        let end = after.find(['/', '?', '#']).unwrap_or(after.len());
        netloc = &after[..end];
        rest = &after[end..];
    }
    let (rest, fragment) = rest.split_once('#').unwrap_or((rest, ""));
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (path, params) = if USES_PARAMS.contains(&scheme.as_str()) && path.contains(';') {
        split_params(path)
    } else {
        (path, "")
    };
    UrlParts {
        scheme,
        netloc: netloc.to_owned(),
        path: path.to_owned(),
        params: params.to_owned(),
        query: query.to_owned(),
        fragment: fragment.to_owned(),
    }
}

/// `_splitparams` (only called when the path contains ';').
fn split_params(path: &str) -> (&str, &str) {
    let index = match path.rfind('/') {
        Some(slash) => match path[slash..].find(';') {
            Some(offset) => slash + offset,
            None => return (path, ""),
        },
        None => path.find(';').unwrap_or(path.len()),
    };
    (&path[..index], &path[index + 1..])
}

/// `urlunparse` (`urlunsplit` of CPython 3.12).
fn urlunparse(parts: &UrlParts) -> String {
    let mut url = parts.path.clone();
    if !parts.params.is_empty() {
        url = format!("{url};{}", parts.params);
    }
    if !parts.netloc.is_empty() {
        if !url.is_empty() && !url.starts_with('/') {
            url = format!("/{url}");
        }
        url = format!("//{}{url}", parts.netloc);
    } else if url.starts_with("//")
        || (!parts.scheme.is_empty()
            && USES_NETLOC.contains(&parts.scheme.as_str())
            && (url.is_empty() || url.starts_with('/')))
    {
        url = format!("//{url}");
    }
    if !parts.scheme.is_empty() {
        url = format!("{}:{url}", parts.scheme);
    }
    if !parts.query.is_empty() {
        url = format!("{url}?{}", parts.query);
    }
    if !parts.fragment.is_empty() {
        url = format!("{url}#{}", parts.fragment);
    }
    url
}

/// `urllib.parse.urljoin(base, url)`.
pub(crate) fn urljoin(base: &str, url: &str) -> String {
    if base.is_empty() {
        return url.to_owned();
    }
    if url.is_empty() {
        return base.to_owned();
    }
    let base_parts = urlparse(base, "");
    let mut parts = urlparse(url, &base_parts.scheme);
    if parts.scheme != base_parts.scheme || !USES_RELATIVE.contains(&parts.scheme.as_str()) {
        return url.to_owned();
    }
    if USES_NETLOC.contains(&parts.scheme.as_str()) {
        if !parts.netloc.is_empty() {
            return urlunparse(&parts);
        }
        parts.netloc.clone_from(&base_parts.netloc);
    }
    if parts.path.is_empty() && parts.params.is_empty() {
        parts.path.clone_from(&base_parts.path);
        parts.params.clone_from(&base_parts.params);
        if parts.query.is_empty() {
            parts.query.clone_from(&base_parts.query);
        }
        return urlunparse(&parts);
    }
    let mut base_segments: Vec<&str> = base_parts.path.split('/').collect();
    if base_segments.last().is_some_and(|last| !last.is_empty()) {
        base_segments.pop();
    }
    let segments: Vec<&str> = if parts.path.starts_with('/') {
        parts.path.split('/').collect()
    } else {
        let mut segments = base_segments;
        segments.extend(parts.path.split('/'));
        // `segments[1:-1] = filter(None, segments[1:-1])`
        if segments.len() > 2 {
            let last = segments[segments.len() - 1];
            let mut filtered = vec![segments[0]];
            filtered.extend(
                segments[1..segments.len() - 1]
                    .iter()
                    .copied()
                    .filter(|segment| !segment.is_empty()),
            );
            filtered.push(last);
            segments = filtered;
        }
        segments
    };
    let mut resolved: Vec<&str> = Vec::new();
    for segment in &segments {
        match *segment {
            ".." => {
                resolved.pop();
            }
            "." => {}
            other => resolved.push(other),
        }
    }
    if segments
        .last()
        .is_some_and(|last| *last == "." || *last == "..")
    {
        resolved.push("");
    }
    let joined = resolved.join("/");
    parts.path = if joined.is_empty() {
        "/".to_owned()
    } else {
        joined
    };
    urlunparse(&parts)
}
