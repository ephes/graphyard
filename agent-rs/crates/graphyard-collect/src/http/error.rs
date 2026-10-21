//! Errors of the HTTP layer in Python's exception classes (PLAN.md 6.1).
//!
//! The executors store `str(err)` as the spec's `last_error`, and the page
//! probe decides warning versus critical by the class (`except
//! (httpx.HTTPError, _PageProbeLimitExceeded, _PageProbeDeadlineExceeded)`).
//! [`ErrorKind`] names the class, [`HttpError::message`] is `str(err)`.
//! graphyard-authored and httpx template texts are P1; texts httpx takes from
//! the OS or a library (connect failures, TLS, protocol errors, JSON decode
//! positions) are P2 and rendered in the Python form where the category is
//! known: `[Errno N] <strerror>` for OS errors, `timed out` for timeouts.

use std::error::Error as StdError;
use std::fmt;
use std::io;

/// The Python exception class of an [`HttpError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    ConnectError,
    ConnectTimeout,
    ReadTimeout,
    WriteTimeout,
    ReadError,
    WriteError,
    RemoteProtocolError,
    LocalProtocolError,
    UnsupportedProtocol,
    DecodingError,
    TooManyRedirects,
    HttpStatusError,
    InvalidUrl,
    /// `services._PageProbeLimitExceeded` (body cap).
    LimitExceeded,
    /// `services._PageProbeDeadlineExceeded` (total deadline, abandoned cap).
    DeadlineExceeded,
    JsonDecodeError,
    UnicodeDecodeError,
    /// The `ValueError` of `json.loads` for an integer token beyond
    /// CPython's 4300-digit limit.
    ValueError,
}

impl ErrorKind {
    /// Every kind, for tests against the fixture's class table.
    pub const ALL: [Self; 18] = [
        Self::ConnectError,
        Self::ConnectTimeout,
        Self::ReadTimeout,
        Self::WriteTimeout,
        Self::ReadError,
        Self::WriteError,
        Self::RemoteProtocolError,
        Self::LocalProtocolError,
        Self::UnsupportedProtocol,
        Self::DecodingError,
        Self::TooManyRedirects,
        Self::HttpStatusError,
        Self::InvalidUrl,
        Self::LimitExceeded,
        Self::DeadlineExceeded,
        Self::JsonDecodeError,
        Self::UnicodeDecodeError,
        Self::ValueError,
    ];

    /// The Python class name (`type(err).__name__`).
    pub const fn python_class(self) -> &'static str {
        match self {
            Self::ConnectError => "ConnectError",
            Self::ConnectTimeout => "ConnectTimeout",
            Self::ReadTimeout => "ReadTimeout",
            Self::WriteTimeout => "WriteTimeout",
            Self::ReadError => "ReadError",
            Self::WriteError => "WriteError",
            Self::RemoteProtocolError => "RemoteProtocolError",
            Self::LocalProtocolError => "LocalProtocolError",
            Self::UnsupportedProtocol => "UnsupportedProtocol",
            Self::DecodingError => "DecodingError",
            Self::TooManyRedirects => "TooManyRedirects",
            Self::HttpStatusError => "HTTPStatusError",
            Self::InvalidUrl => "InvalidURL",
            Self::LimitExceeded => "_PageProbeLimitExceeded",
            Self::DeadlineExceeded => "_PageProbeDeadlineExceeded",
            Self::JsonDecodeError => "JSONDecodeError",
            Self::UnicodeDecodeError => "UnicodeDecodeError",
            Self::ValueError => "ValueError",
        }
    }

    /// `isinstance(err, httpx.HTTPError)`.
    pub const fn is_http_error(self) -> bool {
        !matches!(
            self,
            Self::InvalidUrl
                | Self::LimitExceeded
                | Self::DeadlineExceeded
                | Self::JsonDecodeError
                | Self::UnicodeDecodeError
                | Self::ValueError
        )
    }

    /// Whether the page probe's `except` takes the error as a warning (two
    /// failure points) rather than a critical collector failure.
    pub const fn is_page_probe_warning(self) -> bool {
        self.is_http_error() || matches!(self, Self::LimitExceeded | Self::DeadlineExceeded)
    }
}

/// An HTTP layer failure: its Python class and `str(err)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpError {
    pub kind: ErrorKind,
    pub message: String,
}

impl HttpError {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// `_PageProbeLimitExceeded`: `response body exceeded max_body_bytes=<n>`.
    pub fn body_limit(max_body_bytes: u64) -> Self {
        Self::new(
            ErrorKind::LimitExceeded,
            format!("response body exceeded max_body_bytes={max_body_bytes}"),
        )
    }

    /// `httpx.TooManyRedirects`.
    pub fn too_many_redirects() -> Self {
        Self::new(
            ErrorKind::TooManyRedirects,
            "Exceeded maximum allowed redirects.",
        )
    }

    /// The error of a request or body read on an open connection (hyper),
    /// classified like httpcore: a read that waited longer than the timeout
    /// is `ReadTimeout`, a connection closed early `RemoteProtocolError`, a
    /// malformed response `RemoteProtocolError`, other socket errors
    /// `ReadError`.
    pub(crate) fn transport(err: &(dyn StdError + 'static), in_body: bool) -> Self {
        if let Some(io_err) = find_source::<io::Error>(err) {
            match io_err.kind() {
                io::ErrorKind::TimedOut => return Self::new(ErrorKind::ReadTimeout, "timed out"),
                io::ErrorKind::UnexpectedEof => {
                    return Self::new(
                        ErrorKind::RemoteProtocolError,
                        "peer closed connection without sending complete message body",
                    );
                }
                _ => {}
            }
        }
        if let Some(hyper_err) = find_source::<hyper::Error>(err)
            && hyper_err.is_parse()
            && hyper_err.to_string() == "message head is too large"
        {
            // h11's error for a response head over httpcore's limit
            // (`is_parse_too_large` exists only with hyper's server feature).
            return Self::new(ErrorKind::RemoteProtocolError, "Receive buffer too long");
        }
        if let Some(hyper_err) = find_source::<hyper::Error>(err)
            && hyper_err.is_incomplete_message()
        {
            let text = if in_body {
                "peer closed connection without sending complete message body"
            } else {
                "Server disconnected without sending a response."
            };
            return Self::new(ErrorKind::RemoteProtocolError, text);
        }
        if let Some(io_err) = find_source::<io::Error>(err) {
            return Self::new(ErrorKind::ReadError, os_error_text(io_err));
        }
        Self::new(ErrorKind::RemoteProtocolError, innermost_text(err))
    }
}

impl fmt::Display for HttpError {
    /// `str(err)`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for HttpError {}

/// The first error of type `T` in the source chain.
fn find_source<'a, T: StdError + 'static>(err: &'a (dyn StdError + 'static)) -> Option<&'a T> {
    let mut current: Option<&(dyn StdError + 'static)> = Some(err);
    while let Some(error) = current {
        if let Some(found) = error.downcast_ref::<T>() {
            return Some(found);
        }
        current = error.source();
    }
    None
}

/// The text of the deepest error in the chain (the library's own words).
fn innermost_text(err: &(dyn StdError + 'static)) -> String {
    let mut current: &(dyn StdError + 'static) = err;
    while let Some(source) = current.source() {
        current = source;
    }
    current.to_string()
}

/// `str(OSError)`: `[Errno N] <strerror>` for an OS error, else the text.
pub(crate) fn os_error_text(err: &io::Error) -> String {
    match err.raw_os_error() {
        Some(code) => {
            let text = err.to_string();
            let suffix = format!(" (os error {code})");
            let strerror = text.strip_suffix(&suffix).unwrap_or(&text);
            format!("[Errno {code}] {strerror}")
        }
        None if err.kind() == io::ErrorKind::TimedOut => "timed out".to_owned(),
        None => match err.get_ref() {
            Some(inner) => inner.to_string(),
            None => err.to_string(),
        },
    }
}
