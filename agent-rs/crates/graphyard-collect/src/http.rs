//! The HTTP layer of the collectors (PLAN.md 6.1, 6.2): httpx 0.28.1 as
//! `services.py` configures it, on hyper's HTTP/1.1 connection API.
//!
//! - [`client`]: one [`HttpClient`] per spec execution (`verify_tls`, the
//!   httpx request headers and auth, a cookie jar, HTTP/1.1, gzip/deflate),
//!   one [`HttpClient::send`] per hop with httpx's `HTTP Request:` log line
//!   and redirect preparation; connections as httpcore opens them, with the
//!   timeout per connect, TLS handshake and socket read/write;
//! - [`deadline`]: the total deadline and the abandoned-lookup cap;
//! - [`probe`]: the JSON reader, the page-probe redirect loop and automatic
//!   redirects;
//! - [`url`], [`status`], [`error`]: httpx's URL model, `raise_for_status`
//!   texts and exception classes;
//! - [`resolve`]: libc `getaddrinfo` with lookup tracking.
//!
//! Python sources: `services.py` (`_read_http_json`, `_run_http_page_probe`,
//! `_run_http_with_deadline`, `_PageProbeCancellation`, the
//! `_PAGE_PROBE_*` limits) and httpx (`Client.send`, `_build_redirect_request`,
//! `Response.raise_for_status`, `_urlparse`). Pinned by
//! `fixtures/http_errors.json` (`gen_http_errors.py`) and the fake-server
//! tests in `http/tests`.

pub mod client;
mod connect;
pub mod deadline;
mod decode;
pub mod error;
pub mod probe;
pub mod resolve;
pub mod status;
pub mod url;

pub use client::{ClientOptions, HttpClient, HttpContext, Method, Request, Response};
pub use deadline::{Deadline, run_with_deadline};
pub use error::{ErrorKind, HttpError};
pub use probe::{
    PageProbeResult, decode_json, read_http_json, run_page_probe, send_following_redirects,
};
pub use resolve::{Lookup, LookupError, SystemLookup};
pub use url::HttpxUrl;

#[cfg(test)]
pub(crate) mod tests;
