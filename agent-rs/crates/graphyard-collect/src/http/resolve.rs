//! Name resolution through libc `getaddrinfo` and the abandoned-lookup
//! accounting of PLAN.md 6.2 / D-3.
//!
//! `macmini.local` resolves only through glibc's NSS (`mdns4_minimal`), so
//! every lookup is a blocking `getaddrinfo` on tokio's blocking pool (R2).
//! Such a call cannot be cancelled: when the deadline drops a request, its
//! lookup keeps running. Python tracks the worker threads that outlive their
//! deadline (`_abandoned_page_probe_workers`); the Rust request future itself
//! is cancelled, so the lookups are what is counted instead.

use std::fmt;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::watch;

/// A failed lookup; the text is `str(socket.gaierror)` (P2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LookupError(pub String);

impl fmt::Display for LookupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LookupError {}

/// A blocking name lookup (`getaddrinfo(host, None, SOCK_STREAM)`).
pub trait Lookup: Send + Sync + 'static {
    /// Resolve `host` (ASCII, IDNA-encoded) to its addresses in the order
    /// the resolver returns them.
    ///
    /// # Errors
    /// The resolver's failure.
    fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, LookupError>;

    /// `getaddrinfo(host, service, SOCK_STREAM)`; the service only matters
    /// for its validation (a port above 65535 fails here, as in httpcore).
    ///
    /// # Errors
    /// The resolver's failure.
    fn lookup_service(
        &self,
        host: &str,
        service: Option<&str>,
    ) -> Result<Vec<IpAddr>, LookupError> {
        let _ = service;
        self.lookup(host)
    }
}

/// The system resolver: libc `getaddrinfo` (glibc NSS on Linux).
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemLookup;

impl Lookup for SystemLookup {
    fn lookup(&self, host: &str) -> Result<Vec<IpAddr>, LookupError> {
        self.lookup_service(host, None)
    }

    fn lookup_service(
        &self,
        host: &str,
        service: Option<&str>,
    ) -> Result<Vec<IpAddr>, LookupError> {
        let hints = dns_lookup::AddrInfoHints {
            socktype: dns_lookup::SockType::Stream.into(),
            ..Default::default()
        };
        match dns_lookup::getaddrinfo(Some(host), service, Some(hints)) {
            Ok(infos) => Ok(infos
                .filter_map(Result::ok)
                .map(|info| info.sockaddr.ip())
                .collect()),
            Err(err) => Err(LookupError(gai_error_text(err))),
        }
    }
}

/// `str(socket.gaierror)`: `[Errno <EAI code>] <gai_strerror>`, or the
/// `OSError` text for `EAI_SYSTEM`.
fn gai_error_text(err: dns_lookup::LookupError) -> String {
    let code = err.error_num();
    let io_err = std::io::Error::from(err);
    if io_err.raw_os_error().is_some() {
        return super::error::os_error_text(&io_err);
    }
    let text = io_err.to_string();
    let detail = text
        .strip_prefix("failed to lookup address information: ")
        .unwrap_or(&text);
    format!("[Errno {code}] {detail}")
}

/// Marks a lookup finished when dropped (also when the lookup panics).
struct Finished(watch::Sender<bool>);

impl Drop for Finished {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

/// One lookup on the blocking pool; finished once its thread returns.
#[derive(Clone, Debug)]
pub(crate) struct LookupHandle(watch::Receiver<bool>);

impl LookupHandle {
    pub(crate) fn is_running(&self) -> bool {
        !*self.0.borrow()
    }

    /// Wait until the lookup's thread returns.
    pub(crate) async fn finished(&mut self) {
        // An error means the sender is gone, which happens only after it
        // sent `true`.
        let _ = self.0.wait_for(|finished| *finished).await;
    }
}

/// The lookups one client has started (one client per spec execution).
#[derive(Debug, Default)]
pub(crate) struct LookupScope {
    lookups: Mutex<Vec<LookupHandle>>,
}

impl LookupScope {
    fn register(&self, handle: LookupHandle) {
        let mut lookups = self.lookups.lock().unwrap_or_else(PoisonError::into_inner);
        lookups.retain(LookupHandle::is_running);
        lookups.push(handle);
    }

    /// The lookups still running.
    pub(crate) fn running(&self) -> Vec<LookupHandle> {
        let lookups = self.lookups.lock().unwrap_or_else(PoisonError::into_inner);
        lookups
            .iter()
            .filter(|handle| handle.is_running())
            .cloned()
            .collect()
    }
}

/// The process-wide set of abandoned lookups: lookups of cancelled requests
/// still running after the close wait (`_abandoned_page_probe_workers`).
#[derive(Debug, Default)]
pub struct AbandonedLookups {
    lookups: Mutex<Vec<LookupHandle>>,
}

impl AbandonedLookups {
    pub(crate) fn add(&self, handle: LookupHandle) {
        self.lookups
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(handle);
    }

    /// `_live_abandoned_page_probe_workers()`: drop the finished ones and
    /// count the rest.
    pub fn live(&self) -> usize {
        let mut lookups = self.lookups.lock().unwrap_or_else(PoisonError::into_inner);
        lookups.retain(LookupHandle::is_running);
        lookups.len()
    }
}

/// `getaddrinfo(host, port, 0, SOCK_STREAM)` on the blocking pool, as
/// `socket.create_connection` does it, recorded in `scope` while it runs.
/// An IP literal needs no lookup.
///
/// # Errors
/// The lookup's `gaierror`, or the text of a failed blocking task.
pub(crate) async fn resolve(
    lookup: &Arc<dyn Lookup>,
    scope: &LookupScope,
    host: &str,
    service: Option<String>,
) -> Result<Vec<IpAddr>, LookupError> {
    if service.is_none()
        && let Ok(ip) = host.parse::<IpAddr>()
    {
        return Ok(vec![ip]);
    }
    let (sender, receiver) = watch::channel(false);
    scope.register(LookupHandle(receiver));
    let lookup = Arc::clone(lookup);
    let host = host.to_owned();
    tokio::task::spawn_blocking(move || {
        let _finished = Finished(sender);
        lookup.lookup_service(&host, service.as_deref())
    })
    .await
    .map_err(|err| LookupError(err.to_string()))?
}
