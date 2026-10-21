//! The whole-operation deadline (`services._run_http_with_deadline`,
//! PLAN.md 6.2).
//!
//! httpx's timeouts bound each connect and each read, so an endpoint that
//! drips one byte per read never trips them. The HA executors,
//! `http_json_metric` and `http_page_probe` therefore run the whole
//! operation (resolution, connect, TLS, headers, body, redirects, JSON
//! parse) under `total_timeout_seconds`. Python runs it in a worker thread
//! and abandons the thread at the deadline; Rust drops the request future,
//! which cancels everything except a blocking `getaddrinfo` (D-3). Those
//! lookups are waited for up to `CLOSE_JOIN` and counted as abandoned when
//! still running; with `MAX_ABANDONED` of them alive no new operation starts.

use std::future::Future;
use std::time::{Duration, Instant};

use graphyard_core::pycompat::py_format_g;

use super::client::{HttpClient, escape_log};
use super::error::{ErrorKind, HttpError};

/// The deadline of one operation, for the checks Python makes inside the
/// worker (`_PageProbeCancellation.check`), such as after the JSON parse.
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    at: Option<Instant>,
    total_timeout_seconds: f64,
}

impl Deadline {
    /// `total_timeout_seconds` from now.
    pub fn after(total_timeout_seconds: f64) -> Self {
        Self {
            at: Instant::now().checked_add(Duration::from_secs_f64(total_timeout_seconds)),
            total_timeout_seconds,
        }
    }

    /// A deadline that never passes (`_PageProbeCancellation(None)`).
    pub fn none() -> Self {
        Self {
            at: None,
            total_timeout_seconds: f64::INFINITY,
        }
    }

    /// The deadline at `at` (tests).
    pub fn at(at: Instant, total_timeout_seconds: f64) -> Self {
        Self {
            at: Some(at),
            total_timeout_seconds,
        }
    }

    /// `_PageProbeCancellation.check()`.
    ///
    /// # Errors
    /// [`deadline_error`] once the deadline has passed.
    pub fn check(&self) -> Result<(), HttpError> {
        match self.at {
            Some(at) if Instant::now() >= at => Err(deadline_error(self.total_timeout_seconds)),
            _ => Ok(()),
        }
    }
}

/// `HTTP collector exceeded total_timeout_seconds=<x:g>`.
pub fn deadline_error(total_timeout_seconds: f64) -> HttpError {
    HttpError::new(
        ErrorKind::DeadlineExceeded,
        format!(
            "HTTP collector exceeded total_timeout_seconds={}",
            py_format_g(total_timeout_seconds)
        ),
    )
}

/// `<n> earlier HTTP collectors are still running past
/// total_timeout_seconds; not starting another`.
pub fn abandoned_error(count: usize) -> HttpError {
    HttpError::new(
        ErrorKind::DeadlineExceeded,
        format!(
            "{count} earlier HTTP collectors are still running past total_timeout_seconds; \
             not starting another"
        ),
    )
}

/// `_run_http_with_deadline(client, url=…, operation=…,
/// total_timeout_seconds=…)`: refuse to start while too many lookups are
/// abandoned, run `operation` (given the [`Deadline`]) under
/// `total_timeout_seconds`, and on expiry cancel it, wait for its lookups up
/// to the close wait and record the ones still running.
///
/// `client` must be the client the operation uses; its lookups are the
/// ones checked. `url` names the operation in the warning log line.
///
/// # Errors
/// The operation's error, [`deadline_error`] at the deadline, or
/// [`abandoned_error`] before starting.
pub async fn run_with_deadline<T, F, Fut>(
    client: &HttpClient,
    url: &str,
    total_timeout_seconds: f64,
    operation: F,
) -> Result<T, HttpError>
where
    F: FnOnce(Deadline) -> Fut,
    Fut: Future<Output = Result<T, HttpError>>,
{
    let context = &client.context;
    let abandoned = context.abandoned.live();
    if abandoned >= context.max_abandoned {
        return Err(abandoned_error(abandoned));
    }
    let deadline = Deadline::after(total_timeout_seconds);
    let budget = Duration::from_secs_f64(total_timeout_seconds);
    // The timeout owns the operation's future and drops it on expiry, which
    // cancels the request: sockets close, nothing more is sent.
    if let Ok(result) = tokio::time::timeout(budget, operation(deadline)).await {
        return result;
    }
    let mut pending = client.scope.running();
    if !pending.is_empty() {
        let wait_all = async {
            for lookup in &mut pending {
                lookup.finished().await;
            }
        };
        let _ = tokio::time::timeout(context.close_join, wait_all).await;
        for lookup in pending.into_iter().filter(|lookup| lookup.is_running()) {
            context.abandoned.add(lookup);
            log::warn!(
                "HTTP collector worker for {} still running after it was cancelled",
                escape_log(url)
            );
        }
    }
    Err(deadline_error(total_timeout_seconds))
}
