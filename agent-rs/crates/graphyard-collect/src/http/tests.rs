//! Tests of the HTTP layer: the `http_errors.json` fixture cases and
//! fake-server tests per rule of PLAN.md 6.1/6.2 (no test reaches the
//! network; names resolve through stubs or to loopback). The fake servers,
//! the log capture and the TLS material are shared with the executor tests.

mod deadline;
mod fixture;
pub(crate) mod logs;
mod scenarios;
pub(crate) mod server;
mod texts;
pub(crate) mod transport;
mod urls;
