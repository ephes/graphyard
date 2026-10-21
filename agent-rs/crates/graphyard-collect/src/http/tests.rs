//! Tests of the HTTP layer: the `http_errors.json` fixture cases and
//! fake-server tests per rule of PLAN.md 6.1/6.2 (no test reaches the
//! network; names resolve through stubs or to loopback).

mod deadline;
mod fixture;
mod logs;
mod scenarios;
mod server;
mod texts;
mod transport;
mod urls;
