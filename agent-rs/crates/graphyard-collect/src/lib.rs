//! HTTP layer and metric collection executors of the graphyard agent
//! (PLAN.md section 6).
//!
//! Slice S6 adds the HTTP layer ([`http`]), S7a the Home Assistant and HTTP
//! JSON executors, S7b the page probe and UniFi executors.

pub mod executors;
pub mod http;
