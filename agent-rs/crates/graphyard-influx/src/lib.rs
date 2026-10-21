//! InfluxDB clients of the graphyard agent (PLAN.md sections 6.4 and 6.5).
//!
//! Slice S4a adds the write client ([`InfluxWriter`], `influx.py:write_points`)
//! and the lazy configuration check of `_build_client` ([`config`]). Slice
//! S4b adds the condition queries: the lookback window of a condition
//! ([`window`]), the v2 Flux text, request body and annotated CSV decoding
//! ([`flux`]), the v3 SQL text and row decoding ([`sql`]) and the HTTP client
//! for the modes `v2`, `v3` and `auto` ([`query`]). Python sources:
//! `influx.py:query_condition_window`, `query_range`, `_query_range_v2_flux`,
//! `_query_range_v3_sql`, `_read_v3_sql_rows`, `_parse_influx_ts`,
//! `_parse_query_value`, and `influxdb_client` 1.50.0's
//! `QueryApi.query`/`FluxCsvParser`.
//!
//! HTTP goes through `reqwest` with rustls (`ring`) and the system trust
//! store (`rustls-platform-verifier`, D-12); no OpenSSL.

pub mod config;
pub mod error;
pub mod flux;
pub mod query;
pub mod sql;
pub mod window;
pub mod write;

pub use config::{InfluxClientConfig, InfluxConfigurationError, RequestTimeout};
pub use error::QueryError;
pub use query::{QueryClient, SQL_QUERY_MAX_BODY_BYTES, SQL_QUERY_TOTAL_TIMEOUT};
pub use window::{ConditionQuery, QueryRange, condition_window};
pub use write::{InfluxWriter, WriteError};

/// Install `ring` as the process-wide rustls crypto provider, unless one is
/// installed already (reqwest is built without a provider of its own).
pub fn install_crypto_provider() {
    // An error only means another provider was installed first.
    let _ = rustls::crypto::ring::default_provider().install_default();
}
