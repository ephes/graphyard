//! InfluxDB clients of the graphyard agent (PLAN.md sections 6.4 and 6.5).
//!
//! Slice S4a adds the write client ([`InfluxWriter`], `influx.py:write_points`)
//! and the lazy configuration check of `_build_client` ([`config`]); S4b adds
//! the condition queries (v2 Flux, v3 SQL, `auto`).
//!
//! HTTP goes through `reqwest` with rustls (`ring`) and the system trust
//! store (`rustls-platform-verifier`, D-12); no OpenSSL.

pub mod config;
pub mod write;

pub use config::{InfluxClientConfig, InfluxConfigurationError, RequestTimeout};
pub use write::{InfluxWriter, WriteError};

/// Install `ring` as the process-wide rustls crypto provider, unless one is
/// installed already (reqwest is built without a provider of its own).
pub fn install_crypto_provider() {
    // An error only means another provider was installed first.
    let _ = rustls::crypto::ring::default_provider().install_default();
}
