//! SQLite store of the graphyard agent (PLAN.md section 5).
//!
//! One dedicated OS thread owns the agent's only connection ([`Store`];
//! PLAN.md 4.1, 5.2): opened read-write without create, with Django's
//! connection setup and the graphyard pragmas of
//! `apps.py:_configure_sqlite_connection`. Single statements run in
//! autocommit like Django's `save(update_fields=...)`; multi-statement
//! operations run in `BEGIN IMMEDIATE` (`transaction_mode: IMMEDIATE`).
//!
//! The store runs no DDL: [`schema`] verifies the Django schema contract
//! (D-2). Every operation reproduces the Django ORM call it replaces, row
//! for row, pinned by `fixtures/store_ops.json`; stored datetimes go through
//! Django's conversion ([`datetime`], `fixtures/datetime_codec.json`).
//! Where Python reads the clock inside an operation, the Rust operation
//! takes `now` as a parameter.
//!
//! Slice S3a: spec and condition loads and updates, heartbeats. Slice S3b:
//! the registry touch (PLAN.md 5.5, [`Store::touch_registry`]).

// The unit tests share the integration tests' helpers (`tests/common`),
// which name this crate.
#[cfg(test)]
extern crate self as graphyard_store;

mod actor;
mod conditions;
pub mod datetime;
mod error;
mod heartbeat;
mod registry;
mod rows;
pub mod schema;
mod specs;

pub use actor::Store;
pub use conditions::ConditionRow;
pub use datetime::{DateTimeError, DbDateTime};
pub use error::{NOT_UPDATED_MESSAGE, StoreError};
pub use heartbeat::{HeartbeatRecord, details_json};
pub use registry::REGISTRY_LAST_SEEN_UPDATE_INTERVAL;
pub use schema::{MIN_GRAPHYARD_MIGRATION, REQUIRED_COLUMNS, SchemaReport, check_schema};
pub use specs::{SpecRow, SpecRunUpdate};
