//! SQLite store of the graphyard agent (PLAN.md section 5).
//!
//! Slice S3a adds the DB thread owning the `rusqlite` connection, the schema
//! contract, the spec/condition/heartbeat operations; S3b the registry touch.
