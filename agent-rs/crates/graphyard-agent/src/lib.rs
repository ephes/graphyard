//! The graphyard agent binary (PLAN.md section 3): a drop-in for `manage.py
//! start_agent`.
//!
//! - [`cli`]: the flags and their validation (3.1);
//! - [`scheduler`]: the loop of `start_agent.py:handle` (3.2);
//! - [`runs`]: the metrics and conditions runs with executor isolation and
//!   heartbeats (3.4, 3.5);
//! - [`collectors`]: the real executors, metric sink and condition samples;
//! - [`app`]: startup, report lines, signals and shutdown (3.6, 3.7, 4.1);
//! - [`logging`]: the bare-message stderr logger (3.6).

pub mod app;
pub mod cli;
pub mod collectors;
pub mod logging;
pub mod runs;
pub mod scheduler;

pub use app::main_entry;
