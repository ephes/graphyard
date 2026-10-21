//! The graphyard agent binary (PLAN.md section 3).
//!
//! Slice S8 adds the CLI, the scheduler and the runs. Until then the binary
//! only reports that it is not usable, so nothing can mistake it for the
//! agent.

use std::process::ExitCode;

fn main() -> ExitCode {
    eprintln!(
        "graphyard-agent {}: not implemented yet (agent-rs/PLAN.md slice S8)",
        env!("CARGO_PKG_VERSION")
    );
    ExitCode::FAILURE
}
