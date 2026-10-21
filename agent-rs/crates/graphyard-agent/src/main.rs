//! The graphyard agent binary (PLAN.md section 3).
//!
//! Slice S8 adds the CLI, the scheduler and the runs. Until then the binary
//! answers only `--version` (needed by the release smoke test, PLAN.md 8.2)
//! and otherwise reports that it is not usable, so nothing can mistake it for
//! the agent.

use std::process::ExitCode;

/// `--version` text after the program name (PLAN.md 3.1): the package
/// version and the git commit `build.rs` embeds.
const VERSION: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    " (",
    env!("GRAPHYARD_AGENT_GIT_COMMIT"),
    ")"
);

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    if let (Some(first), None) = (args.next(), args.next())
        && first == "--version"
    {
        println!("graphyard-agent {VERSION}");
        return ExitCode::SUCCESS;
    }
    eprintln!("graphyard-agent {VERSION}: not implemented yet (agent-rs/PLAN.md slice S8)");
    ExitCode::FAILURE
}
