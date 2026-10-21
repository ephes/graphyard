//! Embeds the git commit into `graphyard-agent --version` (PLAN.md 3.1, 8.2).
//!
//! Source of the commit, in order:
//! 1. `GRAPHYARD_AGENT_GIT_COMMIT` (set by the release workflow to the commit
//!    it checked out, and by `just rs-build-linux`, whose container sees only
//!    `agent-rs/` and no `.git`);
//! 2. `git rev-parse HEAD` in the source tree;
//! 3. `unknown` (no git, not a repository).
//!
//! The value must be 7 to 40 lowercase hex digits (or `unknown`); anything
//! else fails the build so an odd value never reaches a release binary. The
//! release smoke test then requires the full 40-digit commit of the workflow.

use std::path::{Path, PathBuf};
use std::process::Command;

const ENV: &str = "GRAPHYARD_AGENT_GIT_COMMIT";

pub(crate) fn is_commit(value: &str) -> bool {
    (7..=40).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    Some(text.trim().to_owned())
}

/// The paths whose change means HEAD may have moved (`--git-path` resolves
/// worktree and common directories):
/// - the HEAD file (detached commits, branch switches);
/// - `packed-refs`, when it exists;
/// - the nearest existing directory above the branch's loose ref, not the ref
///   file: a branch that lives only in `packed-refs` (after `git pack-refs`,
///   which also removes the empty directories of a nested branch such as
///   `feature/x`) gets a new loose file below it on its next commit, which a
///   directory watch sees (Cargo scans watched directories recursively). A
///   missing path is never watched, because Cargo would rerun the script on
///   every build.
pub(crate) fn git_head_paths(dir: &Path) -> Vec<PathBuf> {
    let git_path = |name: &str| {
        git(
            dir,
            &["rev-parse", "--path-format=absolute", "--git-path", name],
        )
        .map(PathBuf::from)
    };
    let mut paths = Vec::new();
    paths.extend(git_path("HEAD"));
    paths.extend(git_path("packed-refs").filter(|path| path.exists()));
    if let Some(file) =
        git(dir, &["symbolic-ref", "-q", "HEAD"]).and_then(|reference| git_path(&reference))
    {
        // Packing a nested branch (`feature/x`) also removes its now empty
        // directories, so climb to the nearest directory that exists.
        paths.extend(
            file.ancestors()
                .skip(1)
                .find(|dir| dir.is_dir())
                .map(Path::to_path_buf),
        );
    }
    paths
}

// The helpers above are also compiled into `tests/build_script.rs`.
fn main() {
    println!("cargo:rerun-if-env-changed={ENV}");
    println!("cargo:rerun-if-changed=build.rs");
    let commit = match std::env::var(ENV) {
        Ok(value) if !value.is_empty() => {
            assert!(
                is_commit(&value),
                "{ENV} must be 7 to 40 lowercase hex digits, got {value:?}"
            );
            value
        }
        _ => {
            let dir = Path::new(".");
            for path in git_head_paths(dir) {
                println!("cargo:rerun-if-changed={}", path.display());
            }
            git(dir, &["rev-parse", "HEAD"])
                .filter(|value| is_commit(value))
                .unwrap_or_else(|| "unknown".to_owned())
        }
    };
    println!("cargo:rustc-env={ENV}={commit}");
}
