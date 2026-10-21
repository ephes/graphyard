//! `graphyard-agent --version` (PLAN.md 3.1): `graphyard-agent <version>
//! (<commit>)`, the commit embedded by `build.rs`. The release workflow
//! checks the same line against the tag and its own commit
//! (`scripts/release_checks.py smoke`).

use std::process::{Command, Output};

const COMMIT: &str = env!("GRAPHYARD_AGENT_GIT_COMMIT");

fn agent(args: &[&str], env: &[(&str, &str)]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_graphyard-agent"))
        .args(args)
        .env_clear()
        .envs(env.iter().copied())
        .output()
        .unwrap()
}

#[test]
fn version_names_the_package_version_and_the_commit() {
    let expected = format!("graphyard-agent {} ({COMMIT})\n", env!("CARGO_PKG_VERSION"));
    // Other flags and an environment the settings reject do not matter.
    for (args, env) in [
        (&["--version"][..], &[][..]),
        (&["--run-once", "--version"], &[]),
        (&["--version"], &[("GRAPHYARD_SQLITE_TIMEOUT_SECONDS", "x")]),
    ] {
        let output = agent(args, env);
        assert!(output.status.success(), "{output:?}");
        assert_eq!(String::from_utf8(output.stdout).unwrap(), expected);
        assert!(output.stderr.is_empty());
    }
}

#[test]
fn the_embedded_commit_is_hex_or_unknown() {
    assert!(
        COMMIT == "unknown"
            || ((7..=40).contains(&COMMIT.len())
                && COMMIT
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))),
        "{COMMIT:?}"
    );
}
