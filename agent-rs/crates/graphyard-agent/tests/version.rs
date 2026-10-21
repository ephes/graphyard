//! `graphyard-agent --version` (PLAN.md 3.1): `graphyard-agent <version>
//! (<commit>)`, the commit embedded by `build.rs`. The release workflow
//! checks the same line against the tag and its own commit
//! (`scripts/release_checks.py smoke`).

use std::process::{Command, Output};

const COMMIT: &str = env!("GRAPHYARD_AGENT_GIT_COMMIT");

fn agent(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_graphyard-agent"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn version_names_the_package_version_and_the_commit() {
    let output = agent(&["--version"]);
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!("graphyard-agent {} ({COMMIT})\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
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

#[test]
fn anything_else_is_not_implemented_yet_and_exits_1() {
    for args in [&[][..], &["--run-once"], &["--version", "--run-once"]] {
        let output = agent(args);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(stderr.contains("not implemented yet"), "{args:?}: {stderr}");
    }
}
