//! Tests of the helpers in `build.rs` (which commit `--version` embeds and
//! when Cargo reruns the script).

#[allow(dead_code)]
#[path = "../build.rs"]
mod build_script;

use std::path::{Path, PathBuf};
use std::process::Command;

fn git(dir: &Path, args: &[&str]) {
    let status = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "user.name=t", "-c", "user.email=t@example.invalid"])
        .args(["-c", "commit.gpgsign=false"])
        .args(args)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

fn watched(dir: &Path) -> Vec<PathBuf> {
    build_script::git_head_paths(dir)
        .into_iter()
        .map(|path| path.canonicalize().unwrap())
        .collect()
}

#[test]
fn commits_are_7_to_40_lowercase_hex_digits() {
    assert!(build_script::is_commit("abcdef1"));
    assert!(build_script::is_commit(&"0".repeat(40)));
    assert!(!build_script::is_commit("abcdef"));
    assert!(!build_script::is_commit(&"0".repeat(41)));
    assert!(!build_script::is_commit("ABCDEF1"));
    assert!(!build_script::is_commit("abcdefg"));
    assert!(!build_script::is_commit("unknown"));
}

#[test]
fn a_branch_watches_head_and_its_ref_directory() {
    let repo = tempfile::tempdir().unwrap();
    let dir = repo.path();
    git(dir, &["init", "-q", "-b", "main"]);
    git(dir, &["commit", "-q", "--allow-empty", "-m", "one"]);
    let git_dir = dir.canonicalize().unwrap().join(".git");
    // No packed-refs file yet: a missing path is never watched.
    assert!(!git_dir.join("packed-refs").exists());
    assert_eq!(
        watched(dir),
        [git_dir.join("HEAD"), git_dir.join("refs/heads")]
    );
}

#[test]
fn a_commit_after_pack_refs_is_seen_through_the_watched_ref_directory() {
    let repo = tempfile::tempdir().unwrap();
    let dir = repo.path();
    git(dir, &["init", "-q", "-b", "main"]);
    git(dir, &["commit", "-q", "--allow-empty", "-m", "one"]);
    git(dir, &["pack-refs", "--all"]);
    let git_dir = dir.canonicalize().unwrap().join(".git");
    assert!(!git_dir.join("refs/heads/main").exists());

    // Every watched path exists, so Cargo does not rerun on every build.
    assert_eq!(
        watched(dir),
        [
            git_dir.join("HEAD"),
            git_dir.join("packed-refs"),
            git_dir.join("refs/heads"),
        ]
    );
    // The next commit writes a loose ref into the watched directory.
    git(dir, &["commit", "-q", "--allow-empty", "-m", "two"]);
    assert!(git_dir.join("refs/heads/main").exists());
}

#[test]
fn a_packed_nested_branch_watches_the_nearest_existing_directory() {
    let repo = tempfile::tempdir().unwrap();
    let dir = repo.path();
    git(dir, &["init", "-q", "-b", "feature/nested/deep"]);
    git(dir, &["commit", "-q", "--allow-empty", "-m", "one"]);
    git(dir, &["pack-refs", "--all"]);
    let git_dir = dir.canonicalize().unwrap().join(".git");
    assert!(!git_dir.join("refs/heads/feature").exists());

    assert_eq!(
        watched(dir),
        [
            git_dir.join("HEAD"),
            git_dir.join("packed-refs"),
            git_dir.join("refs/heads"),
        ]
    );
    git(dir, &["commit", "-q", "--allow-empty", "-m", "two"]);
    assert!(git_dir.join("refs/heads/feature/nested/deep").exists());
}

#[test]
fn a_linked_worktree_watches_its_own_head_and_the_common_refs() {
    let repo = tempfile::tempdir().unwrap();
    let main = repo.path().join("main");
    std::fs::create_dir(&main).unwrap();
    git(&main, &["init", "-q", "-b", "main"]);
    git(&main, &["commit", "-q", "--allow-empty", "-m", "one"]);
    let linked = repo.path().join("linked");
    let linked_path = linked.to_str().unwrap();
    git(
        &main,
        &["worktree", "add", "-q", "-b", "slice", linked_path],
    );
    let common = main.canonicalize().unwrap().join(".git");

    // HEAD lives in the worktree's own git directory, refs in the common one.
    assert_eq!(
        watched(&linked),
        [
            common.join("worktrees/linked/HEAD"),
            common.join("refs/heads"),
        ]
    );
}

#[test]
fn a_detached_head_watches_head_only() {
    let repo = tempfile::tempdir().unwrap();
    let dir = repo.path();
    git(dir, &["init", "-q", "-b", "main"]);
    git(dir, &["commit", "-q", "--allow-empty", "-m", "one"]);
    git(dir, &["checkout", "-q", "--detach"]);
    let git_dir = dir.canonicalize().unwrap().join(".git");
    assert_eq!(watched(dir), [git_dir.join("HEAD")]);
}

#[test]
fn without_git_nothing_is_watched() {
    let dir = tempfile::tempdir().unwrap();
    assert!(build_script::git_head_paths(&dir.path().join("missing")).is_empty());
}
