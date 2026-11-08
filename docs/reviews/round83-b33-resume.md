# Round 83 B33 resume

Status: implemented and locally validated, **not reviewed and not merged**.
The owner explicitly requested a parked branch when the Claude review cannot run.

Branch: `fix/round83-condition-inputs-privacy`.
Trunk verified and fetched: `origin/main` at
`d4cf24cd33311cac62a8692dc5cc1f02bf9c364d` on 2026-11-08.
Origin: `git@github.com:ephes/graphyard.git` (fresh SSH clone).
Both scout issues remained on this trunk; neither slice was skipped.

## Scope and order

1. `graphyard-condition-duration-domain`: enforce inclusive 1–10,080 minute
   window/breach limits at model/admin/seed and Python/Rust evaluation/query
   boundaries. Keep breach > window and extended anchor lookback. Invalid
   historical rows fail before any query, count as failed evaluations, publish
   critical `evaluation error: ValueError`, retain last value, and leave
   enabled/configuration unchanged for explicit correction. Migration 0015
   adds validators without rewriting data; the conservative Rust schema floor
   and its fixtures/tests now include that migration.
2. `graphyard-private-condition-response-cache-policy`: Django's standard
   private/no-store/no-cache/immediate-expiry policy on authenticated condition
   list/detail and the owner host/service index. Anonymous status/name visibility
   and access rules remain unchanged; tests cover the session logout journey.

README, condition/runtime docs and CHANGELOG Unreleased match the implementation.
All new fixtures are synthetic. Existing unrelated publication tests that used
zero breach durations now use a valid one-minute duration. Generated Python/Rust
fixtures and offline parity include invalid durations and a sparse extended breach.

## Validation

- New Python regressions on the baseline: 23 failed, 2 passed. The new Rust
  historical-row regression also failed on the old runtime before its fix.
- Focused Python suite: 136 passed.
- Full Python suite: 1,140 passed, 7 existing PostgreSQL-only tests skipped
  (inventory admission/projection and spec-apply concurrency, outside this bundle).
- `just lint`, including new files explicitly: passed Ruff, format, rustfmt,
  clippy with warnings denied. Tool caches redirected into the workspace as needed.
- `just typecheck`: passed mypy and workspace/all-targets cargo check.
- `just schema-check`: passed Django system and missing-migration checks.
- Offline `just rs-parity`: 8 rounds, 19 conditions, zero unmasked differences.
- Python dependency audit and `cargo deny --locked check`: passed.
- `cargo build --release --locked`: passed.
- Full Rust suite: 289 passed, no failures, with `RUST_TEST_THREADS=1`.
  The initial parallel run exceeded wall-clock bounds in seven unrelated
  collector/HTTP deadline tests under load. All pass serially. Intermediate
  migration-floor expectation failures were synchronized before the final run.

## Review blocker and finishing steps

The required `claude-review-loop` invocation with `--model claude-opus-5-5
--effort high` returned `PROVIDER_ERROR` / `api_error` immediately:
“Failed to authenticate: OAuth session expired and could not be refreshed”.
No code was reviewed; there is no CLEAN verdict and no findings to classify.
No retry or substitute reviewer was used. Review artifacts remain local under
`../.cache/review-b33-r1/` in the original workspace.

After authentication is restored, review the entire implementation against
`origin/main` (the branch contains the parked commit, so a worktree-only review
would omit it). Use the installed `claude-review-loop` skill to prepare the full
review delta. Drive repairs/re-review up to four rounds while findings shrink;
stop on a recurring required finding. Follow scout bundle B33 and the owner's
constraints. Before any merge, fetch; rebase if trunk moved and rerun the local
checks. Only a clean or suggestion-only reviewed result may merge locally with
`--no-ff` into main. Run the full local checks again after merging before pushing
main, then verify with `git ls-remote`.

No PRs, remote branch deletion, Actions runs, deploys, releases, tags, host access,
work-app writes or changes under `~/projects`. This feature-branch push is solely
the owner's explicit unfinished-work parking exception. Use `[skip ci]` in the
parking commit message to avoid starting paid workflows.

## Finisher outcome (round 84)

B33 reviewed by the Claude finisher (Opus): no Critical/Warning findings.
Suggestion only: for a historical invalid row whose invalid value is submitted
unchanged in admin, `clean()` may repeat the field validator's message. Full
lint, typecheck, schema-check, Python and serial Rust suites and offline parity
rerun outside the sandbox before the `--no-ff` merge to `main`. Not deployed.
