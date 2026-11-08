# Round 84 resume

Work is parked on `fix/r84-private-inventory`; do not merge until the required
review completes. Origin is `git@github.com:ephes/graphyard.git`, default `main`.
Starting main: `fa5af7448661a03eb9f944c79d5c53f2e5ce792c`.

## B30 implemented, review blocked

- `graphyard-inventory-duplicate-key-refusal`: reproduced on current main and
  fixed with an object-pairs hook after existing streaming budget checks.
- `graphyard-private-inventory-response-cache-policy`: reproduced on current
  main and fixed with one response middleware covering inventory readers,
  including errors and login redirects. Public condition policy is unchanged.
- New regression run before fixes: 11 failed, 1 passed (repeated array entries).
- Targeted inventory/SBOM/release suite: 243 passed after fixes.
- Python lint (repository pre-commit hooks), mypy (both commands), Django system
  and missing-migration checks passed. Full local check results follow below.

Opus review invocation through the installed `claude-review-loop` harness used
`--model claude-opus-5-5 --effort high --record-baseline`. It returned
`PROVIDER_ERROR`: **Failed to authenticate: OAuth session expired and could not
be refreshed**. No reviewer findings or verdict exist. The owner explicitly
requires parking the branch and stopping on this condition. No merge, default
branch push, PR, deployment, tag, release or Actions operation was performed.

After restoring Claude authentication, inspect this branch and run:

```sh
python3 ~/projects/agent-stuff/codex/skills/claude-review-loop/bin/claude-review-loop \
  --repo "$PWD" --run-dir "$(mktemp -d)/review" \
  --baseline-ref fa5af7448661a03eb9f944c79d5c53f2e5ce792c \
  --record-baseline --model claude-opus-5-5 --effort high \
  --lock-dir ../.cache/review-locks --ledger-dir ../.cache/review-ledger \
  --slice-id graphyard-r84-b30
```

The baseline above includes the parked commit in review; a default worktree-only
review after committing would otherwise review no implementation changes. Supply
caller-authored context with the slice requirements and verification results.
Fix/re-review required findings under the owner's four-round/repeated-finding
stop rule. Only a completed passing review permits merging. Fetch immediately
before merging; if main moved, rebase and rerun checks. Merge with `--no-ff`, run
full local checks after merging and before pushing only main. Do not use Actions.

## Remaining bundles, not implemented or re-verified

Read their full descriptions and validation notes in
`~/workspaces/ws-work-planning/reviews/scout-round84.md`, in this order:

- B31: `graphyard-version-page-read-generation`, then
  `graphyard-version-page-batched-host-evidence`.
- B32: `graphyard-release-decoding-preallocation-boundary`, then
  `graphyard-release-malformed-source-isolation`.

Re-verify each against fresh main first and skip any already fixed. Use synthetic,
platform-independent regressions proven to fail without each fix; update README,
`docs/inventory.md` and Unreleased notes. B31 requires real isolated SQLite and
PostgreSQL snapshot interleavings and constant-query/selected-host evidence tests.
No feature branch push except parking unfinished work; no paid Actions fallback.

## Full-check outcome before parking

`just test`: Python **1152 passed, 7 skipped**. Rust reached the collector unit
suite (**67 passed, 5 failed**) and failed these elapsed-time assertions:

- `executors::tests::ha_json_budget::ha_deadline_covers_headers_and_body`
  (headers returned after 2.291 s).
- `executors::tests::page_probe::deadline_bounds_dripping_body`
  (returned after 3.496 s).
- `executors::tests::page_probe::deadline_bounds_dripping_headers`
  (returned after 3.617 s).
- `executors::tests::page_probe::default_deadline_is_three_request_timeouts`
  (returned after 3.679 s).
- `executors::tests::unifi::continuously_arriving_unifi_body_hits_collection_deadline`
  (exceeded the 2 s assertion).

These Rust files are unchanged. Baseline reproduction was not attempted after
the authentication stop instruction; do not assume these failures are resolved.
The sequential full-check runner stopped at `just test`, so Rust check/clippy/fmt,
remaining Rust tests, cargo-deny, standalone parity, Python dependency audit and
release build remain unverified. Python lint/types/schema checks were separately
completed successfully. No PostgreSQL contracts ran for this parked bundle.

Local artifacts (outside the repo): `../b30-red.log`, `../b30-checks.log`,
`../review-b30-r1b/result.json` and `../review-b30-r1b/events.jsonl`.
Tool caches and review locks were redirected under the authorized workspace.
