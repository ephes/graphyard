# Round 85 resume

Parked branch: `fix/r85-store-boundaries`. No merge is authorized until the
required review completes. Fresh SSH origin was verified as
`git@github.com:ephes/graphyard.git`; default branch is `main`.
Base: `4cb16f6298ec797afbf99e2520662d8a952068b6` (fetched before implementation).

## B12 implemented, review blocked

All three slices were re-verified against current main and remained applicable:

1. `graphyard-registry-sql-variable-budget`: use rusqlite's public connection
   limit API; chunk host/service and subject cross-product lookups and last-seen
   updates. Retain encounter order, cross-type identity, small-batch statement
   fixtures and the existing atomic write transaction. New synthetic regression
   lowers the variable limit to eight, creates/refreshes more distinct identities
   than that limit and verifies complete rollback after a later-chunk SQL fault.
   Before the fix it failed with `too many SQL variables`.
2. `graphyard-rust-heartbeat-clock-regression-fence`: compare observation times
   under the write reservation. Older/equal-time records preserve status, error,
   details and updated_at (first committed projection wins ties); successful
   evidence advances only the maximum last_success. Independent store handles
   reorder commits with a queue gate and a separate SQLite writer. The new test
   failed on old code when a delayed warning replaced newer OK status.
3. `graphyard-store-cancelled-caller-transaction-oracle`: preserve the existing
   actor contract. A manually polled public touch proves enqueue, then a SQLite
   trace gate pauses inside the actual transaction after host/subject inserts.
   Drop the caller, release the gate and verify complete commit or injected
   service-error rollback, FIFO settlement, reopened rows and writer-lock release.
   This is the report's coverage gap, not a claimed production cancellation bug.

Docs, test map and Unreleased notes match the changes. No schema/version change.
All new fixtures are synthetic and the new tests have no platform dependencies.

## Validation

- `just lint`: passed Ruff, formatting, rustfmt and workspace/all-targets clippy
  with warnings denied. The initial uvx cache denial was resolved with
  UV_TOOL_DIR, UV_CACHE_DIR and PRE_COMMIT_HOME under the workspace `.cache`.
  Clippy initially caught duplicate test-helper module loading; helpers now load
  once at the crate test root, and the gate passes.
- `just typecheck`: both mypy commands and workspace/all-targets cargo check pass.
- `just schema-check`: Django system check and missing-migration check pass.
- `just test` (includes `just rs-test`): Python 1,152 passed, seven existing
  PostgreSQL-only inventory/spec-apply oracles skipped; full Rust suite: 292 passed, no failures or skips.
  The new store suite also passed separately after the helper-module adjustment.
- `git diff --check`: passed.

No PostgreSQL instance was required by B12's SQLite store regressions. The seven
existing PostgreSQL-only checks above were not executed. No Actions fallback.
Local logs are outside the repo: `../b12-lint.log`, `../b12-typecheck.log`,
`../b12-test.log`, `../b12-store.log`.

## Review and finishing

The installed `claude-review-loop` harness ran with `--model claude-opus-5-5
--effort high --record-baseline`, and returned `PROVIDER_ERROR` / `api_error`:
**Failed to authenticate: OAuth session expired and could not be refreshed**.
No review occurred and no CLEAN verdict or findings exist. Review artifacts:
`../reviews/b12-r1/result.json` and `../reviews/b12-r1/events.jsonl`.
The owner explicitly requires parking the branch and stopping on this condition.
No merge, main push, PR, deploy, release, tag or Actions operation was performed.

After restoring authentication, review the entire parked implementation, using
its base so the committed implementation is not omitted from the review:

```sh
python3 ~/projects/agent-stuff/codex/skills/claude-review-loop/bin/claude-review-loop \
  --repo "$PWD" --run-dir "$(mktemp -d)/review" \
  --baseline-ref 4cb16f6298ec797afbf99e2520662d8a952068b6 \
  --record-baseline --model claude-opus-5-5 --effort high \
  --lock-dir ../.cache/review-locks --ledger-dir ../.cache/review-ledger \
  --slice-id r85-graphyard-b12
```

Supply caller-authored scope/validation context. Fix and re-review while findings
shrink, with at most four rounds and no recurring Critical/Warning finding.
Before merging, fetch; if main moved, rebase and rerun checks. Only after a
completed passing review merge with `--no-ff`, rerun all full local checks, push
main and verify with `git ls-remote`. Suppress CI on the push commit; do not use
paid Actions. Never delete remote branches.

## B13 remains unimplemented

Source inspection on current main confirmed all three findings still present;
none was skipped as already fixed. Stop occurred before any B13 implementation
or regression work. Read full slices and validation notes in
`~/workspaces/ws-work-planning/reviews/scout-round85.md`, in bundle order:

1. `graphyard-collection-claim-binds-current-definition`: Python and Rust initial
   claims still compare due/enabled only; bind the captured definition before
   external I/O, preserving renew/completion fences and parity.
2. `graphyard-python-collector-live-lease-parity`: Python still executes
   synchronously without claim renewal; add Rust-equivalent renewable ownership
   and cancellation on loss, preserving explicit manual-run policy. Validate with
   synthetic independent-process gated collectors, death/recovery and stale
   completion refusal.
3. `graphyard-unifi-device-identity-ambiguity`: both runtimes still choose the
   first matching name OR MAC; require deterministic exact MAC selection and
   refuse ambiguous name-only selection. Document the migration to MAC config.

Re-verify against fresh main before starting each slice. Use tests proven to fail
on the old implementation, synthetic fixtures and full local lint/types/tests.
No deploys, PRs, releases, tags, host access, work-app writes or Actions runs.
