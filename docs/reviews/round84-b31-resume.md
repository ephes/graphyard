# Round 84 B31 resume

Parked on `fix/r84-version-projection`, based on `main` at
`4cb16f6298ec797afbf99e2520662d8a952068b6`. Origin was verified as
`git@github.com:ephes/graphyard.git`. No merge is authorized until review and
blocked local checks pass. This branch is pushed only to park unfinished work.

## Re-verification and implementation

B30 (duplicate inventory keys and private cache policy) is already merged on
current main and was skipped. B31 remained reproducible:

- `graphyard-version-page-read-generation`: both real SQLite WAL modes returned
  installed 2.0 against registry 1.0 when another connection refreshed the cached
  release and ingested/pruned reports between the registry and host reads.
- `graphyard-version-page-batched-host-evidence`: 1/50/200-host regressions failed
  on the original code; each shared application/package report decoded four times.

Version pages now materialize registry, source count, host selector and comparison
rows inside the existing backend-correct read snapshot. They reuse `_host_states`
and pass loaded evidence to `rows_for_host`. Selected hosts restrict evidence in
SQL. Rendering runs after the transaction. Counts before status/search filtering,
sort order and 50-row pagination are preserved. README, inventory docs and
Unreleased notes match the change. New fixtures are synthetic and portable.

## Validation

- Targeted inventory projection/release/version suite: 67 passed, 2 PostgreSQL
  oracle skips under SQLite.
- Full Python suite: 1160 passed, 8 PostgreSQL oracle skips.
- Repository pre-commit Ruff hooks, both mypy commands and cargo check: passed.
- Django system and missing-migration checks: passed.
- Rust fmt/clippy and the full Rust test suite: passed.
- Cargo-deny: advisories, bans, licenses and sources passed.
- Offline Python/Rust parity: all eight rounds passed with zero differences.
- Frozen runtime/development Python audit: no known vulnerabilities. Its initial
  sandbox cache denial was resolved by redirecting UV_TOOL_DIR and caches into
  the authorized workspace.
- PostgreSQL initdb could not run: `shmget(..., size=56, ...)` returned
  `Operation not permitted`. No PostgreSQL contract ran. Run the new version-page
  generation race, projection isolation oracle and existing credential contracts
  on an isolated local PostgreSQL database outside this sandbox before merging.
- A release-profile Rust build was not run; no Rust source changed.

## Review blocker and resume

The installed claude-review-loop harness was invoked with
`--model claude-opus-5-5 --effort high --record-baseline`. It returned
`PROVIDER_ERROR` / `api_error`: **Failed to authenticate: OAuth session expired
and could not be refreshed**. No review verdict or findings exist. No subagents
were used. The owner explicitly requires parking and stopping on this condition;
B32 remains unstarted and was not re-verified. There is no Actions fallback.

After authentication is restored, review the full committed bundle, not merely
an empty worktree diff:

```sh
python3 ~/projects/agent-stuff/codex/skills/claude-review-loop/bin/claude-review-loop \
  --repo "$PWD" --run-dir "$(mktemp -d)/review" \
  --baseline-ref 4cb16f6298ec797afbf99e2520662d8a952068b6 \
  --record-baseline --model claude-opus-5-5 --effort high \
  --lock-dir ../.cache/review-slots --ledger-dir ../.cache/review-ledger \
  --slice-id r84-graphyard-version-projection
```

Supply caller-authored requirements and verification context. Follow the owner's
four-round/repeated-finding stopping rule. A passing review and completed local
checks are required before merging. Fetch immediately before each merge; if main
moved, rebase and rerun tests. Merge locally with `--no-ff`, rerun full local checks
before pushing only main, then verify with `git ls-remote`. No PRs or paid Actions.

Next, re-verify B32 on fresh trunk, in report order:
`graphyard-release-decoding-preallocation-boundary`, then
`graphyard-release-malformed-source-isolation`.

Local artifacts outside the repository: `../version-before.log`,
`../race-before.log`, `../python-tests.log`, `../python-lint.log`,
`../types-schema.log`, `../rust-checks.log`, `../python-audit.log`, and
`../review-version-1/result.json` / `events.jsonl`.

Workflow lesson: the existing OAuth stop rule correctly prevented an unreviewed
merge; no new process rule is needed. Promotion: incident.
