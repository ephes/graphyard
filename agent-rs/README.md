# graphyard-agent (Rust)

A Rust port of the graphyard agent (`manage.py start_agent`, run by
`graphyard-agent.service`). The contract is [`PLAN.md`](PLAN.md); the Python
agent in `src/graphyard` stays the reference implementation and the rollback
target. Nothing runs this code in production yet: the deployment selects it
only through a systemd drop-in that later slices add (PLAN.md 8.3), and
deploys ship `agent-rs/` only as a release archive.

Status: slices S1 (scaffold), S2, S3a, S3b, S4a, S5, S6, S7a, S7b, S8, S9a, S9b, S10a, S10b (first release `agent-rs-v0.1.0`, see
[Published releases](#published-releases)) and S15a (the `evaluate-conditions`
subcommand, release `agent-rs-v0.2.0`); release `agent-rs-v0.3.0` carries the
fixes of scout rounds 79 to 85, release `agent-rs-v0.4.0` those of scout
rounds 87 to 99, release `agent-rs-v0.4.1` (the first one built locally) the
later agent fixes, release `agent-rs-v0.4.2` the agent fixes merged after it,
release `agent-rs-v0.4.3` the Home Assistant entity metadata and climate
attributes of the env scan, release `agent-rs-v0.4.4` the agent fixes merged
after it (CHANGELOG). S1: the workspace, the toolchain pin,
the cargo-deny policy, the CI `rust` job, the `just rs-*` recipes, the shared
model types (`graphyard_core::model`) and the fixture pipeline with its drift
test. S2: the CPython compatibility helpers (`graphyard_core::pycompat`), the
environment settings with django-environ semantics and the SQLite pragma
values (`graphyard_core::settings`), and the typed spec config knobs
(`graphyard_core::spec_config`), each pinned by its fixture. S4a: the metric
point normalisation (`graphyard_core::normalize`), the line protocol of
`influxdb-client` 1.50.0 and the pure part of `write_points`
(`graphyard_core::line_protocol`, log records as values in
`graphyard_core::logline`), and the InfluxDB write client with the lazy
configuration check (`graphyard_influx::InfluxWriter`,
`graphyard_influx::config`), pinned by `normalize.json` and
`line_protocol.json`; the write tests replay every fixture case against fake
servers. S5: the
condition evaluation (`graphyard_core::conditions`: `evaluate_condition`
over a `SampleSource`, `evaluate_samples` on samples already read), pinned
by `conditions.json`. Multi-series diagnostics use the shared
[structural tie-break and quoted-label contract](../README.md#conditions-that-match-several-series);
the corpus includes delimiter collisions, missing versus empty tags, Unicode and
escaped quotes in both input orders.

Slice S8 makes `graphyard-agent` the agent (PLAN.md 3): the CLI of
`manage.py start_agent` plus the Rust-only flags, the scheduler loop, the
metrics and conditions runs with executor isolation and heartbeats, the
stdout/stderr report lines, signals and `--run-once`, `--check-schema`,
`--version` and the shadow mode `--influx-write-file`; see
[Running the agent](#running-the-agent). The runs are pinned by `runs.json`
(the Python runs with stubbed executors and condition samples).

Slice S15a adds the subcommand `graphyard-agent evaluate-conditions
[--condition-id N]` (PLAN.md 11.4), the port of `manage.py
evaluate_conditions` that replaces it when the Python agent is retired; see
[Evaluating conditions once](#evaluating-conditions-once). Pinned by the
`condition_id` scenarios of `runs.json`, the subcommand tests
(`crates/graphyard-agent/tests/evaluate_conditions.rs`), an e2e case and
three parity-harness rounds.

Slice S9a adds the offline parity harness (PLAN.md 9.4, `just rs-parity`,
CI job `parity`): the Python agent (`manage.py start_agent --run-once`) and
the Rust agent run on two copies of one seeded database against their own
fake collector endpoints and fake InfluxDB, round by round, and every
unmasked difference fails the run; see [Parity harness](#parity-harness).

Slice S9b adds the process e2e of the binary
(`crates/graphyard-agent/tests/e2e.rs`, PLAN.md 9.3: a `--run-once` over
every collector family against fake servers with every row, heartbeat,
registry row and written line checked, the loop's due selection, SIGTERM
and SIGINT while a spec waits on a stalled server, the write file and the
schema check's missing tables), the shadow-run tooling in `parity/`
(`shadow_prepare.py`, `shadow_record.py`, `shadow_compare.py`, PLAN.md 11.2;
tested by `tests/test_agent_rs_shadow.py` on two local databases) and the
operator documentation [`docs/agent-rs.md`](../docs/agent-rs.md).

Slice S4b adds the condition queries in `graphyard-influx` (PLAN.md 6.5):
the lookback window of a condition, the v2 Flux text and request, the
decoding of InfluxDB's annotated CSV, the v3 SQL request with its byte cap
and deadline, and the `auto` fallback, plus `datetime.fromisoformat`,
`base64.b64decode` and `int / int` in `graphyard_core::pycompat`. Pinned by
`flux.json`, `sql_v3.json` and `isoformat.json`; the end-to-end cases run
against a loopback fake InfluxDB that replays the responses the fixture
generator served to the Python code.

Slice S10a adds the release pipeline (PLAN.md 8.2): the workflow
`.github/workflows/agent-rs-release.yml` (replaced on 2026-11-18 by the local
`scripts/local_release.py`), the checks in
`scripts/release_checks.py` and the commit stamp of `--version`
(`crates/graphyard-agent/build.rs`); see [Releases](#releases).

Slice S6 adds the collectors' HTTP layer in `graphyard-collect::http`
(PLAN.md 6.1, 6.2): httpx 0.28.1 as `services.py` configures it, on hyper's
HTTP/1.1 connection API. One `HttpClient` per spec execution with httpx's
request headers, auth and a cookie jar; connections opened like httpcore
(`getaddrinfo` through libc on the blocking pool, every address in turn)
except that all addresses share one connect timeout (see
[Collector TCP connection budget](#collector-tcp-connection-budget), PLAN.md
D-14), with the timeout per TLS handshake and socket read/write; httpx's URL model
(`HttpxUrl`: `urlparse`, the redirect join, request target and `Host`), its
`raise_for_status` texts internally, decoders and exception classes; the total deadline
with the abandoned-lookup cap (`run_with_deadline`), the JSON reader, the
page-probe redirect loop and automatic redirects. Pinned by
`http_errors.json` and fake-server tests; the executors follow in S7a/S7b.
The [HTTP admission contracts](../docs/agent-rs.md#http-request-header-authority)
document explicit Host/Cookie precedence, cookie-domain and retention limits,
and the decoder-count/raw-byte envelope. These security admissions are pinned
by synthetic loopback regressions in `http/tests/admission.rs`.

Slice S7b adds the first executors in `graphyard-collect::executors`
(PLAN.md 6.3): `http_page_probe` (the probe under the total deadline, five
timing and status points, two failure points on the warning path) and
`unifi_device_traffic` (legacy and UniFi OS login with redacted error
summaries, CSRF header and cookies, controller-origin redirects, a shared byte
budget and total deadline, the device and
interface lookup, receive/transmit rate points). Executors deliver their
points through a `MetricSink` (InfluxDB write and registry touch, wired up in
S8) and return the Python `(status, ingested, skipped, message)` tuple as a
`SpecOutcome`, or an `ExecutorError` for what Python lets escape. Pinned by
`page_probe.json` and `unifi.json`, whose executor scenarios ran the Python
executors with the real httpx client on `MockTransport` and are replayed on
fake servers with a recording sink.

Slice S7a adds the Home Assistant and HTTP JSON executors
(`graphyard-collect::executors`, PLAN.md 6.3): `home_assistant_sensor`,
`home_assistant_env_scan` and `http_json_metric` on the same interface; the
HA executors also take the metrics run's `RunState` (warn-once keys,
compiled regexes). The pure parts are `graphyard_core::home_assistant` (HA
state normalisation, subject and metric mappings on `fancy-regex`) and
`graphyard_core::json_path`. Pinned by `ha_mapping.json`, `json_path.json`
and `ha_json_executors.json`, whose scenarios ran the Python executors on a
scripted transport and are replayed here against a fake server; deadline,
TLS and per-run tests use real sockets.

Slice S3a adds the store core in `graphyard-store` (PLAN.md 5): the DB thread
owning the connection, Django's connection setup and the graphyard pragmas,
the schema contract and `check_schema`, Django's datetime column conversion,
spec and condition loads and updates, and the heartbeat upsert with its lock
retry.

Slice S3b adds the registry touch (`Store::touch_registry`, PLAN.md 5.5): the
batched `touch_registry_from_points` of graphyard `4d5baf1` without a bound
subject, which reads the registry rows once per call and writes only missing,
changed or stale (60 s) rows in one transaction, pinned by the
`registry_cases` of `store_ops.json` including Django's statement sequence.

The store admits at most 64 running/queued DB jobs per actor through an async
semaphore. Excess callers wait before sending their captured operation; cancelling
such a waiter sends no write. Admitted jobs own their permit until completion,
including when the caller stops waiting. This bounds actor-owned payloads, not
the number or payload size of application futures waiting for admission.
`Store::close` closes admission on all clones, wakes permit waiters with
`ThreadGone`, and orders Stop after every accepted send. Later calls fail without
running; the closer owning the join handle joins the draining thread without
holding the admission mutex. Repeated close is harmless; if another closer already
owns the join handle, it may return before that closer finishes draining. Actor panic drops queued
jobs and replies, releasing permits and returning `ThreadGone` to callers.

Registry lookups and last-seen updates split large batches at the connection's
SQLite variable limit. Subject type/ID pairs and encounter order stay distinct;
all creation/update chunks share one transaction and roll back together. Once
queued on the store actor, an operation completes even if its awaiting caller is
canceled. The real registry cancellation test gates a transaction after subject
and host creation, checks isolation through another connection, and verifies
complete commit or complete rollback after an injected service failure.

Rust heartbeat projection compares observation timestamps while holding the
writer reservation. Older observations cannot replace current status, error,
details or `updated_at`; equal times retain the first committed projection.
Successful observations, including delayed/equal-time ones, retain the maximum
`last_success` without changing a newer projection. If SQLite ignores an update
that would advance `last_success`, the store returns a storage error and rolls
back the transaction. Fresh transitions still publish normally. Independent
store handles exercise reordered commits.

The slice table is PLAN.md section 12; the test coverage per slice is
[`TESTMAP.md`](TESTMAP.md).

The Rust Python-compatibility integer parser checks the 4,300-digit limit
before removing digit separators. Plain digit runs are parsed directly from
the input. The bounded JSON evidence decoder admits at most 4,096 items per
array. After framing the document, it refuses the first excess item before
scanning that item again.

## Layout

```
agent-rs/
  Cargo.toml            workspace; release profile (thin LTO, stripped)
  Cargo.lock            committed; every build uses --locked
  rust-toolchain.toml   pinned toolchain 1.99.0 (also the MSRV)
  deny.toml             cargo-deny policy (licenses, advisories, sources)
  PLAN.md               the contract
  TESTMAP.md            Python test -> Rust counterpart, per slice
  crates/
    graphyard-core/     pure logic, no I/O: model types (S1); pycompat,
                        settings, spec_config (S2); normalize, line_protocol,
                        logline (S4a); conditions (S5); json_path,
                        home_assistant (S7a); UniFi helpers (S7b)
    graphyard-store/    rusqlite store: DB thread, schema contract, datetime
                        codec, specs, conditions, heartbeats (S3a); registry
                        (S3b)
    graphyard-influx/   InfluxDB write client and config check (S4a),
                        query clients (S4b)
    graphyard-collect/  HTTP layer (S6: `http`) and the five executors
                        (`executors`: page probe and UniFi in S7b, HA and
                        JSON metric in S7a)
    graphyard-agent/    the binary (S8): cli, scheduler, runs, collectors
                        (real executors, sink, samples), app (startup,
                        signals, shutdown), logging; evaluate (the
                        `evaluate-conditions` subcommand, S15a)
  fixtures/
    generate.py         fixture driver (`just rs-fixtures`)
    gen_*.py            builder modules, one or more per slice
    *.json              generated fixtures, embedded by the Rust tests
    schema/             the Django schema contract (gen_schema.py): schema.sql,
                        from which every store test builds its database, and
                        agent_tables.json
  parity/
    run_parity.py       offline parity harness (`just rs-parity`): corpus,
                        rounds, comparison
    fakes.py            scripted collector endpoints and fake InfluxDB
    settings_parity.py  Django settings of the Python agent in the harness
    shadow_prepare.py   shadow run: database copy, T0, D-11/D-13 scan (stdlib only)
    shadow_record.py    shadow run: samples both databases and units (stdlib only)
    shadow_compare.py   shadow run: report against production (stdlib only)
  scripts/
    release_checks.py   release workflow checks (version, GLIBC floor, smoke, pack)
```

## Requirements

- The pinned toolchain via rustup: `rustup toolchain install` in `agent-rs/`
  reads `rust-toolchain.toml`. A `cargo` that is not the rustup proxy (for
  example Homebrew's) ignores the pin; put `~/.cargo/bin` first in `PATH`.
- `cargo-deny` 0.20.2 for `just rs-deny` (CI installs the pinned release
  and checks its sha256).
- `uv` for the fixture generator (the Python project's environment).
- Docker only for `just rs-build-linux`.
- `openssl` (the CLI) for `just rs-parity`, which makes the TLS certificate
  of its fake UniFi controller.

## Commands

From the repository root:

| Recipe | What it does |
| --- | --- |
| `just rs-test` | `cargo test --workspace --locked` |
| `just rs-lint` | `cargo fmt --check` and `cargo clippy -D warnings` |
| `just rs-fmt` | format the workspace |
| `just rs-deny` | `cargo deny --locked check` |
| `just rs-fixtures` | regenerate every fixture from the Python code |
| `just rs-parity` | debug build, then the parity harness (`--keep` keeps the databases and per-round output) |
| `just rs-build` | release build for this host |
| `just rs-build-linux` | release build for macmini (`x86_64-unknown-linux-gnu`, glibc 2.36 in `rust:1.99.0-bookworm`), output in `target/docker-linux-amd64` |

`just test`, `just lint` and `just typecheck` run the Python checks and then
the Rust ones. `just rs-parity` runs separately (CI job `parity`).

Shadow append I/O runs on Tokio's blocking pool, one queued/running job per
writer. Cancellation retains the job's admission and file lock until completion;
filesystem writes and sync are not interruptible. Startup and later suffix
validation each admit 64 MiB, 250,000 total lines and five seconds of cooperative
monotonic scan work; a single batch outside that envelope is refused before
writing. Over-budget captures, including a peer-grown suffix that strands a
slower writer until restart, remain nonqualified and unchanged;
stop writers and rotate, or qualify the preserved history offline. See the
[shadow framing and recovery contract](../docs/agent-rs.md#complete-shadow-batches-and-recovery)
for uncertain acceptance and operator recovery.

## Running the agent

```bash
graphyard-agent --db /opt/apps/graphyard/site/src/django/db.sqlite3        # the loop
graphyard-agent --run-once --db db.sqlite3                                 # both runs once, exit 0/1
graphyard-agent --check-schema --db db.sqlite3                             # read-only, exit 0/1
graphyard-agent --run-once --influx-write-file /tmp/writes.lp --db copy.sqlite3   # shadow: no InfluxDB writes
graphyard-agent --version
```

The Python flags are unchanged: `--metrics-interval N`,
`--condition-interval N` (defaults from
`GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS` /
`GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS`), `--disable-metrics` (alias
`--disable-ha`), `--disable-conditions`, `--run-once`. The settings come
from the process environment only (the unit's `EnvironmentFile`; no `.env`,
D-9). Values the agent reads must be valid UTF-8, including values reached
through `$` proxies. Invalid text stops startup and names the original
setting without printing its value. Unused environment entries are ignored.
Proxy cycles report a recursion error for the original setting before looking
up a name twice. Acyclic proxy chains allow up to 1,000 lookups per setting.
The Rust Influx client configuration's debug output redacts the URL and token
and includes the configured request timeout.
The agent's settings debug output shows numeric settings and the effective
Influx mode. Client construction still uses the configured credentials.
`--db` defaults to `db.sqlite3` in the working directory, which must
exist and pass the schema check (D-2); the agent never creates it.
If neither loop has a representable next deadline, the scheduler stays idle
until SIGINT or SIGTERM.

stdout carries the start line, `Metric specs: ...`, `Condition eval: ...` and
`graphyard-agent interrupted, exiting`, each written promptly by a bounded
output worker that may drop lines under backpressure (see "Bounded output").
stderr carries
the log records as bare messages (INFO and above from the agent's crates,
warnings from libraries), `CommandError: ...` and the `Metric collection
failed: ...` / `Condition eval failed: ...` lines. Exit codes: 0 for
a successful `--run-once`, SIGINT and SIGTERM; 1 if an enabled one-shot loop
reports failed work or a run error, or for a `CommandError`, an invalid environment value, a missing database or a failed
schema check; 2 for a usage error. `--influx-write-file PATH` appends every
batch the agent would write with a `# write <utc> points=<n> bytes=<b>`
header, its line protocol and a matching `# commit` trailer, then syncs and
counts it as written; queries still go to InfluxDB.

Condition writes use the attempt start time as a watermark: stale or tied
publications (including failures) are ignored. A failure advances
`last_evaluated` and preserves `last_value`. Breach coverage checks every adjacent
gap and the final carry against `CONDITION_DATA_STALE_WARNING_SECONDS`, matching
Python and the shared condition/store fixtures.

The Rust store decodes enabled metric specs directly from SQLite in name order.
If a row cannot be decoded, the store returns the first error and discards the
partial result.

The Rust agent checks that each spec’s next run time fits in SQLite before
claiming or executing it, even in manual mode. An overflowing schedule
counts as a failed spec and leaves its row untouched. Later specs still run.
The heartbeat includes the failure in its totals and can show a fixed
diagnostic with the spec ID among its five samples.

Due-only metric runs claim the observed `next_run_time` before invoking a
collector. Claims use a 3–300 second lease (the clamped interval), renewed every one third
of that duration until the complete collector/sink/registry future returns.
Renewal and completion compare the exact persisted claim and captured name,
enabled state, type, interval and stored config. Lost ownership cancels the
collector without publishing a result or heartbeat; renewal storage errors fail
the run. Process death or a dropped run stops renewal; recovery waits until the
last persisted claim expires. A process suspended beyond its lease, or database contention delaying renewal
past expiry, can still lose ownership, and cancellation cannot recall a remotely accepted write.
The Python scheduler follows the same lease and renewal contract (round 85) from a
background thread; it cannot interrupt a running collector, so a lost renewal
refuses its sink/registry publication and completion instead. The initial claim
of both agents also compares the captured definition. Do not run the two
schedulers together for the same specs.
Explicit non-due/manual runs still execute every enabled spec regardless of due
time, with completion fenced by the captured definition and original schedule.
Superseded completions contribute no run counts, errors or heartbeat evidence;
when there are no accepted completions or schedule failures, the heartbeat is
untouched. The Rust agent counts every
accepted critical or warning outcome and retains only the first five eligible
messages in spec order (critical outcomes and warnings with a message). A fixed
`Collection superseded spec_id=<id>` event retains execution diagnostics. Metrics
already emitted during collection cannot be recalled. Python and Rust share
claim/recovery/completion fixtures and run only synthetic collectors in tests.

### Evaluating conditions once

```bash
graphyard-agent evaluate-conditions --db db.sqlite3                    # every enabled condition
graphyard-agent evaluate-conditions --condition-id 7 --db db.sqlite3   # only condition 7
```

The port of `manage.py evaluate_conditions` (PLAN.md 11.4): one conditions
run evaluates the enabled conditions in primary key order, or only the enabled
condition with the given id, and publishes each condition result.
Only full runs update the global `condition_evaluator` heartbeat. Targeted
runs leave it untouched even when the id selects nothing. stdout `Condition evaluation completed (<total>).` and
exit 0, or `Condition evaluation completed with failures
(<failed>/<total>).`, stderr `CommandError: <failed> condition evaluation(s)
failed.` and exit 1. A failure outside a condition (loading the conditions,
the heartbeat) exits 1 with one stderr line `Condition
evaluation failed: <class>: <error>` where Python prints a traceback; a
missing database or a failed schema check exits 1 as at the agent's startup
(D-2); a usage error exits 2. `--condition-id` is Python's `int()` like the
intervals (an id beyond the 64-bit key range selects nothing, as in
Django). The settings come from the process environment only (D-9), so the
operator call on macmini passes the unit's environment:

```bash
systemd-run --wait --pipe --quiet -p User=graphyard -p Group=graphyard \
    -p EnvironmentFile=/etc/graphyard/graphyard.env \
    -p WorkingDirectory=/opt/apps/graphyard/site/src/django \
    /opt/graphyard-agent/<version>/graphyard-agent evaluate-conditions \
    --db /opt/apps/graphyard/site/src/django/db.sqlite3
```

The subcommand installs no signal handlers (SIGINT and SIGTERM end it like
the Python command) and does not take the agent's flags.

## Parity harness

`agent-rs/parity/run_parity.py` (PLAN.md 9.4) runs both implementations on
identical inputs and compares what they did:

```bash
just rs-parity                 # cargo build, then the harness
just rs-parity --keep          # keep the work directory (databases, stdout/stderr per round)
uv run python agent-rs/parity/run_parity.py --rust-binary agent-rs/target/debug/graphyard-agent
```

Two databases built from `fixtures/schema/schema.sql` get the same corpus:
every spec type (Home Assistant sensor and env scan with macmini's
production mapping, HTTP JSON metrics with basic and bearer auth, page probes
with redirects, a redirect loop, slow, dripping and oversized bodies, UniFi
legacy and UniFi OS login over TLS with `verify_tls: false`), config errors,
an unknown spec type, a disabled spec and condition, and conditions for every
branch of the evaluation (thresholds, several series, staleness, the anchor
carry, a sample at the window start, a non-host `host_filter`, an unknown
operator, a `tags_filter` that is no object, a v3 query error). Each agent
gets its own fakes (`fakes.py`), which answer by request index; the fake
InfluxDB places its samples relative to the `stop` of each query. The first
five rounds run `manage.py start_agent --run-once` (settings
`parity/settings_parity.py`) and `graphyard-agent --run-once --db` at the
same time with `INFLUX_API_MODE` `v2`, `v2`, `v3`, `auto` (the Flux endpoint
answers 404) and finally without `INFLUX_TOKEN`. Three more rounds run
`manage.py evaluate_conditions` and `graphyard-agent evaluate-conditions
--db` on the same databases (S15a): every enabled condition (two fail, so
both exit 1 with the `CommandError` line), one condition by
`--condition-id` in `v3` mode, and the id of the disabled condition.

After each round the harness compares the exit codes, the rows of the six
agent tables, the stdout lines, the operator log lines on stderr (with the
`CommandError` line), and every request the fakes
received (target, headers, body; written lines parsed). Only the
documented masks apply: `now`-based timestamps, page-probe durations, the
query bounds (the window length is compared) and the fakes' port numbers.
The Python results must also match the statuses the corpus was written for
(and, in the `evaluate_conditions` rounds, the exit code and summary line),
so a corpus that stops exercising its branches fails too. A failing run
prints every difference per round and exits 1. Fake listeners close even if
setup fails partway through. The temporary directory is removed by default.
Use `--keep` to retain any files created before a failure. If a worker cannot
launch an agent or decode its output, the harness waits for both workers,
reports the affected agent and exception, and exits 2. The helpers are tested
in `tests/test_agent_rs_parity.py`.

## Shadow run

Before the cutover the binary runs beside the Python agent on a copy of the
production database with `--influx-write-file` (PLAN.md 11.2). The tools in
`parity/` use only the Python standard library, so they run with macmini's
`python3`: `shadow_prepare.py` makes the copy and records T0,
`shadow_record.py` samples both databases and both units every minute,
`shadow_compare.py` compares the record and the written points with what the
Python agent wrote to InfluxDB. The procedure with the exact commands is in
[`docs/agent-rs.md`](../docs/agent-rs.md#shadow-run).

## Fixtures

The Python code is the oracle (PLAN.md 9.2). `fixtures/generate.py`
discovers every `fixtures/gen_*.py` module, calls the builders of its
`GENERATORS` mapping (fixture file name → function) under the Django test
settings, and writes the files (JSON, and `schema/schema.sql` as text). It
re-executes itself with `PYTHONHASHSEED=0`, removes `DJANGO_*`, `GRAPHYARD_*` and `INFLUX_*`
variables and keeps django-environ from reading `src/django/.env`, so a
developer environment cannot change a fixture. Files are rendered with
`indent=1`, ASCII escapes, the key order the builder wrote, and no NaN or
infinity (builders encode non-finite floats explicitly).

```bash
just rs-fixtures                                        # rewrite all fixtures
uv run python agent-rs/fixtures/generate.py --check     # exit 1 on drift
uv run python agent-rs/fixtures/generate.py --list      # fixture names
```

`tests/test_agent_rs_fixtures.py` (part of `uv run pytest`) runs `--check`
and fails on a stale, missing or orphaned fixture, so a Python change that
alters pinned behaviour cannot merge without regenerated fixtures and the
matching Rust change. A slice adds a fixture by adding a builder module (or
a `GENERATORS` entry in its existing one), running `just rs-fixtures` and
loading the file in Rust with `include_str!`.

Rust tests parse fixtures with `serde_json` (`preserve_order`,
`arbitrary_precision`) through the helpers in
`crates/graphyard-core/src/fixture.rs` (the store tests through
`crates/graphyard-store/tests/common`).

## Schema contract

The agent runs no DDL; Django's migrations own the schema (PLAN.md 5.1).
`fixtures/gen_schema.py` migrates a temporary database and writes
`fixtures/schema/schema.sql` and `fixtures/schema/agent_tables.json` (the
columns of the six agent tables and the minimum graphyard migration, the last
one whose operations touch an agent model). It also generates
`fixtures/schema/storage_contract.rs`: column affinity/nullability/primary keys
and unconditional unique identity keys, compiled into the readiness check.
Verification requires ordinary SQLite tables for the six agent tables and
`django_migrations`. Views and other object types are reported as incompatible
storage before column or migration-marker checks on those objects.
The foreign-key catalog must include a single-column reference from
`service_registry.host_id` to `host_registry.id`.
Verification reads one deferred SQLite snapshot, never mixes migration generations
and never issues DDL or changes journal settings. Stop the agent for migrations;
a readiness check does not make live migration safe. Incompatible storage fails
before collectors or metric sink calls; repair it through Django migrations or restore
the migrated schema. Extra indexes and nullable or generated registry columns
are supported. An ordinary `NOT NULL` registry extension must supply a
database default. Preflight diagnoses absent and literal `NULL` defaults
without evaluating default expressions.
`tests/test_agent_rs_schema_contract.py`
fails when a migration changes them; then run `just rs-fixtures`, update
`REQUIRED_COLUMNS` / `MIN_GRAPHYARD_MIGRATION` in
`crates/graphyard-store/src/schema.rs` (its tests compare them with
`agent_tables.json`) and the store code that uses the changed columns.

## Releases

Releases are built and published locally by `scripts/local_release.py`,
driven by `local-release.json` (PLAN.md 8.1, 8.2); there is no release
workflow and no CI run (the tag-triggered
`.github/workflows/agent-rs-release.yml` was removed on 2026-11-18). The same
program releases mailgun-relay, opsgate and the nyxmon agent; ops-deploy's
`rust/scripts/release.py` follows the same approach. Deploys install only
such a release archive, pinned by version and sha256; nothing builds on the
host.

Needs Docker with working `linux/amd64` containers (on an Apple silicon Mac:
colima with `vmType: vz` and `rosetta: true`; the build checks that `docker
run --platform linux/amd64 ubuntu:22.04 uname -m` prints `x86_64`), `uv`,
`git`, `cargo` (for `cargo metadata`) and an authenticated `gh` 2.74.0 or newer.

- **Release**: bump the workspace `version` in `Cargo.toml` (and
  `Cargo.lock`), add the CHANGELOG entry, merge to `main` and push, then from
  a clean checkout of that commit run `just rs-release` (builds and checks,
  publishes nothing) and `just rs-release-publish` (see below).
- **Dry build**: `just rs-release --compare-released` builds a version whose
  tag already exists; `publish` refuses such a build.

`build` refuses a worktree with changes or untracked files, a commit that is
not on `main` on GitHub, a non-empty output directory and an untagged
`--compare-released` or a tagged plain build. Then:

1. On the host, the old workflow's `fixtures` job: `uv run --frozen pytest`
   over `tests/test_agent_rs_fixtures.py`, `tests/test_agent_rs_release_checks.py`
   and `tests/test_local_release.py`.
2. Image phase, the only one with network: two `ubuntu:22.04` images (base
   pinned by the digest pulled at the start), one of the Docker engine's own
   platform and one `linux/amd64` (glibc 2.35, the floor below macmini's
   2.39). Each gets `binutils ca-certificates curl gcc git libc6-dev python3`,
   `ca-certificates` at least at the USN-8944-1 version and a CA bundle without
   the 2026-10-21 incident roots (checked after the APT installation that
   brings the CA update, before the rustup, toolchain and dependency
   downloads), rustup 1.29.2
   (sha256-checked), the source as `git archive` of the build
   commit (`agent-rs/` and `LICENSE`), the pinned toolchain and `cargo fetch
   --locked`.
3. Checks job on the engine's own platform (under Rosetta, emulation
   artefacts could fail tests that are about the code, not the target):
   `cargo fmt --all --check`, clippy with `-D warnings`, `cargo test
   --workspace --locked`.
4. Release job on `linux/amd64`: `cargo build --release --locked --target
   x86_64-unknown-linux-gnu` with `GRAPHYARD_AGENT_GIT_COMMIT` set to the
   build commit, then `scripts/release_checks.py`:

| Check | Rule |
| --- | --- |
| `version` | the crate version from `cargo metadata`; the tag must be `agent-rs-v<that version>` |
| `glibc-floor` | `objdump -T`/`-p`: no `GLIBC_*` symbol version above 2.35, `NEEDED` only glibc and libgcc (`libc`, `libm`, `libgcc_s`, `ld-linux-x86-64`, ...) |
| `smoke` | `graphyard-agent --version` prints exactly `graphyard-agent <version> (<build commit>)` and exits 0 |
| `pack` | deterministic `graphyard-agent-<version>-x86_64-unknown-linux-gnu.tar.gz` (`graphyard-agent` 0755 and `LICENSE` 0644 at the root, root:root, commit time as mtime) and its `.sha256` in `sha256sum` format, checked with `sha256sum --check` |

Both jobs run with `--network none`, no mounts, and memory, process and time
limits. The checks and build containers have distinct names that include the
same invocation ID as the temporary images. Each Docker create has a 60-second
deadline. If its result is lost, cleanup checks for the job-specific ownership
label and exact name, then removes the matching container by full ID if Docker
already reports it. Each Docker cleanup command has a 15-second deadline.
Removal commands discard their output, and the ownership and image lookups
accept up to 4 KiB. A temporary image tag that was never built is skipped
instead of being reported as a failed removal. Cleanup failures are reported on stderr alongside the original build
error, when present.

The archive, its `.sha256`, `build.json` (commit, sha256, image digest, package
and toolchain versions) and `notes.md` (the release notes, naming the commit
and the sha256 the deployment pins) land in
`agent-rs/target/local-release/graphyard-agent-<version>/`.

Remote facts (is the commit on `main`, does the tag exist) are read from
GitHub, and the tag is created there, through `gh api --hostname github.com`
on `ephes/graphyard`: nothing is pushed with git, so no git remote, URL rewrite
or `GH_HOST` decides where the tag or the release goes (a gh `api_host`
override for github.com is refused; `main` is the branch `refs/heads/main`,
resolved exactly). Before creating a tag, `publish` checks the archive against
`build.json`, stages a private copy of the readable regular `notes.md`
and checks that `gh` is 2.74.0 or newer (older versions report no asset digests).
It then creates the
annotated tag `agent-rs-v<version>` on the build commit (only when that commit is
on `main`; an existing tag must be annotated and point at that commit, it is
never moved), creates the GitHub release with exactly the archive and its
`.sha256` (`gh release create --verify-tag`; a semver pre-release such as
`0.4.0-rc.1` becomes a GitHub pre-release), then downloads both again and
compares them byte for byte in 1 MiB blocks. It succeeds only when afterwards the same
release (same release and asset ids) is published, with the expected
prerelease flag, exactly those two assets and GitHub's sha256 of each equal
to the built one. A draft (left by an interrupted `gh release create`) is
refused: check it, delete the draft by hand (the tag stays) and run
`publish` again. It never replaces an asset and can be rerun until that end
state is reached. Each GitHub metadata command has a 60-second deadline,
and each release create, upload or download command has a 30-minute deadline.
After a timeout, rerun `publish` to read back the tag and release and resume
under the same checks, including the draft rule above. Fetch the tag afterwards
with `git fetch --tags`. The
output directory is staged privately and moved into place in one rename, so
a concurrent build never mixes into it.

`--version` names the git commit `build.rs` embeds: the
`GRAPHYARD_AGENT_GIT_COMMIT` environment variable when set (the local release
sets it to the build commit, `just rs-build-linux` to the checkout's `HEAD` because the
container sees no `.git`), else `git rev-parse HEAD`, else `unknown`; a value
that is not 7 to 40 lowercase hex digits fails the build.

### Published releases

Deploys pin the version and the archive's sha256 (ops-control host vars,
slice S11c). macmini has had `agent-rs-v0.2.0` installed since slice S12
(2026-10-22). It ran there as the S13 shadow from 2026-10-22 to
2026-10-23 (PLAN.md 11.2) and was the selected agent from the S14
cutover of 2026-10-23 (PLAN.md 11.3) until 2026-11-12, when the
`graphyard_core` deploy of ops-control `10de3426` installed and selected
`agent-rs-v0.3.0` (T1 15:18:42 UTC). Since 2026-11-15 the `graphyard_core`
deploy of ops-control `281d408f` ran `agent-rs-v0.4.0` (T1 07:08:36 UTC).
Since 2026-11-18 the deploy of ops-control `7f7cf38d` (graphyard source pinned
to `e6384ce`) ran `agent-rs-v0.4.1`, the first release built locally
(T1 16:42:05 UTC). Since 2026-11-21 the deploy of ops-control `81ce2c3f`
(graphyard source pinned to `2308723`) ran `agent-rs-v0.4.2` (T1 18:25:43
UTC). Since 2026-11-24 the deploy of ops-control `76553901` runs
`agent-rs-v0.4.3` (first start 16:32:16 UTC; its 24 h gate passed on
2026-11-25); 0.4.2 stays installed as the rollback target.
`agent-rs-v0.4.4` is published (2026-11-25) and not yet deployed.

| Tag | Commit | Archive sha256 |
| --- | --- | --- |
| [`agent-rs-v0.1.0`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.1.0) | `19f56e666173d9cb51639269e1219fcb7edd3e12` | `42d6dbb5b931599c1b35ab7f19a29823bf9583d65b45e2259a69b5dbc64b80e2` |
| [`agent-rs-v0.2.0`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.2.0) | `17a929f240ddf3f63fc7a2ae404dd3c041e93838` | `d8efcb78fd09f5047d5e747696bc7966c94e70de8a44bbe1d5d34111a7a058d4` |
| [`agent-rs-v0.3.0`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.3.0) | `eba6ea16af73c322232b28b1035876557e7d9871` | `55f947fd84edfacf04dd0f28fe00db3018fd4cc08b69bf96c1a28ebbb61643f3` |
| [`agent-rs-v0.4.0`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.4.0) | `3d6616df0df4dd7d4ca0e3e89df69ff56b2dc091` | `1528155d1c6fc8208ac22109ca158232c0302605fda1540b7721dc07a078eb7a` |
| [`agent-rs-v0.4.1`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.4.1) | `e6384ce7bd69456a03cc62f7d26aeb2366cbfe71` | `cb9826f953eb1b38ee90a9fe4c0955d28c8f61e7c77e8dc644643a97c3fdbc45` |
| [`agent-rs-v0.4.2`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.4.2) | `230872353e0a30a11d9b62494d42819171009121` | `d0f4748b782e4d7566c2a6a99b532ac72d1713daedde1de451ff0e9a5a890dde` |
| [`agent-rs-v0.4.3`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.4.3) | `670c01e4c5b07d1335c4b347bd0f49a2e5dc0a3f` | `00160481f04bc4a3cc893efe3e53bfa4105b746682a19329eea33dfd4bef839b` |
| [`agent-rs-v0.4.4`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.4.4) | `fd0423b1a5e14f1fcc5155f31cce5ec9a5efcf20` | `9edb53d6d467cdacc676d47e958a441abfaa7dd3f3a574751c2b0dd787c3dcc7` |

`agent-rs-v0.1.0` (slice S10b) was checked after publishing: the downloaded
archive matches its `.sha256` file and is byte-identical to the archive of a
`workflow_dispatch` dry build of the same commit; it holds `graphyard-agent`
(0755, 8938360 bytes) and `LICENSE` (0644), owned by root; `release_checks.py
glibc-floor` passes on the extracted binary (newest symbol version
`GLIBC_2.34`; `NEEDED`: `libc`, `libm`, `libgcc_s`, `ld-linux-x86-64`); the
workflow's smoke test printed `graphyard-agent 0.1.0 (19f56e666173d9cb51639269e1219fcb7edd3e12)`.

`agent-rs-v0.2.0` (slice S15a, the `evaluate-conditions` subcommand) was
checked after publishing: the downloaded archive matches its `.sha256` file;
it holds `graphyard-agent` (0755, 8958360 bytes) and `LICENSE` (0644), owned
by root; `release_checks.py glibc-floor --max 2.35` passes on the extracted
binary (newest symbol version `GLIBC_2.34`, libraries OK); the workflow's
smoke test printed `graphyard-agent 0.2.0 (17a929f240ddf3f63fc7a2ae404dd3c041e93838)`.

`agent-rs-v0.4.1` (the first release built and published locally by
`scripts/local_release.py`) was checked by the program itself: the archive
was downloaded again after the upload and compared byte for byte, and the
published release's assets carry GitHub's sha256 of the built files; a
separate download of the public URL gave the same sha256. It holds
`graphyard-agent` (0755, 9210616 bytes) and `LICENSE` (0644); the glibc
floor check (newest symbol version `GLIBC_2.34`, libraries OK) and the smoke
test (`graphyard-agent 0.4.1 (e6384ce7bd69456a03cc62f7d26aeb2366cbfe71)`) ran
in the `linux/amd64` release container.

`agent-rs-v0.4.2` was checked the same way by the program (re-download and
byte comparison, GitHub's asset sha256); a separate download of the public URL
gave the same sha256 and matches its `.sha256` asset. It holds
`graphyard-agent` (0755, 9210360 bytes) and `LICENSE` (0644), root:root;
`release_checks.py glibc-floor --max 2.35` on the extracted binary passes
(newest symbol version `GLIBC_2.34`, libraries OK); the smoke test printed
`graphyard-agent 0.4.2 (230872353e0a30a11d9b62494d42819171009121)`.

`agent-rs-v0.3.0` (the fixes of scout rounds 79 to 85, rusqlite 0.41) was
checked after publishing on 2026-11-12: the downloaded archive matches its
`.sha256` file; it holds `graphyard-agent` (0755, 9007608 bytes) and `LICENSE`
(0644), owned by root; `release_checks.py glibc-floor --max 2.35` passes on
the extracted binary (newest symbol version `GLIBC_2.34`, libraries OK); the
workflow's smoke test passed for `graphyard-agent 0.3.0 (eba6ea16af73c322232b28b1035876557e7d9871)`.

### Collector TCP connection budget

All resolved addresses for a single TCP connection share one
`request_timeout_seconds` budget. A quick failure falls through
to the next address in resolver order. If the budget expires, the agent
reports `ConnectTimeout`. DNS runs under the collector's total deadline,
and TLS and individual socket reads and writes have separate timeouts.
The retained Python agent (httpcore) gives every address its own connect
timeout; this intentional divergence is listed under D-14 in PLAN.md.

### Influx response budgets

Flux exchanges have a 30 s total deadline and 10 MiB decoded response limit;
writes have a 30 s deadline and 64 KiB decoded response limit, including error
bodies. Error diagnostics retain at most 1 KiB. Limits are independent of the
inactivity timeout and also cover success bodies. Budget failures close the
response; writes report uncertain acceptance and are never replayed. Only a
complete bounded Flux 404 permits auto-mode SQL fallback. See
[Influx transfer limits](../docs/influx_grafana.md#transfer-limits).

Both one-shot loops run before failure status is returned; disabled loops are
ignored and warning-only outcomes succeed. Continuous mode still isolates errors
and keeps scheduling. The parity corpus deliberately includes failed work and
therefore expects exit 1 for each agent one-shot round.

## Bounded output

The Rust binary submits stdout reports and stderr diagnostics to separate worker
threads, using nonblocking bounded standard-library channels. Each stream holds
at most 128 queued records plus one in flight, each at most 8 KiB UTF-8 including
its newline (overlong records end in `...[truncated]`). The final formatter
streams `Display` values into that byte envelope and stops formatting on overflow.
It renders C0, DEL and C1 controls as visible `\xNN` escapes and Unicode line/
paragraph separators as `\u{2028}`/`\u{2029}`, with exactly one physical newline
per event. Existing literal backslash escapes remain unchanged. This projection
also covers direct command diagnostics and stdout reports; stored condition names,
messages and semantic/parity fixtures remain unchanged. A slow journal or unread
supervisor pipe therefore cannot stall collection, heartbeats or signal handling.
FIFO order is preserved within each stream; there is no cross-stream ordering.
Panic messages (for example a collector panic caught by its run's
`catch_unwind`) use the same path: `logging::install()` sets a panic hook that
submits one escaped `thread '<name>' panicked at ...` record instead of std's
synchronous fd-2 write; it prints no backtrace. An uncaught panic leaving
`main` first gets the same bounded 200 ms drain, then ends the process with
exit code 101 as before.

A full queue drops the **new** record. Closed/failed sinks also lose records;
`logging::dropped_records()` reads cumulative stdout/stderr loss counts without
I/O. When a sink recovers, its owner writes
`graphyard-agent output_dropped stream=stdout|stderr total=N` directly, without
recursive logging. A permanently unread sink cannot display its count.
Command exit allows both streams together 200 ms to drain, then abandons their
workers, including on SIGTERM/SIGINT and one-shot/validation failures. Pending
output can be lost on exit; the exit status still describes the command result.
No deployment is performed; adoption requires rebuilding and rolling out the agent.

Regressions: `cargo test -p graphyard-agent --test log_pipes --locked` runs the
actual binary with prefilled unread stdout/stderr pipes, real fast synthetic
HTTP collection and SQLite heartbeats, and verifies SIGTERM exit. The logging
unit test blocks a sink, fills the exact queue, checks losses/byte bounds, and
releases it to verify the recovery notice and bounded shutdown wait. The same
`log_pipes` test binary also checks that a caught panic is one escaped stderr
record and does not block on a prefilled unread stderr pipe, and that an
uncaught panic's message is drained before the process exits.
`cargo test -p graphyard-agent --test log_records --locked` captures real stderr
from the installed logger with controls and an incrementally generated huge
`Display`, and from the actual agent evaluating a synthetic hostile condition name.
The byte ceiling bounds final formatting/queue memory, not allocations performed
inside a caller's `Display` implementation or existing semantic message builders.

## Failure diagnostics

For deadline-enabled metric specs, Rust config validation checks the request
timeout once and uses the admitted value to derive the default total timeout.
If the request timeout is invalid, validation reports that error first and
checks the total using the standard request timeout as the fallback.

Collector persistence follows README's bounded error contract: safe allowlisted
class or numeric HTTP status, never transport exception text, response bodies or
URL queries/userinfo. The low-level HTTP errors still retain their library text
internally; executors normalize it before returning an outcome. Collector HTTP
logs contain only method/status. Condition ERROR events report condition ID,
`evaluation`/`publication` stage, category and actual fallback publication success.
`failure_published=false` covers superseded/deleted/disabled/edited rows as well
as storage errors; later
conditions run even if that fallback fails. See `tests/diagnostics.rs` and
[TESTMAP](TESTMAP.md). No migration rewrites historical messages.

## Collector publication boundary

`AgentSink` sends each batch to Influx before touching the SQLite registry.
An HTTP rejection or a disconnect after receiving the write fails that spec,
leaves its registry rows untouched and does not retry the write within that
invocation. Later independent specs still run. A disconnect can leave remote
acceptance uncertain.

If Influx acknowledges the write but registry bookkeeping fails, the spec is
critical with zero reported ingested points even though Influx already accepted
them. The registry transaction rolls back in full; the remote write cannot be
rolled back and is not repeated within that invocation. A later scheduled or
explicit invocation may send those points again: this is not exactly-once
delivery across retries. Recovery belongs to the existing scheduler/operator,
without a new sink retry policy.

`crates/graphyard-agent/tests/collectors.rs` exercises these boundaries through
`run_metric_specs`, the real HTTP JSON executor, `AgentSink`, `InfluxWriter` and
`Store`, using a synthetic loopback Influx endpoint and temporary SQLite schema.
It covers HTTP rejection, disconnect, a targeted service-registry INSERT trigger
(after subject/host creation), sibling isolation and recovery after trigger
removal. Run it with `cargo test -p graphyard-agent --test collectors --locked`
from `agent-rs/`.

### Process death and uncertain sink acceptance

A process killed after its claim commits leaves that claim in SQLite. A fresh
process skips the spec before the last persisted lease expires and may execute
it once that timestamp is due; no heartbeat or completion is inferred from the
claim alone. Renewals stop with the process.

A kill after Influx acknowledges a batch but before registry publication leaves
no completed/healthy bookkeeping for that attempt. After lease expiry, recovery
can deliver the same observation again. Count the earlier accepted delivery as
uncertain local completion, plus the independent recovery delivery; neither
SQLite nor scheduler fencing rolls back the remote sink. This remains a
potentially repeated-delivery boundary, with no automatic within-run replay and
no exactly-once guarantee across process death.

`crates/graphyard-agent/tests/crash_journeys.rs` starts fresh test processes using
the production runner, HTTP JSON executor, AgentSink/InfluxWriter and Store.
Pipe gates stop after committed claim admission or acknowledged remote acceptance;
the parent kills and reaps the child, then starts new processes on the same
scratch SQLite DB at synthetic times before/at expiry. A loopback accepting sink
records complete requests and payloads; the fixed synthetic observation is
byte-identical across the uncertain attempt and recovery. The tests require no healthy/completed rows before recovery,
one eligible recovery delivery and no immediate second run at that same time.
The process journeys cover the initial persisted lease; the separate
`tests/lease.rs` checks live renewal, expiry and replacement fencing. These are
test-only trait wrappers; no production pause flags were added.

UniFi controller routes and explicit port selectors use the shared
[selector and transfer contract](../README.md#unifi-controller-transfer-policy):
validate a single literal site component and reject base URL query/fragment
syntax before login; select exactly one port with a JSON integer index (no
boolean/float/string coercion). The shared `unifi.json` corpus covers malformed
routes, reordered duplicate ports, typed identities and zero-write failures.
