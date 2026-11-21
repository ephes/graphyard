# Rust agent (`agent-rs/`)

`graphyard-agent` is a Rust port of the long-running graphyard agent
(`manage.py start_agent`, unit `graphyard-agent.service`). The Django web app
stays Python and keeps owning the database schema; only the agent process is
replaced. The binding contract is [`agent-rs/PLAN.md`](../agent-rs/PLAN.md);
build and test instructions are in [`agent-rs/README.md`](../agent-rs/README.md);
the Python test → Rust test map is [`agent-rs/TESTMAP.md`](../agent-rs/TESTMAP.md).

Status: the agent is implemented and tested, and it is the production agent
on macmini since the cutover of 2026-10-23 11:31:46 UTC (slice S14,
release `agent-rs-v0.2.0`; PLAN.md 11.3 "As run"). The 7-day soak runs from
11:41:37 UTC after the rollback drill. The shadow run (S13, 2026-10-22/23)
missed two lean goals narrowly and showed explained deviations, which the
owner accepted (PLAN.md 11.2). Release `agent-rs-v0.3.0` (the fixes of scout
rounds 79 to 85) replaced 0.2.0 on macmini on 2026-11-12 at 15:18:42 UTC,
and release `agent-rs-v0.4.0` (scout rounds 87 to 99) replaced 0.3.0 on
2026-11-15; 0.3.0 stays installed as the Rust rollback target. The deployment selects the Rust binary
through a systemd drop-in that the ops repositories add; the Python agent
stays installed as the rollback target until its retirement (S15b).

## Architecture

One process, one `current_thread` tokio runtime, the metrics and condition
loops sequential in one task exactly like Python (PLAN.md 3.2, 4.1):

| Crate | Role |
| --- | --- |
| `graphyard-core` | pure logic without I/O: CPython compatibility helpers, settings with django-environ semantics, spec config, metric normalisation, line protocol, JSON path, Home Assistant mappings, condition evaluation |
| `graphyard-store` | `rusqlite` on one dedicated DB thread: schema contract, Django's datetime conversion, spec/condition loads and updates, heartbeats, registry touch |
| `graphyard-influx` | InfluxDB writes (and the shadow write file), Flux and v3 SQL queries, `auto` fallback |
| `graphyard-collect` | the HTTP layer (httpx 0.28.1 semantics, deadlines, body caps, redirects, cookies, `getaddrinfo` resolution) and the five executors |
| `graphyard-agent` | the binary: CLI, scheduler, runs, logging, signals |

The agent writes only the six agent tables (`metric_collection_spec`,
`condition_definition`, `pipeline_heartbeat`, `subject_registry`,
`host_registry`, `service_registry`) and InfluxDB. It never touches the
ingest tokens, the web ingest path or Vector (PLAN.md 5.7). It verifies TLS
against the system CA store (D-12), reads only the process environment (no
`src/django/.env`, D-9) and writes stdout/stderr through bounded output
workers that never block monitoring and drop lines under backpressure (D-8).

## Schema contract

Django migrations own the schema. At startup and with `--check-schema`, the
Rust agent requires ordinary SQLite tables for its six agent tables and for
`django_migrations`. It reports views and other object types as incompatible
storage. The check verifies that every required agent column exists and that
the migration table contains
`graphyard.0015_alter_conditiondefinition_breach_minutes_and_more`, the conservative
migration floor (the last migration touching an agent model, including field
validators). It also checks each required column's SQLite affinity, nullability
and primary-key position, the auto-generated integer row identity, and the
unconditional binary unique keys used by scheduling, heartbeat and registry
identity lookups. Partial, expression and nonunique indexes cannot supply those
keys. Equivalent declared types with the same affinity are accepted (the rowid
key must remain `INTEGER PRIMARY KEY`); extra columns and indexes are accepted.
These storage expectations are generated from a migrated synthetic database in
`fixtures/schema/storage_contract.rs`, compiled into the agent and covered by the
fixture drift gate. An incompatible database exits 1 before collection or metric
sink calls, with a single diagnostic naming the failed contract properties. Stop the agent,
apply missing Django migrations or restore the damaged migrated schema, then
restart; the check itself never repairs storage.

All table, storage/key, migration-marker and journal-mode reads share one explicit
deferred read transaction on the opened connection. A concurrent atomic rebuild
cannot combine observations from different schema generations. SQLite's existing
busy timeout still applies; `--check-schema` issues no DDL or journal/settings
writes. This startup snapshot does not authorize live migration while collection
is running: stop the agent for operational migrations.

The rule for Python changes: `tests/test_agent_rs_schema_contract.py`
regenerates `agent-rs/fixtures/schema/` from the migrations and fails when a
migration changes an agent table. Run `just rs-fixtures`, update
`REQUIRED_COLUMNS` / `MIN_GRAPHYARD_MIGRATION` in
`agent-rs/crates/graphyard-store/src/schema.rs` and the store code that uses
the changed columns, in the same change. Likewise every behaviour of the
Python agent that the port reproduces is pinned by a fixture generated from
the Python code; `tests/test_agent_rs_fixtures.py` fails when the Python code
and a committed fixture disagree.

## Parity classes and divergences

Every difference between the two implementations has a class (PLAN.md 2.3):

- **P1 exact**: SQL effects on the agent tables, InfluxDB series identity
  (measurement, tags, field, timestamp), Flux and SQL query texts, statuses,
  counts, graphyard-authored messages, CLI flags, exit codes, stdout lines,
  environment parsing;
- **P2 equivalent**: internal library exception text, measured durations and
  other log lines. Persisted collector/condition failures now use the P1 bounded
  type/status contract in README; raw exception text is not persisted.
- **P3 intentional divergence**: the exhaustive list D-1 to D-15 in PLAN.md
  2.4. The operationally visible ones: SIGTERM ends the agent at the next
  await point with exit 0 (D-1); a database without the required schema stops
  the agent at startup instead of failing every run (D-2); non-standard JSON
  (`NaN`, UTF-16 bodies) from a collector endpoint is a decode error (D-4);
  user regexes run on `fancy-regex` (D-5); stdout/stderr go through bounded,
  non-blocking output workers that drop lines under backpressure and bound the
  exit drain (D-8); no `.env` file (D-9); log values have `\n`/`\r` escaped and
  the final output line escapes C0/DEL/C1 controls and U+2028/U+2029 within an
  8 KiB record limit (D-10); stored JSON that
  CPython cannot load fails only its spec or condition (D-11); system CA store
  (D-12).

## Operations

### Running

```bash
graphyard-agent --db /opt/apps/graphyard/site/src/django/db.sqlite3   # the loop
graphyard-agent --run-once --db db.sqlite3                            # both runs once
graphyard-agent --check-schema --db db.sqlite3                        # read-only, exit 0/1
graphyard-agent --version                                             # version and commit
graphyard-agent evaluate-conditions --db db.sqlite3                   # manage.py evaluate_conditions
```

The flags of `manage.py start_agent` are unchanged (`--metrics-interval`,
`--condition-interval`, `--disable-metrics`/`--disable-ha`,
`--disable-conditions`, `--run-once`). The settings come from the unit's
`EnvironmentFile`. Exit codes: 0 successful `--run-once`, SIGINT or SIGTERM;
1 failed enabled one-shot work, `CommandError`, invalid environment, database
or schema failure; 2 usage error. One-shot runs every enabled loop before
reporting failure; warnings alone succeed. Continuous mode keeps scheduling
after failures. Metrics and conditions run sequentially. The scheduler
checks the clock again after metrics, so a condition run that became due
during metrics starts before the next sleep. Between passes, the agent sleeps
until the earliest enabled-loop deadline, with a minimum 200 ms backoff.
SIGINT and SIGTERM still cancel the wait immediately.
`--influx-write-file PATH` is the shadow mode: every batch the agent
would write to InfluxDB is appended to `PATH` as a framed batch: a
`# write <utc> points=<n> bytes=<b>` header, line-protocol body and matching
commit trailer. Queries still go to InfluxDB.

### Evaluating conditions by hand

`graphyard-agent evaluate-conditions [--condition-id N]` (from release
0.2.0) replaces `manage.py evaluate_conditions` (PLAN.md 11.4): it evaluates
the enabled conditions (or the one with the given id) once, writes their
rows, and updates the global `condition_evaluator` heartbeat only on a full
run. Targeted checks leave the global status, counts and timestamps untouched.
A result can replace an observation only if its attempt started after the
stored timestamp and the evaluated definition still matches. The agent
compares timestamps as UTC instants at microsecond precision, including
older SQLite datetime formats.
The command prints `Condition evaluation completed (<total>).` and exits 0,
or prints the failures and
`CommandError: <n> condition evaluation(s) failed.` and exits 1. It reads
only the process environment, so on macmini it runs with the unit's
environment:

```bash
systemd-run --wait --pipe --quiet -p User=graphyard -p Group=graphyard \
    -p EnvironmentFile=/etc/graphyard/graphyard.env \
    -p WorkingDirectory=/opt/apps/graphyard/site/src/django \
    /opt/graphyard-agent/<version>/graphyard-agent evaluate-conditions \
    --db /opt/apps/graphyard/site/src/django/db.sqlite3
```

The documented manual call (ops-control `docs/GRAPHYARD_NYXMON_CHECKS.md`)
switches to this command with the Python retirement (slice S15b); until then
`manage.py evaluate_conditions` stays the documented one.

### Deployment, cutover and rollback

Release archives come from `agent-rs-v<version>` tags
(built locally by `agent-rs/scripts/local_release.py`, see `agent-rs/README.md`); the
first is `agent-rs-v0.1.0`, the one macmini runs `agent-rs-v0.4.0` (the
fixes of scout rounds 87 to 99; `agent-rs-v0.3.0`, the Rust rollback target
that stays installed, carried rounds 79 to 85,
`agent-rs-v0.2.0` added `evaluate-conditions`; `agent-rs-v0.4.1` is the first
release built locally);
their sha256 values are listed in `agent-rs/README.md`
("Published releases").
The ops-library role `graphyard_deploy` and the ops-deploy pyinfra service
install a pinned release under `/opt/graphyard-agent/<version>/` and select
it with the variable `graphyard_agent_impl` (`python` | `rust`), which adds
or removes the drop-in
`/etc/systemd/system/graphyard-agent.service.d/agent-impl.conf`. The base
unit (environment, working directory, user, restart policy) is the same for
both implementations. Rollback is `graphyard_agent_impl: python` and a
`graphyard_core` deploy; in an emergency on the host, remove the drop-in,
`systemctl daemon-reload` and restart the unit (PLAN.md 8.5). The operator
runbook is ops-control `docs/GRAPHYARD_RUST_AGENT.md`.

### Shadow run

Before the cutover the Rust agent runs about 24 hours beside the Python agent
on a copy of the database, reading production InfluxDB for conditions and
writing nothing to it (PLAN.md 11.2, approved with these limits: the copy in
a 0700 directory deleted afterwards, doubled collector traffic). The tools
are in `agent-rs/parity/`, standard library only, so they run with the host's
`python3` on macmini. Every production step takes the deploy lock and is
agreed with the coordinator first (PLAN.md 8.4).

1. Copy the database and record T0 (as `graphyard`):

   ```bash
   install -d -o graphyard -g graphyard -m 0700 /var/lib/graphyard-shadow
   sudo -u graphyard python3 shadow_prepare.py \
       --source /opt/apps/graphyard/site/src/django/db.sqlite3 \
       --shadow-dir /var/lib/graphyard-shadow
   ```

   It copies through SQLite's backup API (consistent while both production
   processes write), checks the copy, writes `shadow.json` (T0, enabled specs
   and conditions) and lists rows on which the two implementations would
   disagree (D-11 documents, D-13 storage classes); exit 2 means "look at
   them before starting". `--disable-spec NAME` disables a spec in the copy
   only (for example UniFi when the controller limits logins, R6).

2. Start the shadow agent as a transient unit with the production
   environment:

   ```bash
   systemd-run --unit graphyard-agent-shadow -p User=graphyard -p Group=graphyard \
       -p EnvironmentFile=/etc/graphyard/graphyard.env \
       -p WorkingDirectory=/var/lib/graphyard-shadow \
       /opt/graphyard-agent/<version>/graphyard-agent \
       --db /var/lib/graphyard-shadow/db.sqlite3 \
       --influx-write-file /var/lib/graphyard-shadow/points.lp
   ```

3. Record both databases and both units every 60 s (as `graphyard`, in
   tmux; stop with Ctrl-C at the end):

   ```bash
   sudo -u graphyard python3 shadow_record.py \
       --prod-db /opt/apps/graphyard/site/src/django/db.sqlite3 \
       --shadow-db /var/lib/graphyard-shadow/db.sqlite3 \
       --out /var/lib/graphyard-shadow/record.jsonl
   ```

   Each JSON line holds the rows that changed since the previous tick (spec
   rows without `config`, which holds secrets; conditions; heartbeats;
   registry) and the units' `systemctl show` and `/proc` memory numbers.

4. Compare (as root, for `docker`):

   ```bash
   python3 shadow_compare.py --shadow-dir /var/lib/graphyard-shadow \
       --save-prod-csv /root/graphyard-shadow-prod.csv \
       --json /root/graphyard-shadow-report.json
   ```

   It queries the points the Python agent wrote over the window through
   `docker exec graphyard-influxdb influx query --raw` (the container's CLI
   configuration; the tool handles no token) and compares them per spec with
   `points.lp`: series keys exact, Home Assistant points by series and
   timestamp exact, JSON and UniFi values paired by time within a tolerance,
   page-probe status sequences exact and timings as medians. From the record
   it compares spec status sequences, condition and heartbeat statuses,
   registry rows (timestamps masked) and the shadow unit's restarts and
   memory/CPU against the lean goals (PLAN.md 10). Findings are differences
   to explain (an endpoint that changed between the two samples) or bugs;
   notes are P2 differences for the report. `--fail-on-findings` makes
   findings exit 1.

5. Teardown: `systemctl stop graphyard-agent-shadow`, keep the report, and
   delete `/var/lib/graphyard-shadow` (it holds the spec secrets).

## Tests

- Rust: `just rs-test` (unit, fixture and fake-server tests per crate; the
  process e2e of the binary in `agent-rs/crates/graphyard-agent/tests/`:
  CLI validation and exit codes, `--check-schema`, `--run-once` over every
  collector family against fake servers with the rows, heartbeats, registry
  and written lines checked, the loop's due selection, SIGINT/SIGTERM during
  a stalled request, a blocked name lookup and a locked database, the shadow
  write file, `evaluate-conditions` with its summary lines, exit codes and
  `--condition-id`);
- Parity: `just rs-parity` (CI job `parity`, PLAN.md 9.4) runs
  `manage.py start_agent --run-once` and `graphyard-agent --run-once`, then
  `manage.py evaluate_conditions` and `graphyard-agent evaluate-conditions`, on two
  copies of one seeded database against identical scripted fakes and fails on
  any unmasked difference (`agent-rs/parity/run_parity.py`, see
  `agent-rs/README.md`);
- Python: `uv run pytest` includes the fixture drift test, the schema
  contract test, the release check tests, the parity harness helper tests
  and the shadow tool tests
  (`tests/test_agent_rs_shadow.py`, which run the three tools on two local
  databases with fake `systemctl`, `/proc` and `influx`).

## Crash recovery boundary

A persisted due-spec claim survives process death; its last renewed expiry is
the earliest recovery admission. A claim is not evidence of a completed or
healthy run. Influx acceptance precedes registry and spec-result publication,
so a kill between these boundaries can leave accepted bytes with no matching
healthy bookkeeping. A restarted agent waits for the lease, then may send the
observation again. This uncertainty is intentional; local transactions cannot
undo remote acceptance. See the [process-death contract](../agent-rs/README.md#process-death-and-uncertain-sink-acceptance)
for the isolated, pipe-gated regression harness and exact recovery assertions.
Those process journeys cover the initial persisted claim. Live renewal and
replacement fencing are checked separately by `tests/lease.rs`; renewed-lease
process-death coverage is not claimed. The initial claim also binds the loaded
definition, so an edit between load and claim never reaches external I/O
(`tests/lease.rs::claim_refuses_a_definition_edited_after_load`).
Rust lease operations share one captured definition per spec. Renewal
reuses that identity regardless of the size of the previous diagnostics. The
retained Python agent follows the same due-lease and renewal contract
(`tests/test_due_collection_claim.py`).

## Condition duration admission

Python and Rust require `window_minutes` and `breach_minutes` in the inclusive
range 1–10,080 (one week), before querying samples. Historical invalid rows
produce a failed evaluation with critical `evaluation error: ValueError`; their
configuration and last value remain intact for explicit operator correction.
A breach may be longer than the window: lookback includes the full breach and
carry-forward anchor. See [Condition Evaluation](../README.md#condition-evaluation)
and [Response privacy](../README.md#response-privacy) for configuration and web
response storage policy.

### Query selection admission

Python and Rust admit an Influx response only when every decoded sample matches
its issued metric, dimension/tag filters and half-open time range (`start <= ts < stop`).
Missing metrics and missing filtered dimensions also fail the entire query; empty
responses and differing unfiltered dimensions remain valid. Evaluation failures
retain the previous observed value. The parity corpus supplies matching filter
dimensions explicitly; the selection regressions separately inject mismatched
rows, including a valid prefix followed by invalid evidence.

## Writer admission and timestamp compatibility

The shared identity/dimension contract is documented in the root README under
“Shared writer admission”. Rust prepares at most 8 MiB of line protocol and one
bounded line at a time; the later joined body can add another 8 MiB. It retains
at most 32 diagnostic events with bounded identity/error excerpts, while rejection
counts include every invalid point. An over-budget batch fails before configuration,
HTTP or shadow-file effects and reports zero successfully written points. Registry
publication by AgentSink is skipped when writing fails. The request size limit
is Rust-specific; identity/dimension admission and probe URL privacy apply to both
runtimes. Query-bearing page-probe URLs require an explicit nonsecret
`source_entity_id` before transport, so removing query values cannot silently
merge distinct endpoints. Paths remain part of the default identity.

When normalization rejects points, Rust emits one summary before the HTTP
request or shadow-file append, including on writes that later fail. The
summary's `written_points` field counts admitted points in the prepared batch.
The write result reports the transport outcome.

The Rust writer renders response headers for HTTP error diagnostics. Successful
responses still pass the bounded body read before any points are counted.

The supported production baseline in this repository is InfluxDB v2; local dev
also supports InfluxDB 3 Core. A real disposable-sink oracle pins 2.7.12 and 3.12.2
using the actual Rust writer and `/api/v2/write?precision=ns`. At Python-compatible
microsecond precision, both accept the lower boundary
`1677-09-21T00:12:43.145225Z`. V2 accepts through
`2262-04-11T23:47:16.854775Z`; v3 accepts through
`2262-04-11T23:39:59.999999Z`. Adjacent unsupported microseconds and years 1/9999
are refused by the sinks. A mixed valid/invalid request returns an error, never
a successful written count; v2 may already have accepted its valid members, so
retrying the entire batch is unsafe.

With `INFLUX_API_MODE=auto`, both Python and Rust writers now admit only the
inclusive common domain, `1677-09-21T00:12:43.145225Z` through
`2262-04-11T23:39:59.999999Z` (epoch nanoseconds `-9223372036854775000` through
`9223371599999999000`). The comparison uses UTC and the microsecond precision
actually encoded on the wire; Python naive datetimes mean UTC. This is a static
admission policy because auto can write to either server, not server discovery.
Out-of-domain points follow the existing invalid-point policy: log and skip them,
write valid siblings in order, and count only written points. An all-invalid batch
returns zero without HTTP or shadow-file append. Rust shadow writes apply the same
policy. Explicit `v2`/`v3` modes keep their existing sink-side timestamp checks;
`v2` can still write its wider upper range. Query fallback is unchanged.

The disposable-sink oracle explicitly uses writer mode `v2` to bypass the auto
preflight and continue measuring the actual sink rejection/partial-write behavior.
No host/deployment inspection was performed.

Run the oracle only on disposable loopback servers with synthetic credentials:

```bash
docker run --detach --rm --name graphyard-r89-influx2 \
  -p 127.0.0.1:18290:8086 \
  -e DOCKER_INFLUXDB_INIT_MODE=setup \
  -e DOCKER_INFLUXDB_INIT_USERNAME=synthetic \
  -e DOCKER_INFLUXDB_INIT_PASSWORD=synthetic-password \
  -e DOCKER_INFLUXDB_INIT_ORG=oracle -e DOCKER_INFLUXDB_INIT_BUCKET=oracle \
  -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=synthetic-oracle-token influxdb:2.7.12
# In another terminal (InfluxDB 3 Core 3.12.2):
influxdb3 serve --node-id scout89 --object-store memory \
  --http-bind 127.0.0.1:18289 --without-auth
cd agent-rs
GRAPHYARD_TIMESTAMP_ORACLE=v2 cargo test --locked --test timestamp_oracle -- --ignored
GRAPHYARD_TIMESTAMP_ORACLE=v3 cargo test --locked --test timestamp_oracle -- --ignored
# After tests: stop the native v3 process and the disposable container.
docker stop graphyard-r89-influx2
```

`tests/timestamp_oracle.rs` uses only fixed loopback ports, a synthetic bucket and
synthetic credentials. Normal workspace tests leave it explicitly ignored.

### Recorder file and restart contract

The recorder appends only to a caller-owned regular 0600 file, refuses symlink
outputs and compares its opened inode with both input databases (including hard
links). The first successful snapshot of each new DatabaseRecorder session carries
`"baseline": true`; replay replaces that database's state before applying it.
Earlier events remain historical evidence; a baseline is not a collector run.
Legacy recordings without markers retain incremental replay compatibility, but a
legacy file concatenated across restarts cannot prove offline deletions: start a
new recording for qualification. Older comparators do not support baseline markers;
use the updated recorder and comparator together.

Recorder intervals must be finite and in `(0, 86400]` seconds; `--ticks` must
be a positive integer. Invalid options fail before any database or output open.
Each unit probe has a two-second monotonic budget and 64 KiB stdout cap; stderr
is discarded. Timeout, cancellation, excess output and helper failures emit only
`unit probe unavailable`. The recorder kills and reaps its dedicated helper
process group (including descendants holding the pipe); SIGTERM interrupts probe
polling. Missing unit evidence remains unavailable in qualification. The same
group retirement applies to `shadow_compare`'s production CSV helper. Where
`os.waitid` exists (e.g. Linux) the leader's exit status is read without reaping it,
so the group kill runs while its process-group ID is still reserved; an EPERM
from a zombie-only group (Darwin) counts as already gone and never hides the
original deadline/limit error. Wrongly shaped record lines (non-object `dbs`,
`units`, samples, tables or rows, non-list `_deleted`) fail with a location-only
`invalid record shape` error.

Shadow backups have a 30-second monotonic total budget, 50 ms SQLite busy waits
and bounded-page progress callbacks. Failure removes only the newly created
incomplete backup. A requested disable set commits atomically: any missing spec
rolls back every flag in that request.

### Preparation completion and evidence limits

Preparation stages a private `.prepare-*` generation. It syncs the database,
publishes `db.sqlite3` without replacing any target, syncs the directory, then
publishes and syncs `shadow.json` last. This v2 completion record contains
`preparation_version: 2` and `complete: true`. Scan findings still require review
(exit 2). Never launch a copy without its completion record. On failure or process
death, an incomplete generation is preserved and the next invocation refuses with
a recovery instruction: inspect/preserve that directory and retry in a **fresh
0700 directory**. No existing copy is automatically replaced or removed. Late
publication/sync failure can leave visible artifacts with uncertain durability;
a failure message does not mean rollback. Legacy metadata remains readable, but
has no durable completion guarantee; prepare new evidence with the updated tool.

The comparator admits at most 64 MiB per file, 128 MiB across metadata, LP,
JSONL and production CSV, 1 MiB per physical line, 500,000 combined input rows
(physical file lines plus parsed CSV rows), and 1,000,000 JSON history nodes
(including member names), with depth at most 64. Exact limits are accepted.
JSON nodes/depth are counted before decoding containers. Regular opened files
are checked by size and bounded reads, including growth;
limits include ignored/out-of-window evidence. Fetched CSV has a 30-second owned
process budget and the same file cap. Admission failure exits 1 before publishing
a qualification report; split a larger recording into meaningful complete windows.

Qualification controls are admitted before evidence I/O: tolerance must be finite
and nonnegative; alignment must be finite and between one nanosecond and 86400
seconds inclusive. Numeric point values, numeric LP fields and JSON observations
must be finite; point timestamps must fit signed 64-bit nanoseconds. Invalid
observations refuse the complete input set with a location-only diagnostic.
Finite negative/zero measurements remain supported; relative differences scale
before subtraction when opposite-sign finite values would overflow.

Nearest-time alignment uses ordered binary lookup, preserving the earlier-time
tie rule, first production point for duplicate timestamps, and reuse of production
points. Search is O(P log P + S log P) per series rather than P×S. Every selected
enabled spec needs observations on both sides; supported exact/aligned types also
need at least one comparable pair. Missing evidence is an `inconclusive` finding,
so `--fail-on-findings` fails even when both sides are empty. Explicitly disabled
specs remain excluded. Matching empty sets cannot qualify a required collector.

### Home Assistant collection envelopes

Rust HA collection admission and its fixed matching/output ceilings are specified
in the [HA configuration contract](../README.md#metric-collection-specs) and
[PLAN.md D-5](../agent-rs/PLAN.md). Admission precedes point construction; neither
limit exhaustion nor regex runtime failure publishes a partial batch or registry
update. HTTP timeout/body knobs do not increase synchronous work/output limits.
The Python reference fixtures continue to cover admitted ordinary batches;
Rust-specific boundary/exhaustion cases live in core `home_assistant/tests.rs`
and collector `executors/tests/ha_json_budget.rs`.

### HTTP cancellation and resolver admission

The Rust HTTP layer owns its connection task from spawn through response-body
retirement, including cancellation while waiting for headers. Production
contexts share four running-or-queued blocking resolver permits. Admission waits
inside the collector's total deadline; the blocking closure retains its permit
until it actually finishes, even after caller cancellation. Abandoned lookup
diagnostics remain available; IP literals bypass resolution. This limits
resolver jobs, not the number of application futures waiting for admission.

### Rust integer diagnostics

Invalid Rust integer literals include at most the first 200 characters of
the Python-compatible representation, including quotes and escape characters.
The formatter allocates only for that prefix and considers the full literal
when choosing the quote.

### Collector TLS trust-store readiness

The Rust agent loads system roots when it creates its HTTP context. If no
certificate is usable, verifying HTTPS connection setup fails before DNS or
TCP with the fixed critical diagnostic
`collector TLS trust store has no usable roots`. A partially usable store
still works with its admitted roots. Plain HTTP and collectors explicitly
configured with `verify_tls: false` can run with an empty store. Restart the
agent after restoring the system CA certificates.

### Flux materialization and sparse labels

Python and Rust admit at most 100,000 Flux records, 500,000 cells and 16 MiB
expanded cell storage (UTF-8 value/default and label bytes plus 64 bytes per map
cell). These limits supplement the 10 MiB wire cap. Exceeding a limit refuses the
entire query as `InfluxTransferError`, preserving the distinction from empty
results. Duplicate-column warnings contain only a fixed category and count.

Condition diagnostics visit present dimensions once and format only the selected
structural series; quoted labels, missing versus empty dimensions and tie order
remain shared with Python.

Rust shadow output requires Unix regular-file semantics. The final pathname
component must not be a symlink, FIFO, device or multiply linked file. New files
use mode 0600; existing admitted files are tightened to 0600 through the opened
descriptor. Parent directories must be trusted and remain operator-controlled.

### Complete shadow batches and recovery

New captures frame each batch as `# write <utc> points=<n> bytes=<b>`, exactly
`n` line-protocol lines totaling `b` UTF-8 bytes (including final newline), then
`# commit points=<n> bytes=<b>`. The writer syncs each completed batch before
crediting it. The comparison tool refuses incomplete framed evidence, publishing
no partial report. Earlier unframed captures remain readable; their historical
format has no commit-trailer guarantee. The writer verifies their declared point
count before reuse; comment-only preambles remain supported.

Cooperating writers hold the same opened data inode's OS lock over suffix
validation, complete append and sync. In-process and OS-lock admission share a
one-second wait budget, including blocking-pool queueing. Suffix validation,
append, flush and sync run on Tokio's blocking pool, with at most one queued or
running job per writer. The job owns its descriptor, in-process slot and OS lock
until it actually finishes, even when its async caller is canceled. Cancellation
can therefore leave acceptance uncertain; it does not interrupt filesystem I/O
or permit an overlapping append. No automatic replay is attempted. Process death
releases the OS lock. This contract requires local filesystem locking and trusted
parent directories. Never unlink, rotate, replace or externally truncate the
file while writers are running; locks do not exclude noncooperating writers.

Opening the capture validates the existing file once under the same lock (up to
five seconds of lock wait) before collectors start, so the first append of a
process only checks what cooperating writers added since. Invalid, over-budget
or locked captures remain nonqualified at offset zero; append must validate them
before any write rather than treating them as empty. Batches whose line
protocol rows start with `#` (an `INFLUX_MEASUREMENT` beginning with `#`, which
InfluxDB reads as a comment) are refused before appending, so they never make
the capture unreadable for later writers or the comparison tool.

Each startup scan and each later suffix scan admits at most **64 MiB**, **250,000
lines** (headers, points, trailers, comments and blank lines combined), and
**five seconds** of monotonic validation work. Bytes are admitted from locked file
metadata before reading; reads stay within that snapshot. Line and elapsed work
are checked before each line, elapsed work again after each bounded line and
before success. The time limit is cooperative: a stalled filesystem call or sync
cannot be interrupted. Startup lock wait has its separate five-second limit;
append lock/queue wait retains its one-second limit above. A framed batch whose
own bytes or lines (header, points and trailer) exceed this envelope is refused
before OS-lock wait or writing, so no single batch can make the capture
unscannable for peers or a later reopen.

Over-budget evidence is refused, never accepted as empty. Errors report the
budget and last complete offset; neither the capture bytes nor the writer's
previously qualified offset change. Repeated refusals do not incrementally
qualify an oversized history. A writer's own synced batches advance its offset,
so a capture can grow past the reopen envelope while it runs. Batches from
cooperating peers do not: if peers add more than one envelope between two
appends of a slower writer, that writer's suffix scan stays over budget and it
refuses every later append until restart, where the reopen scan applies again.
Plan rotation before those limits: stop all writers, preserve the full private capture,
and start a separate file. Qualify larger historical captures offline with
explicit operator-controlled resources; the agent never trims or resets them
and the comparison tool does not share this online validation envelope.

An incomplete tail refuses subsequent appends without modifying earlier evidence.
The error reports the last complete byte offset. Stop all writers, preserve a
private copy of the entire file, inspect its boundaries and explicitly truncate
to that offset (or start a separate capture). Never infer completeness from a
leading marker alone. A complete trailer identifies a structural batch boundary,
not authentication of externally modified evidence.

### Accepted writes and unambiguous evaluation evidence

Python and Rust credit a successful HTTP status after its bounded complete body
transfer regardless of body charset. Bounded error diagnostics are handled
separately (Rust decodes with replacement). Failed or over-budget transfers retain
uncertain acceptance and are never automatically replayed. Flux refuses duplicate column labels before
materializing records, emitting only `duplicate_columns count=<n>`. Identical
values in separate records and ordinary multi-table headers remain supported.

Both evaluators refuse conflicting values at the same series and microsecond
timestamp before staleness, duration or anchor decisions. Identical duplicates
(including signed zero equality) remain supported; failure cannot publish a
partial observed value. Cross-series structural ordering remains unchanged.

Rust Flux CSV parsing and sample conversion share the exchange's absolute total
deadline. Parsing checks between rows, yields after at most 32 rows or 64 KiB of
row/expanded-cell work, and checks again before returning; conversion yields in
32-record batches. One CSV row and one typed conversion remain synchronous under
the wire/field/output limits. Expiry keeps the existing Flux total-deadline
category and discards all partial records/samples. Python's synchronous CSV
library remains outside its async transfer timeout; the cooperative scheduling
change is Rust-specific.

### HTTP request header authority

Rust redirects preserve a custom `Host` on the same origin. A changed origin
uses the destination authority (including port and IPv6 brackets), as httpx
0.28.1 does. Authorization is still removed on cross-origin redirects except
for the existing same-host HTTP-to-HTTPS upgrade rule.

Explicit request/client `Cookie` headers, including an empty header, take
precedence over the per-execution jar. Redirects remove that explicit header
and reconstruct cookies from the jar for the destination, matching httpx.

### HTTP cookie scope admission

The Rust jar ignores cookies with an explicit single-label `Domain` (for example
`com`, `.com`, `..com` or `com.`), preventing transfer between unrelated sites
under that label. The check ignores all leading and trailing dots itself instead
of relying on the cookie parser's normalization; the jar's normal
domain/path/secure matching still applies. Host-only cookies (including local
names) and ordinary multi-label domain cookies remain supported. This preserves
Python's top-level-domain refusal for public hosts; it deliberately also refuses
explicit local single-label domains. This is not a full public suffix list check:
for example `co.uk` retains the pinned Python client's compatibility behavior.

Each spec execution retains at most 256 cookies and 64 KiB of combined UTF-8
name/value bytes across all responses and redirects. Replacements charge their
current size, not their history; expired entries still retained by `cookie_store`
count toward both limits. Domain/path metadata is separately bounded by the
100 KiB response-header ceiling and cookie count. An overflowing response leaves
the previous jar intact, closes its connection and fails collection with
`collector cookie jar limit exceeded`, before publishing any successful metric
or registry update (page probes retain their normal failure metrics). New specs
start with an independent empty jar. This finite retention policy is Rust-specific;
normal UniFi session cookies remain supported.

### HTTP compressed transport work limits

All Rust collector response paths admit at most four recognized Content-Encoding
layers (`gzip`, `deflate`, `identity`, case insensitive), including identity
layers. Names are checked before constructing any decoder or reading the body;
an excessive stack fails with `collector content encoding stage limit exceeded`.
The limit is Rust-specific. Two-layer compressed bodies remain supported;
unsupported tokens still pass through and truncated streams retain the existing
httpx-compatible behavior. Installed `max_body_bytes` allowances still apply
cumulatively to every decoded stage, including intermediate output.

Raw response-body bytes have a separate cumulative allowance **per response**:
64 MiB by default, tightened to `min(64 MiB, max_body_bytes + 64 KiB)` when a
decoded cap is installed. For page-probe redirects, `max_body_bytes` here is
the remaining decoded allowance; at most 21 response bodies can be encountered
under the existing redirect cap. This also bounds automatic redirect body reads.
The extra 64 KiB admits ordinary gzip/deflate metadata and framing for small
payloads. Raw bytes count before any decoder runs, including nonproductive
input, optional gzip fields and ignored bytes after stream end; HTTP chunk
framing and response headers are outside this body count. Overflow aborts the
connection immediately and returns `collector encoded response byte limit exceeded`,
without accepting a partial successful metric or registry update. Page probes
retain normal warning/failure metrics and preserve this diagnostic across redirects.
This Rust-specific security admission can reject large compressed metadata or
incompressible transfers even when their final decoded payload would fit.
