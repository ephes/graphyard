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
rounds 79 to 85) replaced 0.2.0 on macmini on 2026-11-12 at 15:18:42 UTC;
0.2.0 stays installed as the rollback target. The deployment selects the Rust binary
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
`src/django/.env`, D-9) and flushes every stdout line (D-8).

## Schema contract

Django migrations own the schema; the Rust agent runs no DDL. At startup (and
with `--check-schema`) it checks that every column of the six agent tables
exists and that `django_migrations` contains
`graphyard.0015_alter_conditiondefinition_breach_minutes_and_more`, the conservative
migration floor (the last migration touching an agent model, including field
validators). It exits 1 with one line naming every missing table, column and
migration otherwise (D-2). Extra columns are accepted.

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
  user regexes run on `fancy-regex` (D-5); stdout is unbuffered (D-8); no
  `.env` file (D-9); log values have `\n`/`\r` escaped (D-10); stored JSON that
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
after failures. `--influx-write-file PATH` is the shadow mode: every batch the agent
would write to InfluxDB is appended to `PATH` as line protocol after a
`# write <utc> points=<n>` comment line; queries still go to InfluxDB.

### Evaluating conditions by hand

`graphyard-agent evaluate-conditions [--condition-id N]` (from release
0.2.0) replaces `manage.py evaluate_conditions` (PLAN.md 11.4): it evaluates
the enabled conditions (or the one with the given id) once, writes their
rows and the `condition_evaluator` heartbeat, prints `Condition evaluation
completed (<total>).` and exits 0, or prints the failures and
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
(`.github/workflows/agent-rs-release.yml`, see `agent-rs/README.md`); the
first is `agent-rs-v0.1.0`, the current one `agent-rs-v0.3.0` (the fixes
of scout rounds 79 to 85; `agent-rs-v0.2.0` added `evaluate-conditions`);
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
(`tests/lease.rs::claim_refuses_a_definition_edited_after_load`). The retained
Python agent follows the same lease and renewal contract
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

**Open policy:** `INFLUX_API_MODE=auto` controls query fallback, not discovery of a
write server's timestamp domain. Production timestamp admission is unchanged until
the owner selects v2 or the narrower common domain for auto writes. The oracle
records a demonstrated storage incompatibility, not a completed preflight fix.
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
