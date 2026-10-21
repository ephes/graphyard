# Rust agent (`agent-rs/`)

`graphyard-agent` is a Rust port of the long-running graphyard agent
(`manage.py start_agent`, unit `graphyard-agent.service`). The Django web app
stays Python and keeps owning the database schema; only the agent process is
replaced. The binding contract is [`agent-rs/PLAN.md`](../agent-rs/PLAN.md);
build and test instructions are in [`agent-rs/README.md`](../agent-rs/README.md);
the Python test → Rust test map is [`agent-rs/TESTMAP.md`](../agent-rs/TESTMAP.md).

Status: the agent is implemented and tested but not deployed. Production runs
the Python agent until the cutover (PLAN.md 11.3); the deployment selects the
Rust binary only through a systemd drop-in that the ops repositories add.

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
`graphyard.0010_condition_stale_allowance`, the last migration that changes an
agent table, and exits 1 with one line naming every missing table, column and
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
- **P2 equivalent**: messages derived from library exceptions (connect
  failures, timeouts, JSON decode positions), measured durations, other log
  lines;
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
`EnvironmentFile`. Exit codes: 0 normal end (`--run-once`, SIGINT, SIGTERM),
1 `CommandError`, invalid environment, database or schema failure, 2 usage
error. `--influx-write-file PATH` is the shadow mode: every batch the agent
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
first is `agent-rs-v0.1.0`, its sha256 is listed in `agent-rs/README.md`
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
