# Rust port of the graphyard agent — implementation plan

Status: slices S1 to S10b implemented (first release `agent-rs-v0.1.0`,
section 8.2) and S15a (`evaluate-conditions`, version 0.2.0, section 11.4);
nothing deployed yet. Stream `rust-graphyard-agent` of the
campaign "Rust-Ports Runde 2" (umbrella item `rust-ports-campaign`). The
owner decided the open questions on 2026-10-21 (section 14).

This document is the contract for porting the long-running graphyard agent
(`src/graphyard/management/commands/start_agent.py`, run by
`graphyard-agent.service` on macmini) to a Rust binary `graphyard-agent` in
`agent-rs/`. It is written for the implementers of the slices in section 12
and for the reviewers of each slice. Every behavioural statement cites the
Python code that defines it (graphyard `85cdb60`; section 5.5 re-checked
against `4d5baf1`, which batched the registry writes, in slice S1). When this
document and the Python code disagree, the Python code wins and this document
must be corrected. The method follows the two Rust ports already in production
(nyxmon `agent-rs/PLAN.md`, mailgun-relay `rust/PLAN.md`): Python is the
oracle for the contract and for every decision, not for byte parity of
internals; maintained crates instead of re-implemented libraries or protocols;
fixtures generated from the Python code; a parity harness running both
implementations; a release built on an old glibc baseline with `--locked`, a
pinned toolchain and `cargo deny`; shadow run before cutover; rollback drill;
Python retirement as its own later slice.

The Django web app (`graphyard-web.service`, Granian) stays Python and keeps
owning the database schema. Only the agent process is replaced.

---

## 1. Live facts (read-only, 2026-10-21, `ssh root@macmini`)

| Fact | Value |
| --- | --- |
| Host | macmini, Ubuntu 24.04.5 LTS, x86_64, kernel 7.2.9, glibc 2.39, 6 CPUs, 64 GiB RAM; root's login shell is fish (tooling sends `bash -s` scripts) |
| Other hosts | none: `pyinfra_support/services.py:GRAPHYARD_HOSTS = {"macmini"}` (ops-control); the agent runs only on macmini |
| Process replaced | `graphyard-agent.service`, `ExecStart=/opt/apps/graphyard/site/.venv/bin/python manage.py start_agent`, `WorkingDirectory=/opt/apps/graphyard/site/src/django`, `User/Group=graphyard` (uid 981), `EnvironmentFile=/etc/graphyard/graphyard.env`, `Restart=always`, `RestartSec=5`, `Requires/After=graphyard-runtime.service`, `TimeoutStopSec=90s`, no drop-ins, no hardening directives |
| Memory (PID 2401015, up 26 h) | `ps` RSS **94 584 KiB (≈ 92 MiB)**, `VmHWM` 96 340 KiB, `RssAnon` 78 096 KiB, `RssFile` 16 488 KiB, 1 thread; cgroup `MemoryCurrent` 80.4 MB, `MemoryPeak` 83.8 MB; CPU 0.3 % (≈ 4.7 CPU-min/day) |
| Restarts | `NRestarts=0`; last deploy restart 2026-10-20 08:15 CEST |
| Runtimes in the venv | Python 3.12, Django 6.0.8, httpx 0.28.1 (no brotli/zstandard/h2 installed, so `Accept-Encoding: gzip, deflate`, HTTP/1.1 only), influxdb-client 1.50.0, certifi **2026.02.25** (the relock to 2026.10.19 is merged as `85cdb60` but not yet deployed; live source commit `7d46493` in `.ops-deploy-source`) |
| Database | SQLite `/opt/apps/graphyard/site/src/django/db.sqlite3` (14.8 MB + 20.6 MB WAL), `journal_mode=wal`, migrations applied up to `graphyard.0012_ingest_token_subject_binding`, `graphyard:graphyard 0640`; host SQLite CLI 3.45.1 |
| Env (non-secret values) | `DJANGO_SETTINGS_MODULE=config.settings.production`, `INFLUX_URL=http://127.0.0.1:8086`, `INFLUX_ORG=graphyard`, `INFLUX_BUCKET=graphyard`, `INFLUX_MEASUREMENT=graphyard_metrics`, `INFLUX_TIMEOUT_MS=10000`, `INFLUX_API_MODE=v2`, metric and condition intervals 60 s, `GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS=600`, `GRAPHYARD_SQLITE_TIMEOUT_SECONDS=30`, `GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS=30000`, `GRAPHYARD_SQLITE_JOURNAL_MODE=WAL`, `GRAPHYARD_SQLITE_SYNCHRONOUS=FULL` |
| Legacy `.env` | `src/django/.env` exists (Mar 2026, **mode 0644**, contains secrets). django-environ loads it with `setdefault` semantics; the only keys it adds beyond the EnvironmentFile are unused `HOME_ASSISTANT_*` legacy keys. The Rust agent does not read it (D-9); its removal is the separate item `graphyard-stale-env-macmini`, not part of this plan (section 14, Q5) |
| InfluxDB | container `graphyard-influxdb`, `influxdb:2.7` = InfluxDB v2.7.12, on `127.0.0.1:8086`; Grafana container alongside |
| Specs (`metric_collection_spec`) | 32 enabled: 24 `http_json_metric` (300 s; fractal/macmini storage, thermal, IPMI, fan metrics over HTTP Basic), 6 `http_page_probe` (300 s; public websites), 1 `home_assistant_env_scan` (60 s, `http://macmini.local:10020`, an mDNS name), 1 `unifi_device_traffic` (60 s, `https://127.0.0.1:8443`, `verify_tls: false`); all `last_status=ok` |
| Conditions | 2 enabled (`host.filesystem_used_ratio`, `gte`, `tags_filter` `mountpoint`), both currently `warning` with "No samples available in condition window" (pre-existing, not caused by the agent; reported to the coordinator) |
| Heartbeats | agent-owned rows `metric_collectors` and `condition_evaluator` (ok, details `{"total": …}` / `{"evaluated": 2, "failed": 0}`); `metric_ingest` is the web's; `ha_poller` is a stale legacy row from 2026-03 that nothing writes |
| Registry | 33 subjects, 10 hosts, 15 services. Agent specs touch hosts `fractal`/`macmini` (24 host-subject specs without `service_id`) and services `homeassistant`, `unifi` and the six page-probe services, all parented to host `macmini` via `collector_host`. The agent never touches the Vector services `<host>_host_metrics` |
| Journal | ≈ 48.6 k lines/7 days, of which 39.6 k are httpx `HTTP Request: …` INFO lines (stderr, real time). The command's own stdout lines (`Metric specs: …`, `Condition eval: …`, even `Starting graphyard-agent …`) are **block-buffered** (no `PYTHONUNBUFFERED` in the unit) and reach the journal in bursts, e.g. the start line 81 min after the start (D-8) |
| Resolver | `hosts: files mdns4_minimal [NOTFOUND=return] dns`, nameservers `127.0.0.1`, `192.168.178.94`; the HA spec needs nss-mdns for `macmini.local` |

The Python agent is the only long-running Python process this stream
replaces. Not replaced: `graphyard-web` (Django/Granian, 31 MiB RSS, stays
Python), the weekly `inventory-releases` oneshot timer (not long-running,
non-goal).

---

## 2. Scope, non-goals, parity definition

### 2.1 Scope

A Rust binary `graphyard-agent` that is a drop-in for `manage.py start_agent`:

- same CLI flags, defaults, validation and exit codes (section 3.1);
- same scheduler loop (section 3.2);
- the five metric collection spec types
  (`services.py:_SPEC_EXECUTORS`: `home_assistant_sensor`,
  `home_assistant_env_scan`, `http_json_metric`, `http_page_probe`,
  `unifi_device_traffic`) with the spec-config parsing of `spec_config.py`;
- the metric normalisation and InfluxDB writes of `influx.py`
  (`normalize_metric_point`, `write_points`), the condition queries
  (`query_condition_window`, `query_range` in the modes `v2`, `v3`, `auto`);
- condition evaluation (`services.py:evaluate_condition`,
  `evaluate_conditions_once` and helpers);
- every write to the shared SQLite database (`metric_collection_spec`,
  `condition_definition`, `pipeline_heartbeat`, `subject_registry`,
  `host_registry`, `service_registry`; section 5);
- stdout/stderr lines and operator log lines (section 3.6).

### 2.2 Non-goals

- No change to Django models, migrations, views, admin or management commands
  other than the Python retirement slice (S15c). The web app must not be able
  to tell which agent wrote a row.
- No port of the web ingest path (`views.py`, `record_token_host_usage`,
  `touch_registry_from_points(bound_subject_id=…)` with a bound subject),
  `apply_metric_collection_specs`, the token commands, inventory code,
  `evaluate_conditions` (manual operator command; replaced by
  `graphyard-agent evaluate-conditions` in S15a and removed in S15c, Q4).
- No change to Vector (`graphyard_vector`), the ingest tokens or their
  bindings. The agent never reads or writes `ingest_token` or
  `ingest_token_host_usage` (section 5.7).
- No new features, no behaviour fixes except the divergences of section 2.4.
  Python quirks are reproduced (section 13.1 R3 lists the notable ones).
- No unit hardening change (the base unit has none; adding it is a separate
  change for both implementations).
- No Django system checks (`BaseCommand.requires_system_checks`), no
  `--verbosity/--settings/--pythonpath/--traceback/--no-color/--force-color/--skip-checks`
  options (D-7).
- No Windows/macOS production target; macOS builds only for development.

### 2.3 Parity classes

Every reviewer finding about parity names its class.

**P1 — exact.** Identical, pinned by fixtures or tests:

- every SQL effect on the six agent tables: which rows, which columns, which
  values, the datetime text format (section 5.3), JSON text of
  `pipeline_heartbeat.details`, the order in which specs and conditions are
  processed, transaction boundaries and the 0-row-update behaviour;
- InfluxDB **series identity**: measurement, tag set (keys and exact values
  after escaping and empty-tag dropping), field `value`, timestamp in ns, and
  the set of points written per spec; the Flux query text and the v3 SQL text;
- spec statuses (`ok`/`warning`/`critical`), the `(ingested, skipped)` counts,
  and every graphyard-authored message (required-field messages, config
  errors, deadline/body-cap messages, condition messages, heartbeat
  `last_error`, the "evaluation error: …" prefix);
- CLI flags, defaults, validation messages, exit codes, the stdout lines of
  section 3.6, the scheduler semantics, environment variable names, defaults
  and parsing (section 3.3).

**P2 — equivalent.** Same meaning, text may differ:

- messages derived from library exceptions (`str(err)` of httpx, json,
  influxdb-client `ApiException`, urllib3): connect failure, timeout, TLS
  failure, JSON decode error position. Where operators see them
  (`last_error`, heartbeat) Rust renders the Python form when the category is
  known: `[Errno N] <strerror>` for OS errors, `timed out` for timeouts,
  httpx's `raise_for_status` text (P1, it is a template, section 6.1);
- measured durations (page probe TTFB/total) and wall-clock timestamps of
  `now()`-based points and rows;
- log lines not listed in section 3.6; tracebacks (Rust logs one line);
- Python `repr` of strings with non-printable or non-ASCII characters inside
  config error messages (Rust escapes ASCII control characters like Python,
  other non-printable code points may differ);
- HTTP request headers other than those listed in section 6.1.
- the response text inside UniFi error summaries (S7b) when the server
  declares a charset other than UTF-8 in `Content-Type`: httpx decodes with
  that charset, Rust always as UTF-8 with replacement characters (httpx's
  default); UTF-8 and ASCII bodies are identical.

**P3 — intentional divergence.** Exhaustive list in section 2.4. Anything else
that differs is a bug.

### 2.4 Intentional divergences (P3, exhaustive)

- **D-1 Signals.** SIGTERM: Python dies by the default signal action at any
  point; Rust stops at the next await point (an in-flight SQLite statement
  completes first), prints nothing and exits 0. systemd reports both as
  "Deactivated successfully". SIGINT behaves like Python's
  `KeyboardInterrupt` handler: stdout `graphyard-agent interrupted, exiting`,
  exit 0. Rust does the same during `--run-once`, where Python's handler is
  not active (the interrupt escapes `handle`: a traceback, and the process
  ends by SIGINT).
- **D-2 Startup schema check.** Python starts against any database and fails
  each run ("no such table …"); Rust verifies the schema contract
  (section 5.1) before the first run and exits 1 with one error line naming
  every missing table/column/migration. A missing database file: Django
  would create an empty file; Rust exits 1 and creates nothing.
- **D-3 Abandoned HTTP workers.** Python tracks daemon threads that outlive
  their deadline (`_abandoned_page_probe_workers`, cap 4). Rust cancels the
  whole request future at the deadline; only a blocking `getaddrinfo` can
  outlive it, so Rust counts abandoned resolver lookups instead (section
  6.2). Same cap and same message; the trigger differs only in what is
  counted.
- **D-4 Non-standard JSON in HTTP bodies.** Python's `json.loads` accepts
  `NaN`, `Infinity`, `-Infinity` and UTF-16/32 bodies; `serde_json` does not.
  Such a body is a JSON decode error in Rust (spec `critical`), where Python
  parsed it and then usually rejected the non-finite value (`warning`). None
  of the 32 production endpoints emits them (checked again in the shadow run).
  An integer token of more than 4300 digits in a collector's JSON body is
  CPython's `ValueError` (`Exceeds the limit (4300 digits) …`, P1) in both
  (`graphyard-collect::http::decode_json`, S7a); Python raises it while
  parsing, so a body that is also invalid JSON after such a token is a
  `JSONDecodeError` in Rust (P2 text, both `critical`).
  The same holds for the v3 SQL response (S4b): a `NaN` token or a UTF-16/32
  body is `InfluxDB SQL query failed: invalid JSON` in Rust where Python
  reports `InfluxDB query sample must be a finite number` (both a condition
  evaluation error), and nesting deeper than `serde_json`'s 128 levels is
  "invalid JSON" where Python parses up to its recursion limit (SQL rows are
  flat). A UTF-8 BOM is skipped like `json.loads(bytes)` does. The UniFi
  login check (S7b) counts an unparseable body as a successful login, so a
  2xx login body with such a token and `meta.rc` other than `ok` is a
  success in Rust and a failed attempt in Python.
- **D-5 Regex dialect.** User regexes in spec configs run on `fancy-regex`
  (backtracking features like Python `re`), not CPython `re`. Patterns valid
  in one and not the other, and Unicode case-folding corner cases, differ
  (also `$`, which in Python matches before a trailing newline, and `\Z`).
  All production patterns are in the fixtures. `fullmatch` is the pattern
  anchored as `\A(?:…)\z` (S7a; a pattern whose leading global flag
  groups, such as `(?i)(?x)`, set verbose mode gets a line break before the
  closing parenthesis, which ends a trailing comment). A pattern that exhausts `fancy-regex`'s backtracking
  limit while matching fails with `regex match failed: <error>`, which the
  HA executors report as `critical` with that message; Python keeps
  backtracking (the agent hangs on such a pattern).
- **D-6 Unicode digits and spaces.** Python `str.isdigit()` in
  `_resolve_json_path`, `float()` on Unicode digit strings, and `str.strip()`
  of non-`White_Space` separators (`\x1c`–`\x1f`) accept more than ASCII.
  Rust accepts ASCII digits only and strips Unicode `White_Space` plus
  `\x1c`–`\x1f` (matching `str.isspace`). A Unicode-digit path segment is
  "metric_path not found" in Rust (Python: indexes the list for a decimal
  digit such as `٣`, and `int()` raises → `critical` for other digits such
  as `²`; both pinned by `json_path.json`).
  The same holds for Django's `datetime_re` fallback when reading a stored
  datetime (5.3): its `\d` matches Unicode digits, Rust's ASCII only, so
  such text loads as NULL in Rust (`fromisoformat` is ASCII-only in both).
  The UniFi selector `port_idx:<n>` (S7b) likewise takes ASCII digits only:
  Python's `isdigit()`/`int()` accept other decimal digits (`port_idx:٣`
  is port 3), Rust reports `invalid port_idx selector: …` (`critical` in
  both unless such a port exists).
- **D-7 Django command plumbing.** No Django system checks, no Django global
  options, clap usage text on bad flags (exit 2 as argparse). Settings that
  only the web reads (secret key, login throttle, ingest windows) are not
  validated, so the agent starts even when they are invalid.
- **D-8 Unbuffered stdout.** Rust writes and flushes every stdout line;
  Python's lines are block-buffered (section 1).
- **D-9 No `.env` file.** Rust reads only the process environment (the unit's
  EnvironmentFile). django-environ's `read_env(src/django/.env)` is not
  ported; on macmini the file adds only unused keys.
- **D-10 Log injection.** Rust escapes `\n`/`\r` in values it interpolates
  into log lines (spec names, entity ids, metric names), like the web ingest
  path already does (`views.py`, `test_metric_log_escapes_newline`).
- **D-11 Stored JSON that CPython cannot load.** Rust parses `config` and
  `tags_filter` with `serde_json` (`arbitrary_precision`), which accepts every
  RFC 8259 document `JSON_VALID` accepts, including numbers beyond `f64`
  range: Django can store `10**400` (`json.dumps` writes the digits) and such
  values flow through the Python conversions exactly (`float(int)` →
  `OverflowError` → config error message; `float(str(…))` → `inf` → "not a
  finite number"), pinned by `spec_config.json`/`normalize.json` cases.
  Documents on which the two parsers disagree, each marking only the
  affected spec `critical` (condition: evaluation error) in Rust, pinned by
  fixtures:
  - integer tokens longer than CPython's 4300-digit `int` string limit (not
    writable through Django, `json.dumps` raises the same limit; only raw SQL
    stores them): Python fails the whole run while iterating the queryset;
    Rust uses the CPython message `Exceeds the limit (4300 digits) for
    integer string conversion: value has <n> digits; use
    sys.set_int_max_str_digits() to increase the limit`
    (`pycompat::int_digit_limit_error`; fixtures at 4300/4301 digits in
    `pycompat.json`);
  - lone UTF-16 surrogate escapes in strings (`{"unused": "\ud800"}`) and
    nesting deeper than `serde_json`'s 128-level limit: Django can write both
    (`json.dumps`/`json.loads` round-trip them, `JSON_VALID` accepts them)
    and Python runs the spec when the value is unused; Rust cannot represent
    a lone surrogate in a `String` and keeps the default recursion limit
    (no `unbounded_depth`), so it reports `config is not valid JSON: <serde
    error>`. Production configs have neither (depth ≤ 5, ASCII escapes only;
    re-checked by `shadow_prepare.py`, which scans the copied rows and lists
    any spec or condition that would hit one of these cases).
  - `pipeline_heartbeat.details` is never parsed by Rust: the heartbeat
    upsert overwrites it (5.4). Python's `update_or_create` loads it first,
    so a stored document `json.loads` cannot read for one of the reasons
    above (4300-digit integers, nesting beyond Python's recursion limit)
    fails Python's heartbeat write while Rust replaces it. Both writers
    store small flat objects of integers.
- **D-12 TLS trust anchors** (Q1, decided 2026-10-21). Python verifies
  against certifi; Rust verifies against the system CA store (Ubuntu
  `ca-certificates`), so a CA incident is fixed by the ops trust rollout plus
  a restart instead of a new agent release. `graphyard-agent.service` is
  added to macmini's `restart_units` in ops-control
  `playbooks/ca-trust-consumers.yml` (S11c), so the `ca_trust_consumers`
  rollout restarts the agent after a CA update.
- **D-13 Stored values of another SQLite storage class.** Rust reads the
  `integer` columns of the agent tables (ids, `interval_seconds`,
  `next_run_time`, `window_minutes`, `breach_minutes`,
  `stale_after_seconds`) as INTEGER, the `real` columns (thresholds,
  `last_value`) as REAL or INTEGER, the text and JSON columns as TEXT.
  A value of another storage class can only be written with raw SQL past
  Django's field conversion; Rust fails the load (stderr `Metric collection
  failed: …` / `Condition eval failed: …`), where Python hands the value on
  and may coerce it or fail later. The `bool` and `datetime` columns follow
  Python's converters exactly, whatever the storage class (5.3).
- **D-14 HTTP client corner cases** (S6). The collectors' HTTP layer follows
  httpx 0.28.1 byte for byte where the fixtures pin it (section 6.1); it
  differs where reproducing httpx would mean re-implementing a library:
  internationalised host names are encoded with UTS 46 (`idna` crate), not
  IDNA 2008 (Python `idna`), so names valid in one and not the other are
  `InvalidURL` in one implementation only; the gzip trailer (CRC32, length)
  is not checked (zlib rejects a corrupt one as `DecodingError`); cookies
  are matched by `cookie_store`, not `http.cookiejar` (same `Cookie` order,
  domain and path corner cases may differ); httpx's `trust_env` proxy
  variables (`HTTP(S)_PROXY`, `ALL_PROXY`, `NO_PROXY`) are not read
  (`SSL_CERT_FILE`/`SSL_CERT_DIR` are, by `rustls-native-certs`); a
  response head over 100 KiB always fails, where h11 checks the limit only
  while the head is incomplete, so httpx accepts a head of up to 128 KiB
  that arrives in two 64 KiB reads; a head of more than 24,576 header
  lines (possible within 100 KiB only with lines of about 4 bytes on
  average, such as `a:` and a bare LF) fails with the same error, where
  h11 accepts it. The production unit sets none of these variables, and
  the 32 production endpoints use ASCII host names, gzip from regular
  servers and (UniFi) plain session cookies.
- **D-15 Agent runtime corner cases** (S8). A panicking executor (a Rust
  bug; Python has no counterpart) is isolated like a raising one: the spec
  turns `critical` with `collector raised panic: <message>`, the run goes
  on. An interval beyond `tokio::time::Instant`'s range (about 2^63
  seconds) never fires again; Python's float arithmetic does the same up to
  the float range and raises `OverflowError` after the first run beyond it.
  Unique prefixes of long flags are matched among all flags, the Rust-only
  ones included, so `--c` is ambiguous in Rust (`--condition-interval`,
  `--check-schema`) where argparse expands it.

---

## 3. Process contract

### 3.1 CLI (`start_agent.py:Command.add_arguments`, `handle`)

| Flag | Default | Rule |
| --- | --- | --- |
| `--metrics-interval N` | `GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS` | argparse `type=int` |
| `--condition-interval N` | `GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS` | `type=int` |
| `--disable-metrics` | off | |
| `--disable-ha` | off | deprecated alias of `--disable-metrics` |
| `--disable-conditions` | off | |
| `--run-once` | off | run the enabled loops once (`due_only=False`), exit 0 |

Validation order and messages (stderr `CommandError: <msg>`, exit 1):
both loops disabled → `At least one loop must be enabled`; metrics enabled and
interval ≤ 0 → `--metrics-interval must be greater than 0`; conditions enabled
and interval ≤ 0 → `--condition-interval must be greater than 0`. A disabled
loop's interval is not validated. Bad flag syntax: exit 2 (D-7).

Then stdout `Starting graphyard-agent (metrics=True@60s, conditions=True@60s)`
(Python `bool` repr `True`/`False`, the parsed intervals).

Rust-only additions (absent from the Python CLI, used by deploy and shadow):

- `--db PATH` — the SQLite file; default `db.sqlite3` in the working
  directory, which is what `BASE_DIR / "db.sqlite3"` resolves to under the
  unit's `WorkingDirectory`. The drop-in passes the absolute path.
- `--check-schema` — open read-only, verify section 5.1, print one line,
  exit 0/1. Used by the deploy before switching.
- `--version` — `graphyard-agent <version> (<full git commit>)`.
- `--influx-write-file PATH` — shadow mode: no InfluxDB write request is ever
  built; every batch that would be written is appended to PATH as line
  protocol (with a `# write <utc> points=<n>` comment line, `<utc>` in RFC
  3339 with microseconds and `Z`) and counted as written. The batch passes
  the same normalisation, configuration, timeout and URL checks as a real
  write first, so spec statuses match. PATH is opened (created) for
  appending at startup; a failure exits 1. Queries still go to InfluxDB.

As implemented in S8: argparse conveniences are kept (unique prefixes of
long flags, the last of repeated flags wins, a negative interval reaches the
validation); `--version` prints and exits 0 whatever other valid flags
are given and even with an invalid environment (a usage error still exits
2); otherwise the environment is read first, as Python imports the settings
before argparse runs, so an invalid environment value exits 1 even with bad
flags. `--check-schema` prints its line on stdout when the schema passes,
on stderr when it fails, and needs no loop validation and no InfluxDB
settings. The start line is printed before the database is opened, as in
Python; a missing database or failed schema check then exits 1 (D-2).
The signal handlers are registered before the database is opened, and the
open runs off the runtime thread, so a SIGTERM or SIGINT while the startup
waits on a locked database ends the agent as in D-1 (exit 0; the open or
schema read in flight is abandoned, neither writes).

Exit codes: 0 normal end (`--run-once`, SIGINT, SIGTERM), 1 `CommandError`,
invalid environment value, schema/database failure, 2 usage error.
`--run-once` exits 0 even when a run failed (Python catches every exception
in `_run_metric_specs`/`_run_condition_eval`).

### 3.2 Scheduler loop (`start_agent.py:handle`, P1)

Monotonic clock. `next_metrics = now` if enabled else +∞, likewise
`next_conditions`. Loop: if metrics enabled and `now >= next_metrics`: run
metrics (`due_only=True`), `next_metrics += interval`, and if that is already
in the past, `next_metrics = monotonic() + interval`. Then the same for
conditions with the `now` taken at the top of the iteration. Then sleep
`max(0.2, min(1.0, min(next_metrics, next_conditions) - monotonic()))`. Both
runs are sequential in one task; a slow metrics run delays conditions. The
`--run-once` path calls metrics (`due_only=False`) then conditions. Pinned by
Rust tests with an injected clock.

### 3.3 Environment (django-environ semantics, P1)

Read once at startup:

| Variable | Default | Used for |
| --- | --- | --- |
| `GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS` | `env.int("GRAPHYARD_HA_POLL_INTERVAL_SECONDS", 60)` (evaluated even when the outer one is set) | `--metrics-interval` default |
| `GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS` | 60 | `--condition-interval` default |
| `GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS` | 600 | staleness, anchor carry, lookback |
| `GRAPHYARD_SQLITE_TIMEOUT_SECONDS` | 30 | connect busy timeout (superseded by the pragma below, kept for parity of startup validation) |
| `GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS` | 30000 | `PRAGMA busy_timeout = max(0, v)` |
| `GRAPHYARD_SQLITE_JOURNAL_MODE` | `WAL` | `PRAGMA journal_mode`, stripped and upper-cased; not in `DELETE TRUNCATE PERSIST MEMORY WAL OFF` → warning `Invalid GRAPHYARD_SQLITE_JOURNAL_MODE=<v>, falling back to WAL` |
| `GRAPHYARD_SQLITE_SYNCHRONOUS` | `NORMAL` | `PRAGMA synchronous`; not in `OFF NORMAL FULL EXTRA` → warning `… falling back to NORMAL` |
| `INFLUX_URL` | "" | required at first use, not at startup |
| `INFLUX_TOKEN` | "" | v2: required; v3: default `dev-influxdb3-token` |
| `INFLUX_ORG` | "" | v2: required; v3: default `INFLUX_BUCKET or "graphyard"` |
| `INFLUX_BUCKET` | `graphyard` | |
| `INFLUX_MEASUREMENT` | `graphyard_metrics` | |
| `INFLUX_TIMEOUT_MS` | 10000 | per-request timeout |
| `INFLUX_API_MODE` | `auto` | lower-cased; `v3`, `auto`, anything else = v2 |

Parsing rules (fixtures `settings.json`): `env.int` is Python `int()` on the
raw string (surrounding whitespace and `_` between digits accepted, sign
allowed); a value starting with `$` is a django-environ proxy and resolves the
named variable; an invalid integer exits 1 at startup (Python: traceback from
the settings import). Strings are not stripped (only the SQLite modes are).
Integer settings are exact Python integers of any size
(`graphyard_core::settings::Settings`, `num-bigint`); the slices that use
them convert where Python converts. A proxy chain longer than 1000 steps (a
cycle) is the `RecursionError` of the import (Python's exact depth depends on
its stack, P2). Environment entries that are not UTF-8 are read lossily
(Python: `surrogateescape`, P2). `Settings::sqlite_pragmas` computes the
three pragma values and the warnings below; the fixture records the
statements `_configure_sqlite_connection` executes.
The pragmas are applied on the one connection exactly as
`apps.py:_configure_sqlite_connection` does on every Django connection
(busy_timeout, journal_mode, synchronous, in that order; a failing pragma
logs `Failed to apply SQLite pragmas on new DB connection` and continues).

InfluxDB configuration errors surface lazily, per write or query, as in
`influx.py:_build_client`: `Influx setting missing: INFLUX_URL is required`,
`Influx settings missing: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG are required`
(P1 texts, stored as spec `last_error` / condition message).

### 3.4 Metrics run (`services.py:run_metric_collection_specs_once`, P1)

1. Reset the per-run warn-once set (`_subject_mapping_warning_keys.clear()`).
2. `now` = current UTC time (µs), `now_ts = floor(now.timestamp())`, taken
   once for the whole run.
3. Select `enabled = 1` (and `next_run_time <= now_ts` when `due_only`),
   `ORDER BY name` (model `Meta.ordering`). Rows are loaded once at the start.
4. Empty: with `due_only` return all-zero counters without a heartbeat; else
   raise `No matching enabled metric collection specs` (stderr
   `Metric collection failed: …`, no heartbeat).
5. Per spec, sequentially: run its executor (section 6) isolated by
   `_run_single_metric_collection_spec` (unknown type →
   `critical`, `unsupported spec_type: <t>`; `SpecConfigError` → `critical`,
   its message; any other failure → `critical`,
   `collector raised <Type>: <err>` plus an error log line
   `Metric collection spec '<name>' (<type>) raised`); then update the spec
   row (section 5.4) with `last_run_at=now`, `last_status`, `last_error`,
   `next_run_time = now_ts + max(1, interval_seconds)`. Counters: `total`,
   `failed` (critical), `warning`, `ingested`, `skipped`; error list entries
   `"<name>: <msg>"` for every critical and for warnings with a message.
6. Heartbeat `metric_collectors`: all failed (`failed > 0` and
   `total - failed == 0`) → `critical`, success false; any failed or warning →
   `warning`, success true; else `ok`, success true. `last_error` =
   `"; ".join(errors[:5])`, details
   `{"total": …, "failed": …, "warning": …, "ingested": …, "skipped": …}`.
7. stdout `Metric specs: total=<t> failed=<f> warning=<w> ingested=<i> skipped=<s>`.

A failure outside a spec (database error, spec deleted during the run →
0-row update, section 5.4) aborts the run: error log `Metric collection
failed`, stderr `Metric collection failed: <err>`, no heartbeat, the loop
continues.

As implemented in S8 (`graphyard-agent::runs`, pinned by `runs.json`): the
executors receive the stored `config` decoded with
`pycompat::json_loads` after the spec type is known; D-11 documents become
the spec's `critical` message (the CPython digit-limit text, or `config is
not valid JSON: <err>`), and a condition's `tags_filter` likewise its
`evaluation error: ...`. A `next_run_time` beyond SQLite's INTEGER range
aborts the run with sqlite3's `OverflowError` text `Python int too large to
convert to SQLite INTEGER`. A panicking executor is isolated (D-15).

### 3.5 Conditions run (`services.py:evaluate_conditions_once`, P1)

Select `enabled = 1 ORDER BY name`. Per condition, sequentially:
`evaluate_condition` (section 7) with its own `now`; success → update
`status, message, last_value, last_evaluated`; any exception → `failed += 1`,
update only `status='critical'`, `message='evaluation error: <err>'`. If that
second update fails too, the run aborts (`Condition evaluation failed`,
stderr `Condition eval failed: <err>`, no heartbeat). Heartbeat
`condition_evaluator`: `ok`/`""`/success when nothing failed, else
`warning`/`"<n> condition(s) failed"`/no success; details
`{"evaluated": <total>, "failed": <failed>}`. stdout
`Condition eval: evaluated=<total> failed=<failed>`.

### 3.6 Output and logging

stdout (P1 text, one line each, flushed): the start line, `Metric specs: …`,
`Condition eval: …`, `graphyard-agent interrupted, exiting`. stderr (P1
text): `CommandError: …`, `Metric collection failed: <err>`,
`Condition eval failed: <err>`.

Log records go to stderr as the bare message (production `LOGGING` has a root
`StreamHandler` without formatter, level INFO). Operator-relevant lines (P1
text up to the interpolated exception text; D-10 escaping):

- httpx's request line, emitted by Rust for every collector request and every
  v3 SQL request: `HTTP Request: <METHOD> <url> "HTTP/1.1 <code> <reason>"`
  (InfluxDB v2 traffic goes through urllib3 in Python and is not logged);
- subject/metric mapping warnings of `_resolve_subject_mapping` and
  `_resolve_home_assistant_metric_mapping` (warn-once keys as in Python),
  `Spec <name> failed subject resolution for entity_id=<id>`;
- `Metric point validation failed metric=… subject_type=… subject_id=…: <err>`,
  `Ignoring custom tag override for reserved key <k>`,
  `metrics_write_rejected category=normalization rejected_points=<r> written_points=<w> total_points=<t>`;
- `Ignoring host_filter for non-host condition id=<id> name=<name>`;
- `HTTP collector worker for <url> still running after it was cancelled`;
- the SQLite pragma warnings of 3.3; `Metric collection failed`,
  `Condition evaluation failed`, `Metric collection spec … raised` (one line
  with the error instead of a traceback, P2). As implemented in S8 the line
  ends like the traceback's last line: `Metric collection failed: <class>:
  <err>`, `Condition evaluation failed: <class>: <err>`, `Metric collection
  spec '<name>' (<type>) raised: <class>: <message>`.

Rust adds three INFO lines at startup (version, schema check result with
journal mode, effective intervals) and nothing per run beyond the above.
The graphyard crates log at INFO; other crates (reqwest, rustls, hyper) only
warnings and errors.

### 3.7 systemd behaviour

Same unit, same `EnvironmentFile`, `WorkingDirectory`, user and restart
policy; the Rust binary is selected by a drop-in (section 8.3). The agent
never forks, opens no listening socket, needs no capabilities and writes only
the database file (plus WAL/SHM) and, in shadow mode, the write file. Stop:
exits within 1 s of SIGTERM unless an SQLite statement is waiting on a lock
(bounded by `busy_timeout`, 30 s < `TimeoutStopSec` 90 s); a blocked
`getaddrinfo` never delays the exit (section 4.1).

---

## 4. Design

### 4.1 Runtime shape

One tokio runtime, `current_thread`, plus tokio's blocking pool for
`getaddrinfo` and one dedicated thread for SQLite (below). The two loops stay sequential in one task,
exactly like Python; nothing in the agent needs concurrency, and sequential
execution is what keeps the P1 ordering (single `now` per run, spec order,
heartbeat after the last spec). A second task handles SIGTERM/SIGINT and
cancels the main task.

Shutdown must not wait for blocking work that cannot be cancelled: a
`getaddrinfo` stuck in tokio's blocking pool would otherwise hold
`Runtime::drop` until systemd's SIGKILL. SQLite therefore does not run on
the blocking pool but on one dedicated DB thread owning the connection
(callers send a closure and await the reply, as in nyxmon `nyxmon-store`).
On a signal the main task is cancelled, `main` waits for the DB thread to
finish its current statement (bounded by `busy_timeout`), drops the
connection, prints the SIGINT line if applicable, and ends the runtime with
`Runtime::shutdown_timeout(Duration::from_millis(500))`, abandoning resolver
threads; the process then exits 0. S8 tests SIGTERM with a resolver stub that
never returns and with a DB thread that holds a statement for a few seconds.

### 4.2 Workspace

```
agent-rs/
  Cargo.toml          # workspace; [profile.release] lto="thin", codegen-units=1, strip=true
  Cargo.lock          # committed; every build --locked
  rust-toolchain.toml # same pin as nyxmon agent-rs (1.99.0) = MSRV
  deny.toml           # copied from nyxmon agent-rs, same policy
  PLAN.md TESTMAP.md README.md
  crates/
    graphyard-core/     # pure: pycompat, settings, model types, spec_config,
                        # normalisation, line protocol, JSON path, mappings,
                        # condition evaluation; no I/O
    graphyard-store/    # rusqlite: schema contract, all SQL, registry touch
    graphyard-influx/   # write and query clients (v2 Flux, v3 SQL, auto)
    graphyard-collect/  # HTTP layer (deadline, caps, redirects) and the five executors
    graphyard-agent/    # binary: CLI, scheduler, runs, logging, signals
  fixtures/           # generate.py (driver) + gen_*.py builders, schema/, *.json (section 9.2)
  parity/             # offline harness, fakes, shadow tooling (sections 9.4, 10)
  scripts/release_checks.py
```

Crate boundaries let slices S2–S7 proceed in parallel with few conflicts and
keep every fixture-tested function in `graphyard-core`.

### 4.3 Crate choices

| Need | Crate | Justification |
| --- | --- | --- |
| async runtime | `tokio` (rt, time, signal, macros) | |
| HTTP (InfluxDB) | `reqwest` 0.13 (no default features; `rustls-no-provider` with `rustls` + `ring` installed as the process provider, as in nyxmon; `gzip`, `deflate` for the query responses (S4b)) | the InfluxDB clients need plain requests with one timeout (S4a, S4b) |
| HTTP (collectors, S6) | `hyper` 1 HTTP/1.1 client connection API (`hyper::client::conn::http1`) over tokio sockets wrapped with a per-operation timeout, `tokio-rustls`, `http-body-util`; as in the nyxmon port | reqwest was the first choice but its `read_timeout` bounds the whole wait for the response head with one timer (`Pending::poll`), where httpx bounds each socket read: a server dripping its headers must run into the total deadline, not a read timeout (`test_page_probe_deadline.py`). On hyper's connection API the connector is ours: httpcore's `create_connection` (lookup unbounded, every address with its own connect timeout), the TLS handshake under the timeout, every read and write under the timeout, httpx's exact request target and `Host`. One connection per request (httpx reuses keep-alive connections within a client; P2, timings only) |
| response decoding | `flate2` (streaming `GzDecoder`/`ZlibDecoder`/`DeflateDecoder`) | httpx's `GZipDecoder`/`DeflateDecoder` (zlib, raw deflate when the first chunk is not zlib), decoded bytes count towards `max_body_bytes`; D-14 |
| cookies | `cookie_store` | the client's jar (UniFi session, cookies across manual redirects); D-14 |
| name resolution | `dns-lookup` (libc `getaddrinfo`) on tokio's blocking pool inside the collectors' connector | `macmini.local` needs nss-mdns, so glibc resolution is mandatory (section 13.1 R2); each lookup is tracked so the deadline can count abandoned ones (D-3) |
| TLS roots | collectors: the system store through `rustls-native-certs`, loaded once per process into the `HttpContext`; InfluxDB: reqwest's platform verifier (`rustls-platform-verifier`, which reads the same store on Linux; S4a) | Q1, D-12 (a CA update needs the restart the trust rollout does) |
| InfluxDB v2 write | plain HTTP `POST /api/v2/write` with our line formatter (section 6.4) | the Python client is itself a thin wrapper over this documented API (`SYNCHRONOUS` write, no batching, no retries); the Rust InfluxDB crates are either unmaintained (`influxdb2`, last release 2024-07) or InfluxQL/v1-oriented (`influxdb`); series identity needs `influxdb_client`'s exact escaping and float rendering, which no crate promises |
| line protocol parsing (tests only) | `influxdb-line-protocol` (InfluxData) as dev-dependency | semantic comparison of written lines in fixtures, harness and shadow tooling |
| fake servers (tests only) | `hyper` 1 (`server`, `http1`) + `hyper-util`, `http-body-util` as dev-dependencies | fake InfluxDB and collector endpoints without a hand-written HTTP parser |
| InfluxDB v2 Flux query | plain HTTP `POST /api/v2/query` + `csv` crate for the annotated CSV | same reasoning; annotated CSV is plain CSV plus `#datatype/#group/#default` rows, read with the maintained `csv` crate; `FluxCsvParser`'s state machine and value typing are ported on top (S4b) |
| InfluxDB v3 SQL | plain HTTP `POST /api/v3/query_sql` + `serde_json` | mirrors `_read_v3_sql_rows` |
| SQLite | `rusqlite` (`bundled`) | same as nyxmon; synchronous API keeps Django's statement boundaries reviewable |
| JSON | `serde_json` (`preserve_order`, `arbitrary_precision`) | key order of configs and payloads (rule order, tag order), big integers survive like in Python |
| Python integers | `num-bigint` (+ `num-traits`) | exact `int` of any size where Python keeps one: `py_int` and the integer settings (3.3), `int == float` in `py_eq`; same crate as nyxmon's `pyjson` |
| regex | `fancy-regex` | user regexes from spec configs need lookaround/backreference support like Python `re` (D-5) |
| fixed regexes | `regex` | Django's `datetime_re` fallback when reading stored datetimes (5.3); `fancy-regex` builds on it |
| time | `chrono` | Django datetime text, ISO parsing of HA `last_updated`, RFC 3339 of Flux |
| CLI | `clap` (derive) | |
| logging | `log` + a 20-line stderr logger printing the bare message | matches the production format; no `tracing` stack needed |
| tests | `tempfile`, `tokio-test`, `rcgen` + `time` (TLS fake servers with a CA and a short-lived server certificate); the collectors' fake servers write raw HTTP/1.1 over tokio sockets to drip headers and bodies byte by byte | |

No OpenSSL, no `hickory-resolver`, no musl.

### 4.4 Python compatibility helpers (`graphyard-core::pycompat`, S2)

Small, fixture-pinned functions where no crate reproduces CPython:
`py_float_repr(f64)` (`repr(float)`), `py_format_g(f64)` (`format(x, "g")`,
for `total_timeout_seconds={x:g}`), `py_float(&str)` (Python `float()` on
ASCII input incl. `_`, whitespace, `inf`/`nan` spellings), `py_int(&str)`,
`py_str(&Value)` (`str()` of a JSON value: `True`, `None`, `1.0`, dict/list
`repr`), `py_repr(&Value)` (incl. string quote selection and escapes),
`py_describe` (`spec_config.py:_describe`, 64-char truncation),
`py_strip`, `py_split_whitespace`, `py_eq(&Value, &Value)` (Python `==`
across bool/int/float). The nyxmon `pyjson` module implements several of
these; S2 copies the relevant functions with their fixture cases rather than
depending on another repository.

As implemented in S2: every JSON document the agent decodes (stored
`config`/`tags_filter`, HTTP bodies) goes through `pycompat::json_loads`,
not `serde_json::from_str::<Value>`: with `arbitrary_precision` (and
`raw_value`) `Value`'s deserializer reads an object whose first key is one
of `serde_json`'s private tokens (`{"$serde_json::private::Number": "5"}`)
as a number, or rejects it, where Python keeps the `dict`. `json_loads`
accepts exactly what `serde_json` accepts and walks a document that may
spell such a key through `RawValue` slices, keeping every object an object.
A JSON number is read only through `py_number`, which
decodes the `arbitrary_precision` token like `json.loads` (an `int` exactly
when the token has no fraction and no exponent, exact at any size; float
tokens as `float(token)`, `inf` on overflow); `py_float_of(&Value)` is
`float()` of a decoded value (`OverflowError` for huge integers, `TypeError`
for `None`/containers); `py_type_name` is `type(x).__name__`; `py_int`
returns an exact integer (`num-bigint`); `int_digit_limit_error(text)` gives
the full CPython `ValueError` text of `json.loads` for the first integer token
beyond 4300 digits (D-11), for the slices that load stored JSON. `py_repr`
renders such integers with all digits (they never pass the loader) and
`py_describe` reproduces `_describe`'s `<int>` fallback for them.

---

## 5. Database contract

### 5.1 Schema ownership and startup check

Django migrations own the schema. The Rust agent runs no DDL. At startup
(and for `--check-schema`) it verifies with `PRAGMA table_info` that every
column of the six agent tables exists. Django loads whole rows (and converts
their datetimes, 5.3), so the store selects every column too, including
`created_at`/`updated_at` of specs and conditions, which it never writes:

- `metric_collection_spec`: `id, name, enabled, spec_type, interval_seconds, next_run_time, config, last_run_at, last_status, last_error, created_at, updated_at`
- `condition_definition`: `id, name, enabled, metric_name, host_filter, service_filter, tags_filter, operator, warning_threshold, critical_threshold, window_minutes, breach_minutes, status, last_evaluated, message, last_value, created_at, updated_at, subject_id_filter, subject_type_filter, alert_when_stale, stale_after_seconds`
- `pipeline_heartbeat`: `id, name, status, last_success, last_error, details, updated_at`
- `subject_registry`: `id, subject_type, subject_id, display_name, source_system, last_seen_at`
- `host_registry`: `id, host_id, display_name, grafana_url, enabled, last_seen_at`
- `service_registry`: `id, service_id, display_name, grafana_url, enabled, last_seen_at, host_id`
- `django_migrations` contains `('graphyard', MIN_GRAPHYARD_MIGRATION)`,
  `0010_condition_stale_allowance`: the last migration that changes an agent
  table (`0011`/`0012` touch only `ingest_token*`).

Extra columns are accepted (a newer, compatible schema). A failed check is
one line naming every missing table, column and the migration, e.g.
`database schema is older than the agent contract; missing columns:
condition_definition.stale_after_seconds; missing migration:
graphyard.0010_condition_stale_allowance; run `manage.py migrate` before
starting the agent`; a passing one reads `database schema satisfies the agent
contract (graphyard.0010_condition_stale_allowance, journal_mode=wal)`.

`agent-rs/fixtures/gen_schema.py` (part of `just rs-fixtures`) migrates a
temporary database with the test settings and writes
`agent-rs/fixtures/schema/schema.sql` (the DDL in `sqlite_master` order plus
the `django_migrations` rows) and `agent_tables.json` (the columns of the six
tables in `PRAGMA table_info` order and the minimum migration, computed as
the last graphyard migration whose operations name an agent model; an
operation without a model, `RunPython`/`RunSQL`, counts as touching one). The
Python test `tests/test_agent_rs_schema_contract.py` regenerates both and
fails with "run `just rs-fixtures` and update the Rust schema contract" when
they differ; `graphyard-store`'s tests assert that `REQUIRED_COLUMNS` and
`MIN_GRAPHYARD_MIGRATION` equal `agent_tables.json`. So a migration touching
an agent table cannot merge without a conscious Rust change. Every Rust store
test builds its database from `schema.sql`.

### 5.2 Connection

One `rusqlite` connection for the process lifetime, owned by the dedicated
DB thread of section 4.1 (Django keeps one connection in a management
command). Opened `SQLITE_OPEN_READ_WRITE` (no create) with Django's
connection setup (`sqlite3.connect(timeout=GRAPHYARD_SQLITE_TIMEOUT_SECONDS)`,
`PRAGMA foreign_keys = ON`, `PRAGMA legacy_alter_table = OFF`), then the
three graphyard pragmas of 3.3 (a failure logs `Failed to apply SQLite
pragmas on new DB connection: <err>` on one line). `--check-schema` opens
`SQLITE_OPEN_READ_ONLY` and applies only the busy timeouts, so it never
changes the journal mode. Single statements run in autocommit, as Django's
`save(update_fields=…)` does; multi-statement operations run in `BEGIN
IMMEDIATE` (settings `transaction_mode: IMMEDIATE`). The web app and the
agent share the WAL database exactly as today. On shutdown the DB thread
finishes the operation it is running (a cancelled caller never cuts a
statement or transaction short) and closes the connection.

### 5.3 Value formats (P1)

- Datetimes: Django stores aware datetimes as naive UTC text
  `YYYY-MM-DD HH:MM:SS.ffffff`, and **without** `.ffffff` when the
  microsecond is 0 (`str(datetime)`). Reads reproduce Django's loader: the
  backend registers `parse_datetime` as the `sqlite3` converter of the
  `datetime` declared type, so the value's text form (also of an INTEGER or
  REAL value) goes through CPython 3.12's `datetime.fromisoformat` (the C
  implementation, ported byte for byte), then Django's `datetime_re`, then
  `convert_datetimefield_value` (naive is UTC). Text neither accepts loads as
  NULL; text `datetime_re` matches with out-of-range fields (`2026-13-01
  00:00:00`) raises `ValueError`, which fails the whole load of specs or
  conditions (P2 message) as in Python. Writing a loaded value back (the
  heartbeat's `last_success`, 5.4) uses the UTC text and raises
  `OverflowError: date value out of range` outside Python's years 1–9999.
  Pinned by `datetime_codec.json` (curated plus a seeded random corpus).
- Booleans: written `0/1`; read through Django's `bool` converter
  `b"1".__eq__` on the raw bytes of the value's text form (no UTF-8
  decoding), so only a stored `1` loads as true. `WHERE enabled` still
  selects any truthy value (a stored `2` is loaded, with `enabled` false).
- `pipeline_heartbeat.details`: `json.dumps` default separators and
  `ensure_ascii` (`{"evaluated": 2, "failed": 0}`), key order as in 3.4/3.5.
- `last_value` REAL (NaN is stored as NULL by SQLite in both); `next_run_time`
  integer; `config`/`tags_filter` are handed to their parsers as the stored
  text (D-11).

### 5.4 Writes per operation (P1, Django ORM as the oracle)

- **Spec run result** — `UPDATE metric_collection_spec SET last_run_at=?,
  last_status=?, last_error=?, next_run_time=? WHERE id=?`. `updated_at` is
  **not** touched (`update_fields` excludes `auto_now` fields). 0 rows →
  error `Save with update_fields did not affect any rows.`, which aborts the
  run (3.4).
- **Condition success** — `status, message, last_value, last_evaluated`;
  **condition failure** — `status, message` only. Same 0-row rule.
- **Heartbeat** (`record_heartbeat`, `min_update_interval_seconds=0`) —
  Django 6 `update_or_create(name=…, defaults)` inside `BEGIN IMMEDIATE`:
  load the row (all columns, datetimes converted: an out-of-range stored
  value fails the call); existing row → update `status, last_error, details`
  plus every field whose class overrides `pre_save`, which Django adds to
  `update_fields`: `updated_at` (`auto_now`, `now`) and `last_success`
  (`now` after a success, otherwise the loaded value written back in
  Django's text form, so an unparseable stored value becomes NULL and an
  offset form is rewritten in UTC); missing row → insert with `last_success`
  NULL unless success. Python reads the clock separately for `last_success`
  and `updated_at`; Rust writes one `now` (P2). Up to 3 attempts, retrying
  only on `database is locked` with sleeps of 50 ms and 100 ms (await points
  in Rust, so a shutdown never waits for them). The exact column set per path
  is pinned by the `store_ops.json` fixture, not by this paragraph.
- **Registry** (`touch_registry_from_points(points)`, `bound_subject_id=None`)
  — section 5.5.

The fixture generator (`fixtures/gen_store.py`) runs each Python operation
against a temporary database built with the test settings and records the
rows before and after (all columns), so the Rust store tests assert identical
row images (timestamps injected).

### 5.5 Registry touch (`services.py:touch_registry_from_points`, P1)

Graphyard `4d5baf1` ("Batch registry writes per ingest request") replaced
the per-point get-or-create loop with a batch over the distinct rows of one
call; the agent path is `bound_subject_id=None`. An empty point list
returns at once; otherwise, per call:

1. **Collect** (`_collect_registry_batch`): `normalize_metric_point` on every
   point in order. A `ValueError` propagates before any registry read or
   write and turns the spec `critical`, after `write_points` already wrote
   the valid points (reproduced on purpose; no row is touched). Collected in
   first-seen order: the distinct subjects `(subject_type, subject_id)`, each
   with the last non-empty `source_system` of the batch; the host ids (subject
   type `host`) with the index of the first point naming them; per service
   the list of `(candidate host, point index)` where the candidate is the
   point's subject id for a host subject, else its `collector_host`.
2. `now = timezone.now()` once.
3. **Load** (`_load_registry_rows`, autocommit, no write lock), one query
   per table in this order: hosts whose `host_id` is a batch host or a
   non-empty service candidate; subjects by `subject_type IN (…) AND
   subject_id IN (…)` (the cross product of the batch's types and ids: every
   row it returns is converted, then the rows are narrowed to the batch's
   pairs); services by `service_id`. An empty `__in` list runs no query
   (Django's `EmptyResultSet`), so a batch without hosts and services reads
   only subjects. Django loads whole rows: `last_seen_at` goes through the
   datetime conversion of 5.3 (unparseable text loads as NULL, so the row is
   stale; an out-of-range value raises `ValueError` and fails the call, the
   spec turns `critical`), text columns must decode as UTF-8.
4. **Plan** (`_plan_registry_updates`) over the loaded rows:
   - subject: batch `source_system` non-empty and different →
     `source_system` + `last_seen_at`; else stale → `last_seen_at`;
   - host (batch hosts only): stale → `last_seen_at`;
   - service: target host (below) not none and different from the current
     host → re-parent (`host_id` + `last_seen_at`); else stale →
     `last_seen_at`.

   Stale means `last_seen_at` NULL or `now - last_seen_at >= 60 s`
   (`REGISTRY_LAST_SEEN_UPDATE_INTERVAL`); a recent row is not written. The
   service target host (`_service_target_host`) is the host row of the last
   candidate, in batch order, whose host row is known (loaded, or created in
   step 6) and either existed before the batch (was in the load; a host the
   batch creates, or finds in `get_or_create`'s select, does not count) or
   is registered by a host point at the same or an earlier index (the
   per-point order of the old loop); none when no candidate qualifies. The
   source system comparison is Python's `!=`: only a stored TEXT equal to
   the batch value is unchanged (NULL is replaced).
5. No row missing and an empty plan → return without a transaction (steady
   state writes nothing).
6. Otherwise one `transaction.atomic()` (`BEGIN IMMEDIATE`, settings
   `transaction_mode: IMMEDIATE`): reload the rows, create the missing ones
   with Django `get_or_create` (select; insert inside a savepoint; on
   `IntegrityError` select again) in the order subjects, hosts, services:
   subject `display_name=subject_id`, `source_system=<batch value>`,
   `last_seen_at=now`; host `display_name=host_id`, `last_seen_at=now`;
   service `display_name=service_id`, `host=<target host>`,
   `last_seen_at=now`. Then plan again over all rows and run the updates in
   this order: one `UPDATE subject_registry SET last_seen_at WHERE id IN
   (…)`, one `UPDATE … SET source_system, last_seen_at WHERE id = ?` per
   changed subject, one `UPDATE host_registry SET last_seen_at WHERE id IN
   (…)`, one `UPDATE service_registry SET last_seen_at WHERE id IN (…)`, one
   `UPDATE … SET host_id, last_seen_at WHERE id = ?` per re-parented service.
   Each is a queryset `update()`, so the row images are pinned by
   `store_ops.json`, not by the statement text; the statement sequence
   (verb, table, inside the transaction or not) is pinned by the
   `statements` of the fixture's `registry_cases` (S3b). Under `BEGIN
   IMMEDIATE` after the reload no other connection can insert a row, so the
   realistic race is a row the web app creates between the autocommit load
   and the transaction: the reload finds it and the plan refreshes it like
   any existing row. The `IntegrityError` branch of `get_or_create` stays
   (select again, re-raise when still missing); an error anywhere rolls the
   whole transaction back.

The bound-subject filter on the service updates is not ported (non-goal,
5.7).

### 5.6 Coexistence with Django

The agent's UPDATEs touch only the listed columns, so concurrent admin edits
of `config`, thresholds or `enabled` survive, as today. A spec edited during
a run is used as it was loaded at run start.

### 5.7 Token binding and Vector boundaries

The agent writes InfluxDB directly with `INFLUX_TOKEN`; it never calls the
ingest API, never reads or writes `ingest_token`/`ingest_token_host_usage`,
and never runs the bound-subject registry branch. The `<host>_host_metrics`
services and the per-host ingest tokens (ops-control `4394ecd`) are therefore
outside its reach, and `GRAPHYARD_REQUIRE_BOUND_INGEST_TOKENS` does not affect
it. A Rust test (S3a) greps every source file of `graphyard-store`, which
holds all of the agent's SQL, for `ingest_token` and fails if found.

---

## 6. Collector contract

### 6.1 Common HTTP rules (httpx 0.28.1 as configured by `services.py`)

- One client per spec execution (`httpx.Client(...)`), closed after it; its
  cookie jar lives for that execution (redirect chains, UniFi login session).
  Each request opens its own connection (no keep-alive reuse, P2).
- Timeout `request_timeout_seconds` per operation as httpcore applies it
  (S6, `graphyard-collect::http::connect`): `getaddrinfo` is not bounded
  (only the total deadline bounds it), each resolved address gets its own
  connect timeout (`socket.create_connection`; the last error wins:
  `ConnectTimeout` `timed out` or `ConnectError` `[Errno N] …`), the TLS
  handshake has the timeout, and every later socket read and write has it
  separately (`ReadTimeout` `timed out`), so a response dripping one byte
  per read never trips it. Pool timeouts do not exist.
- URLs are httpx's (`HttpxUrl`, a port of `_urlparse.urlparse` pinned by
  the `urls` fixture cases): `InvalidURL` texts, `str()` (log line,
  `raise_for_status`), `UnsupportedProtocol` for a URL without scheme or host
  (`_merge_url`) or a scheme other than http/https/ws/wss (ws/wss are plain
  HTTP/TLS requests as in httpcore), the host text `getaddrinfo` gets, the
  request target (`raw_path`) and `Host` (netloc). Credentials in the URL
  are sent as basic auth when the client has no `auth=`
  (`_build_request_auth`). The auth flow runs once per `send()`: on every
  manual page-probe hop (each is its own `client.send`), but only on the
  first request of an automatic redirect chain, whose hops keep or drop
  `Authorization` as `_build_redirect_request` decides.
- `verify_tls: false` disables certificate and hostname verification (the
  handshake signature is still checked, as with `CERT_NONE`).
- Request headers sent by Python and kept (P1): `Accept` as set by the
  executor (default `*/*`), `Accept-Encoding: gzip, deflate`,
  `User-Agent: python-httpx/0.28.1` (kept for zero-change cutover like the
  nyxmon port; a `graphyard-agent/<ver>` UA is a later, separate change),
  `Authorization` as set by the executor. Decoded bytes count towards
  `max_body_bytes`, as `iter_bytes()` yields decoded content.
- HTTP/1.1 only. The response head is limited by size: 100 KiB
  (httpcore's `MAX_INCOMPLETE_EVENT_SIZE`), over it `RemoteProtocolError`
  `Receive buffer too long`. h11 has no limit on the number of header
  lines; hyper's default of 100 is lifted to 24,576, the most an
  `http::HeaderMap` holds (D-14).
- `raise_for_status()` text (P1, template from httpx):
  `Client error '404 Not Found' for url '<url>'\nFor more information check: https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/404`
  (and the `Redirect response` / `Server error` / `Informational response`
  variants with `Redirect location: '<loc>'` for 3xx); pinned by a fixture
  built from `httpx.Response(...).raise_for_status()`. The reason phrase is
  the server's (non-ASCII bytes dropped; an empty one stays empty, as
  httpcore passes the status line's reason through).
- Redirects: `Client.send(follow_redirects=False)` still builds
  `next_request` for 301/302/303/307/308 with `Location`, so an invalid
  `Location` fails the request (`RemoteProtocolError` `Invalid URL in
  location header: …`, or `InvalidURL` for a host-less absolute one) even
  where nothing follows it (HA, JSON metric). The join is
  `urllib.parse.urljoin` (CPython 3.12), method, `Authorization` and body
  handling as `_build_redirect_request`; pinned by the `redirects`,
  `urljoin` and `page_probe` fixture cases. Automatic following (UniFi) reads
  every redirect body, at most 20 redirects (`send_following_redirects`).
- `HTTP Request: <METHOD> <url> "<version> <code> <reason>"` (INFO) after
  each response head, for every hop, as `Client._send_single_request`.
- Exception classification for the page probe (`except (httpx.HTTPError,
  _PageProbeLimitExceeded, _PageProbeDeadlineExceeded)`): transport errors,
  timeouts, `TooManyRedirects`, `UnsupportedProtocol` are `HTTPError`
  (warning path); `httpx.InvalidURL` and other errors are not (critical
  path). Pinned by fixture `http_errors.json` (URL and failure corpus
  classified by the Python code).

### 6.2 Total deadline (`_run_http_with_deadline`, P1 messages)

Used by both HA executors, `http_json_metric` and `http_page_probe` (not by
UniFi, not by InfluxDB). The whole operation (resolution, connect, TLS,
headers, body, redirects, JSON parse) runs under
`total_timeout_seconds` (default 3 × request timeout,
`spec_config.py:page_probe_total_timeout_config`). Messages:
`HTTP collector exceeded total_timeout_seconds=<py_format_g>`; when 4 or more
abandoned workers are still alive before starting:
`<n> earlier HTTP collectors are still running past total_timeout_seconds; not starting another`.
A deadline that passes while the JSON body is being parsed still fails the
probe (Python checks after `json.loads`). Abandoned workers in Rust (D-3):
resolver lookups whose request was cancelled and that are still running 1 s
after the cancellation; each one logs the "still running" warning once.
Like Python's `worker.join(timeout=1.0)`, the caller waits up to that second
for a running lookup before it reports the deadline, so the error returns
within the deadline plus at most one second. A cancelled request never
connects afterwards: its future is gone when the lookup returns
(`run_with_deadline`, `graphyard-collect::http::deadline`).

### 6.3 Executors

Each returns `(status, ingested, skipped, message)`; the config is the spec's
JSON object (non-object → `critical`, `config must be an object`). Typed
knobs go through `spec_config.py` (`request_timeout_seconds` default 10,
`total_timeout_seconds`, `max_body_bytes` default 10 MiB, `verify_tls`
default true, `follow_redirects` default true; their `SpecConfigError`
messages are P1 including `_describe`; `graphyard_core::spec_config`, S2).
`max_body_bytes` beyond `u64` saturates to `u64::MAX`, which no body
reaches (Python keeps the exact `int`; same outcome). Required-field messages (P1) appear
in the order the Python code checks them.

- **`home_assistant_sensor`** (`_execute_home_assistant_sensor_spec`): GET
  `<base_url rstrip "/">/api/states/<entity_id>` with `Authorization: Bearer
  <access_token>`, `Accept: application/json`, no redirects, body cap,
  deadline, `raise_for_status`, JSON. Non-object → `warning, 0, 1, "invalid
  JSON payload"`; normalisation `None` → `warning, 0, 1, "state payload not
  numeric or mapping failed"`; else write one point, touch the registry,
  `ok, written, 0, ""`; any exception → `critical, 0, 0, str(err)`.
- **`home_assistant_env_scan`** (`_execute_home_assistant_env_scan_spec`):
  GET `/api/states`; `entity_id_regex` compiled case-insensitive (invalid →
  `critical`, `invalid entity_id_regex: <err>`, P2 after the prefix);
  non-list → `critical, "states response is not a list"`; filter
  `sensor.`/`binary_sensor.` entities whose
  `str(attributes.get("device_class", "")).lower()` is in
  `include_device_classes` (default temperature, humidity; non-list → default)
  or whose id matches the regex (`search`); normalise with
  `metric_name_override = metric_prefix + entity_id` and
  `extra_tags={"spec_name": name}`; `None` → `skipped += 1`; no points →
  `warning, 0, skipped, "no matching numeric states"`; else write, touch,
  `ok, written, skipped, ""`.
- **HA state normalisation** (`_normalize_home_assistant_sensor_state`):
  entity id, state via `parse_finite_float`, timestamp from `last_updated`
  (`Z` → `+00:00`, `datetime.fromisoformat`, naive = UTC, failure = now),
  `_resolve_subject_mapping` (rules with `fullmatch`, default mapping,
  warn-once keys), source/collector fields with their defaults and fallbacks,
  `_resolve_home_assistant_metric_mapping` (rules with `search`,
  `value_multiplier` with bool rejection, `extra_tags`), non-finite product →
  `None`, tags `entity_id` → extra tags → mapped tags → `unit` →
  `device_class` (later keys win), host compatibility tag only when subject
  type is `host` and the subject equals the normalised `host_id`.
- **`http_json_metric`** (`_execute_http_json_metric_spec`): every config
  field with its Python default chain (`subject_id` defaults to `service_id
  or host_id or "external"`, `collector_service` to `service_id or
  "graphyard-agent"`, …); required `url`, `metric_path`, `metric_name`; tags
  `spec_name` + configured `tags` (`str()` of keys and values); optional
  Bearer token; Basic auth only when both username and password are
  non-empty; `_resolve_json_path` (`$`, `$.a.0.b`, list index by digits);
  missing → `warning, 0, 1, "metric_path not found: <path>"`; non-finite →
  `warning, 0, 1, "metric value is not a finite number"`; `host` tag only for
  subject type `host`.
- **`http_page_probe`** (`_execute_http_page_probe_spec`,
  `_run_http_page_probe`): GET with manual redirects (status 301/302/303/
  307/308 with `Location`, URL resolved against the request URL as httpx
  does, at most 20 followed, the 21st raises `Exceeded maximum allowed
  redirects.`), body read and counted on every hop (`body_bytes >
  max_body_bytes` → `response body exceeded max_body_bytes=<n>`), TTFB to the
  final response headers measured from the first request, total after the
  final body. Success points (5, one `now`): `service.http_page_ttfb_seconds`,
  `…_total_seconds`, `…_status_code`, `…_success` (1 for 200–399, so an
  unfollowed 3xx is success), `…_redirect_count`; tags `spec_name`,
  `http_method=get` + configured tags; `source_entity_id = url`. Non-success
  status → `warning, written, 0, "unexpected status code: <code>"`. Warning
  path (6.1 classification): 2 points (`status_code` 0, `success` 0),
  `warning, written, 0, str(err)`. A write or registry failure inside that
  warning path is raised out of the `except` block and becomes `collector
  raised …` (P1 status, P2 text) — reproduced.
- **`unifi_device_traffic`** (`_execute_unifi_device_traffic_spec`):
  redirects followed automatically (max 20), no deadline wrapper. Login
  attempts by `auth_mode` (`auto` = legacy `/api/login` then UniFi OS
  `/api/auth/login`; JSON body `{"username", "password", "remember": true}`,
  compact as httpx `json=`), success test `_unifi_login_response_ok`
  (2xx and `meta.rc` `ok`, unparseable/non-dict bodies count as ok), CSRF
  header `x-csrf-token`, cookies kept; failure message
  `UniFi login failed; attempts: <mode> <endpoint> returned HTTP <code>[: <summary>]; …`
  with `_unifi_response_summary` (redact values ≥ 4 chars, regex-redact
  `"password": "…"`, whitespace collapse, 240 chars). Device list GET,
  `_find_unifi_device` (name first, then MAC per item), interface selectors
  `uplink`, `port_idx:<n>`, `port_name:<name>` with their `ValueError`
  messages, Python truthiness for `port_idx or num_port` and Python `==` for
  `port_idx` comparisons, `str()` of `speed`; two points (`rx_bytes-r`,
  `tx_bytes-r`), each skipped when not finite; no points → `warning, 0,
  skipped, "no numeric traffic values found"`.

As implemented in S7b (`graphyard-collect::executors`): an executor is
`async fn(&HttpContext, &impl MetricSink, name, &config) ->
Result<SpecOutcome, ExecutorError>`. `MetricSink` stands for
`influx.write_points` and `touch_registry_from_points` (the agent wires the
InfluxDB writer and the store into it in S8; a `SinkError` carries the
Python class and `str(err)`). `ExecutorError` is what Python lets escape the
executor: `Config` (a `SpecConfigError` of a typed knob, raised before the
required-field checks in both executors) or `Raised` (the page probe's
warning-path sink failure, which the runner turns into `collector raised
<class>: <message>`). The page probe uses `run_with_deadline` +
`run_page_probe` of S6, UniFi `send_following_redirects` (the login `POST`
carries httpx's compact `json.dumps(ensure_ascii=False)` body with
`Content-Type` and `Content-Length`; `client.post`/`client.get` read the
whole body before the status is looked at). UniFi bodies are decoded like
`resp.json()`, including `json.loads`' 4300-digit integer limit (which
the HTTP layer's `decode_json` applies since S7a); `resp.text` for the summaries is UTF-8 with
replacement characters (P2 for other declared charsets, 2.3). Points carry
`datetime.now(UTC)` at microsecond precision, one instant per execution.

As implemented in S7a (`graphyard-collect::executors::home_assistant`,
`::http_json`; pure parts in `graphyard_core::home_assistant` and
`graphyard_core::json_path`), on the executor interface above: the HA
executors also take the metrics run's `RunState` (the warn-once keys of
`_subject_mapping_warning_keys` and the compiled regexes; S8 creates one per
run), and read `datetime.now(UTC)` through `MetricSink::now` (default: the
wall clock), so the fixture scenarios pin it. `ExecutorError::Config` is
returned exactly where Python's `SpecConfigError` leaves the executor (the
typed knobs, read before the `try`: in `http_json_metric` before the
required-field checks, in the HA executors after them). Everything else is an
outcome, as the executor's `except Exception` makes it: HTTP errors, the
deadline, the body cap, the JSON path's `int()` error, normalisation errors
that Python lets escape the mapping (`ValueError` of the default subject
slug, `OverflowError` of a huge `value_multiplier` or of the timestamp's UTC
conversion) and sink failures (a registry failure after a successful write
keeps the points written) are `critical, 0, 0, str(err)`. Mapping warnings
are logged after each normalised state, so they follow the request's `HTTP
Request:` line as in Python. Pinned by `ha_mapping.json`, `json_path.json`
and the end-to-end scenarios of `ha_json_executors.json`, replayed against a
fake server.

### 6.4 InfluxDB writes (`influx.py:write_points`, P1)

Normalise each point (`normalize_metric_point`: metric required, subject type
in `{host, network_device, environment_sensor, service}` with the message
`Unknown subject_type '<t>'; expected one of ['environment_sensor', 'host',
'network_device', 'service']`, `normalize_subject_id`,
`_normalize_dimension_value` with fallbacks, `source_entity_id`/`service`
empty → none, host compatibility rules, finite value). Invalid points are
logged and skipped (not counted); all invalid → no request. Tags: the seven
fixed dimensions, then `source_entity_id`, `host`, `service` when set, then
custom tags except reserved keys (warning per reserved key). Line format as
`influxdb_client.Point.to_line_protocol` 1.50.0:

- tags sorted by key; a tag with empty key or empty escaped value is dropped;
- measurement escapes `,`, space, `\n`, `\t`, `\r`; tag keys/values and field
  keys escape `,`, `=`, space, `\n`, `\t`, `\r` (backslash is **not**
  escaped); a tag value ending in `\` gets a trailing space;
- field `value=<str(float)>` with a trailing `.0` removed (`21.0` → `21`,
  `1e+16` kept);
- timestamp in ns from the UTC datetime (µs × 1000);
- lines joined with `\n`.

Request: `POST <INFLUX_URL>/api/v2/write?org=<org>&bucket=<bucket>&precision=ns`,
`Authorization: Token <token>`, `Content-Type: text/plain`, timeout
`INFLUX_TIMEOUT_MS`, no retries (SYNCHRONOUS write). Non-2xx → error (P2
text, Python's `ApiException` string). `ingested` = number of valid points.

As implemented in S4a (pinned by `line_protocol.json`, recorded from the
Python client talking to a loopback server): `<INFLUX_URL>` loses one
trailing `/` and the path is appended as text (a path prefix is kept); a URL
without a scheme is `http` like urllib3's `parse_url`, other schemes than
`http`/`https` are `URLSchemeUnknown`; `org` is `INFLUX_ORG` and `bucket`
`INFLUX_BUCKET` as given (in v3 mode too, where only the client's default
org falls back to the bucket: an empty org is sent as `org=`), encoded like
`urllib.parse.urlencode` (`quote_plus`); headers `Accept-Encoding:
identity`, `Content-Type: text/plain`, `Accept: application/json`,
`Authorization: Token <token>` (v3: `dev-influxdb3-token` when empty),
`User-Agent: influxdb-client-python/1.50.0` (kept like the collectors'
httpx agent, 6.1); one fresh connection per call, no redirects followed (a
3xx is an `ApiException`), no proxy from the environment. The timeout
`urllib3.Timeout(total=ms/1000)` is not a deadline for the whole exchange:
it bounds the connect, and every later socket read waits at most what is
left of it (an inactivity timeout, so a slowly dripping body completes);
Rust uses the full value for the connect and for every read (the connect
time is not subtracted, P2). The response body is read in full and decoded
as UTF-8 before the status counts, so a truncated (`ProtocolError`) or
stalled (`ReadTimeoutError`) 2xx body fails the write, and a TLS failure is
the client's `ApiException` with status 0 (`Reason: SSLError`). `0` means
no timeout, a negative
value is urllib3's `ValueError` (`Attempted to set total timeout to …`), an
overflowing division or a value beyond CPython's nanosecond clock is its
`OverflowError`; these and the configuration errors come before any
connection. Order of effects: validation and reserved-key log lines, then
(only when a valid point exists) the configuration check, the request, and
the `metrics_write_rejected` warning only after a successful write or
instead of it when every point was rejected. Transport failures carry the
urllib3 class names (`NewConnectionError` with `[Errno N] <strerror>`,
`NameResolutionError`, `ConnectTimeoutError`, `ReadTimeoutError`,
`ProtocolError`, `UnicodeDecodeError`; texts P2). A measurement starting with `#`
logs the client's `SyntaxWarning` once per process (P2).

### 6.5 InfluxDB queries (`influx.py:query_range`, `query_condition_window`, P1)

- Window: lookback = `max(window_minutes, breach_minutes +
  ceil(stale_warning/60), ceil(stale_limit/60))` minutes before `now`; tag
  filters = `tags_filter` (`str()` of values, JSON key order) + normalised
  `subject_type_filter`/`subject_id_filter`; `host_filter` dropped with the
  warning for non-host subject types.
- Window arithmetic as Python does it: `ceil(seconds / 60)` is a float
  division of Python `int`s (`OverflowError: integer division result too
  large for a float` beyond the double range), `timedelta(minutes=…)` fails
  with `Python int too large to convert to C int` or `days=<d>; must have
  magnitude <= 999999999`, the subtraction with `date value out of range`;
  these come before the `tags_filter` check (`'<type>' object has no
  attribute 'items'` for a non-object) and `normalize_subject_id`.
- v2 Flux text (byte-identical, `_query_range_v2_flux`): `_flux_escape`
  (`\`, `"`, `${`), bracket access `r["key"]`, timestamps `isoformat` with
  `Z` (no fraction when µs is 0), `sort(columns: ["_time"], desc: false)`.
  Request (captured from the Python client in S4b, `flux.json` `requests`):
  `POST <INFLUX_URL minus one trailing "/">/api/v2/query?org=<quote_plus(org)>`,
  headers `Accept: application/json`, `Accept-Encoding: identity` (a gzip or
  deflate response is still decoded, as urllib3 does),
  `Content-Type: application/json`, `Authorization: Token <token>` (Latin-1
  as `http.client` encodes it), `User-Agent: influxdb-client-python/1.50.0`;
  body `json.dumps` with default separators and `ensure_ascii`:
  `{"extern": {"imports": [], "body": []}, "query": …, "dialect": {"header":
  true, "delimiter": ",", "annotations": ["datatype", "group", "default"],
  "commentPrefix": "#", "dateTimeFormat": "RFC3339"}}`. No retries, no
  redirects (a 3xx is an `ApiException`), no environment proxy. Timeout from
  `INFLUX_TIMEOUT_MS` as urllib3 applies `Timeout(total=ms/1000)` (connect and
  per read): 0 means none, a negative value is urllib3's `ValueError`
  (`Attempted to set total timeout to -0.005, …`), above the C time range
  `OverflowError` (P2 text). A non-2xx response is `ApiException` (status,
  reason, `HTTPHeaderDict` and body repr; header names lower-cased in Rust,
  P2; a body read failure raises the transport error instead, so a broken
  404 does not fall back in `auto`); transport errors keep urllib3's class (`NewConnectionError`,
  `ReadTimeoutError`, `ProtocolError`, P2 text).
- Annotated CSV (`FluxCsvParser`, table mode, `fixtures/flux.json` `csv`):
  rows read like Python's `csv` module (excel dialect), annotation rows start
  a table, typed values per `#datatype` (`string`, `boolean` = `"true"`,
  `long`/`unsignedLong`/`duration` = `int()`, `double` = `float()`,
  `base64Binary` = `base64.b64decode`, `dateTime:RFC3339[Nano]` =
  `datetime.fromisoformat` (fraction truncated to µs), unknown = `None`),
  empty cells take `#default` (the table id cell does not: `int('')` fails),
  a missing or short `#group`/`#default`/header row is an `IndexError`, an
  `error,reference` table raises `FluxQueryException` whose `str()` is the
  repr of both values, duplicated column names keep the first position and
  the last value (Python prints a warning to stdout and through `warnings`,
  Rust logs it, P2). Then per record: `KeyError` without `_value`/`_time`,
  skip when either is `None`, `AttributeError` for a non-datetime `_time`,
  `_value` must be a finite number (`InfluxDB query sample must be a finite
  number`, via `float(str(v))`), tags = every column except the known ones,
  all values as `str()` (`None`, `True`, `b'…'`, `str(datetime)`).
- `datetime.fromisoformat` is a port of CPython 3.12's C parser
  (`graphyard_core::pycompat::py_fromisoformat`, `fixtures/isoformat.json`);
  `influxdb_client` uses it because `ciso8601` is not installed. S7a reuses it
  for Home Assistant's `last_updated`.
- v3 SQL (`_query_range_v3_sql`, `_read_v3_sql_rows`): SQL text identical,
  `POST <INFLUX_URL minus trailing "/">/api/v3/query_sql` with httpx's compact
  JSON `{"db":…,"q":…}` (UTF-8, not ASCII-escaped), `Accept:
  application/json`, `Accept-Encoding: gzip, deflate` (decoded),
  `User-Agent: python-httpx/0.28.1`, Bearer token when set (the v3 default
  token of `_build_client` is not used here), per-operation timeout
  `max(1, INFLUX_TIMEOUT_MS/1000)` s (its `OverflowError` is not caught),
  10 MiB decoded cap, 30 s total including the JSON decode; the error texts
  of `_read_v3_sql_rows` (404 hint, `HTTP <code>` for any other non-2xx,
  `InfluxDB SQL query failed: <httpx class>` for transport errors:
  `ConnectError`, `ConnectTimeout`, `ReadTimeout`, `RemoteProtocolError`,
  `ReadError`, `DecodingError`, `UnsupportedProtocol` (also for an empty or
  scheme-less `INFLUX_URL`), `UnicodeEncodeError` for a non-ASCII token,
  `LocalProtocolError` for a control character in it; invalid JSON, size
  and deadline). The httpx request log line is written once the response
  headers arrive. Rows: non-objects and rows without `value`/`time` are
  skipped, `time` goes through `_parse_influx_ts(str(time))` (every `Z`
  replaced, a `ValueError` skips the row, the UTC conversion's
  `OverflowError` propagates), values and tags as `str()` of the JSON value
  (`None`, `True`, dict/list repr).
- `auto`: v2 first; only a 404 falls back to v3; any other error propagates.

---

## 7. Condition evaluation (`services.py`, pure, P1)

`evaluate_condition`: no samples → `warning`/"No samples available in
condition window" (or `ok` with "…, not alerting (intermittent)" when
`alert_when_stale` is false). Group by `_series_key` (metric + non-empty
canonical dimensions + all custom tags, sorted), sort each series by time,
label with `_series_labels`, evaluate each with `_evaluate_series`:
staleness `int((now - latest.ts).total_seconds())` against
`stale_limit_seconds()` (`stale_after_seconds` or the global), then critical
before warning via `_is_breached_for_duration` (window start, anchor carry
bounded by the global stale setting, a sample exactly at the window start
supersedes the anchor, one-minute grace without anchor, all evaluated samples
must breach). One series → its result. Several → worst status, ties by
oldest latest sample then label; `ok` reports the newest series value with
` (<n> series)`; otherwise ` [series: <label>] (<k> of <n> series not OK)`.
Thresholds in messages use `py_float_repr`. Pinned by `conditions.json`
(generated from `evaluate_condition` with the query stubbed and the clock
injected), covering every test of `tests/test_condition_evaluation.py`.

As implemented in S5 (`graphyard-core::conditions`): `ConditionRule` holds
the fields the evaluation reads (metric name, stored operator text,
thresholds, `breach_minutes`, `stale_after_seconds`, `alert_when_stale`);
the query filters stay with the `SampleSource` the caller builds per
condition (S4b/S8; a closure `FnMut(now) -> Future<Result<Vec<MetricSample>,
E>>` is one). `evaluate_condition(rule, stale_warning_seconds, now, source)`
truncates `now` to microseconds, queries, then runs `evaluate_samples`;
`EvaluationError` (`Query(E)` or the Python exception) displays as `str(err)`
for the `evaluation error: …` message of 3.5. Time runs on Python's
microsecond timeline (`timestamp_micros`); `pycompat::py_timedelta_minutes`
and `py_datetime_add` raise CPython's `OverflowError` texts (P2) for a
`breach_minutes` beyond `timedelta`'s range or a window outside
`datetime`'s, and `py_total_seconds` rounds like `timedelta.total_seconds()`
(correctly rounded beyond 2**53 µs; `conditions.json` pins it). An unknown
operator is the `KeyError` (`'<op>'`) of `_operator_fn`, raised only when a
threshold is compared. The anchor carry uses the global setting
(`max_carry_seconds=settings.CONDITION_DATA_STALE_WARNING_SECONDS`) even when
the condition has its own allowance, and `stale_after_seconds = 0` means the
global setting, both as in Python.

---

## 8. Build, release, deployment

### 8.1 Target: `x86_64-unknown-linux-gnu`, glibc floor 2.35

Same decision as nyxmon: a glibc binary (only glibc/libgcc dynamic
dependencies, rustls instead of OpenSSL) built on `ubuntu-22.04` (glibc 2.35
≤ macmini 2.39). musl is excluded: its resolver ignores `nsswitch.conf`, so
`macmini.local` (HA spec, mDNS) would not resolve. `release_checks.py
glibc-floor` asserts no symbol newer than `GLIBC_2.35` and only
glibc/libgcc `NEEDED` entries.

### 8.2 Release pipeline (graphyard repo)

`.github/workflows/agent-rs-release.yml`, adapted from nyxmon's: tag
`agent-rs-v<semver>` (must equal `crates/graphyard-agent/Cargo.toml`), plus
`workflow_dispatch` for a dry build without release; full Rust tests, the
fixture drift test (own job `fixtures` with the locked Python environment,
which the release job also needs), `cargo build --release --locked --target
x86_64-unknown-linux-gnu`, glibc floor check, `--version` smoke test against
the workflow's commit, deterministic archive
`graphyard-agent-<ver>-x86_64-unknown-linux-gnu.tar.gz` (binary + `LICENSE`)
and `<archive>.sha256`, GitHub release (only that job has `contents:
write`), pinned action SHAs, no cargo cache in the release job. CI
(`.github/workflows/ci.yml`) gains a `rust` job (fmt, clippy `-D warnings`,
`cargo test --locked`, release build size report, `cargo deny --locked
check`) and a `parity` job (section 9.4). First release `agent-rs-v0.1.0`
(S10b, published 2026-10-21 from `19f56e6`; archive sha256
`42d6dbb5b931599c1b35ab7f19a29823bf9583d65b45e2259a69b5dbc64b80e2`; recorded in
`agent-rs/README.md`, "Published releases").

### 8.3 Deploy mechanism today and the changes

**Today.** `just deploy-one graphyard_core` in ops-control. Its
`services-metadata.yml` entry has `engine: pyinfra` (since 2026-10-16) and a
`source:` block (graphyard default branch, or `DEPLOY_REF`;
`DEPLOY_SOURCE=local` syncs `$PROJECTS_ROOT/graphyard`), so `deploy.just`
execs `just pyinfra-deploy-one graphyard_core macmini`. ops-control's
`pyinfra_support/services.py` builds a `GraphyardConfig` from the inventory
(`macmini_graphyard_*` host vars, SOPS secrets `secrets/prod/graphyard.yml`
and the spec secrets) and runs ops-deploy (pinned in
`requirements-ops-deploy.txt`, currently `f8f24ce`)
`ops_deploy.services.graphyard.core.deploy_graphyard_core`: rsync of the tree,
`uv sync`, migrate/collectstatic when needed, units rendered byte-identical to
the Ansible role `graphyard_deploy` (ops-library), restart of web and agent
when code/deps/env changed, `/v1/health` check, both units running, source
commit recorded. The Ansible role remains live in two places: the engine
fallback (delete the `engine` line) and `just deploy-one graphyard` (the whole
stack via `playbooks/deploy-graphyard.yml`, engine Ansible, which also
redeploys Vector and therefore must not be used while the Atlas step is
open). The port deploys exactly through these paths; nothing new is invented.

**ops-library `graphyard_deploy`** (S11a), mirroring `nyxmon_deploy`'s
`agent_rust.yml`/`agent_verify.yml`:

```yaml
graphyard_agent_impl: python                 # python | rust
graphyard_agent_rust_version: ""             # e.g. "0.1.0"; required for rust
graphyard_agent_rust_sha256: ""              # sha256 of the release archive
graphyard_agent_rust_release_url: >-
  https://github.com/ephes/graphyard/releases/download/agent-rs-v{{ graphyard_agent_rust_version }}/graphyard-agent-{{ graphyard_agent_rust_version }}-x86_64-unknown-linux-gnu.tar.gz
graphyard_agent_rust_local_archive: ""       # controller path (development)
graphyard_agent_rust_install_root: /opt/graphyard-agent
graphyard_agent_rust_keep_versions: 3
```

Steps after `django.yml` (migrations applied) and before `service.yml`:
assert the settings; when a version is set (independent of the impl), stage
the archive with `get_url` + `checksum: sha256:` into
`<root>/<version>/` (root:root 0755; an installed version is never modified;
a different archive for an installed version fails the run); validate by the
versioned path as `graphyard` without any `environment:` (no secrets needed):
`--version` must report the version and `--check-schema --db
<django_path>/db.sqlite3` must pass; drop-in
`/etc/systemd/system/graphyard-agent.service.d/agent-impl.conf` present iff
`rust`:

```ini
[Service]
ExecStart=
ExecStart=/opt/graphyard-agent/<version>/graphyard-agent --db /opt/apps/graphyard/site/src/django/db.sqlite3
```

The drop-in change notifies `reload systemd` + `restart graphyard agent`;
the base unit template is unchanged (Python `ExecStart`, `EnvironmentFile`,
`WorkingDirectory`, `Requires=graphyard-runtime.service` apply to both). After
handlers: verify the live process (30 s retry): `rust` →
`readlink /proc/<pid>/exe` equals the versioned binary; `python` →
`/proc/<pid>/cmdline` contains `manage.py` and `start_agent` and the exe is
not under the install root. Then prune versions beyond `keep_versions`, never
the active or the previous one. Also: add `agent-rs/target/` to
`graphyard_rsync_excludes` (a local-checkout deploy must never ship a cargo
target directory). README, CHANGELOG under "Unreleased", molecule/test
coverage. Workers never bump `galaxy.yml`: the coordinator makes the
collection release commit (`galaxy.yml` bump + CHANGELOG heading) on
ops-library main right before the production deploy that needs it (S12, and
again before S15b; section 14).

**ops-deploy `services/graphyard`** (S11b): `GraphyardConfig` gains
`agent_impl`, `agent_rust_version`, `agent_rust_sha256`,
`agent_rust_release_url`, `agent_rust_local_archive`,
`agent_rust_install_root`, `agent_rust_keep_versions` with the role's
validation; a graphyard agent module with `install_agent`/`select_agent`/
`check_agent`/`prune_agents` ported from `services/nyxmon/agent.py` and its
`files/agent.sh` (install/validate/prune), wired into `core_steps` after
migrations and around `service_rollout` of the agent unit; the drop-in
template byte-identical to the role's (parity copy in
`tests/parity/ansible/`); `agent-rs/target/` in `SOURCE_EXCLUDES`. Whether
nyxmon's module is generalised into a shared helper is a coordinator decision
(section 13.2); the default is a graphyard module that reuses ops-deploy's
existing helpers and leaves nyxmon untouched.

**ops-control** (S11c): `pyinfra_support/services.py` maps
`graphyard_agent_impl/_rust_version/_rust_sha256` from macmini's host vars
into `GraphyardConfig` and refuses the other `graphyard_agent_rust_*`
variables when set (pattern `NYXMON_AGENT_VARS`/`NYXMON_AGENT_REFUSED`);
`inventories/prod/host_vars/macmini.yml` gets `graphyard_agent_impl: python`
plus the pinned version and sha256 (the only place to flip; the playbooks do
not set them as role vars); `requirements-ops-deploy.txt` pins the ops-deploy
commit with S11b; `docs/GRAPHYARD_RUST_AGENT.md` (install, shadow, cutover,
verification, rollback), CHANGELOG; tests (`tests/test_pyinfra_graphyard.py`
literals, refusal of unmapped vars; the real host-var value may be `python` or
`rust`, the lesson of nyxmon S16). Trust (Q1): `graphyard-agent.service` in
macmini's `restart_units` of `playbooks/ca-trust-consumers.yml` (health
command `sleep 5 && systemctl is-active --quiet graphyard-agent.service`, as
for `vector.service`), the macmini row of `docs/CA_TRUST_CONSUMERS.md` and
`tests/test_ca_trust_consumers_playbook.py`. For the Ansible path,
`playbooks/deploy-graphyard-core.yml` gets a pre-task asserting the
ops-library version that knows `graphyard_agent_impl`; that version exists
only after the coordinator's release commit, so the assert is added at the
start of S12 (11.1), not in S11c.

### 8.4 Production deploy rules (binding)

- Every production deploy step (S12, S14, S15b) takes the shared lock on the
  controller: `mkdir /tmp/ops-control-deploy.lock` and an owner file inside;
  wait and retry when it exists; remove only a lock this step created. The
  shadow setup/teardown on macmini (S13) takes it too.
- Only `just deploy-one graphyard_core` (pyinfra), never the `graphyard`
  stack target while the Atlas step is open; no change to `graphyard_vector`
  or the ingest tokens.
- Long runs in tmux. Check with the coordinator before each production step
  (the eMudhra/certifi `graphyard_core` deploy, Traefik ACME on `production`
  and fractal MinIO TLS may be scheduled; the page probes hit sites on
  `production` and twenty specs read fractal endpoints).
- A `graphyard_core` deploy ships the current graphyard main to macmini
  (including, today, the not-yet-deployed certifi relock) and restarts web
  and agent when code changed.
- No production step of this stream runs alongside the
  `echoport-command-guard` cutover, which is scheduled last in the campaign
  and alone (section 14).

### 8.5 Rollback

- Normal: `graphyard_agent_impl: python` in macmini host vars, `just
  deploy-one graphyard_core`: the drop-in is removed, systemd restarts the
  unit on the venv Python. Nothing in the database or InfluxDB depends on
  which agent wrote it.
- Emergency on the host: `rm /etc/systemd/system/graphyard-agent.service.d/agent-impl.conf
  && systemctl daemon-reload && systemctl restart graphyard-agent`, then flip
  the host var before the next deploy.
- Rust to Rust: pin the previous version and sha256; it is still installed
  (pruning keeps it).

### 8.6 Documentation

graphyard: `agent-rs/README.md` (build, test, run), `docs/agent-rs.md`
(architecture, schema contract rule, parity classes, divergences,
operations), README "Agent Runtime" section, CHANGELOG per user-visible
slice. ops-library, ops-deploy, ops-control as in 8.3.

---

## 9. Test strategy

### 9.1 Rust tests

- `graphyard-core`: fixture-driven tests per module (9.2) plus unit tests.
- `graphyard-store`: every operation of section 5 against databases built
  from `schema.sql`, asserting the fixture row images; 0-row updates;
  heartbeat locked-retry with a second connection holding the write lock;
  registry statement sequences, a row inserted between the registry check
  and its transaction, `get_or_create`'s `IntegrityError` path (a trigger
  rejecting the insert) and a row that appears inside the transaction; the
  `ingest_token` grep test.
- `graphyard-influx`: fake InfluxDB (tokio server) for write requests (path,
  query string, headers, body parsed with `influxdb-line-protocol`), Flux
  responses (annotated CSV with several tables, defaults, NaN, empty cells),
  v3 SQL (404, size cap, slow drip deadline, invalid JSON), auto fallback.
- `graphyard-collect`: fake HTTP servers for every rule of section 6
  (timeouts via stalled servers, dripping headers/body, body caps incl.
  redirect bodies, gzip, redirects incl. relative `Location`, cookies across
  redirects, 21 redirects, TLS with `rcgen` certs and `verify_tls: false`,
  UniFi legacy and UniFi OS login, CSRF, redaction), abandoned-lookup cap
  with a resolver stub that blocks.
- `graphyard-agent`: scheduler with an injected clock; run aggregation and
  heartbeat rules with fake executors; CLI validation; process e2e (9.3).

### 9.2 Fixtures (Python is the oracle)

`agent-rs/fixtures/generate.py` (`just rs-fixtures`, `uv run python …` with
`DJANGO_SETTINGS_MODULE=config.settings.test`, `PYTHONHASHSEED=0`) imports the
real modules and writes deterministic JSON (clocks and `datetime.now`
injected by monkeypatching). The driver (S1) discovers the builder modules
`fixtures/gen_*.py`, each exporting `GENERATORS` (file name → builder), so
slices add fixtures without editing it; it removes `DJANGO_*`, `GRAPHYARD_*`
and `INFLUX_*` variables from its environment and keeps django-environ from
reading `src/django/.env` (builders set what they need). Files are rendered
with `json.dumps(indent=1, ensure_ascii=True, allow_nan=False)` in the key
order the builder wrote (key order is contract in configs and tags);
non-finite floats are encoded explicitly by the builder. A builder returning
a string produces a text file written verbatim (`schema/schema.sql`).

| Fixture | Python source | Content |
| --- | --- | --- |
| `model.json` (S1) | `models.py` choices, `_STATUS_SEVERITY`, `_SPEC_EXECUTORS`, `_operator_fn`, the `MetricPoint`/`MetricSample`/result dataclasses | value tables, comparisons, dataclass fields and defaults |
| `pycompat.json` | `repr(float)`, `format(x, "g")`, `float()`, `int()`, `str()`/`repr()`/`float()` of JSON values, `_describe`, `str.strip/split`, `==`, the `json.loads` integer digit limit | value corpus |
| `settings.json` | `config/settings/base.py` via django-environ (the module executed per case), `apps.py:_configure_sqlite_connection` | raw env → parsed value or startup error, `$` proxies, pragma statements and warnings |
| `spec_config.json` | `spec_config.py` (all helpers, `validate_spec_config`) | config corpus → value or message |
| `normalize.json` | `normalize_subject_id`, `_normalize_dimension_value`, `parse_finite_float`, `normalize_metric_point` | point corpus → normalised point or message |
| `line_protocol.json` | `write_points` with a capturing client, `Point.to_line_protocol` | points → lines, logs, counts (escape and float corner cases) |
| `json_path.json` | `_resolve_json_path` (incl. the 24 production metric paths) | payload × path → `repr` of the value, not found, or the `int()` error; D-6 cases with the Rust result |
| `ha_mapping.json` | `_resolve_subject_mapping`, `_resolve_home_assistant_metric_mapping`, `_normalize_home_assistant_sensor_state` (incl. the production spec config of section 1) | per case a sequence of calls on one spec sharing the warn-once set → point, subject or metric mapping, `None` or the escaping exception, the warning records, the warn-once keys |
| `ha_json_executors.json` (S7a) | `_execute_home_assistant_sensor_spec`, `_execute_home_assistant_env_scan_spec`, `_execute_http_json_metric_spec` on a scripted `MockTransport` with recording (optionally failing) `write_points`/`touch_registry_from_points` | spec + replies → outcome or escaping `SpecConfigError`, requests (target, `Authorization`, `Accept`), points written and touched, log lines |
| `unifi.json` (S7b, `gen_page_probe_unifi.py`) | `_find_unifi_device`, `_resolve_unifi_interface_stats`, `_unifi_response_summary`, `_unifi_response_error`, `_unifi_login_response_ok`, `_execute_unifi_device_traffic_spec` on `httpx.MockTransport` with a recording sink | device/selector/response corpus; executor scenarios: config × scripted replies → requests (method, target, selected headers, body), sink calls with their points, outcome or raised exception, `HTTP Request:` lines |
| `page_probe.json` (S7b, same builder) | `_execute_http_page_probe_spec` on `httpx.MockTransport` with a recording sink | executor scenarios as for UniFi (timing values masked) |
| `http_errors.json` | httpx `raise_for_status`, exception classes for the page probe | responses/URLs → message and class |
| `flux.json` | `query_condition_window` (with `query_range` stubbed), `_query_range_v2_flux` (query text), `query_range` mode v2 end to end against a loopback fake InfluxDB (request, `FluxCsvParser` on canned CSV, errors), `base64.b64decode` | condition × now → window, tags, warning, Flux text; scripted responses → request, samples or error |
| `sql_v3.json` | `_query_range_v3_sql` (SQL text, payload, headers), `query_range` modes v3 and auto end to end (row decoding, `_read_v3_sql_rows` errors, the httpx log line) | |
| `isoformat.json` | `datetime.fromisoformat` (C, 3.12), `str(datetime)`, `influx._ensure_utc` | string corpus → fields, offset, `str()`, UTC, or the `ValueError` |
| `conditions.json` | `evaluate_condition` with stubbed samples, `timedelta.total_seconds()` | condition × samples × now → evaluation or exception; microseconds → `total_seconds()` |
| `store_ops.json` | spec/condition loads and updates, `record_heartbeat` (S3a), `touch_registry_from_points` (S3b) on a temp DB | rows before → loaded values or exception, rows after |
| `runs.json` (S8, `gen_runs.py`) | `run_metric_collection_specs_once`, `evaluate_conditions_once` with `_SPEC_EXECUTORS` and `query_condition_window` stubbed by scripts, clocks fixed | scenario rows before + scripts → counters or the aborting exception, spec/condition/heartbeat rows after |
| `datetime_codec.json` | `parse_datetime` through the `sqlite3` converter, `convert_datetimefield_value`, `adapt_datetimefield_value` | stored text → loaded instant or exception, text written back |
| `schema/` (`gen_schema.py`) | `manage.py migrate`, the migration operations | section 5.1 |

Library-derived (P2) messages are stored as `{"class": …, "p2": true}` and
the Rust tests check only the class. A drift test
`tests/test_agent_rs_fixtures.py` runs `generate.py --check` as a subprocess
(the same fixed hash seed and scrubbed environment as `just rs-fixtures`),
which re-renders every file in memory and fails on any difference, on a
missing file and on an orphaned top-level `*.json` that no builder generates,
so a Python behaviour change cannot merge without regenerated fixtures. Rust
loads fixtures with `include_str!`.

### 9.3 Process e2e (Rust binary)

Against a temp database from `schema.sql` and fake servers: `--run-once`
writes the expected rows and lines; SIGINT prints the interrupted line and
exits 0; SIGTERM during a slow spec exits 0 promptly without a partial row,
also while a `getaddrinfo` lookup is blocked (4.1);
old schema or missing database exits 1; `--check-schema` reports precisely;
`--influx-write-file` produces zero requests to the fake InfluxDB write
endpoint while queries still reach it; CLI validation messages and exit codes.
As implemented in S9b (`graphyard-agent/tests/e2e.rs`, with the S8 cases in
`process.rs`): the blocked `getaddrinfo` case needs a resolver stub, which
the binary cannot take, so it runs on `run_agent`, the function `main`
calls, with the signal future injected (`shutdown.rs`, S8).

### 9.4 Offline parity harness (Python vs Rust on identical inputs)

`agent-rs/parity/run_parity.py` (`just rs-parity`, CI job `parity`): two
copies of a database seeded with a corpus covering every spec type and every
condition branch (incl. a config error, an unknown spec type, a disabled
spec, a non-host `host_filter`); each implementation gets its own set of
fakes (`parity/fakes.py`: HA, UniFi with both auth modes, JSON metrics, page
probe with redirects, slow and oversized bodies, and a fake InfluxDB that
records writes and answers Flux/SQL queries with samples placed relative to
the `stop` of the requested range). Python runs as `manage.py start_agent
--run-once` with a harness settings module (`parity/settings_parity.py`,
outside `src/`) pointing `DATABASES` at its copy; Rust as `graphyard-agent
--run-once --db <copy>`. Fakes script responses by request index per
endpoint, so N rounds are deterministic. Compared per round: spec rows,
condition rows, heartbeat rows, registry rows (timestamps masked to
null/non-null), written lines (parsed; values of `now()`-based timing points
and timestamps masked), Flux query and v3 SQL texts with the `range`/`time`
bounds masked (both processes take `now` from their own wall clock, so the
bounds differ between runs; the harness compares the window length
`stop - start` exactly instead), stdout lines, request logs of the fakes
(method, path, query, selected headers, body). The exact query text
including bounds is pinned by the `flux.json`/`sql_v3.json` fixtures with an
injected clock, not by the harness. Gate: zero unmasked differences.

As implemented in S9a: five rounds, each a `--run-once` of both agents at
the same time with the same environment, varying `INFLUX_API_MODE` (`v2`,
`v2`, `v3`, `auto` with the fake's Flux endpoint answering 404) and ending
without `INFLUX_TOKEN` (the lazy configuration errors of 3.3, also on the
page probe's warning path, `collector raised …`). The collector fakes script
by request index per path; the fake InfluxDB keys its query scripts by the
queried metric and advances one entry per answered query (Flux or SQL), so
an `auto` round's 404 does not consume one. The UniFi fakes serve TLS with a
certificate the harness makes with the `openssl` CLI (`verify_tls: false`).
`settings_parity.py` is the base settings with the production `LOGGING` and
no `.env` (D-9), so stderr also carries the operator log lines of 3.6, which
are compared too (`HTTP Request: …`, the mapping, validation and reserved-key
warnings, `metrics_write_rejected`, the host-filter warning, `Metric
collection spec … raised` up to `raised`). Masks beyond the list above:
`next_run_time` is compared as its distance to `last_run_at`; a written
timestamp is masked only when it lies within the agent's run (payload
timestamps such as Home Assistant's `last_updated` are compared exactly);
`Content-Length` of InfluxDB requests is not compared (the bodies hold
`now`-based values). Each round also checks that the Python agent reached
the status the corpus expects for every spec and condition, so the
comparison cannot pass on a corpus that no longer exercises its branches.
S15a adds three rounds after the five agent rounds that run `manage.py
evaluate_conditions` and `graphyard-agent evaluate-conditions --db` (11.4);
their exit codes are compared too, and the `CommandError` line joins the
compared stderr lines.

### 9.5 TESTMAP (Python test → Rust counterpart)

S1 creates `agent-rs/TESTMAP.md` from this table; every slice updates its
rows.

| Python tests | Rust counterpart | Slice |
| --- | --- | --- |
| `test_agent_command.py` (both) | CLI/run-once tests in `graphyard-agent`, e2e | S8 |
| `test_condition_evaluation.py`: all `evaluate_condition`/series/anchor/staleness/intermittent tests | `conditions.json` cases (`graphyard-core::conditions`) | S5 |
| `test_condition_evaluation.py::test_evaluate_conditions_once_updates_condition_and_heartbeat` | run aggregation test + `store_ops.json` | S8, S3a |
| `test_condition_evaluation.py::test_query_condition_window_*`, `test_metric_dimensions.py::test_query_condition_window_*` | `flux.json` | S4b |
| `test_condition_evaluation.py::test_stale_allowance_must_be_positive`, `test_condition_clean_*`, `test_condition_admin_form_*` | n/a (model/admin validation, web only) | — |
| `test_metric_collection_specs.py` HTTP JSON tests | `graphyard-collect` fake-server tests (`ha_json_executors.json`) | S7a |
| `test_metric_collection_specs.py` page probe tests | fake-server tests | S7b |
| `test_metric_collection_specs.py` HA sensor/env scan tests | fake-server tests + `ha_mapping.json` | S7a |
| `test_metric_collection_specs.py` UniFi tests (incl. redaction) | fake-server tests + `unifi.json` | S7b |
| `test_metric_collection_specs.py::test_http_json_metric_spec_skips_non_finite_value` | `normalize.json` + fake-server test | S4a, S7a |
| `test_metric_collection_spec_isolation.py::test_bad_spec_does_not_stop_later_specs`, `::test_unexpected_collector_exception_is_isolated` | run isolation tests | S8 |
| `test_metric_collection_spec_isolation.py` cap/redirect/timeout tests | `spec_config.json` + fake-server tests | S2, S6 |
| `test_metric_collection_spec_isolation.py::test_full_clean_*`, `::test_validate_spec_config_rejects_non_object`, `::test_model_validates_derived_wait_limit` | `spec_config.json` (`validate_spec_config` is web-side but shares helpers) | S2 |
| `test_page_probe_deadline.py` (all, incl. `caps_abandoned_workers`) | deadline/abandoned-lookup tests | S6 |
| `test_http_json_budget.py`, `test_home_assistant_budget.py` | budget/deadline tests (the next spec after a failing one: run isolation, S8) | S6, S7a, S8 |
| `test_influx_modes.py` normalisation/write tests | `normalize.json`, `line_protocol.json` | S4a |
| `test_influx_modes.py` query mode tests | fake InfluxDB tests | S4b |
| `test_influx_query_safety.py` (all) | v3 SQL fake tests, `flux.json` NaN case | S4b |
| `test_metric_dimensions.py` mapping/emission tests | `ha_mapping.json`, fake-server tests | S7a, S7b |
| `test_metric_dimensions.py::test_touch_registry_*` | `store_ops.json` | S3b |
| `test_registry_write_batching.py` registry semantics without a bound subject (statement counts, 60 s rule, last source system, re-parenting, collector host position) | `store_ops.json` registry cases + statement-count tests | S3b |
| `test_registry_write_batching.py` bound-token parts | n/a (bound-subject branch, web ingest, 5.7) | — |
| `test_query_and_ingest_log_escaping.py::test_flux_interpolation_literal_in_query` | `flux.json` | S4b |
| `test_query_and_ingest_log_escaping.py::test_metric_log_escapes_newline`, `::test_ingest_normalization_record_caps_error_details` | n/a (web ingest) | — |
| `test_evaluate_conditions_command.py` | `evaluate-conditions` subcommand tests + e2e (11.4): `graphyard-agent/tests/evaluate_conditions.rs`, `e2e.rs`, `runs.json`, parity rounds 6 to 8 | S15a |
| web/ingest/inventory/token/admin/login tests | n/a (web only) | — |

### 9.6 just recipes and CI

`rs-test`, `rs-lint` (fmt check + clippy), `rs-fmt`, `rs-fixtures`,
`rs-build`, `rs-build-linux` (Docker, pinned
`rust:<ver>-bookworm`, glibc 2.36 ≤ 2.39, for `*_local_archive`
development deploys), `rs-deny` (S1); `rs-parity` (S9a, with the harness);
`test` runs `uv run pytest` then `rs-test`, `lint` adds `rs-lint`,
`typecheck` adds `cargo check`. Workers never build or test in `~/projects` (worktrees only).

---

## 10. Lean goals (measured like nyxmon PLAN 4.4)

| Metric | Python today | Rust target |
| --- | --- | --- |
| cgroup `MemoryCurrent` steady | 80.4 MB | ≤ 16 MiB |
| cgroup `MemoryPeak` | 83.8 MB | ≤ 40 MiB (an HA `/api/states` body may approach the 10 MiB cap and is parsed in memory) |
| `RssAnon` | 76 MiB | ≤ 12 MiB |
| `ps` RSS | 92 MiB | ≤ 25 MiB (includes the file-backed binary mapping, shared page cache) |
| stripped binary | — | ≤ 15 MiB |
| CPU | ≈ 4.7 min/day | ≤ 1.5 min/day |
| startup to first run | seconds (Django import) | ≤ 1 s |

Measured in the shadow run and after cutover from `systemctl show -p
MemoryCurrent -p MemoryPeak`, `/proc/<pid>/status` (`RssAnon`, `RssFile`) and
`ps`; recorded in the cutover report. If glibc malloc arenas exceed the
budget, the drop-in sets `Environment=MALLOC_ARENA_MAX=2` before any other
allocator is considered.

---

## 11. Rollout

### 11.1 Install (S12)

With S10b's release and S11a–c merged and the coordinator's ops-library
collection release commit on ops-library main (section 14): add the
`deploy-graphyard-core.yml` pre-task asserting that released version (8.3,
reviewed ops-control commit), set the version and sha256 with
`graphyard_agent_impl: python` in macmini host vars, take the deploy lock,
`just deploy-one graphyard_core`. Verify: `/opt/graphyard-agent/0.1.0/` exists,
`--check-schema` passed, no drop-in, `graphyard-agent` still runs Python
(`/proc/<pid>/cmdline`), web healthy.

### 11.2 Shadow run on macmini (S13, ≈ 24 h, no production writes)

Prerequisites: S9b tooling, S12. Owner approval given 2026-10-21 (Q2): about
24 h, the database copy in a 0700 directory deleted afterwards, nothing
written to InfluxDB, doubled collector traffic accepted. Anything beyond
these conditions needs a new owner decision.

1. `parity/shadow_prepare.py` (stdlib only, run as `graphyard`, lock taken):
   online copy of the live database through SQLite's backup API into
   `/var/lib/graphyard-shadow/` (`graphyard`, 0700; the copy holds spec
   secrets and is deleted at the end); record T0.
2. Transient unit with the production environment:
   `systemd-run --unit graphyard-agent-shadow -p User=graphyard -p Group=graphyard -p EnvironmentFile=/etc/graphyard/graphyard.env -p WorkingDirectory=/var/lib/graphyard-shadow /opt/graphyard-agent/<ver>/graphyard-agent --db /var/lib/graphyard-shadow/db.sqlite3 --influx-write-file /var/lib/graphyard-shadow/points.lp`.
   The shadow reads production InfluxDB for conditions and writes nothing to
   it. It doubles collector traffic (HA, UniFi login every minute, fractal
   endpoints, six public pages every 5 minutes).
3. `parity/shadow_record.py` samples both databases read-only every 60 s
   (spec rows, condition rows, heartbeats, registry rows) and both units'
   memory/CPU/restarts into a JSONL file in the shadow directory.
4. `parity/shadow_compare.py` at the end: queries the production series
   written by the Python agent over the window through
   `docker exec graphyard-influxdb influx query` (the container's CLI
   config, no token handling in the tooling) and compares with
   `points.lp`: series keys per spec (exact), HA points by `(series, ts)`
   (values exact), UniFi/JSON values aligned by minute (tolerance, P2), page
   probe status/success sequences (exact) and timings (medians); spec
   `last_status`/`last_error` sequences (P2 texts masked), condition rows,
   heartbeat statuses and details, registry row sets.
5. Acceptance: no series-key difference; every status/message difference
   explained by an endpoint change between the two samples; condition and
   heartbeat statuses identical; registry rows identical (timestamps masked);
   no restart; lean goals met. Findings are fixed in a follow-up slice and
   the shadow repeated (shorter, within the Q2 conditions) if code changed.
   Teardown: stop the unit, delete the shadow directory.

As implemented in S9b (operator commands in `docs/agent-rs.md`): the shadow
directory is created by root (`install -d -o graphyard -g graphyard -m
0700`), `shadow_prepare.py` refuses one that is not the caller's or not 0700
and never overwrites a copy; besides the D-11 documents it lists values of
another storage class (D-13) and `NaN` tokens (D-4), and exits 2 when it
lists anything (the copy is kept); `--disable-spec` is the R6 option. The
record (`record.jsonl`) holds per tick only the rows that changed, and spec
rows without `config`. `shadow_compare.py` pairs a JSON/UniFi shadow point
with the production point of the same series nearest in time within 60 s
(`--align-seconds`) instead of a fixed minute grid, since the two agents run
a spec up to a minute apart; a series whose median relative difference
exceeds the tolerance (default 5 %) is a finding, single pairs beyond it are
notes. Status sequences are compared with repeats collapsed; `last_error`,
condition messages and heartbeat details are notes (P2). It reports
findings and notes and exits 1 on findings only with `--fail-on-findings`.

### 11.3 Cutover and rollback drill (S14)

Owner decision Q3 (2026-10-21): one cutover window after the S13 shadow
report, coordinated with the eMudhra stream. The cutover deploy ships the
then-current graphyard main, so the coordinator agrees the window with that
stream's certifi relock (`85cdb60`): either the eMudhra `graphyard_core`
deploy has already shipped the relock, or the cutover deploy is that deploy,
as agreed with the eMudhra stream. Either way the Python agent of the
rollback drill runs with certifi 2026.10.19. Then coordinator check of the
schedule, deploy lock, tmux.
Set `graphyard_agent_impl: rust`, `just deploy-one graphyard_core`; record T1.
Verification: unit active, exe under `/opt/graphyard-agent/<ver>/`,
`NRestarts=0`; every enabled spec has `last_run_at > T1` within its interval
+ 60 s; `metric_collectors` and `condition_evaluator` heartbeats advance with
`last_success`; `/v1/health` status equals the pre-cutover status; condition
statuses unchanged; no new series in InfluxDB for `collector_service =
graphyard-agent` (series cardinality before/after) and no gaps beyond the
restart; nyxmon's graphyard checks stay green; memory within section 10
after 1 h and 24 h. Rollback drill: flip to `python`, deploy, confirm the
Python agent resumes (spec rows advance), flip back to `rust`, deploy,
re-verify. Soak 7 days with a daily check of the same queries; roll back on
a spec without a run for `2 × interval`, a heartbeat older than 15 minutes, a
restart loop, a series-key change, or memory trending above budget.

### 11.4 `evaluate-conditions` and Python retirement (S15a, S15b, S15c; Q4)

Owner decision Q4 (2026-10-21): the Python agent is retired 30 days after
T1, and `manage.py evaluate_conditions` is replaced by `graphyard-agent
evaluate-conditions`, in separate slices.

**S15a** (graphyard, after S10b, not tied to the 30 days, before S15b):
`graphyard-agent evaluate-conditions [--condition-id N]` ports
`evaluate_conditions.py` (`evaluate_conditions_once(condition_id=…)`):
enabled conditions, optionally only the given id, `ORDER BY name`; the
per-condition writes and the `condition_evaluator` heartbeat exactly as 3.5
(Python records the heartbeat for a single-condition run too). stdout
`Condition evaluation completed (<total>).`, or `Condition evaluation
completed with failures (<failed>/<total>).` followed by stderr
`CommandError: <failed> condition evaluation(s) failed.` and exit 1. A
failure outside a condition exits 1 with one error line (Python: traceback,
P2); no colour (D-7); `--condition-id` parsed like the argparse `type=int`
flags of 3.1. Like the agent it reads only the process environment (D-9), so
the documented operator call uses the unit's environment:
`systemd-run --wait --pipe --quiet -p User=graphyard -p Group=graphyard -p EnvironmentFile=/etc/graphyard/graphyard.env -p WorkingDirectory=/opt/apps/graphyard/site/src/django /opt/graphyard-agent/<ver>/graphyard-agent evaluate-conditions --db /opt/apps/graphyard/site/src/django/db.sqlite3`.
Tests: counterparts of `test_evaluate_conditions_command.py`, an e2e case
and a parity-harness case running both commands on identical inputs; docs,
CHANGELOG; the slice ends with the next `agent-rs-v*` release through the
S10a workflow.

As implemented in S15a (`graphyard-agent::evaluate`, version 0.2.0, release
`agent-rs-v0.2.0`): the subcommand reuses the agent's conditions run
(`runs::run_conditions` with the id), so its writes are the ones `runs.json`
pins, including new `condition_id` scenarios; an id outside the 64-bit range
of the `BigAutoField` selects nothing without a query (Django's empty
result), and the heartbeat records the empty run (`evaluated 0`). The
database is opened and its schema verified as at the agent's startup (D-2;
a missing database exits 1 and is never created); the agent's three INFO
lines are not printed. The failure line outside a condition is stderr
`Condition evaluation failed: <class>: <err>`. The settings are read before
the flags as in 3.1; `--condition-id` takes unique prefixes, the last value
wins and a negative value is an id. The subcommand excludes the agent's
flags (`graphyard-agent --run-once evaluate-conditions` is a usage error,
exit 2), has no `help` subcommand and installs no signal handlers: SIGINT
and SIGTERM end it by their default action, where Python ends by SIGTERM's
default action or the uncaught `KeyboardInterrupt` (both cut the run short;
SQLite transactions keep the rows consistent). The parity harness runs both
commands in three extra rounds after the five agent rounds (all enabled
conditions with two failures, one condition by id in `v3` mode, the
disabled condition's id) and compares exit codes, stdout, the
`CommandError` and operator lines, rows and requests.

**S15b** (ops-library, ops-deploy, ops-control; ≥ 30 days after T1;
production): pin the release containing `evaluate-conditions` in macmini
host vars; `graphyard_agent_impl` default and only value `rust`, Python
option removed (the venv stays for the web); ops-control
`docs/GRAPHYARD_NYXMON_CHECKS.md` and `docs/GRAPHYARD_RUST_AGENT.md` switch
the manual evaluation call to `evaluate-conditions` as above; CHANGELOGs;
the coordinator's ops-library collection release commit before the deploy;
deploy with lock (8.4), verification as in 11.3.

**S15c** (graphyard, after S15b is deployed): remove `start_agent`,
`evaluate_conditions` and the Python collector/evaluation code that the web
does not use; remove the parity harness and its CI job; freeze the fixtures
(drift test removed, fixtures kept as Rust test data); README, docs,
CHANGELOG. The order is deliberate: after S15b no deploy can select the
Python agent and the documented manual call is the Rust one, so a later
`graphyard_core` deploy shipping S15c removes nothing that still runs.

---

## 12. Implementation slices

Rules: one branch per slice (`rust-graphyard-agent-<id>`, the campaign's
branch naming), each reviewed by
`gpt-6.1-sol` at high reasoning through the codex review loop, then merged to
the repo's default branch and pushed. A slice gets at most three review
rounds, the third only over the repair delta of the second (owner rule of
2026-10-21); if round 3 is not clean, the slice stops and is reported to the
coordinator. Merging `agent-rs/` to graphyard main
is safe at any time: nothing runs it until the drop-in exists, and the
default pyinfra deploy syncs a fresh git tree without `target/` (S11a/b add
the exclude for local-source deploys). Every slice keeps `just test`,
`just lint`, `just typecheck` green, updates `TESTMAP.md`, lists the Python
functions it ports in its review context, and stays ≤ 2.5k changed lines
excluding generated fixtures.

| Id | Slice | Scope | Depends on | Parallel | Prod | Est. h |
| --- | --- | --- | --- | --- | --- | --- |
| S1 | Scaffold | workspace, toolchain pin, deny.toml, CI `rust` job, just recipes, shared model types (`MetricPoint`, `MetricSample`, statuses, subject/spec types), fixture generator skeleton (Django test settings bootstrap) + drift test, TESTMAP.md, README | — | with S11a | no | 2.5 |
| S2 | Python compat + settings | `pycompat` (4.4), env readers (3.3), `spec_config` helpers; fixtures `pycompat`, `settings`, `spec_config` | S1 | with S3a, S10a, S11a, S11b | no | 3.5 |
| S3a | Store core | DB thread owning the rusqlite connection + pragmas, schema contract + Python contract test, `--check-schema` core, datetime codec, spec/condition reads and updates (0-row rule), heartbeat upsert + retry; `store_ops.json` (non-registry part), `datetime_codec.json`, `schema/`; `ingest_token` grep test | S1 | with S2, S10a, S11a, S11b | no | 4 |
| S4a | Normalisation + writes | `normalize_*`, `parse_finite_float`, line protocol formatter, write client (6.4), lazy config errors; `normalize.json`, `line_protocol.json` | S2 | with S4b, S5, S6, S3a | no | 3.5 |
| S4b | Queries | window computation, Flux text + annotated CSV, v3 SQL with caps/deadline/errors, auto fallback; `flux.json`, `sql_v3.json`; fake InfluxDB | S2 | with S4a, S5, S6 | no | 4 |
| S5 | Condition evaluation | section 7, pure, sample source trait; `conditions.json` | S2 | with S4a, S4b, S6 | no | 3 |
| S6 | HTTP layer | client builder (6.1), deadline + abandoned lookups (6.2), body cap, JSON reader, `raise_for_status` text, page-probe redirect loop primitive, request log line; `http_errors.json` | S2 | with S4a, S4b, S5 | no | 4 |
| S3b | Registry touch | section 5.5 + `store_ops.json` registry cases | S3a, S4a | with S7a, S7b | no | 2 |
| S7a | HA + JSON executors | `home_assistant_sensor`, `home_assistant_env_scan`, HA normalisation and mappings, `http_json_metric`, JSON path; `ha_mapping.json`, `json_path.json` | S4a, S6 | with S3b, S7b | no | 4 |
| S7b | Page probe + UniFi | `http_page_probe`, `unifi_device_traffic`; `unifi.json` | S4a, S6 | with S3b, S7a | no | 4 |
| S8 | Agent runtime | CLI (3.1), scheduler (3.2), metrics and conditions runs (3.4, 3.5), executor isolation, heartbeats, stdout/stderr/logging (3.6), signals, `--run-once`, `--check-schema`, `--version`, `--influx-write-file` | S3a, S3b, S4b, S5, S7a, S7b | — | no | 4 |
| S9a | Parity harness | `parity/run_parity.py`, `fakes.py`, `settings_parity.py`, CI `parity` job | S8 | with S9b | no | 4 |
| S9b | e2e, shadow tooling, docs | process e2e (9.3), `shadow_prepare/record/compare.py`, `docs/agent-rs.md`, README, CHANGELOG | S8 | with S9a | no | 3.5 |
| S10a | Release workflow | `agent-rs-release.yml`, `scripts/release_checks.py` + `tests/test_agent_rs_release_checks.py`, `build.rs` commit stamp, `--version` in the placeholder binary (S8 moves it into the CLI), `workflow_dispatch` dry build | S1 | with S2–S9 | no | 2.5 |
| S10b | First release | tag `agent-rs-v0.1.0`, verify archive + sha256 | S9a, S9b, S10a | — | no | 0.5 |
| S11a | ops-library role | impl selector (8.3), drop-in template, verify, prune, rsync exclude, README, CHANGELOG "Unreleased", tests with a stub archive | — (contract in 8.3) | with S1–S10 | no | 3.5 |
| S11b | ops-deploy port | `GraphyardConfig` fields, agent module, wiring in `core_steps`, parity template copy, `SOURCE_EXCLUDES`, tests | S11a | with S2–S10 | no | 3.5 |
| S11c | ops-control wiring | pyinfra mapping + refusal, macmini host vars (impl python, version, sha256), ops-deploy pin, `ca-trust-consumers.yml` entry (Q1), `docs/GRAPHYARD_RUST_AGENT.md`, CHANGELOG, tests (playbook assert follows in S12) | S11b, S10b | — | no | 3 |
| S12 | Install on macmini | 11.1 (playbook assert, impl python) | S11c, coordinator's ops-library collection release | — | **yes** | 1.5 |
| S13 | Shadow run | 11.2 (approved, Q2), report | S9b, S12 | — | **yes** (host, no prod writes) | 3 + 24 h |
| S14 | Cutover + drill | 11.3, final docs in all repos | S13 report, window agreed with the eMudhra stream (Q3) | — | **yes** | 3 + 7 d soak |
| S15a | `evaluate-conditions` subcommand | 11.4: port of `manage.py evaluate_conditions`, tests, parity case, docs, next release | S8, S10b | with S11c–S14 (code only) | no | 2.5 |
| S15b | Python retirement (ops repos) | 11.4: pin, impl `rust` only, docs, deploy | S14 + 30 d, S15a, coordinator's ops-library collection release | — | **yes** | 3 |
| S15c | Python code removal (graphyard) | 11.4: `start_agent`, `evaluate_conditions`, harness, fixture freeze | S15b | — | no | 3 |

Parallel waves (maximum five workers):

- W0: S1, S11a
- W1: S2, S3a, S10a, S11b
- W2: S4a, S4b, S5, S6
- W3: S3b, S7a, S7b
- W4: S8
- W5: S9a, S9b
- W6 (serial, production): S10b → S11c → S12 → S13 → S14 → soak → (T1 + 30 d) S15b → S15c
- S15a: any time after S10b, before S15b (code and a release only)

Acceptance per slice (in addition to green checks and a clean review):

- **S1**: CI `rust` job green; drift test fails on a hand-edited fixture.
- **S2–S7b**: every fixture case of the slice matches; every rule of the
  cited section has a test; no test reaches the network.
- **S3a**: contract test fails on a simulated migration that drops an agent
  column; `--check-schema` names a missing column and the migration.
- **S8**: the counterparts of `test_agent_command.py` and the isolation tests
  pass; `--run-once` against fakes writes the fixture rows.
- **S9a**: `just rs-parity` green in CI with zero unmasked differences.
- **S9b**: e2e green; shadow tools tested against two local databases.
- **S10a/b**: dry build proves the glibc floor; release exists with the
  sha256 used in S11c.
- **S11a/b/c**: repo test suites green (ops-control's currently red
  `test-software-inventory` job is unrelated and not fixed here); molecule or
  pyinfra tests show drop-in present only for `rust`, versioned path, verify
  by `/proc/<pid>/exe`.
- **S12**: binary installed, unit still Python, no unintended restart.
- **S13**: 11.2 acceptance with numbers.
- **S14**: every 11.3 check recorded, drill done, soak started.
- **S15a**: summary lines, exit codes, row and heartbeat effects equal the
  Python command's on the same database (fixtures and parity case).
- **S15b**: the Python option is refused by role, ops-deploy and
  ops-control; the deployed agent is the pinned release; the documented
  manual call works on macmini.
- **S15c**: no Python agent code left; `just test` green with frozen
  fixtures.

---

## 13. Risks and coordinator notes

### 13.1 Risks

- **R1 HTTP semantics** (per-read vs per-op timeouts, decoding, redirects,
  cookie handling across manual redirects, `raise_for_status` text) — fake
  server tests per rule, the parity harness, the shadow comparison.
- **R2 Name resolution** — `macmini.local` resolves only through glibc +
  nss-mdns; the glibc target and the `getaddrinfo` resolver are mandatory, a
  hickory/musl resolver would break the HA spec.
- **R3 Python quirks reproduced on purpose** — `str(None).lower() == "none"`
  as a device class, `port_idx or num_port` truthiness, a registry failure
  after a successful write turning the spec `critical` with points already in
  InfluxDB, unfollowed 3xx counting as page-probe success, the warning path
  re-raising write failures. Implementers must not "fix" them; changes need
  an owner decision and a change on both sides.
- **R4 Series identity drift** — a different escaping or float rendering
  would create new series and split conditions/dashboards; pinned by
  `line_protocol.json`, the harness and the shadow series-key comparison.
- **R5 SQLite contention** with the web app (WAL, busy 30 s) — unchanged
  exposure.
- **R6 Shadow side effects** — doubled UniFi logins and HA/fractal/public
  requests for 24 h; if the UniFi controller logs or limits logins, the
  shadow copy's UniFi spec is disabled and compared through one `--run-once`
  instead.
- **R7 Concurrent ops work** — eMudhra/certifi deploy of `graphyard_core`,
  the open Atlas step, Traefik ACME on `production`, fractal MinIO TLS; all
  production slices check with the coordinator first.
- **R8 Merge pressure in ops-deploy/ops-control** from other Runde-2 streams
  touching the same files (agent modules, pins) — small slices, rebase before
  merge.
- **R9 Supply chain** — `Cargo.lock`, `--locked`, `cargo deny`, pinned action
  SHAs, sha256-pinned release archives.
- **R10 Pre-existing issues seen while gathering facts** (not caused by this
  stream, reported to the coordinator): both conditions read "No samples
  available in condition window"; `src/django/.env` on macmini is
  world-readable and holds secrets (separate item
  `graphyard-stale-env-macmini`, section 14 Q5); the venv still has certifi
  2026.02.25 until the next `graphyard_core` deploy.

### 13.2 Coordinator notes (no owner decision needed)

- Shared Rust-agent deploy helper in ops-deploy (nyxmon, graphyard and other
  Runde-2 agents): default here is a graphyard-specific module; extracting a
  shared helper after the Runde-2 cutovers avoids touching nyxmon's live
  deploy mid-campaign.
- Production slices S12–S15b need a slot outside the eMudhra `graphyard_core`
  deploy and must not overlap a `graphyard_vector` or token change while the
  Atlas step is open.

---

## 14. Owner decisions (2026-10-21)

The owner answered the questions of this plan on 2026-10-21 (given in chat
to the campaign coordinator) and accepted every recommendation:

- **Q1 TLS trust anchors**: the Rust agent verifies against the **system CA
  store** (D-12), not an embedded Mozilla bundle; `graphyard-agent.service`
  is added to `ca_trust_consumers` (macmini `restart_units`, S11c).
- **Q2 Shadow run**: **approved**, about 24 h on macmini (S13): a second
  agent process with doubled collector traffic, the database copy with spec
  secrets in a 0700 directory and deleted afterwards, nothing written to
  InfluxDB (11.2).
- **Q3 Cutover window**: **one window after the S13 shadow report**,
  coordinated with the eMudhra stream (certifi relock, 11.3); three agent
  restarts (cutover and drill).
- **Q4 Retirement**: the Python agent is retired **30 days after the
  cutover**, and `manage.py evaluate_conditions` is replaced by
  `graphyard-agent evaluate-conditions`, as **separate slices** (S15a port,
  S15b ops retirement, S15c code removal; 11.4).
- **Q5 Stale `src/django/.env` on macmini**: a **separate item
  `graphyard-stale-env-macmini`**, not part of this plan; the Rust agent
  ignores the file (D-9). That item has to account for the documented
  manual `manage.py evaluate_conditions` call (ops-control
  `docs/GRAPHYARD_NYXMON_CHECKS.md`), which runs without the unit's
  EnvironmentFile and may take its settings from that file; after S15b the
  documented call is the Rust one with the EnvironmentFile.

Campaign-wide decisions of the same day that bind this stream:

- **ops-library releases**: workers never bump `galaxy.yml`; the coordinator
  makes the collection release commit (`galaxy.yml` bump + CHANGELOG
  heading) on ops-library main right before the production deploys that
  need it, here before S12 and before S15b (8.3, 11.1).
- **ops-library GitHub Actions** (foundation slices of the metrics-httpd
  stream) cover ops-library's `rust/` only. This stream releases from the
  graphyard repository (8.2) and does not depend on them.
- **echoport-command-guard** is ported last in the campaign and never
  alongside other production or staging changes; no production step of this
  stream overlaps it (8.4).
- **Review rounds**: a slice, and this plan, may get a third review round
  over the repair delta; if round 3 is not clean, work stops and is reported
  (section 12).
