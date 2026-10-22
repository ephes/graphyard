# Changelog

## Unreleased

- Rust agent port (`agent-rs/`, slices S12 and S13). `agent-rs-v0.2.0` is
  installed on macmini under `/opt/graphyard-agent/0.2.0/` by the
  `graphyard_core` deploy of 2026-10-22 09:12 UTC. The pin moved from
  `0.1.0`, since 0.2.0 only adds `evaluate-conditions`. The Python agent
  stays selected. The same deploy shipped graphyard `24baf74`, with no
  `src/` change, and the `Wants=` units. The approved shadow run started at
  T0 09:13:12 UTC. PLAN.md 11.1 and 11.2 record both steps.

- Rust agent port (`agent-rs/`, slice S11c): ops-control wires the agent
  switch. The pyinfra `graphyard_core` deploy maps macmini's
  `graphyard_agent_impl`, `graphyard_agent_rust_version` and
  `graphyard_agent_rust_sha256` (and refuses the other
  `graphyard_agent_rust_*` variables); macmini selects `python` and pins
  `agent-rs-v0.1.0`, so the next `graphyard_core` deploy installs and
  validates the binary while the Python agent keeps running;
  `graphyard-agent.service` joins macmini's CA trust consumers (D-12); runbook
  ops-control `docs/GRAPHYARD_RUST_AGENT.md`. PLAN.md 8.3 and 11.1 record
  that the ops-deploy pin already contained S11b and that S12 keeps the S11c
  pin.

- Rust agent port (`agent-rs/`, slice S15a): release `agent-rs-v0.2.0`
  (<https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.2.0>) with the
  `evaluate-conditions` subcommand, built by the release workflow from
  `17a929f`. Asset `graphyard-agent-0.2.0-x86_64-unknown-linux-gnu.tar.gz`,
  sha256 `d8efcb78fd09f5047d5e747696bc7966c94e70de8a44bbe1d5d34111a7a058d4`; the binary needs at most `GLIBC_2.34`. The Python
  retirement (slice S15b) pins this release; nothing deploys it yet.

- Rust agent port (`agent-rs/`, slice S15a): new subcommand
  `graphyard-agent evaluate-conditions [--condition-id N] [--db PATH]`, the
  port of `manage.py evaluate_conditions` (agent-rs/PLAN.md 11.4): the same
  condition selection, row and `condition_evaluator` heartbeat writes, summary
  lines and exit codes (`CommandError: <n> condition evaluation(s) failed.`,
  exit 1). Pinned by new `runs.json` scenarios (`condition_id`), subcommand
  and e2e tests, and three new parity-harness rounds that run both commands
  on the same databases. The Rust agent version is now 0.2.0 (release
  `agent-rs-v0.2.0`). The Python command stays until the Python agent is
  retired (slices S15b, S15c); nothing deploys the Rust agent yet.

- Rust agent port (`agent-rs/`, slice S10b): first release of the Rust
  agent, `agent-rs-v0.1.0`
  (<https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.1.0>), built
  by the release workflow from `19f56e6` (the merge of S9b). Asset
  `graphyard-agent-0.1.0-x86_64-unknown-linux-gnu.tar.gz`, sha256
  `42d6dbb5b931599c1b35ab7f19a29823bf9583d65b45e2259a69b5dbc64b80e2`; the binary
  needs at most `GLIBC_2.34` and links only glibc and libgcc. Slice S11c pins
  this version and sha256 in the ops-control host vars. Nothing deploys the
  Rust agent yet; the Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S9b): process e2e tests of the
  `graphyard-agent` binary (a `--run-once` over every collector family
  against fake servers with all rows, heartbeats, registry rows and written
  lines checked, the loop's due selection, SIGTERM/SIGINT while a request is
  stalled, the shadow write file, missing tables in `--check-schema`); the
  shadow-run tooling `agent-rs/parity/shadow_prepare.py` (online database
  copy, T0, scan for stored values the two agents read differently),
  `shadow_record.py` (samples both databases and both units) and
  `shadow_compare.py` (series, values, statuses, registry, restarts and
  memory/CPU against production), tested on two local databases; and the
  operator documentation `docs/agent-rs.md`. Nothing deploys the Rust agent
  yet; the Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S9a): an offline parity harness
  (`agent-rs/parity/`, `just rs-parity`, new CI job `parity`) runs
  `manage.py start_agent --run-once` and `graphyard-agent --run-once` on two
  copies of one seeded database against scripted fake collector endpoints
  (HTTP and TLS) and a fake InfluxDB, over five rounds (`v2`, `v3`, `auto`
  and missing InfluxDB settings), and fails on any unmasked difference in
  the database rows, stdout, operator log lines and the requests sent. The
  helpers are tested in `tests/test_agent_rs_parity.py`. Nothing deploys the
  Rust agent yet.

- Rust agent port (`agent-rs/`, slice S8): `graphyard-agent` is now the
  agent, a drop-in for `manage.py start_agent`: the same flags, defaults,
  validation messages and exit codes, the scheduler loop, the metrics and
  conditions runs with executor isolation (a panicking executor included),
  the `metric_collectors`/`condition_evaluator` heartbeats, the stdout and
  stderr report lines, SIGINT/SIGTERM handling that lets an in-flight SQLite
  statement finish and never waits for a blocked name lookup, plus the
  Rust-only `--db`, `--check-schema`, `--version` and the shadow mode
  `--influx-write-file` (batches appended to a file, no InfluxDB write
  request). Pinned by a new fixture `runs.json` generated from the Python
  runs with stubbed executors. Nothing deploys the Rust agent yet; the
  Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S7a): the Home Assistant and HTTP JSON
  executors (`graphyard-collect::executors`: `home_assistant_sensor`,
  `home_assistant_env_scan`, `http_json_metric`) with the HA state
  normalisation, subject and metric mappings (`graphyard_core::home_assistant`,
  user regexes on `fancy-regex`) and the metric path
  (`graphyard_core::json_path`). Pinned by new fixtures generated from the
  Python code: `ha_mapping.json` (including macmini's production env scan
  config), `json_path.json` (including the production metric paths) and
  `ha_json_executors.json`, which runs the Python executors on a scripted
  transport and is replayed against a fake server. The HTTP layer's JSON
  reader now rejects integers beyond CPython's 4300-digit limit like
  `json.loads`. The plan records the regex backtracking limit (D-5) and
  corrects the Unicode-digit path case (D-6). Nothing runs the Rust code
  yet; the Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S7b): the `http_page_probe` and
  `unifi_device_traffic` executors in `graphyard-collect::executors`, ports
  of `_execute_http_page_probe_spec` and `_execute_unifi_device_traffic_spec`
  with the UniFi login, redaction, device and interface helpers, on the S6
  HTTP layer. Points go through a `MetricSink` (InfluxDB write and registry
  touch) that the agent runtime wires up in S8. Pinned by the new fixtures
  `page_probe.json` and `unifi.json`, generated by running the Python
  executors with httpx on `MockTransport` and a recording sink. The plan
  records three corner cases where the Rust executors differ (UniFi
  summaries of bodies in a non-UTF-8 charset, non-standard JSON in a UniFi
  login response, non-ASCII digits in a `port_idx:` selector). Nothing runs
  the Rust code yet; the Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S6): the HTTP layer of the collectors
  (`graphyard-collect::http`), httpx 0.28.1 as `services.py` uses it, on
  hyper's HTTP/1.1 connection API: httpx's URL model, request headers, auth,
  cookies and decoders, connections with httpx's per-connect and per-read
  timeouts and `getaddrinfo` name resolution, the `HTTP Request:` log line,
  `raise_for_status` texts and exception classes, the total deadline with
  the cap on lookups still running past it, the JSON reader and the
  page-probe redirect loop. Pinned by a new fixture `http_errors.json`
  generated from httpx and the graphyard helpers. Nothing runs the Rust
  code yet; the Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S4b): the condition queries of
  `influx.py` in `graphyard-influx`: the lookback window of a condition, the
  InfluxDB v2 Flux request and the decoding of its annotated CSV (as
  `influxdb_client` does it), the v3 SQL request with its 10 MiB cap, 30 s
  deadline and error texts, and the `auto` fallback on a v2 404. Pinned by
  fixtures generated from the Python code against a loopback fake InfluxDB
  (`flux.json`, `sql_v3.json`, `isoformat.json`). Nothing runs the Rust code
  yet; the Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S3b): the registry touch
  (`graphyard_store::Store::touch_registry`), a port of the batched
  `touch_registry_from_points` without a bound subject: one read per registry
  table, no write in the steady state, creates and refreshes of rows older than
  60 seconds in one `BEGIN IMMEDIATE` transaction, re-parenting by batch
  position. Pinned by the new `registry_cases` of `store_ops.json`, which
  record the row images and the statement sequence Django sends. The plan's
  section 5.5 now states the query order, the datetime conversion of loaded
  rows and the race that remains after the batching. Nothing runs the Rust
  code yet; the Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S5): condition evaluation
  (`graphyard_core::conditions`, a port of `services.py:evaluate_condition`
  and its series, anchor and staleness helpers) behind a sample source trait,
  with Python's `datetime`/`timedelta` range errors and
  `timedelta.total_seconds()` rounding, pinned by `conditions.json`, which is
  generated from `evaluate_condition` with the query stubbed and covers every
  `evaluate_condition` scenario of `tests/test_condition_evaluation.py`.
  Nothing runs the Rust code yet; the Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S4a): metric point normalisation
  (`normalize_metric_point`, `normalize_subject_id`, the dimension rules,
  `parse_finite_float`), the InfluxDB line protocol exactly as
  `influxdb-client` 1.50.0 renders it (escaping, empty-tag dropping, float
  and nanosecond timestamp rendering), and the write client of
  `influx.py:write_points` (`POST /api/v2/write` with the Python client's
  query encoding and headers, the lazy `INFLUX_*` configuration errors, the
  per-read timeout rules, response bodies read in full, TLS and transport
  failures by the Python client's exception class, the validation and
  `metrics_write_rejected` log lines). Pinned by `normalize.json` and
  `line_protocol.json`, generated from the Python client talking to recording
  and byte-level loopback servers. Nothing runs the Rust code
  yet; the Python agent is unchanged.

- Rust agent port (`agent-rs/`, slice S2): CPython compatibility helpers
  (`repr(float)`, `format(x, "g")`, `int()`/`float()` of strings, `str()`/
  `repr()` of JSON values, `==`), the agent's environment settings with
  django-environ semantics (defaults, `$` proxies, the SQLite pragma
  fallbacks) and the typed spec config checks of `spec_config.py`, each pinned
  by fixtures generated from the Python code (`pycompat.json`,
  `settings.json`, `spec_config.json`). Nothing runs the Rust code yet; the
  Python agent is unchanged.

- Add the release pipeline of the Rust agent: `.github/workflows/agent-rs-release.yml`
  builds `graphyard-agent` on ubuntu-22.04 for a tag `agent-rs-v<version>` (the
  tag must equal the crate version), runs the Rust tests and the fixture drift
  test, checks the glibc 2.35 floor and the linked libraries, smoke-tests
  `--version` against the workflow's commit and publishes a deterministic
  `graphyard-agent-<version>-x86_64-unknown-linux-gnu.tar.gz` with its
  `.sha256` as a GitHub release. A manual run (`workflow_dispatch`) is a dry
  build that uploads the archive as a workflow artifact only. The binary now
  answers `--version` with the version and the git commit it was built from;
  it is still a placeholder otherwise and nothing is deployed.

- Start the Rust port of the agent in `agent-rs/` (plan: `agent-rs/PLAN.md`):
  a Cargo workspace with a pinned toolchain and `cargo deny` policy, the shared
  model types, a fixture generator that derives the Rust test data from the
  Python code (`just rs-fixtures`) with a drift test in the Python suite, the
  `just rs-*` recipes and a `rust` CI job. `just test`, `just lint` and
  `just typecheck` now also run the Rust checks. Nothing is deployed; the Python
  agent is unchanged.

- Add the Rust agent's store core (`agent-rs/crates/graphyard-store`): the
  database thread with Django's connection setup and the graphyard SQLite
  pragmas, the schema contract check for `--check-schema`, Django's datetime
  column conversion, the spec and condition loads and updates, and the
  heartbeat upsert with its lock retry, all pinned by fixtures generated from
  the Django ORM. A new test, `tests/test_agent_rs_schema_contract.py`, fails
  when a migration changes the agent's tables until the Rust contract is
  updated (`just rs-fixtures`). Nothing is deployed.

- Fix slow metric ingest for bound ingest tokens. The registry update issued one
  `UPDATE service_registry` per point, each a separate SQLite commit, so a
  50-point batch took several seconds under load and Vector producers fell
  behind. Registry rows are now read once per distinct subject, host and service
  per request. Creates and updates share one transaction and run only when a row
  is missing or changes, or its `last_seen_at` is older than 60 seconds. Steady
  ingest no longer writes to SQLite for the registry. Bound-token ownership
  protection is unchanged: every bound service write stays conditional on the
  service being unowned or owned by the bound host. Registry `last_seen_at` (admin
  and index page) now lags by up to 60 seconds.

- Update the locked certifi CA bundle to 2026.10.19 for the eMudhra/emSign
  CA compromise
  ([GHSA-6q48-6c7w-9gw2](https://github.com/certifi/python-certifi/security/advisories/GHSA-6q48-6c7w-9gw2)).
  Lock-only change. Re-sync deployed environments and restart long-running
  processes so they load the new bundle.

- Return only statuses, timestamps and counts from `/v1/health`, `/v1/conditions`
  and `/v1/conditions/<id>` to unauthenticated callers. Component messages, error
  texts, heartbeat details, the InfluxDB name, condition messages, `config` and
  `last_value` now require a logged-in session. The fields Nyxmon and the deploy
  and backup health checks read (`status`, component and pipeline `status`,
  `summary`, condition `name`) stay public.

- Add nullable single subject/host bindings for ingest tokens (reversible migration
  `0012_ingest_token_subject_binding`). Validate entire batches before writing and
  prevent bound tokens from taking over another host's services. Preserve legacy
  behavior by default; `GRAPHYARD_REQUIRE_BOUND_INGEST_TOKENS=true` rejects unbound
  tokens. Add a credential-free binding report and assignment command, admin
  binding display/editing, and staged rollout/rollback documentation. Rotation
  preserves bindings.

- Make `evaluate_conditions` exit with status 1 after reporting the summary when
  any evaluation failed; independent conditions still run.

- Reject overlong host, service and subject identities at metric ingest using
  registry model field limits (128 characters), including normalized values.
  Validate stored `source_system` too (64 characters), including legacy fallbacks.
  Return an indexed 400 error before metric writes or registry changes.

- Require finite condition thresholds, preserving optional thresholds and unrestricted
  finite values for other metrics. Order disk seed thresholds by comparison
  direction. Validate before persistence and roll back failed creates/updates.

- Pin the production Granian proxy wrapper, trusted client address rewriting, and
  required deployment entrypoint with regression tests for login throttling.

- Escape Flux string interpolation markers in configured query filters and
  escape/cap rejected metric names in ingest validation logs.

- Lower inventory ingest bodies from 8 to 2 MiB and check streaming element
  and nesting budgets before materializing JSON reports. Require UTF-8 JSON
  without a BOM; Vector framing preserves the report nesting allowance.

- Look up fast ingest token hashes directly and restrict password checks to
  legacy rows; warn about enabled legacy hashes in database system checks.

- Throttle failed application and admin logins by username/client and client
  address, with configurable trusted proxy hops and fixed cache windows. Reject
  invalid window/address/proxy settings at startup; locked attempts do not churn
  cache keys. Configure the standalone inventory probe too.

- Bound metric ingest timestamps to a configurable seven-day past / five-minute
  future window and limit supplied tags to 32 and other dimension/tag strings
  to 256 characters. Invalid points reject the batch with their index.

- Reject nonfinite or invalid numeric query samples from both Flux and SQL as
  evaluation errors. Mixed valid/invalid results cannot appear healthy or refresh
  the last successful evidence, including for intermittent conditions.
- Stream InfluxDB 3 SQL results under a 10 MiB decoded response limit and a
  30-second total read deadline; close failed responses and reject partial data.
  These fixed limits also apply when `INFLUX_TIMEOUT_MS` is higher. Narrow existing
  condition filters/windows if results exceed the limit. OS DNS cleanup can
  outlast the deadline (see README). SQL errors retain safe HTTP status or
  transport exception categories without including response bodies or URLs.

- Bound both Home Assistant collectors by decoded response size (10 MiB by
  default) and a total deadline (three request timeouts by default), using the
  shared HTTP cancellation and worker limits. Budget failures close responses,
  write no points and allow remaining specs to run. Configure `max_body_bytes`
  and `total_timeout_seconds` for larger legitimate state inventories.

- Add an optional host scope to ingest tokens. Until now any ingest token could
  write metrics for any host. A token can now carry a list of allowed host
  identifiers (exact names, or a prefix with one trailing `*`), set with
  `create_ingest_token --host-scope` or in the admin. A scoped token gets `403`
  from `POST /v1/metrics` when any point has a `subject_id` or `collector_host`
  outside its scope, and nothing is written. Tokens without a scope behave as
  before, and `--rotate` keeps the scope of the replaced token. The new
  read-only command `report_ingest_token_hosts --days N` lists the hosts each
  token wrote, recorded from this release onwards, to help choose scopes.
  Migration `0011` adds `ingest_token.host_scope` and the
  `ingest_token_host_usage` table; it is reversible.

- Validate derived HTTP deadlines and limit request timeouts for all collectors
  and HTTP total budgets to 86400 seconds (one day) before collection. Upgrade
  oversized legacy specs: they now fail validation and collection. HTTP request
  timeouts above 28800 seconds require an explicit total budget within the limit.

- Report unreadable or invalid UTF-8 collection spec files as command errors
  with the source path and bounded diagnostics before any database changes.

- Bound HTTP JSON collectors by decoded body size and a total request deadline;
  budget failures write no metric and allow the agent to continue.

- Normalize inventory snapshot UUID spellings for idempotent retries, including
  existing reports hashed before normalization; changed content still returns 409.
  Acknowledgments echo the submitted UUID spelling for sender compatibility.

- Prevent older inventory release refreshes from overwriting newer completed
  attempts, including when an older lookup fails after a newer success.

- Bound public inventory release body fetching with a 60-second monotonic
  elapsed budget in addition to the 20-second HTTP operation timeout and
  16 MiB decoded body limit. Budget failures preserve the last successful
  version and timestamp and allow remaining sources to refresh.

- Fix `create_ingest_token --rotate`. It revoked the existing token and then
  failed with an `IntegrityError` on the unique token name, leaving the host
  without any valid token; creating a token for a name that had been revoked
  with `revoke_ingest_token` failed the same way. Rotation now runs in one
  transaction: the previous row is revoked and kept as history under the name
  `<name> (retired #<id>)`, and the new token takes the name. If anything fails,
  nothing changes and the previous token stays valid. No migration.

- `POST /v1/metrics` now rejects malformed request bodies with a bounded JSON
  error instead of an unhandled server error. An authenticated body that is not
  valid UTF-8, or whose JSON is nested too deeply to parse (also as one NDJSON
  line), returns `400`; a body larger than `DATA_UPLOAD_MAX_MEMORY_SIZE` returns
  `413` with a JSON error instead of Django's generic `400` page. All three are
  logged and recorded in the `metric_ingest` heartbeat as one parse rejection
  with a fixed message (no decoder exception text), and nothing is written to
  InfluxDB. Valid JSON and NDJSON bodies are unchanged.

- The HTTP page probe now has a whole-probe deadline, `total_timeout_seconds`
  (default: three times `request_timeout_seconds`, validated on save as a number
  greater than 0). Previously `request_timeout_seconds` bounded only each
  connect, write and read, so an endpoint that dripped header or body bytes
  could hold the shared agent loop, and with it every other collector and every
  condition evaluation, for as long as it kept dripping (18.7 s for a 2 s
  timeout in one test, indefinitely in the worst case). The probe now runs in a
  worker thread; at the deadline the agent cancels it (shuts down its sockets,
  closes its HTTP client, and aborts a connection that completes later, for
  example after a slow DNS lookup) and records `service.http_page_status_code=0` and `service.http_page_success=0`
  with a `total_timeout_seconds` error, as for other timeouts. While four
  cancelled workers are still stuck (for example in DNS), further page probes
  fail immediately instead of starting more threads. Fast probes are unchanged. **Upgrade note:** a slow page that used to finish after more than
  three request timeouts now reports a failure; raise `total_timeout_seconds`
  for it if that is expected.

- Add a per-condition staleness allowance. `ConditionDefinition` gains
  `stale_after_seconds` (empty means the global
  `GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS`) and `alert_when_stale`
  (default on; off reports stale or missing data as `ok`, "not alerting
  (intermittent)", instead of `warning`). Both are editable in the admin, shown
  in the `config` of `GET /v1/conditions/<id>` (with the effective
  `stale_limit_seconds`), and settable with `seed_disk_usage_condition
  --stale-after-seconds N --intermittent`. The condition query looks back over
  the allowance. Existing conditions are unchanged. Needs migration
  `0010_condition_stale_allowance`. This lets the intermittent atlas laptop disk
  condition be enabled without a nightly Nyxmon warning.

- Conditions on series sampled less often than once a minute now breach
  consistently. The breach check carries the last sample before the breach
  window forward: the condition breaches when that sample and every sample in
  the window breach. Previously the first in-window sample had to land within
  one minute of the window start, so a 5-minute series (storage-pool ratio,
  IPMI/lm_sensors temperatures, fan RPMs) breached only about one evaluation in
  five and its status flapped between `ok` and `warning`/`critical`. The carried
  sample may be at most `CONDITION_DATA_STALE_WARNING_SECONDS` older than the
  window start, and the condition query now looks back that far beyond
  `breach_minutes`. A series with no such sample (for example one younger than
  the window) keeps the one-minute grace rule. **Upgrade note:** conditions on
  sparse series that were silently flapping now stay `warning`/`critical` while
  the breach lasts, so Nyxmon, which polls `/v1/conditions`, alerts on them.

- Isolate metric collection specs. A spec whose collector raises, for example on a
  malformed `request_timeout_seconds`, is now marked `critical` with the error and
  rescheduled, and the remaining specs and the `metric_collectors` heartbeat still
  run. Previously one bad spec aborted every tick at the same place and silently
  stopped all collectors after it. Spec configs are now validated on save in the
  admin and in `apply_metric_collection_specs`: timeouts must be positive numbers
  and `verify_tls`/`follow_redirects` must be real booleans (`"false"` used to mean
  `true`). HTTP page probes stream the body under a cap (`max_body_bytes`,
  default 10 MiB) that also covers redirect bodies.

- Reject NaN and +/-Infinity metric values. `POST /v1/metrics` now answers `400`
  instead of counting them as ingested (InfluxDB dropped them silently), and
  collection specs skip them, so a sensor reporting only NaN no longer looks alive
  in the registry.

- Evaluate threshold conditions per series. A condition whose filters match
  several series (for example several mountpoints or collectors) now goes
  critical/warning when any one series breaches or goes stale, independent of
  InfluxDB table order, and its message names the deciding series. The InfluxDB
  v3 query now selects all columns so custom tags separate series there too.
  Single-series conditions are unchanged.

- Update locked dependencies with known advisories to their patched releases:
  Django 6.0.8, granian 2.7.4, urllib3 2.8.0, sqlparse 0.6.0, idna 3.15, anyio 4.14.2
  and click 8.3.3, plus dev tooling (pytest 9.0.3, pygments 2.20.0, virtualenv 21.7.13).
  `pip-audit` reports no known vulnerabilities in the lockfile.

- Accept dedicated read-only Basic authentication on inventory status for Nyxmon,
  alongside the existing Bearer credential, without widening writer access.

- Separate inventory delivery freshness, partial coverage and cached release-source
  health in the private status API, preserving the combined attention field and
  source timestamps without new scans or upstream requests.

- Show bounded, snapshot-bound worker/runtime unit bindings under existing
  applications, preserving failed, missing and historical evidence.

- Add weekly cached inventory release comparison with source timestamps and explicit unknowns.

- Display recorded local software-health observations with source times, explicit
  historical/unknown states and bounded APT security-update scope.

- Add private, snapshot-bound CycloneDX 1.6 Python package SBOM downloads,
  preserving observation provenance and explicitly incomplete dependency coverage.

- Add a searchable application evidence view with separate installed/running
  versions, cached APT update findings, Python requirements and Git provenance.
  Preserve partial/historical labels and unknown coverage; reading never scans
  producers or fetches upstream releases.

- Accept and display opt-in partial application evidence from failed inventory
  attempts while retaining category errors, alerts and last successful reports.
  Deploy the receiver before enabling producer `preserve_partial_applications`.

- Add an opt-in real Vector/Graphyard transport probe with an isolated SQLite FULL
  database, loopback fault injection, authenticated UI checks and optional local
  collector report round trip. Keep normal settings and services untouched.

- Compare ingest timestamps and duplicate digests without decoding stored reports.
- Return retryable errors for credential storage outages, share report objects across
  category views, and expose the latest report download even after failed probes.

- Add a push-only software inventory pilot with host-bound writer credentials,
  private inventory pages/downloads, monitoring status, transactional snapshots,
  duplicate handling and preservation of last successful category observations.
  No producer scheduling or deployment is enabled automatically.

- Add a GitHub Actions CI workflow that runs `just lint`, `just typecheck` and
  `just test` on every push and pull request.
