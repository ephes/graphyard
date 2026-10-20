# Changelog

## Unreleased

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
