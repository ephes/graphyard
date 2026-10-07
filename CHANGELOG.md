# Changelog

## Unreleased

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
