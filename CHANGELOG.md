# Changelog

## Unreleased

- Isolate metric collection specs. A spec whose collector raises, for example on a
  malformed `request_timeout_seconds`, is now marked `critical` with the error and
  rescheduled, and the remaining specs and the `metric_collectors` heartbeat still
  run. Previously one bad spec aborted every tick at the same place and silently
  stopped all collectors after it. Spec configs are now validated on save in the
  admin and in `apply_metric_collection_specs`: timeouts must be positive numbers
  and `verify_tls`/`follow_redirects` must be real booleans (`"false"` used to mean
  `true`). HTTP page probes gain a total deadline (`total_timeout_seconds`, default
  30) and a body cap (`max_body_bytes`, default 10 MiB).

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
