# Changelog

## Unreleased

- List conditions in the shadow-run preparation scan that Python evaluates
  and the Rust agent skips: an enable flag other than `1`.

- Borrow dimension names and values from samples when grouping Rust
  condition series.

- Reuse one borrowed Rust breach window and continuity check for both
  condition thresholds.

- Require the exact stored enable flag `1` for Rust condition sweeps,
  targeted evaluations and publication.

- Continue Rust condition sweeps past unreadable stored rows and count them
  in the heartbeat. Targeted runs report the selected row's read error.

- List collectors in the shadow-run preparation scan that Python runs and the
  Rust agent refuses: an enable flag other than `1` or a config over 1 MiB.

- Stop continuous Rust condition sweeps between evaluations during shutdown.
  Let the current evaluation finish during the existing grace period and
  record a partial heartbeat for work completed before the stop.

- Keep Rust manual collector runs going when one stored row is unreadable.
  Report the failed row by ID and run the remaining eligible specs.

- Apply the decoded enable flag to Rust collector admission, claims,
  renewals and result writes.

- Refuse Rust due collector configs over 1 MiB before loading their contents.
  Count the refused row in the heartbeat and continue with later collectors.

- Keep one candidate per service and host in Python registry planning,
  preserving last eligible host selection in metric batches.

- Load only the exact subject type and ID pairs named by a metric batch in
  both registry reads.

- Page through Rust due candidates and load each config only while its
  observed schedule and definition still match.

- Keep Rust due sweeps running when a collector row has unreadable stored
  metadata. Report the row ID in diagnostics and leave it due for repair.

- Fence Rust collector claims, renewals and local results by the definition's
  update timestamp, including after edits that restore the previous settings.

- Record empty Rust condition sweeps as liveness without advancing the last
  successful evaluation timestamp.

- Exit unsuccessfully when an explicit Rust condition ID matches no enabled
  row, including IDs outside the database key range.

- Give an in-flight Rust metric attempt up to 500 ms to finish and persist its
  outcome on shutdown, then leave the remaining collectors for the next run.

- Preserve inventory release refresh results and the final registry generation
  check when closing the HTTP client fails. Log the close failure separately
  and retain any original refresh error.

- Include Rust collector schedule overflows in partial heartbeats when a later
  store error stops the batch, even before the first committed completion.

- Rust agent: replace the unmaintained `chrono` crate (RUSTSEC-2026-0543) with
  `jiff` 1.x and remove the advisory exception from `agent-rs/deny.toml`. The
  agent's instants are now `graphyard_core::utc::UtcDateTime`, microseconds
  since the Unix epoch like a Python `datetime`; jiff supplies the clock and
  the calendar. Stored datetimes, Flux and SQL query timestamps, line protocol
  and shadow records are unchanged. New Python-derived fixture cases pin the
  ends of the calendar: the last 26 hours of year 9999 (after the last instant
  jiff's own timestamp type can hold), offsets that move a stored value into
  year 0 or 10000, and ISO week dates that name a day of the neighbouring
  year. No release; the change ships with the next agent release.

- Rust agent: update `cookie` to 0.18.3 and `url` to 2.5.9 for
  RUSTSEC-2026-0594 and RUSTSEC-2026-0592, which failed `cargo deny`.

- Publish a partial Rust collector heartbeat when a later store error stops a
  batch after a committed completion. Keep the original error and the last
  successful heartbeat timestamp.

- Isolate Rust collector schedule overflows so later specs still run and the
  collector heartbeat reports the failed spec.

- Rust agent (`agent-rs/`): version 0.4.4, released as `agent-rs-v0.4.4`
  (built and published locally by `agent-rs/scripts/local_release.py`). It
  carries the Rust agent fixes merged since `agent-rs-v0.4.3` (entries below):
  one TCP connect budget shared across resolved addresses (divergence D-14),
  shadow captures that open the literal SQLite source path, refuse orphan
  commit trailers and declared-bound overruns, keep the last validated offset
  after an append failure and credit a synced batch at its calculated end;
  Influx query windows rejected before transport and authorization header
  errors without credentials; Flux base64 output buffers that grow with the
  decoded bytes; structured Home Assistant `last_updated` values that use the
  current time without rendering; and the store's legacy datetime fallback
  with Django's Unicode decimal digits behind a bounded ISO minimum-length
  check. No new schema: `--check-schema` still needs graphyard migration
  `0015_alter_conditiondefinition_breach_minutes_and_more`.

- Share one Rust collector TCP connection timeout across all resolved addresses,
  preserving immediate fallback after a quick connection failure. The Python
  agent keeps httpcore's per-address timeout (intentional divergence D-14).

- Open the exact SQLite source path read-only when preparing a shadow copy,
  including relative filenames with URI delimiters.

- Report parity worker launch and output decoding failures with the affected
  agent's label after both workers finish.

- Close parity fakes and clean up temporary files when setup fails partway
  through, honoring `--keep` for the work directory.

- Refuse orphan commit trailers in Rust shadow captures and report the last
  complete offset for recovery.

- Stop Rust shadow validation when a framed row exceeds the batch's declared
  remaining bytes.

- Redact credentials from Rust Influx authorization header errors for Flux
  queries and v2-compatible writes, including shadow captures.

- Reject empty or reversed Rust Influx query windows before contacting the
  backend in any API mode.

- Format only the five Python collector diagnostics shown in the run heartbeat
  while preserving totals and per-spec results.

- Continue Python condition evaluation and publish its run heartbeat when a
  logging handler fails to report a condition error.

- Check never-attempted and least recently attempted release sources first so
  a retry can reach sources left waiting by an exhausted refresh budget.

- Cancel in-progress public release reads at the remaining source or refresh
  deadline, allowing later lookups while refresh time remains.

- Normalize Python Influx query bounds once and reject empty or reversed windows
  before contacting the backend. Prepare dimension filters once per result scan.

- Measure Flux column labels and defaults once per table when checking expanded
  result limits.

- Credit synced Rust shadow appends at their calculated end, avoiding a
  post-sync metadata failure for a batch already on disk.

- Keep the last validated Rust shadow offset when an append fails so retries
  check any partial write without rescanning the admitted peer batches.

- Count a successful release lookup as skipped if the run deadline has expired
  when its publication fence is acquired.

- Require a complete HTTP 200 response without Content-Range before reading public
  release evidence.

- Mark future-dated application observations as stale in application detail,
  consistent with release comparisons.

- Require a new successful lookup before using release evidence from a reactivated
  inventory source.

- Reject nonstandard JSON numeric literals in public releases, including in
  ignored metadata, while retaining the last successful lookup.

- Require public release bodies to match their declared length before decoding
  and preserve the last successful version and timestamp on mismatch.

- Validate inventory request lengths as bounded ASCII decimals and refuse
  mismatches before decoding or storing reports.

- Return HTTP 401 when an inventory host is removed after authentication and
  before ingest admission.

- Read host summaries and cached release-source health together for inventory
  status, keeping attention totals consistent during concurrent registry changes.

- Read fleet inventory summaries from timestamps, category statuses, item counts
  and gaps projected by the database. Full evidence remains available in detail
  and export views.

- Bound the Rust store’s ISO timestamp minimum-length check to seven Unicode
  scalars so malformed date prefixes can be rejected without scanning long tails.

- Match Django’s Unicode decimal digits when the Rust store reads legacy
  SQLite timestamps, including fractions and numeric timezone offsets.

- Grow Rust Flux base64 output buffers with decoded bytes.

- Use the current time for structured Home Assistant timestamps without
  rendering the payload in either agent.

- Rust agent (`agent-rs/`): version 0.4.3, released as `agent-rs-v0.4.3`.
  `home_assistant_env_scan` gains two opt-in, Rust-only knobs (README "Metric
  collection specs"): `include_entity_metadata` tags every point with the
  entity's `friendly_name`, its Home Assistant area (`area`, cached from one
  `POST /api/template` per `entity_metadata_refresh_seconds`, default 600) and
  `device_role=thermostat`; `climate_attributes` writes numeric `climate.*`
  attributes as their own points (`temperature` as
  `measurement_kind=setpoint`). Both stay inside the HA output envelope; the
  metadata tags never replace a point's tags or exceed 32 tags per point. The
  Python collector ignores them.

- Grafana: `Graphyard Room Climate` is rebuilt around Home Assistant areas on
  a new Flux datasource (`graphyard-influxdb-flux`, token from
  `GRAPHYARD_INFLUX_FLUX_TOKEN`): an overview (current temperature per room,
  outdoor reference, humidity per room), one repeated row per room
  (temperature with the setpoint dashed, valve position, humidity) and the
  previous InfluxQL panels collapsed at the bottom. Legends are the latest
  `friendly_name` per `entity_id`, so a rename in Home Assistant does not
  split a curve.

- Grafana: the `Graphyard Room Climate` dashboard gains a `Heating Valve
  Position` panel (radiator thermostat valve opening in percent, series tagged
  `measurement_kind='valve_position'` by the Home Assistant env scan's
  metric mapping).

- Rust agent (`agent-rs/`): version 0.4.2, released as `agent-rs-v0.4.2`
  (built and published locally by `agent-rs/scripts/local_release.py`). It
  carries the Rust agent fixes merged since `agent-rs-v0.4.1` (entries below),
  among them: a due condition check runs right after the metrics and the
  scheduler sleeps until its next deadline (idle without deadlines), targeted
  condition checks keep the global heartbeat, condition watermarks are ordered
  by parsed UTC instants, metric specs are decoded in database order and
  their schedule is validated before a collection is claimed, heartbeat retries
  are classified by SQLite BUSY result codes and ignored late heartbeat
  successes are reported, stricter schema readiness checks (ordinary tables,
  the service-to-host foreign key, required registry columns), Unicode decimal
  JSON path indices aligned with the Python agent, bounded integer and JSON
  array admission, an empty TLS trust store reported before connecting, and
  redacted debug output of the settings and the Influx client.
  `--check-schema` still needs graphyard migration
  `0015_alter_conditiondefinition_breach_minutes_and_more`.

- Propagate Python heartbeat contention immediately inside a caller-owned
  transaction.

- Classify Python and Rust heartbeat retries by SQLite BUSY result codes.

- Bound Rust invalid-integer formatting to the 200-character diagnostic prefix.

- Align Unicode decimal array indices and literal object keys in Python and
  Rust HTTP JSON metric paths.

- Report Python UniFi streaming decode failures without constructing an
  unadmitted response object graph.

- Count UniFi JSON integer digits during the Python pre-scan without copying
  numeric tokens.

- Diagnose additional required SQLite registry columns with absent or literal
  NULL defaults before the Rust agent attempts inserts.

- Report a missing or mismatched service-to-host foreign key during Rust agent
  schema readiness checks.

- Reject excess items in bounded Rust JSON arrays before rescanning their values.

- Check the Rust Python-compatible integer digit limit before allocating
  normalized digits and parse plain digit runs without a copy.

- Report an empty effective TLS trust store before the Rust collector opens a
  verifying HTTPS connection.

- Preserve the collection spec name on HTTP JSON metrics in Python and Rust
  when a configured tag also uses `spec_name`.

- Report a storage error when SQLite ignores an update that would advance the
  Rust agent heartbeat's last successful observation.

- Reject views and other non-table objects in place of required tables during
  Rust agent schema readiness checks.

- Break token name sort ties by token ID in both ingest-token audit reports.

- Escape control characters in the token name shown by the revocation receipt.

- Disable removed release sources with one filtered update under the registry
  generation lock.

- Read remaining release cache statuses in one query when the refresh budget
  expires, preserving fresh-hit and skipped-source counts.

- Seed disk-usage conditions in a transaction on the condition write database,
  including the name lookup and save.

- Reconcile concurrent first-time disk-usage seeds by updating the winning
  row under lock after a unique name collision.

- Preserve the InfluxDB health result or original probe error when closing
  the client fails, and log the cleanup failure separately.

- Load the three pipeline heartbeats for the health endpoint in one query.

- Log the database alias and effective SQLite journal mode when it differs
  from the requested mode.

- Read the default database directly when checking for enabled legacy ingest tokens.

- Check Python condition series for conflicting duplicates without copying the
  sorted sample list.

- Select the Python condition result in one pass over the series, preserving
  severity, timestamp and structural tie-breaks.

- Reuse cached release evidence across version-page rows for the same source.

- Avoid updating unchanged source rows when refreshing the inventory release registry.

- Report Rust point normalization rejections before transport, including failed writes.

- Skip rendering diagnostic Influx response headers for successful Rust writes.

- Reuse the validated Rust request timeout when checking a metric spec's total
  deadline, preserving diagnostic order.

- Detect cycles in Rust environment setting proxies before repeating a lookup.

- Redact the Influx URL and token in Rust client configuration debug output.

- Limit Rust agent settings debug output to numeric settings and the effective
  Influx mode.

- Compare Rust condition evaluation watermarks as UTC instants so older and
  equal-time attempts preserve observations stored with legacy datetime text.

- Sleep until the next Rust scheduler deadline instead of polling every
  idle second. Signal-driven shutdown remains immediate.

- Share one captured Rust metric spec identity across lease updates, without
  copying historical diagnostics on renewal.

- Require a string project name in PyPI release responses before normalizing
  and matching it to the registered project.

- Check declared release response lengths as ASCII decimal text before reading
  the body. Leading zeroes are accepted without integer conversion.

- Flag incomplete inventory coverage when a category has no latest attempt,
  even when all category rows exist.

- Use the same cached release evidence rules in source health and version
  comparisons. An empty cached version is reported as missing evidence.

- Stop polling the Rust scheduler when neither loop has a next deadline.
  Signal-driven shutdown still works while idle.

- Decode Rust metric specs directly from SQLite rows in name order, returning
  the first decode error without a partial result.

- Reject overflowing metric schedules before the Rust agent claims a spec or
  runs its collector.

- Retain only the first five metric collector heartbeat error samples in the
  Rust agent while keeping the full outcome counts.

- Leave stdout empty if either ingest-token audit exceeds its spool or
  per-token limit. Successful empty audits still print their heading.

- Report an out-of-range datetime cutoff as a command error in both
  ingest-token audits before reading the database.

- Reuse one validated scope matcher per token in the host-usage audit.
  Damaged scopes still mark every identifier outside the current scope.

- Read token metadata and host usage from one snapshot on the database
  selected for the token query in both ingest-token audits.

- Reject malformed UTF-8 in Rust agent settings, including values read via
  proxies. Startup diagnostics name the setting without exposing its value.
  Unused environment entries are ignored.

- Reject invalid `INFLUX_API_MODE` settings at Django and Rust agent
  startup. The diagnostic names the setting without exposing its value.

- Recheck the scheduler clock after metrics in both the Rust agent and
  `manage.py start_agent`, so a newly due condition run starts before the
  scheduler sleeps.

- Preserve the global condition evaluator heartbeat during Python and Rust
  checks targeting a single condition ID. Targeted checks still publish the
  condition result and report their own counts.

- Read enabled Python condition definitions in bounded batches from the writer
  database, closing each read before querying Influx or publishing results.
  Both evaluators visit conditions in primary key order.

- Local Rust releases bound GitHub metadata commands to 60 seconds and
  release transfers to 30 minutes per command.

- Local Rust release builds give the checks and build containers
  invocation-specific names so reused process IDs do not cause collisions.
  After an uncertain create, cleanup verifies ownership before removing the
  container by ID. Docker cleanup has bounded runtime and output and reports
  secondary failures without replacing the original build error. Image tags
  that were never built are not reported as failed removals.

- Local Rust release publishing stages readable regular release notes before
  creating a GitHub tag. Downloaded assets are compared byte for byte in
  bounded blocks.

- Rust agent (`agent-rs/`): version 0.4.1, released as `agent-rs-v0.4.1`, the
  first agent release built and published locally by
  `agent-rs/scripts/local_release.py` (entry below). It carries the Rust agent
  fixes merged since `agent-rs-v0.4.0` (entries below): stdout/stderr records
  escaped, bounded and written by nonblocking output workers (collection,
  heartbeats and signals stay live under backpressure; panics drained before
  exit), bounded collector diagnostics, collector cookie admission (single
  label and edge-dot domains refused, explicit Cookie headers honoured,
  retained cookies bounded per execution), encoded response bytes charged
  before decoding and decoder stacks admitted before allocation, Host
  refreshed on cross-origin redirects, bounded shadow capture validation,
  UniFi admission sized for real sites with validated uplink identity and
  complete traffic rates, and the stricter storage contract check before
  collectors start. `--check-schema` still needs graphyard migration
  `0015_alter_conditiondefinition_breach_minutes_and_more`. `agent-rs/deny.toml`
  ignores RUSTSEC-2026-0543 (chrono unmaintained, no known vulnerability)
  until its replacement (work item `rust-chrono-replacement`).

- Release the Rust agent locally with `agent-rs/scripts/local_release.py`
  (configuration `agent-rs/local-release.json`, `just rs-release` and
  `just rs-release-publish`, agent-rs/README.md "Releases") instead of GitHub
  Actions: fixture drift and release check tests on the host, workspace checks
  and a `linux/amd64` ubuntu-22.04 release build in local containers without
  network, the glibc floor, smoke test and deterministic archive of
  `release_checks.py`, then tag, GitHub release and a byte-for-byte download
  comparison. The tag-triggered `.github/workflows/agent-rs-release.yml` is
  removed. Tag names (`agent-rs-v<version>`), archive names and download URLs
  are unchanged, so ops-control's version and sha256 pins keep working.

- Escape terminal controls and Unicode line separators at the final Rust agent
  stdout/stderr boundary. Stream formatting within the 8 KiB UTF-8 record limit
  with explicit truncation and one physical line per event, while preserving
  stored and semantic/parity message values.

- Keep Rust agent collection, heartbeats and signals live under stdout/stderr
  backpressure using separate bounded output workers (128 queued 8 KiB records
  per stream). Drop new records on saturation, count/report losses after sink
  recovery, and limit the combined exit drain to 200 ms. Panic messages use the
  same bounded, escaped stderr path instead of a synchronous write; an uncaught
  panic is drained before the process exits with code 101.

- Apply and prune metric collection specs in one transaction on the resolved
  write-router alias, including authoritative reads and conflict checks; publish
  success output only after that transaction commits. The PostgreSQL contract
  settings and job include a separate `spec_writer` test database for this oracle.

- Refuse spec files whose opened generation or observed length changes during
  reading, before JSON admission or apply/prune; retain coherent reads across
  atomic path replacement.

- Escape terminal controls and bound displayed names to 128 characters in ingest
  token issuance diagnostics, preserving stored names and exact once-only secrets.
- Keep ingest token issuance conflicts private with fixed diagnostics and no raw
  database exception chain; failed rotation still rolls back without printing a secret.

- Charge raw Rust collector response bytes before decoding: at most 64 MiB per
  response, tightened to the decoded allowance plus 64 KiB when smaller. Abort
  overflowing connections without accepting partial successful metrics.

- Admit at most four recognized Rust collector Content-Encoding layers before
  allocating decoders or reading the response body.

- Bound Rust collector cookie jars to 256 retained entries and 64 KiB of names
  and values per execution. Reject overflowing responses before publication,
  with a fixed diagnostic and without changing the previous jar.

- Ignore explicit single-label cookie domains in Rust collectors to prevent
  top-level-domain credential propagation across unrelated hosts.

- Honor explicit Rust collector Cookie headers ahead of jar credentials;
  redirects continue to rebuild destination cookies from the jar.

- Refresh explicit Rust collector Host headers on cross-origin redirects, while
  preserving same-origin virtual hosts and the existing authorization policy.

- Refuse over-budget Rust shadow reopen/suffix validation before publication:
  64 MiB, 250,000 total lines and five seconds per scan, preserving evidence and
  verified offsets with explicit rotation/offline-qualification guidance.
  Refuse single batches outside that envelope before writing.

- Run Rust shadow validation, append and sync off the async runtime, retaining
  job capacity and file-lock ownership through caller cancellation.

- Read token-audit policy and usage in one credential-free database snapshot,
  spool bounded projections privately, and release the transaction before output.
  Escape stored terminal controls and quote suggested scope operands literally.

- Preserve UniFi uplink port zero, reject invalid integer identities and ambiguous
  port metadata, and warn on missing/invalid/negative receive or transmit rates.

- Refuse ambiguous UniFi login/device JSON in Python and Rust, including escaped
  duplicate members, without using the legacy login fallback. Bound expanded
  nodes (1,000,000), depth (32), arrays (4,096 items) and scalar metadata before
  selection and publication; refusals report `UniFi evidence refused`, while
  undecodable, over-long-integer and syntax errors keep the decoder diagnostic.

- Own Rust HTTP connection tasks before headers and bound running/queued DNS
  jobs across production contexts, retaining admission through cancellation.
- Bound expanded Flux records/cells in Python and Rust and remove remote column
  labels from duplicate-column warnings.
- Format only the selected condition series label with sparse dimension work;
  open Rust shadow evidence privately and refuse links and special files.
- Frame and sync complete Rust shadow batches, refuse incomplete tails and hold
  a bounded-wait OS lock across validation/append; validate framed captures in
  the comparison tool while retaining legacy-read compatibility. Validate an
  existing capture once at open, and refuse `#`-prefixed rows before appending.
- Credit bounded successful writes independently of body charset and reject
  duplicate Flux labels in both runtimes before decision evidence is built.
- Refuse conflicting same-series/same-time values in both condition evaluators;
  cooperatively parse Rust Flux replies within the whole-query deadline.

- Verify Rust agent storage affinity, nullability, generated row identity and
  unconditional unique keys against migrated fixtures before starting collectors.
  Read the complete readiness contract in one deferred SQLite snapshot; schema
  checking remains read-only and operational migrations require a stopped agent.
- Validate and canonicalize ingest host scopes once per locked request while
  retaining fail-closed matching and current credential authority.
- Admit inventory freshness thresholds in the portable 60–2,147,483,647 second
  range before credential rotation; share model/admin validators (migration 0016).
- Project inventory application freshness and software health at one response
  timestamp, including wall-clock steps and exact freshness boundaries.
- Count malformed Home Assistant entity rows as skipped in Rust scans,
  preserving valid neighbors while configuration/runtime failures remain fatal.
- Bound Rust Home Assistant regex backtracking, collection-wide matching work,
  and point/tag amplification before normalization. Exhausted collections fail
  critical without partial metric or registry writes; ordinary reference
  fixtures remain compatible; plain patterns (no lookaround, backreferences,
  atomic/possessive groups or conditionals) do not use the 1000-step backtrack
  limit.

- Rust agent (`agent-rs/`): release `agent-rs-v0.4.0`
  (<https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.4.0>), built by
  the release workflow from `3d6616d`. Asset
  `graphyard-agent-0.4.0-x86_64-unknown-linux-gnu.tar.gz`, sha256
  `1528155d1c6fc8208ac22109ca158232c0302605fda1540b7721dc07a078eb7a`; the binary
  needs at most `GLIBC_2.34`. ops-control `281d408f` pins it, and the
  `graphyard_core` deploy of 2026-11-15 installed and selected it on macmini
  (T1 07:08:36 UTC, `/proc/<pid>/exe` `/opt/graphyard-agent/0.4.0/graphyard-agent`,
  `--check-schema` exit 0 for 0.4.0 and the rollback target 0.3.0). One hour
  later: no restart, all 32 specs ran without failures, the 71 InfluxDB series
  keys of the hour before were written again and no new key appeared,
  `/v1/health` as before, `RssAnon` 5.1 MiB, CPU 3.5 s in the first hour.

- Rust agent (`agent-rs/`): version 0.4.0, released as `agent-rs-v0.4.0`.
  It carries the Rust fixes merged since `agent-rs-v0.3.0` (scout rounds 87
  to 99 and the common timestamp domain), among them due-spec claims renewed
  through collection and publication, fenced delayed observations and query
  replies bound to their selection, a DB actor bounded to 64 jobs, metric
  identities without control characters and with the API's dimension/tag
  limits, write batches refused above 8 MiB, the common InfluxDB v2/v3
  timestamp domain for `INFLUX_API_MODE=auto`, page-probe source identities
  without credentials, query or fragment, and unambiguous UniFi site,
  controller URL and interface selection. Upgrade notes: a page probe whose
  URL has query parameters now needs `config.source_entity_id` (otherwise the
  run is critical); a UniFi spec with a controller URL containing `?` or `#`,
  an invalid `site_id`, or a port selector matching several interfaces fails;
  out-of-domain timestamps are skipped in auto mode. `--check-schema` still
  needs graphyard migration
  `0015_alter_conditiondefinition_breach_minutes_and_more`. macmini keeps
  running 0.3.0 until ops-control pins the new release.

- Order public release publication by owned registry generation across wall-clock
  rollback; report current results that cannot publish as incomplete.
- Reject duplicate members throughout public release JSON, retaining previous
  successful cache evidence and isolating failed sources.
- Choose condition diagnostics by structural series identity in Python and Rust;
  JSON-quote tag names/values and distinguish missing from empty dimensions.

- Enforce the inclusive common InfluxDB v2/v3 timestamp domain in Python and Rust
  writers with `INFLUX_API_MODE=auto`: `1677-09-21T00:12:43.145225Z` through
  `2262-04-11T23:39:59.999999Z`. Skip out-of-domain points before HTTP/shadow
  writes, preserve valid siblings and counts, and leave explicit modes unchanged.

- Use binary nearest-time matching with existing tie/reuse semantics and mark
  enabled specifications with missing observations as inconclusive.

- Reject nonfinite qualification controls, observations and unrepresentable point
  timestamps; preserve finite signed values and avoid difference overflow.

- Stage shadow preparation privately and publish a synced v2 completion marker
  last; preserve incomplete generations with an explicit safe recovery path.
- Bound individual and cumulative comparison evidence bytes, rows, history nodes
  and nesting, including growing files and fetched production CSV.

- Give shadow backups a total elapsed deadline and roll back the whole requested
  disable set when any specification is missing.

- Bound recorder unit probes by deadline/output and retire their owned process
  groups; reject nonfinite or invalid sampling controls before resource access.
  Kill helper groups before reaping their leader where `os.waitid` exists, keep
  the original error when Darwin reports EPERM for a zombie-only group, and
  refuse wrongly shaped comparison records with a located error.

- Refuse recorder output aliases of input databases and replay explicit session
  baselines so offline deletions cannot survive recorder restarts.

- Fence token admin security edits with signed current-state revisions and narrow
  saves; preserve credentials, revocation and ingest usage. Registry admin edits
  preserve observed telemetry/provenance, protect existing identifiers and refuse
  stale identity/service-host edits. SQLite writer contention returns a controlled
  reload response instead of an accidental server error.
- Batch binding and host-scope report metadata/projected usage in groups of 100
  tokens, eliminating per-token usage queries and avoiding credential/usage model
  reads. The default host-scope report excludes disabled-token histories at SQL
  selection time.
- Bound the Rust DB actor to 64 running/queued jobs with async admission. Cancelled
  permit waiters send no write; admitted jobs finish despite caller cancellation.
  Close orders all accepted sends before Stop, rejects new calls across clones
  and wakes pending admission waiters before joining outside admission locks.

- Invalidate cached release success when its source/target definition changes and
  fence in-flight publication against the registry generation and enabled exact
  definition, including removal and A → B → A edits. A refresh superseded by a
  newer one stops looking up sources and reports the rest as skipped; any newer
  refresh before it returns makes it incomplete.
- Bound release refreshes to a five-minute monotonic admission budget, cap each
  remaining lookup allowance, and report skipped sources with an incomplete,
  unsuccessful command outcome without renewing their cache timestamps.

- Refuse ambiguous UniFi site components and controller URL query/fragment forms
  before login in Python and Rust; require unique explicit interface selections
  with typed integer port identities and no writes on selection failure.
- Read health database evidence in one short consistent snapshot with one
  condition-count aggregate, releasing it before remote Influx work. Isolate all
  health responses with private no-store and Cookie variation, including failures
  and authentication transitions. The session login is resolved before the read-only
  PostgreSQL snapshot, so a session flush or key rotation cannot fail health.

- Refuse control characters in metric identities and custom tags before stripping
  or encoding, and apply the API's dimension/tag limits to Python and Rust direct
  writers. Rust rejection diagnostics retain at most 32 bounded events.
- Refuse entire Rust write batches above 8 MiB of UTF-8 line protocol (including
  separators) before network or shadow-file effects; retain normal point order
  and valid-sibling handling below the limit.
- Keep page-probe transport credentials, queries and fragments out of metric and
  registry source identities in both runtimes. Use an explicit nonsecret
  `source_entity_id` for URLs with query parameters; missing IDs fail before
  transport, preventing query-selected endpoints from collapsing into one series.
- Add an opt-in local timestamp compatibility oracle for InfluxDB 2.7.12 and
  3.12.2. Their upper limits differ; auto writers now apply the common
  domain described above, while the oracle measures sink behavior in explicit v2 mode.

- Keep spec-import validation diagnostics private: report only definition indices
  and known fields, without raw values/names or model-validation error chains;
  preserve all-or-nothing reconciliation and its successful summary output.

- Admit spec-import JSON depth, 100,000-node complexity and 1,000-definition
  budgets through the existing streaming parser before building an object tree;
  refuse non-JSON NaN/Infinity constants. Syntax errors found by this pass (empty
  file, trailing data, missing brackets) are reported as `Invalid JSON in <file>`
  without line/column. A spec path the OS refuses (for example with a NUL byte)
  is reported as an invalid path instead of a decoder-limit error.

- Refuse duplicate JSON members throughout spec-import files, with position-only
  diagnostics, and normalize overlong JSON integers to controlled command errors
  before database access or pruning.

- Bind credential command rotation/enrollment to one routed or explicit
  `--database` transaction; reject invalid host-scope report lookbacks before reads.

- Keep inventory registry/usage observations monotonic under delayed writes;
  bind complete Python/Rust Influx query replies to their issued selections.
  On SQLite a corrupt or offset-form registry `last_seen_at` text is no longer
  rewritten automatically; repair it in the admin.
- Admin definition revisions are a keyed MAC instead of a signed bare hash, so
  the change page no longer exposes a hash over masked secrets. Query parity
  fixtures again pin sample decoding for selection-matching replies.

- Protect spec and condition admin edits with definition revision conflicts; keep
  runner scheduling/results read-only and preserve current masked secrets.

- Rust agent (`agent-rs/`): release `agent-rs-v0.3.0`
  (<https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.3.0>), built by
  the release workflow from `eba6ea1`. Asset
  `graphyard-agent-0.3.0-x86_64-unknown-linux-gnu.tar.gz`, sha256
  `55f947fd84edfacf04dd0f28fe00db3018fd4cc08b69bf96c1a28ebbb61643f3`; the binary
  needs at most `GLIBC_2.34`. ops-control `10de3426` pins it, and the
  `graphyard_core` deploy of 2026-11-12 installed and selected it on macmini
  (T1 15:18:42 UTC, `/proc/<pid>/exe` `/opt/graphyard-agent/0.3.0/graphyard-agent`,
  `--check-schema` exit 0 for 0.3.0 and the rollback target 0.2.0). One hour
  later: no restart, every spec run on time, no new InfluxDB series key
  (75 in 24 h), `/v1/health` ok, cgroup anon 5.1 MB, CPU about 1.2 min per
  day. No shadow run or cutover drill: those qualified the switch from the
  Python to the Rust agent, not a version update of the selected one.

- Rust agent (`agent-rs/`): version 0.3.0, released as `agent-rs-v0.3.0`.
  It carries the Rust fixes merged since `agent-rs-v0.2.0` (scout rounds 79
  to 85), among them collection claims bound to the loaded definition, lease
  renewal and publication fences, heartbeat ordering, registry batches
  chunked at SQLite's variable limit, unambiguous UniFi device selection,
  bounded HTTP and Influx transfers, and one-shot runs that exit 1 on a
  failed loop. `--check-schema` now needs graphyard migration
  `0015_alter_conditiondefinition_breach_minutes_and_more`. The release also
  moves `rusqlite` from 0.40 to 0.41 (bundled `libsqlite3-sys` 0.39.1,
  SQLite 3.54.1) for RUSTSEC-2026-0459, which fails `cargo deny`; only test
  code needed changes for the new API. macmini keeps running 0.2.0 until
  ops-control pins the new release.

- Render the inventory version page from one read snapshot with batched host
  pointers and reports (constant queries, each report decoded once); a host
  filter reads only that host's evidence.
- Refuse compressed or over-declared public release responses before reading
  their bodies, never buffer past the 16 MiB limit, and record over-nested JSON
  as a failure of that source only, continuing with the remaining sources.

- Chunk Rust registry reads and last-seen updates at the SQLite connection's
  variable limit, retaining one atomic transaction for the full batch.
- Fence Rust heartbeat status/details against delayed and equal-time records;
  retain the maximum successful observation time (first committed status wins ties).
- Cover caller cancellation during real actor-owned registry transactions,
  including complete commit, injected-error rollback and writer-lock release.
- Bind the initial due-collection claim to the loaded definition in both agents:
  an edit between load and claim takes no claim and sends no request with the
  old URL or credentials; the spec stays due for its current definition.
- Give the Python agent the Rust lease contract: `min(max(3, interval), 300)`
  second claims renewed every third of that while a collector runs; a lost
  renewal refuses sink/registry publication and completion.
- Refuse ambiguous UniFi device selection in both agents: a configured
  `device_mac` is authoritative, a name must match exactly one device, and
  several matches record a warning without points. Specs selecting by a
  duplicated name need `device_mac`.

- Reject duplicate inventory JSON members at every depth before storage, while
  preserving repeated array entries and existing report budgets.
- Mark all inventory reader responses private and no-store, including raw reports,
  version pages, SBOM errors and login redirects.

- Limit condition windows and breach durations to 1–10,080 minutes in admin,
  seeding and both evaluators. Reject invalid historical definitions before
  querying without rewriting them; preserve breach periods longer than windows.
- Mark authenticated condition responses and the owner host/service index
  private and no-store, preserving public status visibility after logout.

- Reject metric JSON/NDJSON integer decoder failures and duplicate object keys
  with bounded parse errors and accurate NDJSON counts, before ingest writes.
- Fail condition evaluation on malformed Flux/SQL row evidence; preserve the last
  value instead of silently dropping corrupt samples. Truly empty results keep
  their existing stale-data policy.
- Require identity-encoded Python SQL responses and reject compressed replies
  before body reads; retain the inclusive 10 MiB limit and total deadline.
- Batch inventory summary pointers and reports across hosts, decoding each
  referenced snapshot once. Materialize summaries and detail-page evidence in
  one short database snapshot so concurrent ingestion/pruning cannot mix
  generations or temporarily erase coverage. Preserve historical successes.

- Fence release-source policy replacement with a monotonic database generation,
  including empty registries and delayed older refreshes.
- Revalidate current metric credentials, scope and binding under a token-row write
  reservation through the sink call; serialize successful revocations and edits
  after already-admitted writes without promising remote rollback. SQLite holds
  its database-wide writer lock throughout the bounded sink call; admission
  database failures return fixed-text 503 without a second heartbeat write.

- Cover actual process death after spec claim commit and acknowledged sink
  acceptance with pipe-gated synthetic child processes and fresh restarts.
- Assert persisted lease recovery without false healthy bookkeeping; document
  and count potentially repeated delivery after uncertain local completion.

- Renew Rust due-spec claims through collection and publication, fenced by the
  exact claim and captured definition; abandoned work retains bounded recovery.
- Verify full/partial SBOM exports against pinned local CycloneDX 1.6 schemas
  offline, with exact snapshot provenance and invalid-artifact counterfactuals.

- Admit spec files through one nonblocking regular descriptor with a 2 MiB actual
  byte cap, explicit nesting bound and safe decoder errors before database work.
- Refuse stale spec-apply updates and prune operations using captured definitions;
  roll back conflicts and duplicate-create races with a clean retry error.

- Order inventory host latest and category pointers consistently by observation
  time, receive time and row ID, including tied clocks and failed categories.
- Cover inventory credential migration refusal, retry and constraint installation
  on an isolated secondary database without changing either database's history.

- Serialize inventory admission with credential rotation on the registered host
  lock; cover simultaneous identical/conflicting snapshot retries and revocation
  ordering with synthetic independent-connection tests.

- Cover real Rust collector publication failures with synthetic Influx rejection,
  disconnect and SQLite registry-trigger tests; document write-before-registry,
  atomic bookkeeping rollback and the partial-success boundary without replay
  within an invocation or an exactly-once promise across retries.

- Fence Python/Rust collector result publication against definition edits,
  disabling, deletion and replaced schedules, including explicit manual runs.
- Exclude superseded collector outcomes from current run counts and heartbeats;
  retain a safe spec-ID diagnostic without replacing newer pipeline evidence.
- Report condition `failure_published` from the actual database write outcome
  in Python and Rust, including definition and attempt-watermark rejections.

- Normalize Python/Rust collector errors before persistence to allowlisted types
  or numeric HTTP status; omit exception text, URL credentials and remote bodies
  from failure records and collector logs. Keep configuration validation messages.
- Emit safe per-condition failure events with ID, evaluation/publication stage
  and error category in both runtimes; continue after failed fallback publication.

- Return structured HTTP 503 health evidence on database failures, stop dependent
  reads and report unknown counts rather than a generic server error.
- Treat missing or implausibly future heartbeat success as at least warning;
  tolerate up to 60 seconds of clock skew without negative ages.

- Make Python and Rust agent `--run-once` exit nonzero after failed work or a
  run exception. Both enabled loops still run; continuous retry behavior and
  warning-only success are unchanged.

- Require Django system and missing-migration checks in CI and expose
  `just schema-check`, using test settings with in-memory SQLite and frozen
  dependencies so checks never create migrations or touch an owner database.

- Bound Influx Flux queries to 10 MiB decoded response data and 30 seconds per
  exchange, and writes to 64 KiB and 30 seconds, in Python and Rust. Error
  responses count too; over-budget writes report uncertain acceptance without
  replay. Error diagnostics retain at most 1 KiB.

- Confine Python/Rust UniFi login and session redirects to the controller
  origin, refusing downgrades. Bound all response bodies across a collection
  by one byte allowance and total deadline; require identity encoding.


- Enforce Rust HTTP output allowances during each decoding stage, including
  stacked encodings, instead of after expanding an unbounded chunk.
- Reserve owner/admin login attempts atomically before password verification;
  settle failures once and preserve concurrent reservations on success/expiry.


- Fence Python and Rust condition publications against definition edits and
  disablement during queries, retaining the existing attempt-order fence.
- Accept metrics once Influx writes succeed even when subsequent database
  bookkeeping fails; report degraded bookkeeping with fixed logs and health.


- Health projection never reduces stored heartbeat severity as it ages. Python
  heartbeat throttling tracks only the latest committed state, so recoveries and
  retries after failed writes are persisted.

- Audit the frozen Python runtime and development graph in CI with pinned
  pip-audit; document the identical local command and hook-environment scope.

- Refresh the Python development toolchain to filelock 4.2.1 and virtualenv
  21.14.10 and its required distlib update to address developer-tool advisories.
  Virtualenv also requires the shared runtime `packaging` dependency to advance
  from 26.0 to 26.3; inventory release version-parsing tests cover that usage.
  Rust dependencies are unchanged.

- Inventory issuance serializes on the registered host and enforces one enabled
  credential per enrolled host. Migration 0013 refuses existing duplicates with
  host IDs for explicit operator repair; it never retires credentials silently.
- Due collectors (Python and Rust) atomically claim scheduling slots before I/O.
  Crashed claims expire within 300 seconds (or the shorter collection interval);
  superseded completions cannot replace a newer schedule/result. Explicit
  non-due manual collection still runs every enabled spec.

- Conditions (Python and Rust): require bounded coverage between all breach-window
  samples and through evaluation time. Sparse sampling remains supported within
  the configured carry allowance. Publish successes and failures only when their
  attempt is newer than the stored result, preventing late evaluations from
  replacing newer evidence; failures now advance `last_evaluated` too.

- Rust agent (`agent-rs/`): bump the locked `tokio` from 1.54.0 to 1.54.1
  for RUSTSEC-2026-0384 (race condition in `tokio::time::Interval`), which
  turned the `cargo deny` supply-chain check red. Lockfile only, no code
  change. The deployed agent keeps 1.54.0 until a new `agent-rs` release is
  built and rolled out.

- Rust agent port (`agent-rs/`, slice S14): `agent-rs-v0.2.0` is the
  production agent on macmini since the cutover of 2026-10-23 11:31:46 UTC.
  The rollback drill (to Python and back) passed, and the 7-day soak runs
  from 11:41:37 UTC. The owner accepted the S13 shadow results; memory and
  CPU are measured again 24 h after the cutover. PLAN.md 11.2 and 11.3 record
  both.

- Rust agent port (`agent-rs/`, slice S13): the 24 h shadow run on macmini
  was evaluated and torn down on 2026-10-23 (unit stopped, database copy
  deleted, Python agent verified). The acceptance is not met. No series-key
  difference, no restart and identical condition statuses. The heartbeat
  statuses and one registry row differ, explained by host restarts and web
  ingest. Steady `MemoryCurrent` 17.4 MiB (target 16) and CPU 1.639 min/day
  (target 1.5) miss the lean goals. PLAN.md 11.2 records
  the numbers.

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
