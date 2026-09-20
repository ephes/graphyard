# Push inventory pilot

This implementation adds a receive-only software inventory area. It never connects
to producers or executes remote commands. No scheduling or fleet deployment is
installed by this change. Metrics and inventory credentials are separate.

## Endpoints and access

- `POST /v1/inventory`: host-bound inventory writer credential; exactly one full
  envelope, optionally wrapped in a one-element JSON array for Vector.
- `/inventory/`: authenticated overview, with per-host detail and report downloads.
- `GET /v1/inventory/status`: authenticated session or a separate read-only bearer
  credential from `GRAPHYARD_INVENTORY_MONITOR_TOKEN`. Writer credentials cannot read.

The monitoring token is disabled by default. Never distribute it to producers.
Enroll expected hosts explicitly so a machine that never reports is still visible.
Host registry entries must already exist and be enabled. Issue a credential with:

```sh
just manage create_inventory_credential --host studio
just manage create_inventory_credential --host atlas --intermittent
```

Capture stdout securely: the command prints a credential once. Only its SHA-256
hash is stored. The credential contains its row ID plus a random secret. Use
`--rotate` to disable existing credentials and issue a replacement. Rotation
preserves existing host freshness settings unless explicitly supplied. Disabling the
host registry entry or the credential also denies future reports. No login/SSH
credential is involved.

## Report contract, schema 1

```json
{
  "schema_version": 1,
  "host": "studio",
  "snapshot_id": "a5a9bc81-0a8c-4caa-bcfb-29c4cb3db664",
  "observed_at": "2026-09-19T08:00:00Z",
  "collector": "software-estate/1",
  "categories": {
    "packages": {"status": "ok", "items": [{"name": "example", "version": "1.0"}]},
    "units": {"status": "ok", "items": []},
    "containers": {"status": "unsupported", "items": []},
    "applications": {"status": "ok", "items": []}
  },
  "gaps": ["dependency_graph:partial"]
}
```

All four categories must be explicit. Status is `ok`, `error` or `unsupported`;
failed/unsupported categories have no successful items. An error string is optional.
Item objects retain the collector's source-specific metadata, including dependency
requirements. The collector must allowlist sources and avoid secrets; the receiver
cannot infer whether arbitrary text in a producer's metadata contains a secret.
Host identity must match the credential. The envelope rejects unknown fields,
invalid identifiers, naive/future timestamps, excessive nesting and non-JSON numbers.
Maximum body size is 8 MiB, at most 100,000 item objects per category. The private
reverse proxy must enforce a matching body/request-rate limit before public use.

Reports are stored transactionally in the existing Django database; Influx does
not hold the inventory documents. HTTP 200 means the transaction committed.
For power-loss durability, deployments using SQLite must set
`GRAPHYARD_SQLITE_SYNCHRONOUS=FULL` (the existing application default is NORMAL).
This setting is a rollout prerequisite, not silently changed for existing metrics.
Storage failures return 503 so Vector can retry. 400/401/403/409/413/415 are permanent
problems requiring operator action; do not assume Vector retries these defaults.

Equal snapshot identity and canonical content is accepted without another write
or refreshed timestamps. Reusing a retained snapshot identity with other content
returns 409. The writer must preserve snapshot ID across retries. Delayed reports
are stored but cannot replace newer category observations. Equal observation
timestamps do not replace an existing category pointer; use a new actual scan time
for a new observation. No server-side refresh of producer time is performed.

A category error updates latest-attempt state while retaining the last successful
category. An empty **successful** category records actual absence. Unsupported
categories and coverage gaps stay visible, not inferred as fully inventoried.
Partial application
probes still mark the entire applications category failed. Producers may attach
`partial_items` only to an `applications` category with `status: error` and empty
`items`. This optional list of objects has the same 100,000-entry, nesting and
whole-request size limits as ordinary items. It contains individual probe results,
including their errors and coverage gaps. The private detail page labels these
latest-attempt results incomplete and keeps the last successful observation
separate. Partial evidence does not clear category-error alerts. The preview shows
at most 50 entries with bounded text and up to 10 coverage gaps per entry. Only
list-shaped coverage is previewed; other shapes remain in the full report. The
attempt-specific download retains complete evidence.

Deploy this receiver before enabling the producer policy boolean
`preserve_partial_applications: true`. The producer default is false; old reports
remain valid and unchanged. Older receivers reject `partial_items`. No database
migration is needed: snapshots retain the full validated report and its digest.

## Retention and monitoring

Retain the newest 32 received snapshots per host plus snapshots referenced by
latest category attempt/success. With four categories, at most eight additional
snapshots can be retained. Old unreferenced rows are pruned inside the ingest
transaction. Identity deduplication applies while a snapshot is retained; replay
of an older pruned report can create a historical row again, without advancing
newer category pointers. Keep the database in the existing backup procedure.

The status response enumerates enrolled, enabled hosts, including those with no
reports. A server observation older than the enrollment threshold (default 48 h)
is stale. Intermittent hosts still show age but suppress freshness alerts once
reports exist. Category errors still need attention. `summary.total` and
`summary.attention` can be consumed by an existing Nyxmon JSON-Metrics check.
That check is not provisioned by this change. Inventory age and receipt age are
separate; retries do not make an old scan fresh.

The detail view displays metadata as reported and links to full JSON. It does not
yet calculate upstream update availability, resolve all dependency graphs, ingest
large CycloneDX artifacts separately, or provide a Grafana inventory dashboard.
Those remain integration work; existing manual ops-control reports are retained.

## Verification and rollout boundary

Django test-client checks exercise actual routing, authorization, transactions,
large requests, duplicate/conflicting reports, out-of-order/error retention, bounded
history, missing hosts, intermittent devices, HTML escaping and credential rotation.
The local Vector/HTTP outage experiment passed after the user enabled local
execution access: exact reports, retry after 503, disk-buffer recovery after
SIGKILL with the source removed, and retry after a lost response. That test uses
a synthetic HTTP receiver. The real Graphyard integration is now covered by the
repeatable probe below; production TLS/ingress verification remains outstanding.

The producer and Vector files are opt-in local pilot tools in ops-library; manual
preparation is documented in ops-control. No private receiver destination has been
selected automatically. The removed macmini pilot is not reinstalled. All changes
require the requested independent review before rollout. The user subsequently
selected Pi with openai-codex/gpt-5.6-sol for earlier reviews; Claude Code Opus is
used again for the transport integration.

Current local checkpoint: all 144 Graphyard tests pass, including 28 inventory
checks; migrations are in sync, Graphyard typecheck and direct Ruff checks pass.
The requested Pi review with openai-codex/gpt-5.6-sol completed. Its three findings
were repaired and independently re-reviewed; round 2 is CLEAN within the supplied
scope. No files were skipped or truncated. Redactions concern only synthetic test
credential literals, not the repaired production paths. The initial Claude and
Pi failures were caused by the earlier restricted execution environment and do
not count as reviews. Claude Code with Opus is available again for final review.
Committing this development checkpoint does not deploy a receiving service; live
verification remains outstanding.

Review repairs: credential lookup is inside the retryable storage-error boundary.
Each distinct referenced report is decoded once per host view, shared across
category attempts/successes. The detail page links the latest report even when
all probes failed; older successful reports retain separately labelled links.

The ingest path compares snapshot timestamps and duplicate digests without loading
stored report JSON. Regression tests cover this for new and duplicate deliveries,
wrong content types, the disabled monitor credential and invalid freshness settings.

Pilot follow-ups: summary/status views still decode retained report payloads and
should receive compact stored summaries before frequent fleet monitoring. Host
revocation and freshness policy editing currently use Django ORM administration;
a complete operator CLI is not included in this checkpoint.

Final Claude Code Opus review: the ingest-loading warning was fixed and the repair
was re-reviewed. Round 2 contains only two advisory suggestions about possible
future query/reordering changes; no Critical/Warning remains. We retain the current
queries: the endpoint regression explicitly detects any stored-report decoding,
and timestamp comparison precedes retention under the same host transaction lock.
No files were omitted or redacted from the repair review. The advisory result is
accepted for this local checkpoint; it is not a CLEAN verdict or rollout approval.


## Repeatable local transport integration

Run the real receiver and the installed Vector binary in an isolated process:

```sh
just test-inventory-transport
just test-inventory-transport --report /private/path/to/local-report.ndjson
```

Requires the Graphyard development dependencies, Vector already on PATH, and the
matching sibling ops-library checkout (`--library /path/to/ops-library` overrides
that source path). The optional report is one schema-1 envelope produced by the
local collector. Only that explicitly supplied report leaves its source directory,
and only towards a new loopback receiver on this same machine.

The probe creates a private temporary directory, explicitly configures Django with
a newly migrated SQLite database and FULL synchronization, and binds two random
127.0.0.1 ports. It never imports the project's normal settings or .env. Generated
credentials and a login account belong solely to this temporary database. Vector
uses the production config generator and directory secret backend; no environment
interpolation override is enabled. The system Vector process stays independent.
Vector uses its default worker count unless the diagnostic `--vector-threads N`
override is supplied. Installed Vector 0.58 on macOS intermittently stalled on
individual small reports until another event or shutdown. Three complete runs with
one worker passed, but a later repeat failed again. One worker is therefore **not**
a reliable workaround; unattended inventory delivery is not approved. The exact
upstream cause is unresolved; the probe must fail when the stall occurs.


Assertions cover a >4 MiB Unicode report, HTTP 503 retries, SIGKILL plus recovery
from the disk buffer with only the synthetic source removed, a dropped committed
response with idempotent retry, last-success preservation after a failed category,
host binding, revoked writers, a working separate monitor credential that writers
cannot substitute, and actual cookie/CSRF login. With `--report`, an
authenticated download must equal the input JSON exactly. A `result.json`, private
HTML captures, database and Vector log remain at the printed artifact path. Treat
that directory as private inventory data; saved HTML is a capture, not a running
service. The probe stops its Vector child and both listeners on exit and removes the
temporary writer secret files. No permanent
service, timer, SSH login or fleet receiver is created.

TLS, ingress body limits, production storage durability and unattended outbox
retention are separate rollout work. Passing loopback tests does not approve those.


Local integration checkpoint (2026-09-19): the complete scenario passed three times
with Vector 0.58 and one worker, including two runs with a real local report. Later runs also verified that missing and empty writer files reject actual
Vector startup. The subsequent single-worker failure supersedes the provisional
workaround recommendation; successful runs do not establish reliable delivery. Artifacts are private and outside the checkout. All 147 application/probe regression
tests and typechecking pass; `just typecheck` now includes the probe's function
bodies. The opt-in probe is intentionally separate from the fast offline suite.


Transport-probe review closure: Claude Code Opus re-reviewed the repairs. All
required findings are resolved or disproved by direct checks; round 3 has only
conditional suggestions about hypothetical mutable status elements or an empty
initial error placeholder. Status elements are integers and no such placeholder
exists, so neither change is needed. The actual Just recipe passed argument-forwarding
checks both without arguments and with a filename containing spaces on macOS.
Review bundles were complete and unredacted. This is an advisory closure, not a
CLEAN verdict or transport acceptance: the intermittent Vector stall remains open.

## Direct sender integration (preferred inventory transport)

The local inventory path now has a source-run stdlib HTTPS sender in the paired
ops-library checkout (`roles/software_estate/files/send.py`). Existing Vector log
and metric routes remain unchanged. Production inventory deployment and weekly
local scheduling are separate rollout steps; no new service is installed here.

Run `just test-inventory-sender` to exercise that sender against real Graphyard
WSGI routes in a fresh private SQLite FULL database on loopback. It never loads
the normal application settings or `.env`, never touches an existing DB, and
requires no Vector binary. `--library /path/to/ops-library` selects the source.
The probe uses synthetic reports and short-lived local credentials. It removes
its credential file and stops its listeners on completion, retaining private
`result.json` and DB artifacts under the printed temporary directory.
This new probe records `listeners_stopped` for its own listener threads; sender
subprocesses are awaited (and killed/reaped by `subprocess.run` on timeout).
The older Vector probe retains its existing `processes_stopped` field.

Coverage includes >5 MiB Unicode reports, a real 503 rejection followed by a new
sender process, a lost reply after commit followed by idempotent replay, delayed
snapshots/category errors, package removal, host-binding rejection and credential
revocation. The retry test first verifies persistent backoff, then advances only
its private fixture's eligibility time rather than waiting an hour. The sender's
ops-library tests additionally exercise 429, invalid acknowledgments, TLS,
process termination, capacity, file permissions and concurrent invocations.

The sender removes a report only after HTTP 200 with `status: stored`, matching
`snapshot_id` and boolean `duplicate`. Both first delivery and duplicate delivery
use this contract. Rejected reports remain on disk and do not obstruct other
eligible reports; network/429/5xx retries are deferred persistently. It sends no
commands and has no SSH functionality. The earlier Vector probe remains available
as a diagnostic for its unresolved inventory-specific low-traffic stall.


## Application evidence view

`/inventory/<host>/applications/` provides an authenticated, read-only view of
already received application data. Open **Applications** on the host overview or
**Applications, versions and dependencies** on its report. No producer contact,
new scan, upstream API request or software upgrade occurs when reading this page.

The view distinguishes installed/running versions, service presence/state, Git
commit/dirty state and installed Python dependency metadata. macOS bundle entries
are expanded into individual applications. All named applications are searchable
by name with 20 entries per page; unnamed entries remain available without a
search filter. Failed or malformed bundle scans retain an explicit diagnostic row. The query is limited
to 100 characters. Missing fields remain Unknown, Not verified or Not assessed.

When an application attempt fails, partial evidence is labelled incomplete. If
there is no usable partial evidence, the last successful application snapshot is
labelled historical, with its own age and download link. A fresh host report never
makes that older evidence current. Latest-attempt downloads remain separate.
An empty application list does not establish absence of installed software.

Update findings display only reported cached APT comparisons; cache age is unknown.
A false update flag means no newer cached candidate, not proof of currentness.
There is no upstream comparison for other applications in this view. Python
requirements are declarations from installed metadata; optional activation and a
complete dependency graph remain unverified. This page is not a complete SBOM.

Each page projects at most 20 applications, with bounded text, ten coverage gaps,
ten cached package comparisons, and ten Python packages with five requirements
each. Counts and the exact source-report download expose the full evidence. Nested
report values are shape-checked and rendered as escaped text, never executable HTML
or external links. Malformed coverage and comparison shapes are labelled explicitly. Unknown nested
shapes do not manufacture success or versions.

## Python SBOM downloads

The application view links to an authenticated, read-only CycloneDX 1.6 JSON
export when an application has successful Python distribution metadata. The URL
`/inventory/<host>/<snapshot-id>/python-sbom/?application=<exact-id>` pins the
export to an immutable received report, including historical or partial reports.
It never contacts a producer or upstream service. Writer tokens cannot download
it; a normal reader session is required. Missing snapshots return 404; missing,
ambiguous or malformed Python/application evidence returns 422.

The export includes **all** reported Python distributions, normalized PyPI package
URLs, original installed metadata (including declared requirements and license
text) as properties, source report digest, snapshot ID, collector and observation
time. Serial identity is deterministic for this export format and exact evidence.
`EXPORT_VERSION` identifies the export contract in tool provenance and serial
identity; bump it when changing the exported meaning or structure.
Git/coverage/runtime evidence is retained as source properties. Application and
category failure states remain explicit even if their Python sub-probe succeeded.

This is an **incomplete, package-only SBOM**. It does not establish artifact
identity, file hashes, active dependency edges, license compliance, update status
or vulnerability status. OS/native/frontend/container contents are outside its
scope. Requirement extras/markers are not evaluated; declared licenses are not
promoted to validated SPDX licenses. No empty dependency graph is invented.
Duplicate normalized package names are rejected rather than merged. An empty
successful package list produces an explicitly incomplete empty package inventory.
Download time does not refresh the observation. Exact source JSON remains available.

Export code uses the standard library only. Tests validate documents with the
CycloneDX project's strict 1.6 JSON schema validator (a development dependency):
<https://cyclonedx-python-library.readthedocs.io/en/stable/autoapi/cyclonedx/validation/json/>.

## Recorded software-health observations

An application's optional `software_health` subprobe carries an allowlisted local
observation from the existing software-live monitor. The view shows the source
observation time, OS support/PostgreSQL/Traefik verdicts and versions, and APT
security-update counts. These are recorded results, not a receiver-side query.
The receiver never contacts producers or monitoring endpoints.

Host identity, schema, timestamp and nested display shapes are checked. An absent,
malformed, future or already-stale-at-collection source is unavailable. An
observation older than 30 minutes when viewed has an explicit historical banner;
weekly inventory collection does not make monitoring current. Unknown APT status
or stale indexes yields an unknown count, never zero. A known count only concerns
the distribution security repositories observed by that monitor, not all software.
Source errors and inventory coverage gaps remain separate from recorded warning
verdicts; a successfully read warning is still a warning. The report download keeps
the exact underlying observation and its timestamp.
