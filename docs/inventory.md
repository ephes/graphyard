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
category. An empty **successful** category records actual absence. Partial application
probes currently mark the entire applications category failed in the pilot emitter;
per-application preservation is future refinement. Unsupported categories and
coverage gaps stay visible, not inferred as fully inventoried.

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
