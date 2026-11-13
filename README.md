# Graphyard

Graphyard is a Django-based metrics platform MVP.

It ingests metrics into InfluxDB with canonical subject/source/collector dimensions, evaluates derived conditions for Nyxmon polling, and provides a small host/service index with Grafana links.

## Software inventory pilot

See [push inventory](docs/inventory.md) for host-bound reporting, private inventory
views and rollout prerequisites. Inventory admission and credential rotation
serialize on the same registered host row; simultaneous snapshot retries preserve
one immutable report, with SQLite contention returned as retryable HTTP 503.
Host latest and category pointers order reports by observation time, receive time,
then row ID; failed categories keep their last successful evidence. Fleet summaries
batch host/category pointers and reports, decoding each referenced report once.
Summary, per-host and version-page evidence is materialized in one short database snapshot, so
concurrent ingestion/pruning cannot mix generations; rendering holds no transaction.
SQLite's configured `BEGIN IMMEDIATE` briefly reserves the database writer during
these reads; competing ingests may need their existing retry on HTTP 503.
PostgreSQL readers own an outer repeatable-read transaction; do not wrap these
projection functions in an application transaction. This pilot does not make remote logins or install
producer schedules.

## MVP Scope

- `POST /v1/metrics` authenticated ingest (bearer token)
  - accepts JSON array payloads, object-with-`metrics` arrays, and NDJSON batches
  - rejects duplicate object keys at every nesting level and integers beyond the
    Python decoder limit with bounded HTTP 400 parse errors, before sink or token
    usage/registry writes. Duplicate diagnostics give the object member position,
    never the key or value; NDJSON rejection counters include all nonempty lines.
    Repeated records in arrays remain supported.
- `GET /v1/conditions`
- `GET /v1/conditions/<id>`
- `GET /v1/health`
- Long-running agent command (`start_agent`) for scheduling
- Generic metric collection specs (`MetricCollectionSpec`) in Django admin
- One-shot condition evaluation command (`evaluate_conditions`)
- One-shot disk-usage condition seed command (`seed_disk_usage_condition`)
- Django admin for tokens, conditions, host/service/subject registry
- Minimal authenticated host/service index UI
- App login page at `/login/` (separate from Django admin URL)

## Project Layout

- `src/django/` Django project wrapper (`manage.py`, `config.settings.*`)
- `src/graphyard/` application code (models, views, Influx boundary, commands)
- `deploy/systemd/` production service units
- `docs/` integration notes

## Local Setup

1. Install dependencies:

```bash
uv sync
```

2. Configure environment:

```bash
cp src/django/.env.example src/django/.env
```

3. Run migrations and create an admin user:

```bash
just manage migrate
just manage createsuperuser
```

4. Start the local development stack (Django + agent + InfluxDB + Grafana):

```bash
just dev
```

## Deployment Shorthand

For production-style rollout through ops-control:

```bash
just deploy
```

This delegates to `ops-control` and runs `just deploy graphyard <host>`.
Default `OPS_CONTROL` path is `/Users/jochen/workspaces/ws-ops-misc/ops-control`.
Set `OPS_CONTROL` and/or `HOST` env vars to override defaults.
If `PROJECTS_ROOT` is unset or points to a location without a `graphyard/` checkout,
the deploy wrapper falls back to the parent directory of the current Graphyard repository.

## Procfile Dev Stack (Nyxmon-style)

Graphyard includes a root `Procfile` compatible with `honcho`, similar to Nyxmon.

Defined processes:

- `django` (Django dev server)
- `agent` (long-running scheduler loop)
- `influxdb` (local `influxd` or `influxdb3`, data under `.dev/`)
- `grafana` (Docker container on `127.0.0.1:3000`, data under `.dev/grafana`)

If `influxd` is missing on macOS:

```bash
brew install influxdb
```

On a fresh local InfluxDB 2 setup, complete one-time setup and copy the created token/org/bucket into `src/django/.env`.
With local `influxdb3` started by Procfile (`--without-auth`), the defaults in `.env.example` are sufficient for development.
`INFLUX_API_MODE=auto` (default) lets Graphyard query with Flux for InfluxDB 2 and automatically switch to SQL for InfluxDB 3 when needed in development.
For production, prefer `INFLUX_API_MODE=v2` to keep query behavior pinned to the current InfluxDB v2 baseline.

For Grafana in the Procfile stack:

- `just dev` starts Grafana via Docker (`grafana/grafana-oss`)
- URL: `http://127.0.0.1:3000`
- default login: `admin` / `admin`
- dev startup resets Grafana admin password to `admin` for deterministic local login
- datasource `Graphyard InfluxDB` is auto-provisioned
- dashboard folders are auto-provisioned from filesystem structure:
  - folder `overview` with dashboard title `Graphyard Overview`
  - folder `host-infrastructure` with dashboard titles `Fractal`, `Macmini`, `Marina`, `Production`, and `Staging`
  - folder `room-climate` with dashboard title `Graphyard Room Climate`
  - folder `device-network` with dashboard titles `FRITZ!Box 7590 AX` and `USW Pro XG 8 PoE`
  - folder `service-http` with dashboard title `Graphyard HTTP Page Probes`
- first run may take longer because Docker pulls the image

## Ingest Token Workflow (Manual Rotation)

Create a token:

```bash
just manage create_ingest_token --name macmini
```

Rotate a token for a host/service name:

```bash
just manage create_ingest_token --name macmini --rotate
```

Metric ingest revalidates the current token (credential, enablement, scope and
binding) after parsing, under a database write reservation held until the Influx
call returns. Revocation, rotation and admin/command scope or binding updates
serialize on that token row: a successful change completes after any previously
admitted write. A change committed before admission prevents the sink call.
On SQLite this holds the **database-wide writer lock** for the entire bounded
Influx call (up to 30 seconds): ingests for other tokens, inventory, scheduling
and heartbeat writers also wait. This serialization trades write concurrency for
revocation ordering; PostgreSQL locks only the token row. SQLite contention may
fail with a retryable 503; a failed administrative update has not revoked the
credential. Admission database failures return fixed-text 503 without attempting
a heartbeat write or misreporting a sink failure. This transaction orders admission only: a remote
write cannot be rolled back if its response or later bookkeeping fails.

Both credential issuance commands (`create_ingest_token` and
`create_inventory_credential`) accept `--database ALIAS`. Without it they use
the write router; inventory issuance refuses split write aliases before reading
any rows. Locks, retirement, related enrollment writes and replacement creation
use one selected transaction. Plaintext is printed only after its commit.

Rotation is atomic: either the new token exists and the previous one is revoked, or the command
fails and nothing changed, so the previous token stays valid. Token names are unique, so the
previous row is kept as history under the name `<name> (retired #<id>)` (revoked, with its
`revoked_at` and `last_used_at`). Creating a token for a name whose token was already revoked
retires that row the same way; `--rotate` is only needed while the existing token is still enabled.

Revoke by id or name:

```bash
just manage revoke_ingest_token --id 1
just manage revoke_ingest_token --name macmini
```

### Single subject/host binding and rollout

Each ingest token now has an optional `bound_subject_id` (one canonical subject/host
identifier, no wildcards). A bound token may write only points whose normalized
`subject_id` **and** `collector_host` equal that binding. An explicit legacy `host`
must also match, including on non-host subjects. Names use the existing subject
normalization (`Edge.LAN` becomes `edge_lan`). These checks apply to every subject
type, so producers reporting distinct service/sensor/device subjects need separate
tokens and matching collector identities before binding. Existing multi-host scopes
remain additional restrictions; setting a binding does not widen them.

The whole JSON/NDJSON batch is checked before writing. A binding mismatch or a
service already registered to another host returns a clear `403`, writes no metrics,
and changes no registry or usage rows. Bound ingests cannot take over foreign services,
including when ownership changes after validation. Unbound tokens keep the current
behavior while `GRAPHYARD_REQUIRE_BOUND_INGEST_TOKENS=false` (the default). Setting it
to `true` rejects all unbound tokens with `403`, even those with a host scope.
Accepted batches update the host, subject and service registry once per distinct row,
in one transaction. `last_seen_at` moves at most once a minute, so steady ingest does
not write registry rows. Bound tokens write services only under the same
unowned-or-own-host condition.
Binding rejections log `metrics_ingest_rejected category=binding` with the token id
and point index (when known), and set a fixed-text warning heartbeat without host,
service or token names. Check web logs during rollout; the global heartbeat can later
return to OK when another producer succeeds.
Bindings are editable and visible in Django admin; an empty admin binding means unbound.
Rotation/re-creation of a revoked token retains its binding, even with `--unrestricted`
(which clears only the older multi-host scope).

Roll out in this order, **after the owner reviews the token-to-host mapping**:

1. Deploy the code and run migrations, including `0012_ingest_token_subject_binding`,
   with `GRAPHYARD_REQUIRE_BOUND_INGEST_TOKENS=false`. Existing tokens migrate to NULL
   bindings; no bindings are inferred by migration.
2. Run the read-only report: `just manage report_ingest_token_bindings --days 30`.
   It lists every token by **name only**, including disabled tokens, recent identifiers,
   last-seen times and a proposed binding only when exactly one identifier was observed.
   It never prints a credential or hash, and identifies tokens by stored name only.
   Retired names include their existing retirement suffix; their usage stays with
   the disabled row, not the new token. A fresh rotated token needs new observations.
3. Review each proposal against the intended producer/host mapping, then assign:
   `just manage bind_ingest_token --name macmini --subject macmini`.
   This command sets one exact binding by token name without printing credentials.
   Resolve multiple-identifier producers and review tokens with no observations;
   proposals are not automatic assignments. In admin, review the ServiceRegistry
   host mapping for every `service` tag the producers send. Service identifiers are
   globally unique: producers on different hosts must use distinct service names
   (for example `edge_mail` and `core_mail`). A conflicting service rejects the whole
   batch with an indexed 403, including unrelated points. Resolve conflicts and
   host-less service ownership before binding; a bound producer can claim an
   unregistered or host-less service. Legacy unbound producers and internal
   collectors keep their existing re-parenting behavior and can move a service away
   from a bound host, causing subsequent 403s. Coordinate their service names too;
   bindings do not reserve service names.
4. Once every active producer has a binding and has successfully ingested, set
   `GRAPHYARD_REQUIRE_BOUND_INGEST_TOKENS=true` and restart the web app.

Report data comes from the existing `ingest_token_host_usage.last_seen_at` table,
which records normalized **subject_id and collector_host** for accepted ingests since
host-usage recording was introduced. Host/subject registry last-seen rows alone cannot
attribute writes to tokens. Usage updates at most hourly, retains at most 500 identifiers
per token, and may miss writes during SQLite contention; there is no historical backfill.
A single observed identifier is evidence to review, not proof of exclusive ownership.
Wait for representative writes if the report is empty or incomplete.

For rollback, disable the requirement setting first. Clear individual bindings in admin
only after reviewing the resulting access. Migration 0012 is reversible and preserves
tokens, hashes and older scopes, but reversing it discards bindings. Older code cannot
enforce the new bindings. This feature applies to bearer metric ingest; inventory has
its own credentials and internal scheduled collectors do not use ingest tokens.

### Host scope (optional)

By default an ingest token may write metrics for any host. A token can be limited to the hosts it
is meant for:

```bash
just manage create_ingest_token --name edge --host-scope edge
just manage create_ingest_token --name webfarm --host-scope 'web-*,lb1'
```

- A scope is a list of host identifiers: exact names, or a prefix followed by one trailing `*`.
  Nothing else is accepted (letters, digits, `.`, `_`, `-`; at most 64 entries). `--host-scope` is
  comma-separated and may be repeated.
- An empty scope means unrestricted. Tokens created before this feature have an empty scope and
  keep working unchanged.
- For a scoped token every point is checked on **both** `subject_id` and `collector_host`, for every
  `subject_type`. A token that also writes service, sensor or network-device subjects needs those
  subject ids in its scope as well.
- Names are compared the way `subject_id` is normalized (lowercase, other characters folded to
  `_`), so `mac-mini.lan` and `mac_mini_lan` are the same host. `web-*` matches `web-1` and
  `web_1` but not `web`; `web*` also matches `web` and `webserver`.
- A request with any point outside the scope is rejected with `403`; nothing is written and no
  registry row is touched. The rejection is logged as `metrics_ingest_rejected category=scope` and
  sets the `metric_ingest` heartbeat to warning (without host names).
- `--rotate`, and re-creating a revoked name, keep the scope of the replaced token. Pass
  `--host-scope` to change it or `--unrestricted` to drop it. The scope can also be edited in the
  Django admin (ingest token → host scope, one entry per line or comma-separated).
- A stored scope that is not a valid list (damaged by hand, for example) allows nothing. Rotating
  or re-creating such a token fails and changes nothing until `--host-scope` or `--unrestricted`
  is given.

To choose scopes, see what each token really writes:

```bash
just manage report_ingest_token_hosts --days 30
```

The report is read-only. `--days` supports 1–36500 days (default 30); values
outside that range or the datetime domain fail before database reads.
Per enabled token (`--include-disabled` adds revoked ones) it lists the
host identifiers written in the period, marks those outside the token's current scope, and prints a
matching `--host-scope` value. For a token with more than 64 identifiers it prints no suggestion,
because a scope holds at most 64 entries: choose prefixes (for example `web-*`) or split the token.
Usage is recorded at ingest from this release onwards (table
`ingest_token_host_usage`, `last_seen` updated at most hourly, at most 500 identifiers per token),
so let it collect for a while before scoping existing tokens.

The scope does not limit metric names or `service` tags, and it does not apply to the
inventory API, which has its own per-host credentials.

Graphyard stores token hashes only.
Ingest tokens use a fast SHA-256 digest with the `graphyard-sha256$...` prefix (constant-time compare), and legacy hashes are upgraded on first successful use.
Hash upgrades are one-way; if you revert to code that only understands Django password hash formats, rotate ingest tokens.

Ingest authentication looks up the SHA-256 digest by equality before trying only
enabled legacy password-hash rows. Invalid credentials still cost one password
check per legacy row until those rows are upgraded or rotated. Run
`just manage check --database default` to report `graphyard.W001` while enabled
legacy rows remain. The check tolerates an unmigrated database during setup.

## Metric Collection Specs

Metric collection is configured in Django admin via `MetricCollectionSpec`.
For production, specs can also be provisioned idempotently from a JSON file as
create/update reconciliation by spec name:

```bash
just manage apply_metric_collection_specs --file /etc/graphyard/metric-collection-specs.json
```

The file may be either a JSON list of spec objects or an object with a
`metric_collection_specs` list. Add `--prune` to delete existing rows whose
`name` is omitted from the file:

```bash
just manage apply_metric_collection_specs --file /etc/graphyard/metric-collection-specs.json --prune
```

Files are opened once without blocking and must resolve to a regular file;
symlinks to regular files remain supported. Both declared size and actual reads
are capped at 2 MiB (an exact-limit valid file is accepted); parsed JSON paths
may traverse at most 64 edges from the document root. Unreadable, nonregular,
oversized, invalid UTF-8/JSON or decoder-depth-exhausting inputs fail before any
database query, with a bounded command error naming the path and omitting content.
Duplicate JSON members at every object level (including escaped-equivalent names
and identical values) are refused with only the member position. Repeated array
records still undergo the ordinary duplicate-spec-name check. Integers beyond
Python's decoder digit limit produce a controlled command error before model access.

Apply compares the captured definition and update timestamp before writing.
Concurrent edits, duplicate creates or recognized database contention produce a
retry/conflict command error. Other database failures report a bounded storage
error category. Both omit private database detail and success output; the entire
apply, including prune, rolls back.
Prune considers only rows captured at the start of the transaction; concurrent new
rows are left for the next explicit reconciliation. An operator retry loads current
definitions before applying the desired file again.

Prune is opt-in and keyed only by `name`. Without `--prune`, unspecified specs
are left untouched. With `--prune`, the command refuses an empty desired spec
set so a broken render cannot wipe every spec row in one shot.

Typed config values are validated on save (Django admin and
`apply_metric_collection_specs`, which aborts on the first invalid spec in the
file, including specs whose stored row is otherwise unchanged):

- Timeouts must be finite and no greater than 86400 seconds (one day), a portable transport/wait limit. The derived three-request total budget is validated too.
- `request_timeout_seconds` must be a number greater than 0 (numeric strings such as `"15"` are accepted; `"5s"` or `null` are not).
- `total_timeout_seconds` (HTTP page probe, HTTP JSON metric, Home Assistant and UniFi) must be a number greater than 0.
- `max_body_bytes` must be a positive integer.
- `verify_tls` and `follow_redirects` must be JSON booleans. Strings such as
  `"false"` are rejected instead of silently meaning `true`.

Each spec runs in isolation. If a spec fails at run time, for example because it
was saved before validation existed, it gets `last_status=critical` with the
error category in `last_error`. Its `next_run_time` still advances and the remaining specs
keep running. The run is reflected in the `metric_collectors` heartbeat on
`/v1/health`: `warning` when some specs failed, `critical` only when all did.

Collector exception messages are normalized before persistence in both runtimes:
`collector failure: <allowlisted type>` (unknown types become `Exception`), or
`collector HTTP error: status=<code>` for HTTP status exceptions. Required-field
and typed configuration validation messages stay useful. Body limits and total
deadlines use `_PageProbeLimitExceeded` and `_PageProbeDeadlineExceeded` types.
No exception messages, tracebacks, response bodies, URL queries or userinfo enter
these failure records or run summaries. Abandoned-worker warnings use fixed text. Python suppresses HTTP client's request
logs; Rust HTTP response logs contain only method and numeric status.
Previously stored errors are not rewritten by this change.

Condition failures emit one ERROR event per condition in the web/agent logs:
`Condition failure condition_id=<id> stage=evaluation|publication category=<type>`
with `failure_published` true only when the fallback critical result was actually
saved. Deleted, disabled, reconfigured or newer/tied condition results report
false, as do storage errors.
Python also exposes ID/stage/category as log record fields. Raw query/SQL text,
exception messages and model representations are excluded. Failed fallback writes
do not stop later conditions; a heartbeat write failure still fails the whole run.
Persisted condition failure messages use the same safe category.

Due-only scheduler runs claim each selected slot with a conditional
`next_run_time` update **before** any collection. The claim also compares the
loaded definition (name, type, interval and config): if the spec was edited
between load and claim, no claim is taken, no request uses the old URL or
credentials, and the spec stays due for the next run with its current
definition. Another runner with the same
snapshot skips a lost claim. In both agents, claims last `min(max(3, interval_seconds), 300)` seconds and
renew every one third of that duration through collection, sink writes and
registry publication. Renewal and completion compare the exact claim timestamp
and captured definition, so an old owner cannot extend a replacement. Losing
renewal ownership cancels the outstanding collector; a store failure fails the
run. A crash or dropped run stops renewal, making the row eligible at its last
persisted lease expiry. Successful and failed attempts schedule the next regular
interval from their original claim start. No database lock spans network I/O.
A process suspended beyond its lease, or database contention that delays renewal
past expiry, can still lose ownership; cancellation cannot
recall bytes already accepted by an external sink. The Python agent renews from a
background thread while its synchronous collector runs; it cannot interrupt the
collector mid-request, so after a lost renewal it refuses the collector's sink
and registry publication and skips completion. Do not run both schedulers for the same specs. Explicit non-due runs
(including manual one-shot collection) continue to run every enabled spec,
regardless of its due time, but completion still requires the captured definition and schedule. Deleted, disabled, edited or
superseded collections contribute no counts, errors or heartbeat evidence; a run
with no accepted completions leaves the heartbeat untouched. Execution diagnostics
remain available, including fixed `Collection superseded spec_id=<id>` log events.
Metrics already sent during collection cannot be recalled by this publication fence.

Current supported `spec_type`:

- `home_assistant_sensor`
- `home_assistant_env_scan`
- `http_json_metric`
- `http_page_probe`
- `unifi_device_traffic`

Collectors emit canonical dimensions:

- `subject_type`, `subject_id`
- `source_system`, `source_instance`, optional `source_entity_id`
- `collector_service`, `collector_host`
- compatibility tags: `host` (host subjects only), optional `service`

Example `config` JSON for a Home Assistant sensor spec:

```json
{
  "base_url": "http://homeassistant.local:8123",
  "access_token": "replace-me",
  "entity_id": "sensor.living_room_humidity",
  "metric_name": "ha.sensor.living_room_humidity",
  "source_instance": "ha-main",
  "collector_service": "graphyard-agent",
  "collector_host": "macmini",
  "subject_mapping": {
    "default": {
      "subject_type": "environment_sensor",
      "subject_id_from": "entity_name_slug"
    }
  },
  "host_id": "homeassistant",
  "service_id": "homeassistant",
  "request_timeout_seconds": 10,
  "verify_tls": true
}
```

Example `config` JSON for a Home Assistant environment scan spec
(single Home Assistant API call that collects all matching temperature/humidity sensors,
plus optional metric remapping for selected infrastructure traffic sensors):

```json
{
  "base_url": "https://homeassistant.example.com",
  "access_token": "replace-me",
  "host_id": "homeassistant",
  "service_id": "homeassistant",
  "source_instance": "ha-main",
  "collector_service": "graphyard-agent",
  "collector_host": "macmini",
  "subject_mapping": {
    "default": {
      "subject_type": "environment_sensor",
      "subject_id_from": "entity_name_slug"
    },
    "rules": [
      {
        "match_entity_id_regex": "^sensor\\.fritz_box_.*_(cpu_temperature|upload_throughput|download_throughput)$",
        "subject_type": "network_device",
        "subject_id_template": "fritz_box_7590_ax"
      },
      {
        "match_entity_id_regex": "^sensor\\.usw_pro_xg_8_poe_temperature$",
        "subject_type": "network_device",
        "subject_id_template": "usw_pro_xg_8_poe"
      }
    ]
  },
  "metric_mapping": {
    "rules": [
      {
        "match_entity_id_regex": "^sensor\\.fritz_box_.*_download_throughput$",
        "metric_name": "network_device.network_receive_bytes_per_second",
        "value_multiplier": 1000,
        "extra_tags": {
          "traffic_direction": "receive",
          "traffic_scope": "wan"
        }
      },
      {
        "match_entity_id_regex": "^sensor\\.fritz_box_.*_upload_throughput$",
        "metric_name": "network_device.network_transmit_bytes_per_second",
        "value_multiplier": 1000,
        "extra_tags": {
          "traffic_direction": "transmit",
          "traffic_scope": "wan"
        }
      }
    ]
  },
  "metric_prefix": "ha.",
  "include_device_classes": ["temperature", "humidity"],
  "entity_id_regex": "(^sensor\\.fritz_box_.*_(upload_throughput|download_throughput)$|temperature|humidity)",
  "request_timeout_seconds": 10,
  "verify_tls": true
}
```

Both Home Assistant collectors stream decoded response bytes with `max_body_bytes`
(default `10485760`, 10 MiB) and `total_timeout_seconds` (default: three request
timeouts, normally 30 seconds). Set these in either HA config when a legitimate
state inventory needs more room or time; both limits must be positive and total
budgets cannot exceed 86400 seconds. The deadline includes connection setup,
headers, body reads and JSON decoding. Budget exhaustion closes the response,
marks the spec critical and writes no points; the next spec still runs. HA shares
HTTP JSON/page probe cancellation and the four abandoned-worker limit below,
so the agent returns within the deadline plus about a second.

Example `config` JSON for an HTTP JSON metric spec:

```json
{
  "url": "https://example.internal/health",
  "metric_path": "$.queue.depth",
  "metric_name": "service.queue_depth",
  "host_id": "macmini",
  "service_id": "mail",
  "source_entity_id": "queue:depth",
  "request_timeout_seconds": 10,
  "verify_tls": true,
  "tags": {
    "source": "mail-health"
  }
}
```

HTTP JSON collectors stream decoded response bytes with `max_body_bytes` (default
`10485760`) and a monotonic `total_timeout_seconds` deadline (default: three
request timeouts), including connection setup and JSON parsing. Authentication and
TLS settings are preserved. Budget exhaustion marks the collector critical and
writes no metric. They share the page probe cancellation and four abandoned-worker
limit described below; the agent returns within the deadline plus about a second.

Example `config` JSON for an HTTP page probe spec:

```json
{
  "url": "https://python-podcast.de/show/",
  "subject_id": "python_podcast_show",
  "service_id": "python_podcast",
  "source_system": "http_probe",
  "source_instance": "public_web",
  "collector_service": "graphyard-agent",
  "collector_host": "macmini",
  "request_timeout_seconds": 15,
  "total_timeout_seconds": 45,
  "follow_redirects": true,
  "verify_tls": true
}
```

This collector uses `GET` and writes bounded page-probe metrics for the target:

- `service.http_page_ttfb_seconds`
- `service.http_page_total_seconds`
- `service.http_page_status_code`
- `service.http_page_success`
- `service.http_page_redirect_count`

`subject_type` is fixed to `service` for this collector type.
`service.http_page_success` is `1` for final HTTP `2xx`/`3xx` responses and `0` otherwise.
When `follow_redirects=true`, `service.http_page_ttfb_seconds` includes redirect time before the final response.
Timeouts and transport errors emit `status_code=0` and `success=0`, while keeping the agent loop alive.
`request_timeout_seconds` is httpx's per-operation timeout: it bounds each
connect, write and read, not the whole probe. The response body is streamed and
capped by `max_body_bytes` (default `10485760`), counted across redirect hops;
exceeding it records the same `status_code=0`/`success=0` failure.
`total_timeout_seconds` (default: three times `request_timeout_seconds`) is a
wall-clock deadline for the whole probe: DNS, connect, TLS, headers, body and
redirects. An endpoint that drips bytes resets the per-operation timeout on every
read, so the probe runs in a worker thread; at the deadline the agent cancels it
(shutting down the probe's open sockets, closing its HTTP client, and making a
connection that only completes later, for example after a slow DNS lookup, close
itself before a request is sent), records the same `status_code=0`/`success=0`
failure with a `total_timeout_seconds` error, and moves on to the next spec. The
agent loop is held for at most the deadline plus about a second. A worker that
cannot be interrupted at once (a blocking DNS lookup) finishes in the background;
while four such workers are still running, further page probes fail immediately
with the same failure metrics instead of starting another thread.

Example `config` JSON for a UniFi device traffic spec:

```json
{
  "base_url": "https://127.0.0.1:8443",
  "username": "homeassistant",
  "password": "replace-me",
  "site_id": "default",
  "device_name": "USW Pro XG 8 PoE",
  "device_mac": "70:49:a2:21:53:45",
  "interface_selector": "uplink",
  "subject_type": "network_device",
  "subject_id": "usw_pro_xg_8_poe",
  "source_system": "unifi",
  "source_instance": "unifi-macmini",
  "collector_service": "graphyard-agent",
  "collector_host": "macmini",
  "service_id": "unifi",
  "request_timeout_seconds": 10,
  "verify_tls": false
}
```

Device selection: when `device_mac` is set it is authoritative (case and
surrounding whitespace ignored) and `device_name` is not consulted; otherwise
`device_name` must match exactly one device. When several devices match (for
example two devices with the same display name, listed in any order), the spec
records the warning `configured UniFi device is ambiguous; set a unique
config.device_mac` and writes no points, instead of using whichever device the
controller listed first. Migration: add the device's `device_mac` to specs that
select by name; a configured MAC no longer falls back to a matching name.

By default, UniFi auth uses `auth_mode=auto`: Graphyard first tries the classic
Network Application flow (`POST /api/login`, then `/api/s/<site>/stat/device`)
and falls back to the UniFi OS flow (`POST /api/auth/login`, then
`/proxy/network/api/s/<site>/stat/device`). Set `auth_mode` to `legacy` or
`unifi_os` only when an installation needs to pin one flow.
Login/device refusals record `collector failure: ValueError`; response bodies
and URLs are never retained as failure messages.

Collected points are written by the long-running agent to InfluxDB and update host/service registry metadata.
`host_id`/`service_id` remain supported for migration compatibility.
Write-path behavior is partial-success: invalid points are rejected per-point, while valid points in the
same batch are still written.
For `POST /v1/metrics`, request-level payload validation is fail-fast: if any point fails ingest payload
parsing/normalization before write, the request is rejected with `400` and no points are written.
Metric ingest timestamps must fall within the inclusive window of now minus
seven days through now plus five minutes. Configure
`GRAPHYARD_INGEST_MAX_AGE_SECONDS` (604800) and
`GRAPHYARD_INGEST_MAX_FUTURE_SECONDS` (300), in seconds, for deliberate backfills
or collector clock tolerance. Both settings accept 0 through 3153600000 (100 years). Collectors here emit current timestamps and have
no metrics retry buffer; external buffered senders must fit this window.
Each point accepts at most 32 supplied tags. Host, service and subject identifiers
are limited to their registry model fields' maximum lengths (currently 128
characters); `source_system` is limited to 64. Limits also apply after normalization
and legacy fallbacks. A service name longer than 64 characters needs an explicit
`source_system` within its limit, since the default uses the service name.
Other metric/dimension strings, tag keys and stringified tag values are limited
to 256 characters.
An invalid point reports its index and rejects the whole request before writes.

Malformed bodies get the same bounded rejection instead of a server error: a body that is not valid
UTF-8 or whose JSON is nested too deeply to parse returns `400`, and a body larger than Django's
`DATA_UPLOAD_MAX_MEMORY_SIZE` (2.5 MB by default) returns `413`. Each response is JSON with a fixed
`error` message, counts as one parse rejection in the `metrics_ingest_rejected` log line and the
`metric_ingest` heartbeat, and writes nothing.
A token with a host scope (see "Host scope" above) gets `403` when any point names a host outside
its scope; that request writes nothing either.
Metric values must be finite numbers: `NaN`, `Infinity`, `-Infinity` (as JSON literals or strings
such as `"nan"`/`"inf"`) are rejected as invalid values, since InfluxDB cannot store them. Collection
specs skip such values like other non-numeric readings, and they are never counted as ingested nor
mark a subject as seen.

Security note: metric collection `config` values are stored in SQLite and may contain secrets
(for example `access_token`, `bearer_token`, `basic_password`). Django admin masks known secret
keys for existing specs, but secrets are still plaintext at rest in the DB for MVP.

## Response privacy

Authenticated `GET /v1/conditions`, `GET /v1/conditions/<id>` and the owner
host/service index use Django's private, no-store, no-cache response policy
with immediate expiry. Browsers and intermediary caches must not store those
authenticated representations or reuse them after logout. This HTTP policy
cannot erase copies saved separately by a user or a page already displayed.
Anonymous condition responses retain the public id, name, status and evaluation
time (plus list summary); messages, configuration and values remain private.
Logout restores this public projection and the index requires login.

## Condition Evaluation

Create conditions in Django admin (`ConditionDefinition`) using:

- metric name
- optional `subject_type_filter` / `subject_id_filter`
- legacy `host_filter` (host-subject compatibility)
- operator (`gt|gte|lt|lte`)
- warning/critical thresholds (finite numbers; either threshold may be empty)
- lookback window and breach duration: each 1–10,080 whole minutes (one week)
- optional staleness allowance (`stale_after_seconds`) and `alert_when_stale`
  (see "Intermittent sources" below)

Both Python and Rust reject out-of-range stored durations before querying Influx.
An invalid historical definition stays enabled and unchanged, reports critical
`evaluation error: ValueError`, preserves its last value, and counts as a failed
evaluation. Review and correct its window/breach fields explicitly in admin or
with the seed command; migration does not rewrite definitions. Breach duration
may exceed the configured lookback window: the query still expands to include
the breach interval plus its carry-forward anchor. The one-week ceiling bounds
these two configured intervals; the existing staleness allowance is unchanged.

Run evaluator once:

```bash
just manage evaluate_conditions
```

The command evaluates independent conditions even if one fails, then prints the
summary. It exits with status 1 if any evaluation failed and 0 otherwise, so
schedulers and operators can detect evaluation errors. A condition reporting
warning or critical after a successful evaluation is not a command failure.

Seed (or update) a practical host disk-usage condition:

```bash
just manage seed_disk_usage_condition --host macmini --mountpoint /
```

Seed ratios must be finite and between 0 and 1. For `gt`/`gte`, warning must
be at most critical (defaults: 0.8/0.9). For `lt`/`lte`, warning must be at
least critical, for example `--operator lte --warning-threshold 0.2
--critical-threshold 0.1`; 0.15 then reports warning.
Seeding validates before saving and creates/updates atomically by unique name.
Validation or database save failures leave new and existing conditions unchanged.

Defaults: warning `0.80`, critical `0.90`, operator `gte`, metric `host.filesystem_used_ratio`.

For a host that is not always online (for example a laptop that sleeps), add
`--stale-after-seconds N` and/or `--intermittent`:

```bash
just manage seed_disk_usage_condition --host atlas --stale-after-seconds 43200 --intermittent
```

Re-running the command without these flags resets them to the defaults, like
every other field it seeds.

### Intermittent sources

A condition turns `warning` when its newest sample is older than
`GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS` (default 600 s), or when the
condition window has no samples at all. Two per-condition fields (admin,
`seed_disk_usage_condition`, and the `config` of `GET /v1/conditions/<id>`)
relax this for intermittent sources:

- `stale_after_seconds`: the age the newest sample may reach before the
  condition turns `warning`. Empty means the global setting. The evaluator
  also looks back this far, so a sample within the allowance is still found
  and evaluated against the thresholds.
- `alert_when_stale` (default on): when off, stale or missing data reports
  `ok` with a message ending in `not alerting (intermittent)` instead of
  `warning`. Fresh data is still evaluated, so a real breach still alerts.

Conditions that set neither field behave exactly as before.

Influx query samples must be finite numbers on both Flux and SQL backends. A
NaN, infinity, invalid numeric value, non-object SQL row, or missing/invalid
time or value rejects the whole query, including when
other series are valid or the condition permits stale data. The evaluation run
marks the condition `critical` with an evaluation error; the previous
`last_value` remains and `last_evaluated` records the failed attempt, rather
than presenting an older valid sample as fresh evidence.

InfluxDB 3 SQL requests require identity encoding; any other response encoding
is rejected before body iteration or decompression. Responses are streamed with
a fixed 10 MiB body limit and a 30-second monotonic total deadline covering connection,
headers, and body reads. OS DNS resolution is an exception: cancellation stops
the request, but Python's event-loop cleanup may wait for the resolver thread to
finish. JSON decoding is size-bounded and its elapsed time is
checked before accepting the result. These limits are independent of
`INFLUX_TIMEOUT_MS`, the per-operation transport timeout. Limit exhaustion closes
the response and rejects the whole query with an evaluation error as above.
Narrow the condition filters or query window if a legitimate result exceeds the
limit; partial results are never evaluated.

Example supported condition: humidity above threshold for N minutes.

A condition breaches when its value is past the threshold for the whole breach
duration (`breach_minutes`). Each sample's value counts until the next sample,
so the last sample before the breach window covers the window start: the
condition breaches when that sample and every sample inside the window are past
the threshold. This works for series sampled every few minutes as well as every
minute; with a breach duration shorter than the sampling interval, the carried
sample alone decides. The carried sample may be at most `CONDITION_DATA_STALE_WARNING_SECONDS`
(default 600 s) older than the window start; without one (for example a series
that started recently), the first sample in the window must lie within one
minute of its start. An anchor that cannot reach the first in-window sample
within its carry lifetime is treated as absent for this initial grace. Every gap between samples, including the anchor-to-first
sample gap and the final carry to evaluation time, must fit that same allowance.
A larger `stale_after_seconds` or `--intermittent` changes freshness alerts only;
it cannot certify a continuous breach across missing data.

Condition results use the attempt start time as `last_evaluated`. A conditional
write rejects an older or equally timed success or failure after another result
has won. Failures advance this watermark while retaining the last observed value;
removed conditions are ignored. Heartbeats still count the attempts and failures
of their own run.

### Conditions that match several series

A condition matches more than one time series when its filters do not pin every
tag, for example a disk condition seeded with `--no-mountpoint-filter`, or a
metric reported by several collectors. Graphyard groups the samples by series
(every tag/dimension except time and value) and evaluates staleness and the
breach duration for each series on its own. The condition takes the worst series
status (`critical` > `warning` > `ok`):

- one series breaching the critical threshold for the full breach window makes the
  condition `critical`, even if other series are healthy;
- one stale series makes the condition at least `warning`, even if other series are fresh;
- the message names the deciding series by the dimensions that differ, for example
  `... [series: mountpoint=/data] (1 of 3 series not OK)`, and `last_value` comes
  from that series. When every series is healthy, `last_value` is the newest
  sample and the message is `Condition is within thresholds (N series)`.

Conditions that match a single series behave exactly as before. A series with no
samples in the condition window at all is not visible to the evaluator; pin the
filters if you need an alert for a series that disappears.

## Agent Runtime (Dev + Production)

Graphyard scheduler tasks run inside one long-lived process (`start_agent`), not per-run Python process spawns.

Useful options:

```bash
just dev
just manage start_agent --run-once
just manage start_agent --metrics-interval 60 --condition-interval 60
just manage start_agent --disable-metrics
just manage start_agent --disable-conditions
```

`start_agent --run-once` runs every enabled loop before exiting. It exits 1 if
any loop reports failed work or raises an exception, and 0 when all enabled
loops succeed. Disabled loops do not affect status; warning-only outcomes are
successful. Continuous mode keeps scheduling after failures. The Rust agent
uses the same contract.


A Rust port of the agent, `graphyard-agent`, lives in `agent-rs/` (contract:
[`agent-rs/PLAN.md`](agent-rs/PLAN.md), build and test:
[`agent-rs/README.md`](agent-rs/README.md), architecture, schema contract,
divergences and operations including the shadow run:
[`docs/agent-rs.md`](docs/agent-rs.md)). It is a drop-in for `start_agent`
with the same flags plus `--db`, `--check-schema`, `--version` and the shadow
mode `--influx-write-file`; its subcommand `graphyard-agent
evaluate-conditions [--condition-id N]` is the port of `evaluate_conditions`
above (same summary lines and exit codes, settings from the process
environment only). The Rust agent has been the production agent on macmini
since 2026-10-23 (release 0.2.0, cutover after a 24 h shadow run); release
0.3.0 replaced 0.2.0 there on 2026-11-12. The Python command above
stays installed as the rollback target until its retirement 30 days later. Release archives of the Rust binary are
built by `.github/workflows/agent-rs-release.yml` from `agent-rs-v<version>`
tags; the first is
[`agent-rs-v0.1.0`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.1.0),
the current one
[`agent-rs-v0.3.0`](https://github.com/ephes/graphyard/releases/tag/agent-rs-v0.3.0)
(the agent fixes of scout rounds 79 to 85; `agent-rs-v0.2.0` added
`evaluate-conditions`).

## Nyxmon Integration Contract

Nyxmon can poll:

- `/v1/health` for pipeline/service health and staleness
- `/v1/conditions` for condition list plus summary counts (`total`, `ok`, `warning`, `critical`)
- `/v1/conditions/<id>` for detailed config/status of one condition

Status values are always `ok|warning|critical`. Heartbeat projection takes the
worse of stored status and age-derived status, preserving the recorded error and
details. Missing success evidence has at least warning severity. Success timestamps
more than 60 seconds in the future also warn with an unknown (`null`) age;
smaller clock skew is tolerated with age zero. Stored critical severity remains.
If the database probe, heartbeat/count reads or session lookup fails, health
returns HTTP 503 with overall/database `critical`, pipeline `warning` with unknown
success/age, and `null` condition counts. It stops database reads immediately,
discards partial evidence and uses a status-only payload if authentication cannot
be established. Database exception text is never included. Other health responses
retain HTTP 200, including stale/critical pipeline evidence.
Python heartbeat throttling suppresses only repeats of the last committed
state per name; transitions and retries after failed/rolled-back writes publish.
The retained Python agent uses this writer too; Rust writes every heartbeat and
needs no throttle correction.

These endpoints need no credential, but unauthenticated callers get a reduced
payload: `/v1/health` returns the overall `status`, `checked_at`, each
component's `status` (pipelines also `last_success` and `age_seconds`) and the
condition counts; `/v1/conditions` and `/v1/conditions/<id>` return `id`,
`name`, `status` and `last_evaluated` per condition plus the `summary` counts.
Messages, error texts, heartbeat details, the InfluxDB instance name, a
condition's `config` and `last_value` can contain internal hosts, URLs and
token names, so they are only included for a logged-in session (`/login/`).

## Production Runtime Pattern (systemd)

Required production entrypoint: `config.wsgi_proxy:application`.
See [web service deployment](docs/deployment.md) for the proxy and login throttle
address contract.

Use units in `deploy/systemd/`:

- `graphyard-web.service` (Django + Granian)
- `graphyard-agent.service` (long-running scheduler loop)

Set environment file based on `deploy/systemd/graphyard.env.example`.
For the SQLite-backed deployment profile, keep web concurrency conservative (`--workers 1`) and
use WAL + busy-timeout settings from `graphyard.env.example`.

## Validation Commands

The uv-managed development toolchain locks filelock 4.2.1 and virtualenv
21.14.10 to address their known advisories in developer tooling. Virtualenv also
requires upgrading the shared runtime dependency `packaging` from 26.0 to 26.3;
the inventory release version-parsing tests cover that usage. Runtime or Rust
exploitation has not been demonstrated. Run `uv sync --frozen` to install the
repaired graph. To rebuild the separately resolved Ruff hook environment with
this virtualenv, use a fresh cache:

```bash
PRE_COMMIT_HOME="$(mktemp -d)" uv run --frozen pre-commit run --all-files
```

Audit the frozen Python dependency graph with the same command CI uses:

```bash
uv run --frozen python scripts/audit_python.py
```

This exports runtime dependencies and all dependency groups (currently `dev`)
from `uv.lock`, excluding the project itself, then runs `pip-audit==2.10.1`
without resolving or installing the exported packages. Export and auditor failures
both fail the command. It needs access to the package index and advisory service.
The audit covers uv-managed pre-commit and its dependencies; Ruff's separately
resolved hook environment is outside this graph. Rust advisories remain covered
by `cargo deny`. The PostgreSQL contract job's separately injected `psycopg`
package is also outside the locked graph.

```bash
just test
just typecheck
just lint
just schema-check
```

`just schema-check` uses frozen dependencies and `config.settings.test` with
in-memory SQLite. It runs these same nonwriting checks as CI without secrets,
owner database access or migration creation:

```bash
uv run --frozen python src/django/manage.py check --settings=config.settings.test
uv run --frozen python src/django/manage.py makemigrations --check --dry-run --settings=config.settings.test
```

`just test`, `just typecheck` and `just lint` also run the Rust agent's checks (`just rs-lint`, `cargo check`,
`just rs-test`); they need the pinned Rust toolchain (`agent-rs/README.md`).

GitHub Actions (`.github/workflows/ci.yml`) runs the same lint, typecheck and test
commands on every push and pull request. The `python` job also runs Django
system and missing-migration checks; the Rust agent has its own `rust` job,
including `cargo deny`. The Python/Rust parity harness runs as `just rs-parity`
in the `parity` job. These checks need no secrets or InfluxDB.

## Influx + Grafana

See `docs/influx_grafana.md` for retention/downsampling notes and Grafana linking guidance.

Migration note: dashboards now filter primarily on canonical `subject_*`/`source_*` tags. Historical
series written before this migration may not have those tags, so old data can be absent from the new
subject-aware panels until fresh points are ingested.

## Login throttling

Required production entrypoint: `config.wsgi_proxy:application`.
See [web service deployment](docs/deployment.md) for the trusted loopback proxy
contract and its REMOTE_ADDR dependency.

Both `/login/` and the Django admin reserve password attempts before verification:
five failed or pending attempts per username/client address, or 50 per address,
in a fixed 15-minute window starting at the first reservation. A failure becomes
one counted failure; success releases only its own pending slots and clears the
pair's completed failures, preserving other concurrent attempts. Address failures
remain until expiry. Backend exceptions release pending slots without counting
a failed password; the exception still propagates. Sync and async authentication
use the same admission boundary. Window generations keep late completions from changing a
replacement window. Admission uses atomic cache add/increment operations; a race
with failure settlement may conservatively refuse an extra request. The throttle
backend owns password verification; the configured ModelBackend remains available
for sessions created before this change and is never a second password attempt. Configure
`GRAPHYARD_LOGIN_FAILURE_LIMIT`, `GRAPHYARD_LOGIN_ADDRESS_FAILURE_LIMIT` and
`GRAPHYARD_LOGIN_FAILURE_WINDOW_SECONDS`; a nonpositive username limit disables
throttling. Blocked attempts show the standard invalid-credentials form response
and do not check the password. Counters use a separate local-memory cache:
limits apply per process and reset on restart. Use a shared Django cache backend
for the `login_throttle` alias if multiple workers must share limits. Supported
backends are local memory and Memcached, whose atomic increments preserve
expiry; other cache backends are not supported. The local cache holds
up to 10000 keys; each window uses a generation pointer and up to two counters,
and exceeding capacity can evict active throttle state. Already locked
attempts do not create new keys. Address limits and window seconds must be
positive; proxy hops must be nonnegative. Invalid settings abort startup.
`GRAPHYARD_LOGIN_THROTTLE_PROXY_HOPS` defaults to 0 (use REMOTE_ADDR without reading forwarded headers in the throttle).
The shipped `config.wsgi_proxy` Granian wrapper already rewrites REMOTE_ADDR
for trusted loopback proxies; keep hops at 0 with that wrapper. With plain
`config.wsgi` behind a proxy that does not rewrite REMOTE_ADDR, hops 0 shares
one address bucket among all clients of the proxy.
Set it to the number of trusted proxy hops only when direct access to the app
is restricted and those proxies append/replace X-Forwarded-For. With one hop,
the rightmost address is used; spoofed entries to its left are ignored.

### Evaluation publication and metric acceptance

Python and Rust condition evaluators publish only while the captured metric,
filters, thresholds, time windows, staleness policy and enabled state still match.
Edits or disabling a condition during an Influx query leave the previous result
and watermark intact; the next normal evaluation uses the new definition. No
database lock spans the network query, and the newer-attempt fence still applies.

A successful Influx metric write is the ingest acceptance boundary (`202` with the
written count). Subsequent registry, token-usage or heartbeat database failures
log fixed `metrics_ingest_bookkeeping_degraded stage=...` diagnostics rather than
ask clients to resend accepted metrics. When available, the ingest heartbeat is
WARNING with `bookkeeping_degraded=true`; actual metric-write failures remain
`503`. Bookkeeping is best effort and is retried by later ingests, not backfilled.

### UniFi controller transfer policy

Use an absolute HTTP(S) `base_url` for the controller (prefer HTTPS with TLS
verification). Login passwords, cookies and CSRF headers stay on that exact
scheme/host/effective-port origin: cross-origin redirects and scheme changes,
including HTTPS downgrades, are refused. Same-origin redirects remain supported.
Point `base_url` at the final origin rather than a redirecting alias.

Both Python and Rust UniFi collectors apply `max_body_bytes` (default 10 MiB)
to the sum of all login attempts, redirect bodies and device responses.
`total_timeout_seconds` (default three times `request_timeout_seconds`, maximum
86400) covers the whole exchange, including fallback authentication and JSON
parsing. HTTP-only work is canceled before metrics can be written. UniFi asks
for identity encoding and refuses unsolicited compressed responses before
reading/decoding them; controllers must honor `Accept-Encoding: identity`.
These changes take effect only after the web/Python command or rebuilt Rust
collector is deployed/restarted; no rollout is performed by this change.

### Rust HTTP decoder allocation limits

Rust bounded JSON collectors and page probes enforce `max_body_bytes` inside each
gzip/deflate/identity decoding stage, including intermediate stacked encodings,
before growing the output vector. The allowance is cumulative across chunks in a
response; the existing full-probe body count still includes redirect responses.
A tiny cap can reject a stacked encoding whose intermediate representation is
larger than its final payload. Normal compressed and split-chunk transfers remain
supported. Limit failures retain the existing body-limit category. This tighter
preallocation guard is Rust-specific; Python's general HTTP decoders still check
decoded chunks afterward. UniFi uses the separate identity-only policy above.

Inventory JSON rejects duplicate object names at every depth, including escaped
equivalents, before storing reports or changing category pointers. Repeated array
entries remain valid. All inventory reader HTML, JSON and SBOM responses carry
`Cache-Control: private, no-store`, including errors and login redirects.

### Admin definition ownership

Collection spec and condition admin pages edit definitions only. Scheduling,
collector results and condition observations are read-only and survive concurrent
runner publications. A signed, row-bound revision detects definition edits since
the page was opened (including secret rotation); conflicts require reloading.
Masked secrets are merged from the current locked spec. The admin holds the row
lock only during its database transaction, with no network work. Definition edits
preserve the last condition observation and watermark; the next scheduled
evaluation uses the new definition, and in-flight old-definition results cannot
publish. A definition edit does not manufacture a healthy observation.

### Observation ordering and query replies

Usage and registry updates recheck their observation watermark in the UPDATE.
Older arrivals cannot move last-seen times backwards or replace newer subject
provenance/service ownership. Equal-time provenance changes keep the first
committed observation. Unchanged rows retain the existing minute/hour refresh
throttles and usage row cap; bound tokens still cannot touch foreign services.

Both query runtimes reject the entire Influx v2/v3 response if a sample has a
missing/wrong metric, differs from any issued dimension/tag filter, or lies
outside the issued half-open range (`start <= timestamp < stop`). Unfiltered
dimensions may differ and sparse or empty series remain supported. A rejected
reply produces an evaluation failure without publishing a partial observed value.
