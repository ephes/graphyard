# Graphyard

Graphyard is a Django-based metrics platform MVP.

It ingests metrics into InfluxDB with canonical subject/source/collector dimensions, evaluates derived conditions for Nyxmon polling, and provides a small host/service index with Grafana links.

## Software inventory pilot

See [push inventory](docs/inventory.md) for host-bound reporting, private inventory
views and rollout prerequisites. This pilot does not make remote logins or install
producer schedules.

## MVP Scope

- `POST /v1/metrics` authenticated ingest (bearer token)
  - accepts JSON array payloads, object-with-`metrics` arrays, and NDJSON batches
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

Revoke by id or name:

```bash
just manage revoke_ingest_token --id 1
just manage revoke_ingest_token --name macmini
```

Graphyard stores token hashes only.
Ingest tokens use a fast SHA-256 digest with the `graphyard-sha256$...` prefix (constant-time compare), and legacy hashes are upgraded on first successful use.
Hash upgrades are one-way; if you revert to code that only understands Django password hash formats, rotate ingest tokens.

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

Prune is opt-in and keyed only by `name`. Without `--prune`, unspecified specs
are left untouched. With `--prune`, the command refuses an empty desired spec
set so a broken render cannot wipe every spec row in one shot.

Typed config values are validated on save (Django admin and
`apply_metric_collection_specs`, which aborts on the first invalid spec in the
file, including specs whose stored row is otherwise unchanged):

- `request_timeout_seconds` must be a number greater than 0 (numeric strings such as `"15"` are accepted; `"5s"` or `null` are not).
- `max_body_bytes` must be a positive integer.
- `verify_tls` and `follow_redirects` must be JSON booleans. Strings such as
  `"false"` are rejected instead of silently meaning `true`.

Each spec runs in isolation. If a spec fails at run time, for example because it
was saved before validation existed, it gets `last_status=critical` with the
error in `last_error`. Its `next_run_time` still advances and the remaining specs
keep running. The run is reflected in the `metric_collectors` heartbeat on
`/v1/health`: `warning` when some specs failed, `critical` only when all did.

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
exceeding it records the same `status_code=0`/`success=0` failure. There is no
total deadline yet, so an endpoint that keeps dripping bytes can still hold the
agent loop for longer than `request_timeout_seconds` (see `BACKLOG.md`).

Example `config` JSON for a UniFi device traffic spec:

```json
{
  "base_url": "https://127.0.0.1:8443",
  "username": "homeassistant",
  "password": "replace-me",
  "site_id": "default",
  "device_name": "USW Pro XG 8 PoE",
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

By default, UniFi auth uses `auth_mode=auto`: Graphyard first tries the classic
Network Application flow (`POST /api/login`, then `/api/s/<site>/stat/device`)
and falls back to the UniFi OS flow (`POST /api/auth/login`, then
`/proxy/network/api/s/<site>/stat/device`). Set `auth_mode` to `legacy` or
`unifi_os` only when an installation needs to pin one flow.
Login and device-fetch failures record the endpoint, HTTP status, and a short
non-secret response summary in the spec `last_error`.

Collected points are written by the long-running agent to InfluxDB and update host/service registry metadata.
`host_id`/`service_id` remain supported for migration compatibility.
Write-path behavior is partial-success: invalid points are rejected per-point, while valid points in the
same batch are still written.
For `POST /v1/metrics`, request-level payload validation is fail-fast: if any point fails ingest payload
parsing/normalization before write, the request is rejected with `400` and no points are written.
Metric values must be finite numbers: `NaN`, `Infinity`, `-Infinity` (as JSON literals or strings
such as `"nan"`/`"inf"`) are rejected as invalid values, since InfluxDB cannot store them. Collection
specs skip such values like other non-numeric readings, and they are never counted as ingested nor
mark a subject as seen.

Security note: metric collection `config` values are stored in SQLite and may contain secrets
(for example `access_token`, `bearer_token`, `basic_password`). Django admin masks known secret
keys for existing specs, but secrets are still plaintext at rest in the DB for MVP.

## Condition Evaluation

Create conditions in Django admin (`ConditionDefinition`) using:

- metric name
- optional `subject_type_filter` / `subject_id_filter`
- legacy `host_filter` (host-subject compatibility)
- operator (`gt|gte|lt|lte`)
- warning/critical thresholds
- breach duration window
- optional staleness allowance (`stale_after_seconds`) and `alert_when_stale`
  (see "Intermittent sources" below)

Run evaluator once:

```bash
just manage evaluate_conditions
```

Seed (or update) a practical host disk-usage condition:

```bash
just manage seed_disk_usage_condition --host macmini --mountpoint /
```

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
minute of its start.

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

## Nyxmon Integration Contract

Nyxmon can poll:

- `/v1/health` for pipeline/service health and staleness
- `/v1/conditions` for condition list plus summary counts (`total`, `ok`, `warning`, `critical`)
- `/v1/conditions/<id>` for detailed config/status of one condition

Status values are always `ok|warning|critical`.

## Production Runtime Pattern (systemd)

Use units in `deploy/systemd/`:

- `graphyard-web.service` (Django + Granian)
- `graphyard-agent.service` (long-running scheduler loop)

Set environment file based on `deploy/systemd/graphyard.env.example`.
For the SQLite-backed deployment profile, keep web concurrency conservative (`--workers 1`) and
use WAL + busy-timeout settings from `graphyard.env.example`.

## Validation Commands

```bash
just test
just typecheck
just lint
```

GitHub Actions (`.github/workflows/ci.yml`) runs the same lint, typecheck and test
commands on every push and pull request. It needs no secrets or InfluxDB.

## Influx + Grafana

See `docs/influx_grafana.md` for retention/downsampling notes and Grafana linking guidance.

Migration note: dashboards now filter primarily on canonical `subject_*`/`source_*` tags. Historical
series written before this migration may not have those tags, so old data can be absent from the new
subject-aware panels until fresh points are ingested.
