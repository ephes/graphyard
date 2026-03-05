# Graphyard

Graphyard is a Django-based metrics platform MVP.

It ingests metrics into InfluxDB with canonical subject/source/collector dimensions, evaluates derived conditions for Nyxmon polling, and provides a small host/service index with Grafana links.

## MVP Scope

- `POST /v1/metrics` authenticated ingest (bearer token)
- `GET /v1/conditions`
- `GET /v1/conditions/<id>`
- `GET /v1/health`
- Long-running agent command (`start_agent`) for scheduling
- Generic metric collection specs (`MetricCollectionSpec`) in Django admin
- One-shot condition evaluation command (`evaluate_conditions`)
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
`INFLUX_API_MODE=auto` (default) lets Graphyard query with Flux for InfluxDB 2 and automatically switch to SQL for InfluxDB 3 when needed.

For Grafana in the Procfile stack:

- `just dev` starts Grafana via Docker (`grafana/grafana-oss`)
- URL: `http://127.0.0.1:3000`
- default login: `admin` / `admin`
- dev startup resets Grafana admin password to `admin` for deterministic local login
- datasource `Graphyard InfluxDB` is auto-provisioned
- dashboard folder `Graphyard` with `Graphyard Home` is auto-provisioned
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
Current supported `spec_type`:

- `home_assistant_sensor`
- `home_assistant_env_scan`
- `http_json_metric`

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
(single Home Assistant API call that collects all matching temperature/humidity sensors):

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
        "match_entity_id_regex": "^sensor\\.fritz_box_.*_cpu_temperature$",
        "subject_type": "network_device",
        "subject_id_template": "fritz_box_7590_ax"
      }
    ]
  },
  "metric_prefix": "ha.",
  "include_device_classes": ["temperature", "humidity"],
  "entity_id_regex": "(temperature|humidity)",
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
  "request_timeout_seconds": 10,
  "verify_tls": true,
  "tags": {
    "source": "mail-health"
  }
}
```

Collected points are written by the long-running agent to InfluxDB and update host/service registry metadata.
`host_id`/`service_id` remain supported for migration compatibility.
Write-path behavior is partial-success: invalid points are rejected per-point, while valid points in the
same batch are still written.

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

Run evaluator once:

```bash
just manage evaluate_conditions
```

Example supported condition: humidity above threshold for N minutes.

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
- `/v1/conditions` or `/v1/conditions/<id>` for derived condition states

Status values are always `ok|warning|critical`.

## Production Runtime Pattern (systemd)

Use units in `deploy/systemd/`:

- `graphyard-web.service` (Django + Granian)
- `graphyard-agent.service` (long-running scheduler loop)

Set environment file based on `deploy/systemd/graphyard.env.example`.

## Validation Commands

```bash
just test
just typecheck
just lint
```

## Influx + Grafana

See `docs/influx_grafana.md` for retention/downsampling notes and Grafana linking guidance.

Migration note: dashboards now filter primarily on canonical `subject_*`/`source_*` tags. Historical
series written before this migration may not have those tags, so old data can be absent from the new
subject-aware panels until fresh points are ingested.
