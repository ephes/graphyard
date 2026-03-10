# InfluxDB and Grafana Integration Notes

## Current Implementation Baseline

Graphyard writes points to one measurement (`INFLUX_MEASUREMENT`, default `graphyard_metrics`) in one bucket (`INFLUX_BUCKET`, default `graphyard`).

Stored point shape:

- Tags:
  - Canonical: `metric`, `subject_type`, `subject_id`, `source_system`, `source_instance`, `collector_service`, `collector_host`
  - Optional canonical: `source_entity_id`
  - Compatibility: `host` (host subjects only), optional `service`
  - Additional low-cardinality custom tags
- Field: `value` (float)
- Timestamp: UTC

## InfluxDB API Mode

Graphyard runtime supports both InfluxDB v2 and v3 query paths:

- `INFLUX_API_MODE=auto` (default):
  - tries Flux/v2-style query path first
  - falls back to SQL/v3-style query path when needed
- `INFLUX_API_MODE=v2`:
  - force Flux query mode
- `INFLUX_API_MODE=v3`:
  - force SQL query mode

For normal development, keep `INFLUX_API_MODE=auto`.
For production on the current InfluxDB v2 baseline, set `INFLUX_API_MODE=v2`.

## Grafana Provisioning and Compatibility

Graphyard includes Grafana provisioning:

- datasource: `Graphyard InfluxDB`
- dashboard folders are derived from `deploy/grafana/dashboards/` filesystem paths
- default home dashboard: `Graphyard Overview` (uid `graphyard-home`)
- folder names are the directory names:
  - `overview`
  - `host-infrastructure`
  - `room-climate`
  - `device-thermals`
  - `device-network`
  - `service-http`
- domain dashboards:
  - `Graphyard Host Infrastructure`
  - `Fractal Thermals`
  - `Graphyard Room Climate`
  - `Graphyard Device Thermals`
  - `Graphyard Device Network`
  - `Graphyard HTTP Page Probes`
  - `Fractal Storage`

Important compatibility note:

- Current provisioned dashboard queries are InfluxQL.
- This is directly compatible with InfluxDB v2 datasource usage.
- If you standardize on InfluxDB v3, you need a different Grafana datasource/query setup (Flight SQL datasource and SQL-based panels).
- The provisioned datasource URL is intentionally `http://graphyard-influxdb:8086`.
  Grafana must be able to resolve that hostname from inside its container:
  local Procfile dev uses `--add-host=graphyard-influxdb:host-gateway`,
  and production should attach Grafana and InfluxDB to a shared Docker network.

### Dashboard Query Alignment (2026-03-06)

- Host Infrastructure dashboard queries are host-only (`subject_type='host'`).
- Fractal Thermals dashboard is host-only and pinned to
  `subject_type='host'`, `subject_id='fractal'`,
  `source_system='fractal_thermal_endpoint'`.
- Room Climate dashboard queries are room-sensor-only (`subject_type='environment_sensor'`).
- Device Thermals dashboard queries are infrastructure device-only (`subject_type='network_device'`).
- Device Network dashboard queries are infrastructure device-only (`subject_type='network_device'`) and uses canonical bytes-per-second traffic metrics.
- HTTP Page Probes dashboard queries are service-only (`subject_type='service'`) and scoped to the `service.http_page_*` metric family from `source_system='http_probe'`.
- Filesystem legend includes host + mountpoint context: `${__field.labels.subject_id}: ${__field.labels.mountpoint}`.
- Dashboard refresh defaults are aligned to collection interval (`1m`).
- Datasource UID remains `graphyard-influxdb` for provisioning stability.
- Provisioning keeps obsolete-file cleanup enabled (`disableDeletion: false`), so dashboards/folders removed from `deploy/grafana/dashboards/` are deleted on the next Grafana provisioning refresh.

## Local Development (`just dev`)

`just dev` starts a Procfile stack:

1. `django` (web/API)
2. `agent` (long-running collector + evaluator)
3. `influxdb` (`influxd` or `influxdb3`, data under `.dev/`)
4. `grafana` (Docker, `http://127.0.0.1:3000`)

Defaults for local Grafana login in this stack:

- username: `admin`
- password: `admin`

If you use InfluxDB 3 locally (`influxdb3`), the app query path still works with `INFLUX_API_MODE=auto`, but Grafana dashboards may need manual datasource/panel adaptation because provisioning assumes InfluxQL.

## Home Assistant Metrics Shape

Home Assistant collection specs normalize values into standard Graphyard metric points.

Typical conventions:

- metric name prefix: `ha.`
- subject mapping from `config.subject_mapping` resolves canonical `subject_type`/`subject_id`
- temperature/humidity series may include tags like `entity_id`, `device_class`, `unit`, `friendly_name`
- optional `config.metric_mapping.rules` can rewrite selected HA entity IDs to canonical metric names and scaled values (for example FRITZ!Box throughput into bytes/s)

The env-scan collector spec fetches `/api/states` once per run and extracts all matching sensors in one pass.

UniFi device traffic specs log in to the local controller, fetch `/api/s/<site>/stat/device`,
and emit canonical `network_device.network_receive_bytes_per_second` /
`network_device.network_transmit_bytes_per_second` metrics with tags such as
`traffic_direction`, `traffic_scope`, `port_idx`, and `port_name`.

HTTP page probe specs fetch public pages with `GET` from the Graphyard agent host
and emit bounded service-subject metrics:

- `service.http_page_ttfb_seconds`
- `service.http_page_total_seconds`
- `service.http_page_status_code`
- `service.http_page_success`
- `service.http_page_redirect_count`

Recommended modeling:

- `subject_type=service`
- stable page-level `subject_id`
- `source_system=http_probe`
- `source_instance=public_web`
- `collector_service=graphyard-agent`
- `collector_host=<probe host>` (currently `macmini` in production-style usage)
- `source_entity_id=<raw URL>`

Current implementation scope for this collector:

- method: `GET`
- cadence: operator-configured per spec (the initial recommended scope is `300s`)
- TTFB semantics: measured to response headers; when redirects are followed, redirect time is included
- success semantics: final `2xx`/`3xx` response => `success=1`, otherwise `success=0`
- transport failure semantics: `status_code=0`, `success=0`, collector run remains a warning, not a crash

## Retention and Downsampling

Current production policy on `macmini`:

- bucket: `graphyard`
- retention: `180d`
- enforcement: applied in place by the `graphyard_deploy` Ansible role via
  `influx bucket list/update` inside the `graphyard-influxdb` container
- downsampling: not enabled

Why this is the current baseline:

- live production volume on `2026-03-09` was about `1.63M` points/day
- the active write model is still one raw Influx bucket queried directly by the
  provisioned Grafana dashboards
- adding rollup buckets/tasks now would increase operator and query complexity
  without solving an observed production problem

Operational consequence:

- Grafana dashboards and ad hoc queries only retain raw history for the last
  180 days
- older raw points age out automatically
- if operators later need multi-year history, revisit this with explicit rollup
  buckets and matching dashboard/query changes instead of silently changing the
  current raw bucket contract

## Backup and Restore

Include both in backups:

- Django SQLite DB (`src/django/db.sqlite3` in this repo layout; production path may differ)
- InfluxDB data directory or export/backup artifacts

Production note:

- in the `ws-ops-misc` workspace, the operator path is the Echoport service-owned runner
  for `graphyard-backup`
- current production scope intentionally excludes Grafana DB state
- if Grafana datasource/admin reconciliation is needed after restore, rerun the
  Graphyard auth-bootstrap playbook from `ops-control`

When Graphyard is operated from the `ws-ops-misc` workspace, the intended
production path is Echoport-triggered backup/restore, not ad hoc `just` or
standalone Ansible backup roles. Any Graphyard-specific backup automation should
feed the Echoport/FastDeploy runner flow.

Restore order:

1. Restore InfluxDB data
2. Restore SQLite DB
3. Restart `graphyard-web.service`
4. Restart `graphyard-agent.service` (or run `manage.py start_agent --run-once` for a one-shot validation pass)
5. Verify `GET /v1/health` reports expected pipeline status
