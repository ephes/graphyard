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
- domain dashboards:
  - `Graphyard Host Infrastructure`
  - `Graphyard Room Climate`
  - `Graphyard Device Thermals`

Important compatibility note:

- Current provisioned dashboard queries are InfluxQL.
- This is directly compatible with InfluxDB v2 datasource usage.
- If you standardize on InfluxDB v3, you need a different Grafana datasource/query setup (Flight SQL datasource and SQL-based panels).

### Dashboard Query Alignment (2026-03-06)

- Host Infrastructure dashboard queries are host-only (`subject_type='host'`).
- Room Climate dashboard queries are room-sensor-only (`subject_type='environment_sensor'`).
- Device Thermals dashboard queries are infrastructure device-only (`subject_type='network_device'`).
- Filesystem legend includes host + mountpoint context: `${__field.labels.subject_id}: ${__field.labels.mountpoint}`.
- Dashboard refresh defaults are aligned to collection interval (`1m`).
- Datasource UID remains `graphyard-influxdb` for provisioning stability.
- Provisioning keeps `disableDeletion: true`; when migrating from older layouts, obsolete dashboards/folders can remain until manually removed in Grafana UI.

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

The env-scan collector spec fetches `/api/states` once per run and extracts all matching sensors in one pass.

## Retention and Downsampling

Graphyard does not currently auto-provision retention/downsampling tasks. For production, use Influx-native policies/tasks.

Recommended baseline:

- Raw data: 30 days
- 5-minute rollup: 365 days
- 1-hour rollup: 365 days

Suggested approach:

1. Create a raw bucket with 30d retention.
2. Create rollup buckets with 365d retention.
3. Add native Influx tasks for rollups.

## Backup and Restore

Include both in backups:

- Django SQLite DB (`src/django/db.sqlite3` in this repo layout; production path may differ)
- InfluxDB data directory or export/backup artifacts

Restore order:

1. Restore InfluxDB data
2. Restore SQLite DB
3. Restart `graphyard-web.service`
4. Restart `graphyard-agent.service` (or run `manage.py start_agent --run-once` for a one-shot validation pass)
5. Verify `GET /v1/health` reports expected pipeline status
