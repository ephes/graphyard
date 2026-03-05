# Metric Dimension Model (Conceptual Spec)

Status: Accepted (2026-03-05), implemented for Phase 1 + Phase 2 baseline
Scope: Graphyard metric point schema, registry semantics, and dashboard query model.

## Problem Statement

Graphyard currently overloads `host`:

- For host metrics (`host.*`), `host` means the measured machine.
- For Home Assistant-derived metrics (`ha.*`), `host` often means the collector runtime host (currently `macmini`), not the measured device/sensor.

This mixes collection context with measured-entity identity and makes dashboards, registry entries, and conditions ambiguous.

## External Guidance (Summary)

- OpenTelemetry separates logical service identity (`service.*`) from compute host identity (`host.*`).
- Prometheus models a series as metric name plus labels, so labels must be semantically stable and bounded.
- InfluxDB schema guidance emphasizes using tags as query dimensions and fields as measured values.

References are listed at the end.

## Design Goals

1. Unambiguous "what is measured" vs "where/how collected".
2. Stable, low-surprise dimensions for dashboards and alert filters.
3. Additive migration path from current schema.
4. Explicit enforcement points for enums and ID normalization.

## Canonical Dimension Model

Each metric point carries three contexts:

1. Subject context (the thing being measured)
2. Source context (the upstream system)
3. Collector context (the Graphyard component/runtime host)

Recommended tags:

- `metric` (existing)
- `subject_type` (enum): `host`, `network_device`, `environment_sensor`, `service`
- `subject_id` (canonical stable ID)
- `source_system`: `vector`, `homeassistant`, `snmp`, ...
- `source_instance`: always present (for example `ha-main`, `vector-macmini`, or `default`)
- `source_entity_id` (optional): raw upstream entity key for traceability (for example HA `entity_id`)
- `collector_service`: `graphyard-agent`, `vector`, ...
- `collector_host`: collector runtime host (for example `macmini`)

Compatibility tags:

- `host`: compatibility alias only when `subject_type='host'` and `host == subject_id`.
- `service`: legacy compatibility tag; new queries should prefer `collector_service`.

Value storage:

- `value` remains the primary field.

## Canonical ID Convention

`subject_id` is a Graphyard ID, not a source-leaked raw key.

Rules:

- Lowercase snake_case.
- No source namespace prefixes like `sensor.`.
- Stable across source moves (for example HA moved from one host to another).
- Keep raw upstream identifier separately when needed (for example `source_entity_id`).

Examples:

- `fritz_box_7590_ax` (not `sensor.fritz_box_7590_ax_cpu_temperature`)
- `usw_pro_xg_8_poe`
- `wohnzimmer_sensor_temperature`

## Entity Taxonomy (Recommended)

- `subject_type='host'`: compute host with OS/filesystem/process context.
- `subject_type='network_device'`: switches/routers/appliances.
- `subject_type='environment_sensor'`: room/environment sensor entities.
- `subject_type='service'`: software service as measured subject (for example queue depth).

Note:

- OTel host semantics can include switches, but Graphyard intentionally keeps `host` narrow to reduce ambiguity in UI and conditions.

## Subject Resolution Specification (Required)

Entity-to-subject mapping is config-driven per metric collection spec.

Proposed location:

- `MetricCollectionSpec.config.subject_mapping`

Proposed shape:

```json
{
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
      },
      {
        "match_entity_id_regex": "^sensor\\.usw_pro_xg_8_poe_temperature$",
        "subject_type": "network_device",
        "subject_id_template": "usw_pro_xg_8_poe"
      }
    ]
  }
}
```

`subject_id_from = entity_name_slug` algorithm (phase 1):

1. Start with upstream entity id (for example `sensor.wohnzimmer_sensor_temperature`).
2. Strip known HA domain prefixes (`sensor.`, `binary_sensor.`).
3. Normalize to lowercase snake_case.
4. Do not reorder tokens.

Example:

- `sensor.wohnzimmer_sensor_temperature` -> `wohnzimmer_sensor_temperature`

Rule template behavior:

- Phase 1 supports static `subject_id_template` only (literal output).
- Regex-capture template expansion is out-of-scope for phase 1 and can be added later if needed.

Operational policy:

- Mapping is maintained by operators in spec config (reviewed like other deploy config).
- Unmapped entities use `default` mapping and emit a warning log for triage.

## Registry and Condition Model Impact

Required model decisions before implementation:

- Keep `HostRegistry` strictly for `subject_type='host'` subjects.
- Introduce `SubjectRegistry` for all subjects with minimum fields:
  - `subject_type`
  - `subject_id`
  - `display_name` (nullable)
  - `source_system` (nullable, latest seen)
  - `last_seen_at`
- Keep `ServiceRegistry` for service identities (collector or subject services as needed).

Conditions:

- Extend condition filtering with `subject_type_filter` and `subject_id_filter`.
- Keep `host_filter` as compatibility filter for host-subject conditions only.

## Concrete Examples

1. Macmini filesystem metric (Vector)

- `metric=host.filesystem_used_ratio`
- `subject_type=host`
- `subject_id=macmini`
- `source_system=vector`
- `source_instance=vector-macmini`
- `collector_service=vector`
- `collector_host=macmini`
- `host=macmini` (compatibility alias)

2. Fritz!Box CPU temperature via Home Assistant

- `metric=ha.sensor.fritz_box_7590_ax_cpu_temperature`
- `subject_type=network_device`
- `subject_id=fritz_box_7590_ax`
- `source_system=homeassistant`
- `source_instance=ha-main`
- `collector_service=graphyard-agent`
- `collector_host=macmini`
- `source_entity_id=sensor.fritz_box_7590_ax_cpu_temperature`

3. USW Pro switch temperature via Home Assistant

- `metric=ha.sensor.usw_pro_xg_8_poe_temperature`
- `subject_type=network_device`
- `subject_id=usw_pro_xg_8_poe`
- `source_system=homeassistant`
- `source_instance=ha-main`
- `collector_service=graphyard-agent`
- `collector_host=macmini`
- `source_entity_id=sensor.usw_pro_xg_8_poe_temperature`

4. Room temperature sensor via Home Assistant

- `metric=ha.sensor.wohnzimmer_sensor_temperature`
- `subject_type=environment_sensor`
- `subject_id=wohnzimmer_sensor_temperature`
- `source_system=homeassistant`
- `source_instance=ha-main`
- `collector_service=graphyard-agent`
- `collector_host=macmini`
- `source_entity_id=sensor.wohnzimmer_sensor_temperature`

5. Service metric subject example

- `metric=service.queue_depth`
- `subject_type=service`
- `subject_id=graphyard_web`
- `source_system=graphyard`
- `source_instance=default`
- `collector_service=graphyard-agent`
- `collector_host=macmini`

## Query Patterns

All metrics for a specific thing:

```sql
WHERE "subject_id" = 'fritz_box_7590_ax'
```

Only compute-host views:

```sql
WHERE "subject_type" = 'host'
```

Home Assistant sourced metrics:

```sql
WHERE "source_system" = 'homeassistant'
```

Filesystem panel:

```sql
WHERE "metric" = 'host.filesystem_used_ratio'
  AND "subject_type" = 'host'
```

## Implementation Scope (Code Impact)

This concept requires explicit code changes:

- `MetricPoint` model/dataclass:
  - add `subject_type`, `subject_id`, `source_system`, `source_instance`, `collector_service`, `collector_host`
  - make `host` optional compatibility alias
- Influx write path:
  - emit canonical tags always
  - emit compatibility `host` only for `subject_type='host'`
- Collector specs (HA in particular):
  - add `subject_mapping` config and resolver logic
- Registry touch/update:
  - host-only updates to `HostRegistry`
  - generalized updates to new `SubjectRegistry`
- Condition filters:
  - add subject-aware filter fields

## Migration Plan

Phase 1 (additive, backward-compatible):

- Emit new canonical tags in parallel with legacy tags.
- Add mapping resolver and logs for unmapped entities.
- When `subject_mapping` is absent in a spec config, fall back to default behavior:
  - `subject_type=environment_sensor`
  - `subject_id` derived via `entity_name_slug`
  - warning log emitted for operator triage
- Keep existing dashboards/conditions functional.

Phase 2 (dashboard and condition migration):

- Shift HA and device panels to subject/source filters.
- Keep host infra panels on `subject_type='host'`.
- Add subject-aware condition fields.

Phase 3 (cleanup):

- Stop using legacy `host` semantics for non-host subjects.
- Limit `host` compatibility alias to `subject_type='host'`.
- Keep `subject_group` explicitly out-of-scope for this iteration (defer to follow-up spec).

## Validation and Enforcement

Enforcement point: ingest/write boundary before point persistence.

- Reject unknown `subject_type` values.
- Normalize/validate `subject_id` format.
- Ensure `source_instance` is always present (use `default` fallback).
- Emit warning/error counters for mapping failures and rejected points.

## Cardinality Guardrails

- Keep `subject_type`, `source_system`, `source_instance`, and `collector_service` bounded.
- `subject_id` should be stable, human-meaningful, and not include random/high-churn fragments.
- Do not tag unbounded per-event IDs.

## References

- OpenTelemetry service semantic conventions:
  - https://opentelemetry.io/docs/specs/semconv/resource/service/
- OpenTelemetry host semantic conventions:
  - https://opentelemetry.io/docs/specs/semconv/resource/host/
- Prometheus data model:
  - https://prometheus.io/docs/concepts/data_model/
- Prometheus metric and label naming best practices:
  - https://prometheus.io/docs/practices/naming/
- Prometheus instrumentation best practices:
  - https://prometheus.io/docs/practices/instrumentation/
- InfluxDB schema design best practices:
  - https://docs.influxdata.com/influxdb/cloud/write-data/best-practices/schema-design/
