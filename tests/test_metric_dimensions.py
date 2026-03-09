from __future__ import annotations

from datetime import UTC, datetime

from graphyard import influx
from graphyard.models import (
    ConditionDefinition,
    HostRegistry,
    MetricCollectionSpec,
    MetricCollectionSpecType,
    ServiceRegistry,
    SubjectRegistry,
)
from graphyard.services import (
    run_metric_collection_specs_once,
    touch_registry_from_points,
)


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self._payload


class _FakeClient:
    def __init__(self, payload: object) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def get(self, url: str) -> _FakeResponse:
        del url
        return _FakeResponse(self._payload)


def test_home_assistant_env_scan_applies_subject_mapping_rule(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="ha mapped env scan",
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
        interval_seconds=60,
        config={
            "base_url": "https://ha.local",
            "access_token": "token",
            "service_id": "homeassistant",
            "subject_mapping": {
                "default": {
                    "subject_type": "environment_sensor",
                    "subject_id_from": "entity_name_slug",
                },
                "rules": [
                    {
                        "match_entity_id_regex": "^sensor\\.fritz_box_.*_cpu_temperature$",
                        "subject_type": "network_device",
                        "subject_id_template": "fritz_box_7590_ax",
                    }
                ],
            },
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient(
            [
                {
                    "entity_id": "sensor.fritz_box_7590_ax_cpu_temperature",
                    "state": "64.2",
                    "last_updated": "2026-03-04T12:00:00Z",
                    "attributes": {"device_class": "temperature"},
                }
            ]
        ),
    )
    captured: dict[str, list[influx.MetricPoint]] = {"points": []}

    def _capture(points: list[influx.MetricPoint]) -> int:
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    result = run_metric_collection_specs_once()

    assert result.failed == 0
    assert result.ingested == 1
    assert len(captured["points"]) == 1
    point = captured["points"][0]
    assert point.subject_type == "network_device"
    assert point.subject_id == "fritz_box_7590_ax"
    assert point.source_system == "homeassistant"
    assert point.source_instance == "default"
    assert point.source_entity_id == "sensor.fritz_box_7590_ax_cpu_temperature"
    assert point.collector_service == "graphyard-agent"
    assert point.service == "homeassistant"
    assert point.host is None

    spec.refresh_from_db()
    assert spec.last_error == ""


def test_home_assistant_env_scan_missing_mapping_uses_default_and_logs_warning(
    db, monkeypatch, caplog
):
    spec = MetricCollectionSpec.objects.create(
        name="ha default env scan",
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
        interval_seconds=60,
        config={
            "base_url": "https://ha.local",
            "access_token": "token",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient(
            [
                {
                    "entity_id": "sensor.Wohnzimmer Sensor Temperature",
                    "state": "21.8",
                    "last_updated": "2026-03-04T12:00:00Z",
                    "attributes": {"device_class": "temperature"},
                }
            ]
        ),
    )
    captured: dict[str, list[influx.MetricPoint]] = {"points": []}

    def _capture(points: list[influx.MetricPoint]) -> int:
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    caplog.set_level("WARNING", logger="graphyard.services")
    run_metric_collection_specs_once()

    assert any(
        "missing config.subject_mapping" in record.message for record in caplog.records
    )
    assert len(captured["points"]) == 1
    point = captured["points"][0]
    assert point.subject_type == "environment_sensor"
    assert point.subject_id == "wohnzimmer_sensor_temperature"

    spec.refresh_from_db()
    assert spec.last_status == "ok"


def test_home_assistant_env_scan_invalid_rule_subject_type_falls_back_to_default(
    db, monkeypatch, caplog
):
    MetricCollectionSpec.objects.create(
        name="ha invalid rule subject type",
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
        interval_seconds=60,
        config={
            "base_url": "https://ha.local",
            "access_token": "token",
            "subject_mapping": {
                "default": {
                    "subject_type": "environment_sensor",
                    "subject_id_from": "entity_name_slug",
                },
                "rules": [
                    {
                        "match_entity_id_regex": "^sensor\\.office_temperature$",
                        "subject_type": "not_a_type",
                        "subject_id_template": "office_temperature",
                    }
                ],
            },
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient(
            [
                {
                    "entity_id": "sensor.office_temperature",
                    "state": "21.8",
                    "last_updated": "2026-03-04T12:00:00Z",
                    "attributes": {"device_class": "temperature"},
                }
            ]
        ),
    )
    captured: dict[str, list[influx.MetricPoint]] = {"points": []}

    def _capture(points: list[influx.MetricPoint]) -> int:
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    caplog.set_level("WARNING", logger="graphyard.services")
    run_metric_collection_specs_once()

    assert any("unknown subject_type" in record.message for record in caplog.records)
    point = captured["points"][0]
    assert point.subject_type == "environment_sensor"
    assert point.subject_id == "office_temperature"


def test_home_assistant_env_scan_warning_cache_resets_between_runs(
    db, monkeypatch, caplog
):
    MetricCollectionSpec.objects.create(
        name="ha warning reset",
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
        interval_seconds=60,
        config={
            "base_url": "https://ha.local",
            "access_token": "token",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient(
            [
                {
                    "entity_id": "sensor.office_temperature",
                    "state": "21.8",
                    "last_updated": "2026-03-04T12:00:00Z",
                    "attributes": {"device_class": "temperature"},
                }
            ]
        ),
    )
    monkeypatch.setattr(
        "graphyard.services.influx.write_points", lambda points: len(points)
    )

    caplog.set_level("WARNING", logger="graphyard.services")
    run_metric_collection_specs_once()
    run_metric_collection_specs_once()

    warnings = [
        record
        for record in caplog.records
        if "missing config.subject_mapping" in record.message
    ]
    assert len(warnings) == 2


def test_home_assistant_env_scan_metric_mapping_rewrites_metric_and_value(
    db, monkeypatch
):
    spec = MetricCollectionSpec.objects.create(
        name="ha traffic env scan",
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
        interval_seconds=60,
        config={
            "base_url": "https://ha.local",
            "access_token": "token",
            "subject_mapping": {
                "default": {
                    "subject_type": "environment_sensor",
                    "subject_id_from": "entity_name_slug",
                },
                "rules": [
                    {
                        "match_entity_id_regex": "^sensor\\.fritz_box_.*_upload_throughput$",
                        "subject_type": "network_device",
                        "subject_id_template": "fritz_box_7590_ax",
                    }
                ],
            },
            "metric_mapping": {
                "rules": [
                    {
                        "match_entity_id_regex": "^sensor\\.fritz_box_.*_upload_throughput$",
                        "metric_name": "network_device.network_transmit_bytes_per_second",
                        "value_multiplier": 1000,
                        "extra_tags": {
                            "traffic_direction": "transmit",
                            "traffic_scope": "wan",
                        },
                    }
                ]
            },
            "entity_id_regex": "upload_throughput",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient(
            [
                {
                    "entity_id": "sensor.fritz_box_7590_ax_upload_throughput",
                    "state": "6.1",
                    "last_updated": "2026-03-04T12:00:00Z",
                    "attributes": {
                        "device_class": "data_rate",
                        "unit_of_measurement": "kB/s",
                    },
                }
            ]
        ),
    )
    captured: dict[str, list[influx.MetricPoint]] = {"points": []}

    def _capture(points: list[influx.MetricPoint]) -> int:
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    result = run_metric_collection_specs_once()

    assert result.failed == 0
    assert result.ingested == 1
    point = captured["points"][0]
    assert point.metric == "network_device.network_transmit_bytes_per_second"
    assert point.value == 6100.0
    assert point.subject_type == "network_device"
    assert point.subject_id == "fritz_box_7590_ax"
    assert point.tags["traffic_direction"] == "transmit"
    assert point.tags["traffic_scope"] == "wan"
    assert point.tags["device_class"] == "data_rate"
    assert point.tags["unit"] == "kB/s"

    spec.refresh_from_db()
    assert spec.last_status == "ok"


def test_touch_registry_tracks_subjects_and_keeps_host_registry_host_only(db):
    points = [
        influx.MetricPoint(
            ts=datetime(2026, 3, 4, 12, 0, tzinfo=UTC),
            metric="ha.sensor.office_temperature",
            value=22.0,
            subject_type="environment_sensor",
            subject_id="office_temperature",
            source_system="homeassistant",
            source_instance="ha-main",
            source_entity_id="sensor.office_temperature",
            collector_service="graphyard-agent",
            collector_host="macmini",
            service="homeassistant",
            tags={"device_class": "temperature"},
        ),
        influx.MetricPoint(
            ts=datetime(2026, 3, 4, 12, 0, tzinfo=UTC),
            metric="host.filesystem_used_ratio",
            value=0.52,
            subject_type="host",
            subject_id="macmini",
            source_system="vector",
            source_instance="vector-macmini",
            collector_service="vector",
            collector_host="macmini",
            host="macmini",
            service="vector",
            tags={"mountpoint": "/"},
        ),
    ]

    touch_registry_from_points(points)

    assert SubjectRegistry.objects.count() == 2
    assert SubjectRegistry.objects.filter(
        subject_type="environment_sensor", subject_id="office_temperature"
    ).exists()
    assert SubjectRegistry.objects.filter(
        subject_type="host", subject_id="macmini"
    ).exists()

    assert HostRegistry.objects.count() == 1
    assert HostRegistry.objects.filter(host_id="macmini").exists()

    assert ServiceRegistry.objects.filter(service_id="homeassistant").exists()
    assert ServiceRegistry.objects.filter(service_id="vector").exists()


def test_touch_registry_links_service_to_collector_host_for_non_host_subject(db):
    collector_host = HostRegistry.objects.create(
        host_id="macmini", display_name="macmini"
    )
    point = influx.MetricPoint(
        ts=datetime(2026, 3, 4, 12, 0, tzinfo=UTC),
        metric="ha.sensor.office_temperature",
        value=22.0,
        subject_type="environment_sensor",
        subject_id="office_temperature",
        source_system="homeassistant",
        source_instance="ha-main",
        source_entity_id="sensor.office_temperature",
        collector_service="graphyard-agent",
        collector_host="macmini",
        service="homeassistant",
        tags={"device_class": "temperature"},
    )

    touch_registry_from_points([point])

    service = ServiceRegistry.objects.get(service_id="homeassistant")
    assert service.host_id == collector_host.id


def test_query_condition_window_applies_subject_filters_and_host_compat(monkeypatch):
    condition = ConditionDefinition(
        name="network sensor",
        metric_name="ha.sensor.fritz_box_7590_ax_cpu_temperature",
        subject_type_filter="network_device",
        subject_id_filter="Fritz Box 7590 AX",
        host_filter="macmini",
        service_filter="homeassistant",
        tags_filter={"device_class": "temperature"},
        operator="gt",
        warning_threshold=70.0,
        critical_threshold=80.0,
        window_minutes=30,
        breach_minutes=5,
    )

    captured: dict[str, object] = {}

    def _capture(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr("graphyard.influx.query_range", _capture)

    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    influx.query_condition_window(condition, now=now)

    kwargs = captured["kwargs"]
    assert kwargs["host"] is None
    assert kwargs["service"] == "homeassistant"
    assert kwargs["tags"]["subject_type"] == "network_device"
    assert kwargs["tags"]["subject_id"] == "fritz_box_7590_ax"
    assert kwargs["tags"]["device_class"] == "temperature"


def test_query_condition_window_keeps_host_filter_for_host_subject(monkeypatch):
    condition = ConditionDefinition(
        name="host disk",
        metric_name="host.filesystem_used_ratio",
        subject_type_filter="host",
        host_filter="macmini",
        tags_filter={},
        operator="gt",
        warning_threshold=0.8,
        critical_threshold=0.9,
        window_minutes=30,
        breach_minutes=5,
    )

    captured: dict[str, object] = {}

    def _capture(*args, **kwargs):
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr("graphyard.influx.query_range", _capture)

    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    influx.query_condition_window(condition, now=now)

    kwargs = captured["kwargs"]
    assert kwargs["host"] == "macmini"
    assert kwargs["tags"]["subject_type"] == "host"


def test_http_json_metric_spec_emits_canonical_dimensions(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="http canonical",
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        interval_seconds=60,
        config={
            "url": "https://example.internal/health",
            "metric_path": "$.queue.depth",
            "metric_name": "service.queue_depth",
            "host_id": "macmini",
            "service_id": "mail",
            "subject_type": "service",
            "subject_id": "graphyard_web",
            "source_system": "graphyard",
            "source_instance": "default",
            "collector_service": "graphyard-agent",
            "collector_host": "macmini",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient({"queue": {"depth": 12}}),
    )
    captured: dict[str, list[influx.MetricPoint]] = {"points": []}

    def _capture(points: list[influx.MetricPoint]) -> int:
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    result = run_metric_collection_specs_once()

    assert result.failed == 0
    assert result.ingested == 1
    assert len(captured["points"]) == 1
    point = captured["points"][0]
    assert point.subject_type == "service"
    assert point.subject_id == "graphyard_web"
    assert point.source_system == "graphyard"
    assert point.source_instance == "default"
    assert point.collector_service == "graphyard-agent"
    assert point.collector_host == "macmini"
    assert point.service == "mail"
    assert point.host is None

    spec.refresh_from_db()
    assert spec.last_status == "ok"
