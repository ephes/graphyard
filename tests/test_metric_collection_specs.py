from __future__ import annotations

from graphyard.models import MetricCollectionSpec, MetricCollectionSpecType, StatusLevel
from graphyard.services import run_metric_collection_specs_once


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


def test_http_json_metric_spec_ingests_value(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="mail queue depth",
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        interval_seconds=60,
        config={
            "url": "https://example.internal/health",
            "metric_path": "$.queue.depth",
            "metric_name": "service.queue_depth",
            "host_id": "macmini",
            "service_id": "mail",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient({"queue": {"depth": 12}}),
    )
    monkeypatch.setattr(
        "graphyard.services.influx.write_points", lambda points: len(points)
    )

    result = run_metric_collection_specs_once()

    assert result.total == 1
    assert result.failed == 0
    assert result.warning == 0
    assert result.ingested == 1

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK
    assert spec.last_error == ""


def test_http_json_metric_spec_missing_path_sets_warning(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="missing path",
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        interval_seconds=60,
        config={
            "url": "https://example.internal/health",
            "metric_path": "$.queue.missing",
            "metric_name": "service.queue_missing",
            "host_id": "macmini",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient({"queue": {"depth": 12}}),
    )
    monkeypatch.setattr(
        "graphyard.services.influx.write_points", lambda points: len(points)
    )

    result = run_metric_collection_specs_once()

    assert result.total == 1
    assert result.failed == 0
    assert result.warning == 1
    assert result.ingested == 0
    assert result.skipped == 1

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.WARNING
    assert "metric_path not found" in spec.last_error


def test_home_assistant_env_scan_collects_temp_and_humidity(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="ha env scan",
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
        interval_seconds=60,
        config={
            "base_url": "https://ha.local",
            "access_token": "token",
            "host_id": "homeassistant",
            "service_id": "homeassistant",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient(
            [
                {
                    "entity_id": "sensor.living_room_temperature",
                    "state": "22.5",
                    "last_updated": "2026-03-04T12:00:00Z",
                    "attributes": {"device_class": "temperature"},
                },
                {
                    "entity_id": "sensor.living_room_humidity",
                    "state": "52.4",
                    "last_updated": "2026-03-04T12:00:00Z",
                    "attributes": {"device_class": "humidity"},
                },
                {
                    "entity_id": "sensor.kitchen_humidity",
                    "state": "unknown",
                    "last_updated": "2026-03-04T12:00:00Z",
                    "attributes": {"device_class": "humidity"},
                },
                {
                    "entity_id": "binary_sensor.door",
                    "state": "on",
                    "attributes": {},
                },
            ]
        ),
    )
    monkeypatch.setattr(
        "graphyard.services.influx.write_points", lambda points: len(points)
    )

    result = run_metric_collection_specs_once()

    assert result.total == 1
    assert result.failed == 0
    assert result.warning == 0
    assert result.ingested == 2
    assert result.skipped == 1

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK


def test_home_assistant_sensor_spec_ingests_single_entity(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="ha single sensor",
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_SENSOR,
        interval_seconds=60,
        config={
            "base_url": "https://ha.local",
            "access_token": "token",
            "entity_id": "sensor.office_temperature",
            "metric_name": "ha.sensor.office_temperature",
            "host_id": "homeassistant",
            "service_id": "homeassistant",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient(
            {
                "entity_id": "sensor.office_temperature",
                "state": "21.2",
                "last_updated": "2026-03-04T12:00:00Z",
                "attributes": {"device_class": "temperature"},
            }
        ),
    )
    monkeypatch.setattr(
        "graphyard.services.influx.write_points", lambda points: len(points)
    )

    result = run_metric_collection_specs_once()

    assert result.total == 1
    assert result.failed == 0
    assert result.warning == 0
    assert result.ingested == 1

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK
