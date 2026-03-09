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


class _FakeUnifiClient:
    def __init__(self, *, login_payload: object, device_payload: object) -> None:
        self._login_payload = login_payload
        self._device_payload = device_payload

    def __enter__(self) -> _FakeUnifiClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def post(self, url: str, **kwargs) -> _FakeResponse:
        del url, kwargs
        return _FakeResponse(self._login_payload)

    def get(self, url: str, **kwargs) -> _FakeResponse:
        del kwargs
        if url.endswith("/stat/device"):
            return _FakeResponse(self._device_payload)
        return _FakeResponse(self._login_payload)


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


def test_unifi_device_traffic_spec_ingests_uplink_rates(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="unifi usw traffic",
        spec_type=MetricCollectionSpecType.UNIFI_DEVICE_TRAFFIC,
        interval_seconds=60,
        config={
            "base_url": "https://unifi.local",
            "username": "homeassistant",
            "password": "secret",
            "site_id": "default",
            "device_name": "USW Pro XG 8 PoE",
            "interface_selector": "uplink",
            "subject_id": "usw_pro_xg_8_poe",
            "service_id": "unifi",
            "collector_host": "macmini",
            "verify_tls": False,
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeUnifiClient(
            login_payload={"meta": {"rc": "ok"}, "data": []},
            device_payload={
                "meta": {"rc": "ok"},
                "data": [
                    {
                        "name": "USW Pro XG 8 PoE",
                        "mac": "70:49:a2:21:53:45",
                        "uplink": {
                            "name": "eth0",
                            "port_idx": 10,
                            "speed": 10000,
                            "rx_bytes-r": 22790.32633644922,
                            "tx_bytes-r": 9224.609959748512,
                        },
                        "port_table": [
                            {"port_idx": 10, "name": "SFP+ 2"},
                        ],
                    }
                ],
            },
        ),
    )
    captured: dict[str, list[object]] = {"points": []}

    def _capture(points):
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    result = run_metric_collection_specs_once()

    assert result.total == 1
    assert result.failed == 0
    assert result.warning == 0
    assert result.ingested == 2
    assert result.skipped == 0

    receive_point = captured["points"][0]
    transmit_point = captured["points"][1]
    assert receive_point.metric == "network_device.network_receive_bytes_per_second"
    assert receive_point.subject_id == "usw_pro_xg_8_poe"
    assert receive_point.tags["traffic_direction"] == "receive"
    assert receive_point.tags["traffic_scope"] == "uplink"
    assert receive_point.tags["port_name"] == "SFP+ 2"
    assert receive_point.tags["device_class"] == "data_rate"
    assert transmit_point.metric == "network_device.network_transmit_bytes_per_second"
    assert transmit_point.tags["traffic_direction"] == "transmit"

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK
