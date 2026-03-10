from __future__ import annotations

import httpx

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


class _FakeStreamResponse:
    def __init__(
        self,
        *,
        status_code: int,
        chunks: list[bytes],
        history: list[object] | None = None,
    ) -> None:
        self.status_code = status_code
        self._chunks = chunks
        self.history = history or []

    def __enter__(self) -> _FakeStreamResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def iter_bytes(self):
        yield from self._chunks


class _FakePageProbeClient:
    def __init__(
        self,
        *,
        response: _FakeStreamResponse | None = None,
        stream_error: Exception | None = None,
        capture: dict[str, object] | None = None,
    ) -> None:
        self._response = response
        self._stream_error = stream_error
        self._capture = capture if capture is not None else {}

    def __enter__(self) -> _FakePageProbeClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def stream(self, method: str, url: str):
        self._capture["method"] = method
        self._capture["url"] = url
        if self._stream_error is not None:
            raise self._stream_error
        assert self._response is not None
        return self._response


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


def test_http_json_metric_spec_ingests_three_level_nested_value(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="fractal thermal nested path",
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        interval_seconds=300,
        config={
            "url": "https://example.internal/thermal",
            "metric_path": "$.lm_sensors.cpu_tctl.value",
            "metric_name": "host.temperature_celsius",
            "host_id": "fractal",
            "subject_type": "host",
            "subject_id": "fractal",
            "source_system": "fractal_thermal_endpoint",
            "source_instance": "fractal-lm-sensors",
            "source_entity_id": "lm_sensors:k10temp:Tctl",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakeClient({"lm_sensors": {"cpu_tctl": {"value": 55.625}}}),
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


def test_http_page_probe_spec_ingests_latency_metrics(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="wersdoerfer blog probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        interval_seconds=300,
        config={
            "url": "https://wersdoerfer.de/blogs/ephes_blog/",
            "subject_id": "wersdoerfer_blog",
            "service_id": "wersdoerfer_blog",
            "collector_host": "macmini",
            "request_timeout_seconds": 15,
        },
    )

    client_capture: dict[str, object] = {}

    def _client_factory(**kwargs):
        client_capture.update(kwargs)
        return _FakePageProbeClient(
            response=_FakeStreamResponse(status_code=200, chunks=[b"<html>", b"..."]),
            capture=client_capture,
        )

    monkeypatch.setattr("graphyard.services.httpx.Client", _client_factory)
    perf_values = iter([0.0, 0.18, 0.24])
    monkeypatch.setattr(
        "graphyard.services.time.perf_counter", lambda: next(perf_values)
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
    assert result.ingested == 5
    assert client_capture["timeout"] == 15.0
    assert client_capture["follow_redirects"] is True
    assert client_capture["verify"] is True
    assert client_capture["method"] == "GET"
    assert client_capture["url"] == "https://wersdoerfer.de/blogs/ephes_blog/"

    points_by_metric = {point.metric: point for point in captured["points"]}
    assert points_by_metric["service.http_page_ttfb_seconds"].value == 0.18
    assert points_by_metric["service.http_page_total_seconds"].value == 0.24
    assert points_by_metric["service.http_page_status_code"].value == 200.0
    assert points_by_metric["service.http_page_success"].value == 1.0
    assert points_by_metric["service.http_page_redirect_count"].value == 0.0

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK
    assert spec.last_error == ""


def test_http_page_probe_spec_redirect_sets_redirect_count(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="python podcast redirect probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        interval_seconds=300,
        config={
            "url": "https://python-podcast.de/show/",
            "subject_id": "python_podcast_show",
            "collector_host": "macmini",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakePageProbeClient(
            response=_FakeStreamResponse(
                status_code=200,
                chunks=[b"<html>"],
                history=[object()],
            )
        ),
    )
    perf_values = iter([0.0, 0.01, 0.16, 0.19])
    monkeypatch.setattr(
        "graphyard.services.time.perf_counter", lambda: next(perf_values)
    )
    captured: dict[str, list[object]] = {"points": []}

    def _capture(points):
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    result = run_metric_collection_specs_once()

    assert result.failed == 0
    assert result.warning == 0
    assert result.ingested == 5
    points_by_metric = {point.metric: point for point in captured["points"]}
    assert points_by_metric["service.http_page_redirect_count"].value == 1.0

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK


def test_http_page_probe_spec_respects_follow_redirects_false(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="no redirect follow probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        interval_seconds=300,
        config={
            "url": "https://example.invalid/redirect",
            "subject_id": "example_redirect",
            "collector_host": "macmini",
            "follow_redirects": False,
        },
    )

    client_capture: dict[str, object] = {}

    def _client_factory(**kwargs):
        client_capture.update(kwargs)
        return _FakePageProbeClient(
            response=_FakeStreamResponse(status_code=302, chunks=[b"redirect"])
        )

    monkeypatch.setattr("graphyard.services.httpx.Client", _client_factory)
    perf_values = iter([0.0, 0.01, 0.05])
    monkeypatch.setattr(
        "graphyard.services.time.perf_counter", lambda: next(perf_values)
    )
    captured: dict[str, list[object]] = {"points": []}

    def _capture(points):
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    result = run_metric_collection_specs_once()

    assert result.failed == 0
    assert result.warning == 0
    assert result.ingested == 5
    assert client_capture["follow_redirects"] is False
    points_by_metric = {point.metric: point for point in captured["points"]}
    assert points_by_metric["service.http_page_status_code"].value == 302.0
    assert points_by_metric["service.http_page_success"].value == 1.0
    assert points_by_metric["service.http_page_redirect_count"].value == 0.0

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK


def test_http_page_probe_spec_non_success_response_sets_warning(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="public page 503 probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        interval_seconds=300,
        config={
            "url": "https://example.invalid/status",
            "subject_id": "example_status",
            "collector_host": "macmini",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakePageProbeClient(
            response=_FakeStreamResponse(status_code=503, chunks=[b"unavailable"])
        ),
    )
    perf_values = iter([0.0, 0.01, 0.07, 0.09])
    monkeypatch.setattr(
        "graphyard.services.time.perf_counter", lambda: next(perf_values)
    )
    captured: dict[str, list[object]] = {"points": []}

    def _capture(points):
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    result = run_metric_collection_specs_once()

    assert result.failed == 0
    assert result.warning == 1
    assert result.ingested == 5
    points_by_metric = {point.metric: point for point in captured["points"]}
    assert points_by_metric["service.http_page_status_code"].value == 503.0
    assert points_by_metric["service.http_page_success"].value == 0.0

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.WARNING
    assert spec.last_error == "unexpected status code: 503"


def test_http_page_probe_spec_empty_body_uses_header_ttfb(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="empty body probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        interval_seconds=300,
        config={
            "url": "https://example.invalid/empty",
            "subject_id": "example_empty",
            "collector_host": "macmini",
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakePageProbeClient(
            response=_FakeStreamResponse(status_code=204, chunks=[])
        ),
    )
    perf_values = iter([0.0, 0.03, 0.09])
    monkeypatch.setattr(
        "graphyard.services.time.perf_counter", lambda: next(perf_values)
    )
    captured: dict[str, list[object]] = {"points": []}

    def _capture(points):
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    result = run_metric_collection_specs_once()

    assert result.failed == 0
    assert result.warning == 0
    assert result.ingested == 5
    points_by_metric = {point.metric: point for point in captured["points"]}
    assert points_by_metric["service.http_page_ttfb_seconds"].value == 0.03
    assert points_by_metric["service.http_page_total_seconds"].value == 0.09
    assert points_by_metric["service.http_page_status_code"].value == 204.0
    assert points_by_metric["service.http_page_success"].value == 1.0

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK


def test_http_page_probe_spec_timeout_records_failure_metrics(db, monkeypatch):
    spec = MetricCollectionSpec.objects.create(
        name="timeout probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        interval_seconds=300,
        config={
            "url": "https://example.invalid/timeout",
            "subject_id": "example_timeout",
            "collector_host": "macmini",
            "verify_tls": False,
        },
    )

    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _FakePageProbeClient(
            stream_error=httpx.ReadTimeout("probe timed out")
        ),
    )
    captured: dict[str, list[object]] = {"points": []}

    def _capture(points):
        captured["points"] = points
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)

    result = run_metric_collection_specs_once()

    assert result.failed == 0
    assert result.warning == 1
    assert result.ingested == 2
    points_by_metric = {point.metric: point for point in captured["points"]}
    assert points_by_metric["service.http_page_status_code"].value == 0.0
    assert points_by_metric["service.http_page_success"].value == 0.0

    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.WARNING
    assert "probe timed out" in spec.last_error


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
