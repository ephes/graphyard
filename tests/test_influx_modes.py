from __future__ import annotations

from datetime import UTC, datetime, timedelta

from influxdb_client.rest import ApiException
import pytest

from graphyard import influx


def test_query_range_auto_falls_back_to_v3_on_v2_404(monkeypatch, settings):
    settings.INFLUX_API_MODE = "auto"

    start = datetime.now(UTC) - timedelta(minutes=5)
    stop = datetime.now(UTC)
    expected = [
        influx.MetricSample(
            ts=datetime.now(UTC),
            value=42.0,
            host="h1",
            metric="m1",
            service="s1",
            tags={},
        )
    ]

    def _fail_v2(*args, **kwargs):
        del args, kwargs
        raise ApiException(status=404, reason="Not found")

    monkeypatch.setattr(influx, "_query_range_v2_flux", _fail_v2)
    monkeypatch.setattr(influx, "_query_range_v3_sql", lambda *a, **k: expected)

    result = influx.query_range("m1", start, stop)

    assert result == expected


def test_query_range_auto_does_not_hide_non_404_errors(monkeypatch, settings):
    settings.INFLUX_API_MODE = "auto"

    start = datetime.now(UTC) - timedelta(minutes=5)
    stop = datetime.now(UTC)

    def _fail_v2(*args, **kwargs):
        del args, kwargs
        raise ApiException(status=500, reason="server error")

    monkeypatch.setattr(influx, "_query_range_v2_flux", _fail_v2)

    with pytest.raises(ApiException):
        influx.query_range("m1", start, stop)


def test_query_range_v2_flux_uses_safe_bracket_access_for_tag_keys(
    monkeypatch, settings
):
    settings.INFLUX_API_MODE = "v2"
    settings.INFLUX_BUCKET = "graphyard"
    settings.INFLUX_ORG = "graphyard"
    settings.INFLUX_MEASUREMENT = "graphyard_metrics"

    captured: dict[str, str] = {}

    class _FakeQueryApi:
        def query(self, query: str, org: str):
            captured["query"] = query
            captured["org"] = org
            return []

    class _FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

        def query_api(self) -> _FakeQueryApi:
            return _FakeQueryApi()

    monkeypatch.setattr(influx, "_build_client", lambda: _FakeClient())

    start = datetime.now(UTC) - timedelta(minutes=5)
    stop = datetime.now(UTC)
    influx.query_range("m1", start, stop, tags={'bad"tag': 'v"1'})

    query_text = captured["query"]
    assert 'r["bad\\"tag"] == "v\\"1"' in query_text
    assert "r.bad" not in query_text


def test_normalize_metric_point_rejects_host_mismatch():
    point = influx.MetricPoint(
        ts=datetime.now(UTC),
        metric="host.cpu_usage",
        value=0.3,
        subject_type="host",
        subject_id="macmini",
        source_system="vector",
        source_instance="vector-macmini",
        collector_service="vector",
        collector_host="macmini",
        host="other-host",
    )

    with pytest.raises(ValueError):
        influx.normalize_metric_point(point)


def test_normalize_metric_point_drops_host_for_non_host_subject():
    point = influx.MetricPoint(
        ts=datetime.now(UTC),
        metric="ha.sensor.office_temperature",
        value=22.4,
        subject_type="environment_sensor",
        subject_id="office_temperature",
        source_system="homeassistant",
        source_instance="ha-main",
        collector_service="graphyard-agent",
        collector_host="macmini",
        host="legacy-host",
    )

    normalized = influx.normalize_metric_point(point)

    assert normalized.host is None


def test_query_range_v3_sql_selects_and_returns_dimension_columns(
    monkeypatch, settings
):
    settings.INFLUX_URL = "http://influx.local"
    settings.INFLUX_TOKEN = "token"
    settings.INFLUX_BUCKET = "graphyard"
    settings.INFLUX_MEASUREMENT = "graphyard_metrics"

    captured: dict[str, object] = {}

    class _FakeResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return [
                {
                    "time": "2026-03-04T12:00:00Z",
                    "value": 22.4,
                    "host": "macmini",
                    "metric": "ha.sensor.office_temperature",
                    "service": "homeassistant",
                    "subject_type": "environment_sensor",
                    "subject_id": "office_temperature",
                    "source_system": "homeassistant",
                    "source_instance": "ha-main",
                    "source_entity_id": "sensor.office_temperature",
                    "collector_service": "graphyard-agent",
                    "collector_host": "macmini",
                }
            ]

    def _fake_post(url: str, json: dict, headers: dict, timeout: float):
        captured["url"] = url
        captured["query"] = json["q"]
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _FakeResponse()

    monkeypatch.setattr(influx.httpx, "post", _fake_post)

    start = datetime(2026, 3, 4, 11, 0, tzinfo=UTC)
    stop = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    samples = influx._query_range_v3_sql("ha.sensor.office_temperature", start, stop)

    assert "subject_type" in str(captured["query"])
    assert "subject_id" in str(captured["query"])
    assert "collector_service" in str(captured["query"])
    assert len(samples) == 1
    assert samples[0].subject_type == "environment_sensor"
    assert samples[0].subject_id == "office_temperature"
    assert samples[0].source_system == "homeassistant"
    assert samples[0].source_instance == "ha-main"
    assert samples[0].source_entity_id == "sensor.office_temperature"
    assert samples[0].collector_service == "graphyard-agent"
    assert samples[0].collector_host == "macmini"


def test_write_points_skips_invalid_points_in_batch(monkeypatch):
    captured: dict[str, int] = {"written": 0}

    class _FakeWriteApi:
        def write(self, bucket: str, org: str, record):
            del bucket, org
            captured["written"] = len(record)

    class _FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

        def write_api(self, write_options):
            del write_options
            return _FakeWriteApi()

    monkeypatch.setattr(influx, "_build_client", lambda: _FakeClient())

    points = [
        influx.MetricPoint(
            ts=datetime.now(UTC),
            metric="host.cpu_usage",
            value=0.3,
            subject_type="host",
            subject_id="macmini",
            source_system="vector",
            source_instance="vector-macmini",
            collector_service="vector",
            collector_host="macmini",
            host="macmini",
        ),
        influx.MetricPoint(
            ts=datetime.now(UTC),
            metric="ha.sensor.office_temperature",
            value=22.4,
            subject_type="not_a_subject_type",
            subject_id="office_temperature",
            source_system="homeassistant",
            source_instance="ha-main",
            collector_service="graphyard-agent",
            collector_host="macmini",
        ),
    ]

    written = influx.write_points(points)

    assert written == 1
    assert captured["written"] == 1
