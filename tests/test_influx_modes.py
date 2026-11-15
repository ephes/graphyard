from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
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

    def query_tables(query):
        captured["query"] = query
        return []

    monkeypatch.setattr(influx, "_query_flux_tables", query_tables)

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
        def json(self):
            return [
                {
                    "time": "2026-03-04T11:59:59Z",
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
                    "mountpoint": "/",
                    "device": None,
                }
            ]

    real_client = httpx.AsyncClient

    def handle(request):
        captured["url"] = str(request.url)
        captured["query"] = json.loads(request.content)["q"]
        captured["headers"] = request.headers
        return httpx.Response(200, json=_FakeResponse().json())

    monkeypatch.setattr(
        influx.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handle), **kwargs),
    )

    start = datetime(2026, 3, 4, 11, 0, tzinfo=UTC)
    stop = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    samples = influx._query_range_v3_sql("ha.sensor.office_temperature", start, stop)

    assert str(captured["query"]).startswith("select * ")
    assert captured["url"] == "http://influx.local/api/v3/query_sql"
    assert captured["headers"]["Authorization"] == "Bearer token"
    assert len(samples) == 1
    assert samples[0].subject_type == "environment_sensor"
    assert samples[0].subject_id == "office_temperature"
    assert samples[0].source_system == "homeassistant"
    assert samples[0].source_instance == "ha-main"
    assert samples[0].source_entity_id == "sensor.office_temperature"
    assert samples[0].collector_service == "graphyard-agent"
    assert samples[0].collector_host == "macmini"
    assert samples[0].tags == {"mountpoint": "/"}


def test_write_points_skips_invalid_points_in_batch(monkeypatch):
    captured: dict[str, int] = {"written": 0}

    def write_batch(points):
        captured["written"] = len(points)

    monkeypatch.setattr(influx, "_write_batch", write_batch)

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


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_normalize_metric_point_rejects_non_finite_value(value):
    point = influx.MetricPoint(
        ts=datetime.now(UTC),
        metric="host.cpu_usage",
        value=value,
        subject_type="host",
        subject_id="macmini",
        source_system="vector",
        source_instance="vector-macmini",
        collector_service="vector",
        collector_host="macmini",
    )

    with pytest.raises(ValueError, match="finite"):
        influx.normalize_metric_point(point)


def test_write_points_does_not_count_non_finite_points(monkeypatch, caplog):
    captured: dict[str, int] = {"written": 0}

    def write_batch(points):
        captured["written"] = len(points)

    monkeypatch.setattr(influx, "_write_batch", write_batch)

    def _point(value: float) -> influx.MetricPoint:
        return influx.MetricPoint(
            ts=datetime.now(UTC),
            metric="host.cpu_usage",
            value=value,
            subject_type="host",
            subject_id="macmini",
            source_system="vector",
            source_instance="vector-macmini",
            collector_service="vector",
            collector_host="macmini",
        )

    written = influx.write_points([_point(0.5), _point(float("nan"))])

    assert written == 1
    assert captured["written"] == 1
    assert any(
        "metrics_write_rejected category=normalization rejected_points=1 written_points=1"
        in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.parametrize("raw", ["nan", "inf", "-Infinity", float("nan")])
def test_parse_finite_float_rejects_non_finite(raw):
    with pytest.raises(ValueError):
        influx.parse_finite_float(raw)


def test_parse_finite_float_accepts_numbers():
    assert influx.parse_finite_float("1.5") == 1.5
    assert influx.parse_finite_float(3) == 3.0


@pytest.mark.parametrize(
    "ts, admitted",
    [
        ("1677-09-21T00:12:43.145224Z", False),
        ("1677-09-21T00:12:43.145225Z", True),
        ("1677-09-21T00:12:43.145226Z", True),
        ("2262-04-11T23:39:59.999998Z", True),
        ("2262-04-11T23:39:59.999999Z", True),
        ("2262-04-11T23:40:00Z", False),
        ("2262-04-11T23:47:16.854775Z", False),  # Valid only for v2.
        ("2262-04-12T00:39:59.999999+01:00", True),
        ("2262-04-12T00:40:00+01:00", False),
        ("2262-04-11T23:40:00", False),  # Naive means UTC.
    ],
)
def test_auto_writer_timestamp_admission(monkeypatch, settings, ts, admitted):
    settings.INFLUX_API_MODE = "auto"
    batches = []
    monkeypatch.setattr(influx, "_write_batch", lambda points: batches.append(points))

    def point(timestamp):
        return influx.MetricPoint(
            ts=datetime.fromisoformat(timestamp),
            metric="timestamp_test",
            value=1.0,
            subject_type="host",
            subject_id="synthetic",
            source_system="test",
            collector_service="test",
            collector_host="synthetic",
        )

    candidate = point(ts)
    assert influx.write_points([candidate]) == int(admitted)
    assert len(batches) == int(admitted)
    if admitted:
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        utc = (
            candidate.ts.replace(tzinfo=UTC)
            if candidate.ts.tzinfo is None
            else candidate.ts.astimezone(UTC)
        )
        delta = utc - epoch
        ns = (
            delta.days * 86400_000000 + delta.seconds * 1000000 + delta.microseconds
        ) * 1000
        assert batches[0][0].to_line_protocol().endswith(f" {ns}")
    batches.clear()
    assert influx.write_points([candidate, point("2026-11-15T00:00:00Z")]) == 1 + int(
        admitted
    )
    assert len(batches) == 1
    assert len(batches[0]) == 1 + int(admitted)


def test_explicit_v2_writer_keeps_v2_only_timestamp(monkeypatch, settings):
    settings.INFLUX_API_MODE = "v2"
    batches = []
    monkeypatch.setattr(influx, "_write_batch", lambda points: batches.append(points))
    point = influx.MetricPoint(
        ts=datetime(2262, 4, 11, 23, 47, 16, 854775, tzinfo=UTC),
        metric="timestamp_test",
        value=1.0,
        subject_type="host",
        subject_id="synthetic",
        source_system="test",
        collector_service="test",
        collector_host="synthetic",
    )
    assert influx.write_points([point]) == 1
    assert batches[0][0].to_line_protocol().endswith(" 9223372036854775000")
