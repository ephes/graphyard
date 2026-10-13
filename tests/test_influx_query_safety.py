from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import json
import time
from types import SimpleNamespace

import httpx
import pytest

from graphyard import influx
from graphyard.models import ConditionDefinition, StatusLevel
from graphyard.services import evaluate_conditions_once


NOW = datetime(2026, 10, 13, 12, tzinfo=UTC)


@pytest.fixture(autouse=True)
def influx_url(settings):
    settings.INFLUX_URL = "http://influx.local"


def install_rows(monkeypatch, settings, backend, values):
    settings.INFLUX_API_MODE = backend
    rows = [
        {
            "time": (NOW - timedelta(minutes=len(values) - index - 1)).isoformat(),
            "value": value,
            "metric": "host.disk_usage",
            "host": "h1",
            "mountpoint": "/" if index else "/data",
        }
        for index, value in enumerate(values)
    ]
    if backend == "v3":
        real_client = httpx.AsyncClient
        monkeypatch.setattr(
            influx.httpx,
            "AsyncClient",
            lambda **kwargs: real_client(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, json=rows)
                ),
                **kwargs,
            ),
        )
        return

    class Record:
        def __init__(self, row):
            self.values = {**row, "_value": row["value"]}

        def get_value(self):
            return self.values["_value"]

        def get_time(self):
            return datetime.fromisoformat(self.values["time"])

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def query_api(self):
            return SimpleNamespace(
                query=lambda **kwargs: [
                    SimpleNamespace(records=[Record(r) for r in rows])
                ]
            )

    monkeypatch.setattr(influx, "_build_client", Client)


@pytest.mark.parametrize("backend", ["v2", "v3", "auto"])
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_nonfinite_query_evidence_is_an_evaluation_error(
    db, monkeypatch, settings, backend, value
):
    # auto uses Flux here: a decoding error must not trigger SQL fallback.
    install_rows(monkeypatch, settings, backend, [20.0, 25.0, value])
    condition = ConditionDefinition.objects.create(
        name="query-safety",
        metric_name="host.disk_usage",
        warning_threshold=60,
        critical_threshold=75,
        alert_when_stale=False,
        status=StatusLevel.OK,
        last_value=20.0,
        last_evaluated=NOW - timedelta(hours=1),
    )
    run = evaluate_conditions_once(condition_id=condition.id)
    condition.refresh_from_db()

    assert run.failed == 1
    assert condition.status == StatusLevel.CRITICAL
    assert "evaluation error" in condition.message
    assert "finite" in condition.message
    assert condition.last_evaluated == NOW - timedelta(hours=1)
    assert condition.last_value == 20.0


@pytest.mark.parametrize("backend", ["v2", "v3"])
def test_finite_query_keeps_multiple_series(monkeypatch, settings, backend):
    install_rows(monkeypatch, settings, backend, [20.0, 25.0])
    samples = influx.query_range("host.disk_usage", NOW - timedelta(minutes=5), NOW)
    assert [s.value for s in samples] == [20.0, 25.0]
    assert [s.tags["mountpoint"] for s in samples] == ["/data", "/"]


class SQLStream(httpx.AsyncByteStream):
    def __init__(self, chunks, delay=0):
        self.chunks = chunks
        self.delay = delay
        self.closed = False
        self.reads = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.reads += 1
            await asyncio.sleep(self.delay)
            yield chunk

    async def aclose(self):
        self.closed = True


def install_stream(monkeypatch, stream, *, status=200, encoding=None, header_delay=0):
    real_client = httpx.AsyncClient

    async def handle(request):
        await asyncio.sleep(header_delay)
        return httpx.Response(
            status,
            stream=stream,
            headers={"Content-Encoding": encoding} if encoding else {},
        )

    monkeypatch.setattr(
        influx.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handle), **kwargs),
    )

    return stream


def query_sql():
    return influx._query_range_v3_sql(
        "host.disk_usage", NOW - timedelta(minutes=5), NOW
    )


def test_sql_oversize_stops_reading_and_closes(monkeypatch):
    monkeypatch.setattr(influx, "SQL_QUERY_MAX_BODY_BYTES", 5)
    stream = install_stream(monkeypatch, SQLStream([b"[", b"12345", b"]"]))
    with pytest.raises(influx.InfluxConfigurationError, match="byte limit"):
        query_sql()
    assert stream.closed
    assert stream.reads == 2


def test_sql_size_limit_counts_decoded_bytes(monkeypatch):
    import gzip

    monkeypatch.setattr(influx, "SQL_QUERY_MAX_BODY_BYTES", 100)
    stream = install_stream(
        monkeypatch,
        SQLStream([gzip.compress(b"[" + b" " * 1000 + b"]")]),
        encoding="gzip",
    )
    with pytest.raises(influx.InfluxConfigurationError, match="byte limit"):
        query_sql()
    assert stream.closed


@pytest.mark.parametrize("delay", [0.04, 1.0])
def test_sql_deadline_cancels_drip_or_blocked_read(monkeypatch, delay):
    monkeypatch.setattr(influx, "SQL_QUERY_TOTAL_TIMEOUT_SECONDS", 0.25)
    stream = install_stream(
        monkeypatch, SQLStream([b"[", *([b" "] * 100), b"]"], delay=delay)
    )
    started = time.monotonic()
    with pytest.raises(influx.InfluxConfigurationError, match="total deadline"):
        query_sql()
    assert time.monotonic() - started < 3.0
    assert stream.closed
    assert stream.reads < 102


def test_sql_deadline_includes_headers(monkeypatch):
    monkeypatch.setattr(influx, "SQL_QUERY_TOTAL_TIMEOUT_SECONDS", 0.25)
    install_stream(monkeypatch, SQLStream([b"[]"]), header_delay=1)
    started = time.monotonic()
    with pytest.raises(influx.InfluxConfigurationError, match="total deadline"):
        query_sql()
    assert time.monotonic() - started < 3.0


def test_sql_decode_deadline_closes_response(monkeypatch):
    monkeypatch.setattr(influx, "SQL_QUERY_TOTAL_TIMEOUT_SECONDS", 0.25)
    stream = install_stream(monkeypatch, SQLStream([b"[]"]))
    real_loads = json.loads

    def slow_loads(body):
        time.sleep(0.3)
        return real_loads(body)

    monkeypatch.setattr(influx.json, "loads", slow_loads)
    with pytest.raises(influx.InfluxConfigurationError, match="total deadline"):
        query_sql()
    assert stream.closed


@pytest.mark.parametrize(
    "status, body, message",
    [
        (404, b"", "endpoint not found"),
        (500, b"", "HTTP 500"),
        (200, b"invalid", "invalid JSON"),
        (200, b"{}", "Invalid response"),
    ],
)
def test_sql_failures_close_response(monkeypatch, status, body, message):
    stream = install_stream(monkeypatch, SQLStream([body]), status=status)
    with pytest.raises(influx.InfluxConfigurationError, match=message):
        query_sql()
    assert stream.closed


def test_sql_exact_byte_limit_and_split_json_keep_series(monkeypatch, settings):
    settings.INFLUX_TOKEN = "test-secret"
    settings.INFLUX_URL = "http://influx.local"
    rows = [
        {"time": NOW.isoformat(), "value": 20, "mountpoint": "/"},
        {"time": NOW.isoformat(), "value": 30, "mountpoint": "/data"},
    ]
    body = json.dumps(rows).encode()
    monkeypatch.setattr(influx, "SQL_QUERY_MAX_BODY_BYTES", len(body))
    stream = install_stream(monkeypatch, SQLStream([body[:10], body[10:]]))
    samples = query_sql()
    assert [s.value for s in samples] == [20, 30]
    assert [s.tags for s in samples] == [{"mountpoint": "/"}, {"mountpoint": "/data"}]
    assert stream.closed


@pytest.mark.parametrize("error_type", [httpx.ConnectError, httpx.ReadTimeout])
def test_sql_transport_error_preserves_safe_category(monkeypatch, error_type):
    real_client = httpx.AsyncClient

    def handle(request):
        raise error_type("secret-url-or-body", request=request)

    monkeypatch.setattr(
        influx.httpx,
        "AsyncClient",
        lambda **kwargs: real_client(transport=httpx.MockTransport(handle), **kwargs),
    )
    with pytest.raises(influx.InfluxConfigurationError) as caught:
        query_sql()
    assert str(caught.value) == f"InfluxDB SQL query failed: {error_type.__name__}"
    assert "secret-url-or-body" not in str(caught.value)


def test_sql_invalid_header_is_not_reported_as_invalid_json(monkeypatch, settings):
    settings.INFLUX_TOKEN = "pasted-smart-quote-’"
    stream = install_stream(monkeypatch, SQLStream([b"[]"]))
    with pytest.raises(influx.InfluxConfigurationError) as caught:
        query_sql()
    assert str(caught.value) == "InfluxDB SQL query failed: UnicodeEncodeError"
    assert stream.reads == 0
