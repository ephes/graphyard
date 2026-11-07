"""Synthetic transport regressions through the public query/write callers."""

import asyncio
from datetime import UTC, datetime
import gzip
import json
import time

import httpx
import pytest

from graphyard import influx


REAL_CLIENT = httpx.AsyncClient

NOW = datetime(2026, 1, 1, tzinfo=UTC)
CSV = (
    b"#datatype,string,long,dateTime:RFC3339,double\n"
    b"#group,false,false,false,false\n#default,_result,,,\n"
    b",result,table,_time,_value\n,,0,2026-01-01T00:00:00Z,2\n"
)


class Stream(httpx.AsyncByteStream):
    def __init__(self, chunks, delay=0):
        self.chunks = chunks
        self.delay = delay
        self.reads = 0
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            self.reads += 1
            await asyncio.sleep(self.delay)
            yield chunk

    async def aclose(self):
        self.closed = True


@pytest.fixture(autouse=True)
def config(settings):
    settings.INFLUX_URL = "http://influx.local"
    settings.INFLUX_TOKEN = "synthetic"
    settings.INFLUX_ORG = "test"
    settings.INFLUX_BUCKET = "test"
    settings.INFLUX_API_MODE = "v2"


def install(monkeypatch, stream, status=200, encoding=None, header_delay=0):
    requests = []
    real_client = REAL_CLIENT

    async def handle(request):
        requests.append(request)
        await asyncio.sleep(header_delay)
        return httpx.Response(
            status,
            stream=stream,
            headers={"Content-Encoding": encoding} if encoding else {},
        )

    monkeypatch.setattr(
        influx.httpx,
        "AsyncClient",
        lambda **kw: real_client(transport=httpx.MockTransport(handle), **kw),
    )
    return requests


def call(write):
    if write:
        return influx.write_points(
            [
                influx.MetricPoint(
                    ts=NOW,
                    metric="m",
                    value=2,
                    subject_type="host",
                    subject_id="test",
                    source_system="test",
                    collector_service="test",
                    collector_host="test",
                )
            ]
        )
    return influx.query_range("m", NOW, NOW)


@pytest.mark.parametrize("write", [False, True])
@pytest.mark.parametrize("status", [200, 500, 404])
def test_decoded_limit_closes_early_and_never_replays(monkeypatch, write, status):
    monkeypatch.setattr(
        influx, "WRITE_MAX_BODY_BYTES" if write else "FLUX_QUERY_MAX_BODY_BYTES", 5
    )
    stream = Stream([b"123", b"456", b"never read"])
    requests = install(monkeypatch, stream, status)
    with pytest.raises(influx.InfluxTransferError, match="decoded byte limit") as err:
        call(write)
    assert stream.closed and stream.reads == 2 and len(requests) == 1
    if write:
        assert "acceptance uncertain; not retried" in str(err.value)


@pytest.mark.parametrize("write", [False, True])
def test_gzip_limit_counts_decoded_bytes(monkeypatch, write):
    monkeypatch.setattr(
        influx, "WRITE_MAX_BODY_BYTES" if write else "FLUX_QUERY_MAX_BODY_BYTES", 100
    )
    stream = Stream([gzip.compress(b"x" * 1000)])
    install(monkeypatch, stream, encoding="gzip")
    with pytest.raises(influx.InfluxTransferError, match="decoded byte limit"):
        call(write)
    assert stream.closed


@pytest.mark.parametrize("write", [False, True])
@pytest.mark.parametrize("status", [200, 500])
@pytest.mark.parametrize("headers", [False, True])
def test_whole_exchange_deadline(monkeypatch, write, status, headers):
    monkeypatch.setattr(
        influx,
        "WRITE_TOTAL_TIMEOUT_SECONDS" if write else "FLUX_QUERY_TOTAL_TIMEOUT_SECONDS",
        0.1,
    )
    stream = Stream([b"x"] * 100, delay=0.02)
    requests = install(monkeypatch, stream, status, header_delay=1 if headers else 0)
    started = time.monotonic()
    with pytest.raises(influx.InfluxTransferError, match="total deadline"):
        call(write)
    assert time.monotonic() - started < 2
    assert len(requests) == 1
    if not headers:
        assert stream.closed and stream.reads < 100


def test_valid_csv_and_204_write(monkeypatch):
    requests = install(monkeypatch, Stream([CSV]))
    assert call(False)[0].value == 2
    assert json.loads(requests[0].content)["dialect"]["annotations"] == [
        "datatype",
        "group",
        "default",
    ]
    install(monkeypatch, Stream([]), status=204)
    assert call(True) == 1


def test_bounded_404_falls_back_but_oversize_404_does_not(monkeypatch, settings):
    settings.INFLUX_API_MODE = "auto"
    install(monkeypatch, Stream([b"missing"]), status=404)
    fallback = []
    monkeypatch.setattr(
        influx, "_query_range_v3_sql", lambda *a, **kw: fallback.append(True) or []
    )
    assert call(False) == [] and fallback == [True]
    monkeypatch.setattr(influx, "FLUX_QUERY_MAX_BODY_BYTES", 1)
    install(monkeypatch, Stream([b"missing"]), status=404)
    with pytest.raises(influx.InfluxTransferError):
        call(False)
    assert fallback == [True]


def test_write_limit_diagnostic_is_bounded(monkeypatch):
    monkeypatch.setattr(influx, "WRITE_MAX_BODY_BYTES", 2048)
    install(monkeypatch, Stream([b"x" * 3000]), status=500)
    with pytest.raises(influx.InfluxTransferError) as err:
        call(True)
    assert len(str(err.value)) < 1200


@pytest.mark.parametrize("connected", [False, True])
def test_write_transport_error_distinguishes_unsent_from_uncertain(
    monkeypatch, connected
):
    from urllib3.exceptions import NewConnectionError, ProtocolError

    class Broken(Stream):
        async def __aiter__(self):
            yield b"diagnostic"
            raise httpx.ReadError("synthetic interrupted response")

    requests = []

    def handle(request):
        requests.append(request)
        if not connected:
            raise httpx.ConnectError("synthetic connection refused")
        return httpx.Response(200, stream=Broken([]))

    monkeypatch.setattr(
        influx.httpx,
        "AsyncClient",
        lambda **kw: REAL_CLIENT(transport=httpx.MockTransport(handle), **kw),
    )
    with pytest.raises(ProtocolError if connected else NewConnectionError) as err:
        call(True)
    assert len(requests) == 1
    assert ("acceptance uncertain" in str(err.value)) is connected
    assert "request not submitted" not in str(err.value)


def test_write_http_error_keeps_only_small_diagnostic(monkeypatch):
    from influxdb_client.rest import ApiException

    install(monkeypatch, Stream([b"x" * 5000]), status=500)
    with pytest.raises(ApiException) as err:
        call(True)
    assert len(err.value.body) == 1024


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("dns", "NameResolutionError"),
        ("tls", "ApiException"),
        ("connect_timeout", "ConnectTimeoutError"),
    ],
)
@pytest.mark.parametrize("write", [False, True])
def test_public_transport_error_categories(monkeypatch, kind, expected, write):
    import socket
    import ssl

    def handle(request):
        if kind == "connect_timeout":
            raise httpx.ConnectTimeout("synthetic timeout")
        cause = (
            socket.gaierror(-2, "synthetic DNS")
            if kind == "dns"
            else ssl.SSLError("synthetic TLS")
        )
        raise httpx.ConnectError("synthetic connect") from cause

    monkeypatch.setattr(
        influx.httpx,
        "AsyncClient",
        lambda **kw: REAL_CLIENT(transport=httpx.MockTransport(handle), **kw),
    )
    with pytest.raises(Exception) as err:
        call(write)
    assert type(err.value).__name__ == expected
    assert "acceptance uncertain" not in str(err.value)
    if kind == "tls":
        assert err.value.status == 0
        assert "SSLError" in err.value.reason


@pytest.mark.django_db
def test_ingest_budget_failure_is_persistence_failure(client, monkeypatch):
    from django.urls import reverse
    from graphyard.models import IngestToken, PipelineHeartbeat

    monkeypatch.setattr("graphyard.views.timezone.now", lambda: NOW)
    credential = IngestToken(name="synthetic")
    credential.set_token("synthetic-credential")
    credential.save()
    monkeypatch.setattr(influx, "WRITE_MAX_BODY_BYTES", 5)
    requests = install(monkeypatch, Stream([b"too large"]))
    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=[
            {
                "ts": NOW.isoformat(),
                "host": "test",
                "metric": "m",
                "value": 2,
            }
        ],
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer " + "synthetic-credential",
    )
    assert response.status_code == 503
    assert response.json()["error"] == "Failed to persist metrics"
    assert (
        "acceptance uncertain"
        in PipelineHeartbeat.objects.get(name="metric_ingest").last_error
    )
    assert len(requests) == 1
