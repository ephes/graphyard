import gzip
import time

import httpx
import pytest

from graphyard.models import MetricCollectionSpec, MetricCollectionSpecType, StatusLevel
from graphyard.services import run_metric_collection_specs_once
from test_page_probe_deadline import _dripping_server


def create_spec(url, **config):
    return MetricCollectionSpec.objects.create(
        name="bounded json",
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        config={
            "url": url,
            "metric_path": "$.value",
            "metric_name": "queue.depth",
            **config,
        },
    )


@pytest.mark.django_db
@pytest.mark.parametrize("oversize", [False, True])
@pytest.mark.parametrize("compressed", [False, True])
def test_streaming_json_byte_budget(monkeypatch, oversize, compressed):
    spec = create_spec("https://example.test", max_body_bytes=64)
    captured = []
    original = httpx.Client
    body = b'{"value": 7}' + (b" " * 4096 if oversize else b"")
    if compressed:
        assert len(gzip.compress(body)) <= 64
    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: original(
            **kwargs,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    content=gzip.compress(body) if compressed else body,
                    headers={"Content-Encoding": "gzip"} if compressed else {},
                )
            ),
        ),
    )
    monkeypatch.setattr(
        "graphyard.services.influx.write_points",
        lambda points: captured.extend(points) or len(points),
    )
    result = run_metric_collection_specs_once()
    spec.refresh_from_db()
    assert result.ingested == (0 if oversize else 1)
    assert len(captured) == (0 if oversize else 1)
    assert spec.last_status == (StatusLevel.CRITICAL if oversize else StatusLevel.OK)
    if oversize:
        assert "max_body_bytes=64" in spec.last_error


@pytest.mark.django_db
@pytest.mark.parametrize(
    "config",
    [
        {"request_timeout_seconds": 1, "total_timeout_seconds": 0.4},
        {"request_timeout_seconds": 0.5},
    ],
)
def test_dripping_json_total_deadline(monkeypatch, config):
    captured = []
    monkeypatch.setattr(
        "graphyard.services.influx.write_points",
        lambda points: captured.extend(points) or len(points),
    )
    with _dripping_server(
        prefix=b'HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{"value":', drip=b" "
    ) as url:
        spec = create_spec(url, **config)
        started = time.monotonic()
        result = run_metric_collection_specs_once()
        elapsed = time.monotonic() - started
    spec.refresh_from_db()
    assert elapsed < 3
    if "total_timeout_seconds" not in config:
        assert elapsed >= 1.4
    assert result.failed == 1
    assert not captured
    assert "total_timeout_seconds" in spec.last_error


@pytest.mark.django_db
@pytest.mark.parametrize(
    "auth, expected",
    [
        ({"bearer_token": "test-bearer"}, "Bearer test-bearer"),
        ({"basic_username": "user", "basic_password": "pass"}, "Basic dXNlcjpwYXNz"),
    ],
)
def test_json_stream_preserves_authentication_and_tls(monkeypatch, auth, expected):
    create_spec("https://example.test", verify_tls=False, **auth)
    original = httpx.Client
    observed = []

    def factory(**kwargs):
        assert kwargs["verify"] is False

        def handler(request):
            observed.append(request.headers["Authorization"])
            assert request.headers["Accept"] == "application/json"
            return httpx.Response(200, json={"value": 7})

        return original(**kwargs, transport=httpx.MockTransport(handler))

    monkeypatch.setattr("graphyard.services.httpx.Client", factory)
    monkeypatch.setattr(
        "graphyard.services.influx.write_points", lambda points: len(points)
    )
    assert run_metric_collection_specs_once().ingested == 1
    assert observed == [expected]
