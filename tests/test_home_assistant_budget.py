"""Synthetic HA transports: no real sockets, sleeps or elapsed-time assertions."""

import gzip
import json

import httpx
import pytest

from graphyard import services
from graphyard.models import MetricCollectionSpec, MetricCollectionSpecType, StatusLevel
from graphyard.spec_config import validate_spec_config


TYPES = [
    MetricCollectionSpecType.HOME_ASSISTANT_SENSOR,
    MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
]
STATE = {"entity_id": "sensor.temperature", "state": "7", "attributes": {}}


class Stream(httpx.SyncByteStream):
    def __init__(self, body, clock, *, drip=False):
        self.body = body
        self.clock = clock
        self.drip = drip
        self.closed = False

    def __iter__(self):
        if self.drip:
            yield self.body[:1]
            self.clock[0] += 31
            yield self.body[1:]
        else:
            yield self.body

    def close(self):
        self.closed = True


@pytest.mark.django_db
@pytest.mark.parametrize("spec_type", TYPES)
@pytest.mark.parametrize("total_timeout", [None, 5])
@pytest.mark.parametrize("mode", ["exact", "bytes", "gzip", "drip", "headers", "parse"])
def test_ha_budgets_and_next_spec(monkeypatch, spec_type, mode, total_timeout):
    payload = STATE if spec_type == TYPES[0] else [STATE]
    body = json.dumps(payload).encode()
    limit = len(body)
    if mode in {"bytes", "gzip"}:
        body += b" " * 4096
    if mode == "gzip":
        body = gzip.compress(body)
    clock = [100.0]
    stream = Stream(body, clock, drip=mode == "drip")
    original_client = httpx.Client
    observed = []

    def factory(**kwargs):
        assert kwargs["verify"] is False
        assert kwargs["timeout"] == 10

        def handler(request):
            observed.append(str(request.url))
            assert request.headers["Authorization"] == "Bearer test-ha"
            assert request.headers["Accept"] == "application/json"
            if mode == "headers":
                clock[0] += 31
            return httpx.Response(
                200,
                stream=stream,
                headers={"Content-Encoding": "gzip"} if mode == "gzip" else {},
            )

        return original_client(**kwargs, transport=httpx.MockTransport(handler))

    monkeypatch.setattr(services.httpx, "Client", factory)
    monkeypatch.setattr(services.time, "monotonic", lambda: clock[0])
    if mode == "parse":
        original_loads = json.loads

        def loads(body, **kwargs):
            result = original_loads(body, **kwargs)
            if isinstance(body, bytearray):
                clock[0] += 31
            return result

        monkeypatch.setattr(services.json, "loads", loads)
    written = []
    monkeypatch.setattr(
        services.influx,
        "write_points",
        lambda points: written.extend(points) or len(points),
    )
    spec = MetricCollectionSpec.objects.create(
        name="a ha",
        spec_type=spec_type,
        config={
            "base_url": "https://ha.test",
            "access_token": "test-ha",
            "entity_id": STATE["entity_id"],
            "verify_tls": False,
            "max_body_bytes": limit,
            **({"total_timeout_seconds": total_timeout} if total_timeout else {}),
        },
    )
    next_spec = MetricCollectionSpec.objects.create(
        name="z next",
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        config={},
    )
    next_calls = []
    monkeypatch.setitem(
        services._SPEC_EXECUTORS,
        next_spec.spec_type,
        lambda spec: next_calls.append(spec.pk) or (StatusLevel.OK, 0, 0, ""),
    )
    result = services.run_metric_collection_specs_once()
    spec.refresh_from_db()
    assert next_calls == [next_spec.pk]
    assert stream.closed
    assert observed == [
        "https://ha.test/api/states"
        + ("/sensor.temperature" if spec_type == TYPES[0] else "")
    ]
    if mode == "exact":
        assert spec.last_status == StatusLevel.OK
        assert result.ingested == 1
        assert len(written) == 1
    else:
        assert spec.last_status == StatusLevel.CRITICAL
        assert result.failed == 1
        assert not written
        assert spec.last_error == (
            f"response body exceeded max_body_bytes={limit}"
            if mode in {"bytes", "gzip"}
            else f"HTTP collector exceeded total_timeout_seconds={total_timeout or 30}"
        )


@pytest.mark.parametrize("spec_type", TYPES)
@pytest.mark.parametrize(
    "config", [{"total_timeout_seconds": 86401}, {"request_timeout_seconds": 30000}]
)
def test_ha_total_deadline_validation(spec_type, config):
    assert validate_spec_config(spec_type, config) == [
        "config.total_timeout_seconds must be <= 86400.0 seconds"
    ]
