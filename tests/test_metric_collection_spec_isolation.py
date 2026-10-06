from __future__ import annotations


import httpx
import pytest
from django.core.exceptions import ValidationError

from graphyard import services
from graphyard.models import (
    MetricCollectionSpec,
    MetricCollectionSpecType,
    PipelineHeartbeat,
    StatusLevel,
)
from graphyard.services import run_metric_collection_specs_once
from graphyard.spec_config import validate_spec_config


class _JsonResponse:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return {"value": 7}


class _JsonClient:
    def __init__(self, **kwargs: object) -> None:
        del kwargs

    def __enter__(self) -> _JsonClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def get(self, url: str, **kwargs: object) -> _JsonResponse:
        del url, kwargs
        return _JsonResponse()


def _json_spec(name: str, **config: object) -> MetricCollectionSpec:
    return MetricCollectionSpec.objects.create(
        name=name,
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        interval_seconds=60,
        config={
            "url": "https://example.test/metrics",
            "metric_path": "$.value",
            "metric_name": "service.example_value",
            "service_id": "example",
            **config,
        },
    )


@pytest.fixture
def fake_json_collection(monkeypatch):
    monkeypatch.setattr("graphyard.services.httpx.Client", _JsonClient)
    monkeypatch.setattr(
        "graphyard.services.influx.write_points", lambda points: len(points)
    )


def test_bad_spec_does_not_stop_later_specs(db, fake_json_collection):
    # Created directly (bypassing full_clean) like a spec saved before validation.
    bad = _json_spec("a bad timeout", request_timeout_seconds="5s")
    good = _json_spec("b good")

    result = run_metric_collection_specs_once(due_only=True)

    assert result.total == 2
    assert result.failed == 1
    assert result.ingested == 1

    bad.refresh_from_db()
    good.refresh_from_db()
    assert bad.last_status == StatusLevel.CRITICAL
    assert "request_timeout_seconds" in bad.last_error
    assert bad.last_run_at is not None
    assert bad.next_run_time > 0
    assert good.last_status == StatusLevel.OK
    assert good.last_run_at is not None

    heartbeat = PipelineHeartbeat.objects.get(name="metric_collectors")
    assert heartbeat.status == StatusLevel.WARNING
    assert "a bad timeout" in heartbeat.last_error

    # The bad spec is no longer due, so the next tick does not abort on it.
    assert run_metric_collection_specs_once(due_only=True).total == 0


def test_unexpected_collector_exception_is_isolated(
    db, fake_json_collection, monkeypatch
):
    crashing = _json_spec("a crashing")
    good = _json_spec("b good")
    real = services._SPEC_EXECUTORS[MetricCollectionSpecType.HTTP_JSON_METRIC]

    def _dispatch(spec):
        if spec.name == "a crashing":
            raise RuntimeError("collector bug")
        return real(spec)

    monkeypatch.setitem(
        services._SPEC_EXECUTORS, MetricCollectionSpecType.HTTP_JSON_METRIC, _dispatch
    )

    result = run_metric_collection_specs_once()

    assert result.total == 2
    assert result.failed == 1
    crashing.refresh_from_db()
    good.refresh_from_db()
    assert crashing.last_status == StatusLevel.CRITICAL
    assert crashing.last_error == "collector raised RuntimeError: collector bug"
    assert crashing.next_run_time > 0
    assert good.last_status == StatusLevel.OK


@pytest.mark.parametrize(
    ("config", "message"),
    [
        ({"request_timeout_seconds": "5s"}, "request_timeout_seconds"),
        ({"request_timeout_seconds": None}, "request_timeout_seconds"),
        ({"request_timeout_seconds": 0}, "request_timeout_seconds"),
        ({"request_timeout_seconds": True}, "request_timeout_seconds"),
        ({"verify_tls": "false"}, "verify_tls"),
        ({"follow_redirects": "true"}, "follow_redirects"),
        ({"max_body_bytes": 1.5}, "max_body_bytes"),
        ({"request_timeout_seconds": 10**400}, "request_timeout_seconds"),
        ({"max_body_bytes": "\u00b2"}, "max_body_bytes"),
        ({"max_body_bytes": "\u0661"}, "max_body_bytes"),
    ],
)
def test_full_clean_rejects_malformed_config(db, config, message):
    spec = MetricCollectionSpec(
        name="probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        config={"url": "https://example.test/", "subject_id": "example", **config},
    )
    with pytest.raises(ValidationError) as excinfo:
        spec.full_clean()
    assert message in str(excinfo.value.message_dict["config"])


def test_full_clean_accepts_valid_config(db):
    spec = MetricCollectionSpec(
        name="probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        config={
            "url": "https://example.test/",
            "subject_id": "example",
            "request_timeout_seconds": "15",
            "max_body_bytes": 1024,
            "verify_tls": False,
            "follow_redirects": True,
        },
    )
    spec.full_clean()


def test_validate_spec_config_rejects_non_object():
    assert validate_spec_config(MetricCollectionSpecType.HTTP_JSON_METRIC, []) == [
        "config must be an object"
    ]


class _DripResponse:
    status_code = 200
    next_request = None

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def iter_bytes(self):
        yield from self._chunks

    def close(self) -> None:
        return None


class _DripRequest:
    def __init__(self) -> None:
        self.extensions: dict[str, object] = {}


class _DripClient:
    max_redirects = 20

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = chunks

    def __enter__(self) -> _DripClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def build_request(self, method: str, url: str) -> _DripRequest:
        del method, url
        return _DripRequest()

    def send(self, request, *, stream: bool, follow_redirects: bool) -> _DripResponse:
        del request, stream, follow_redirects
        return _DripResponse(self._chunks)


def _probe_spec(**config: object) -> MetricCollectionSpec:
    return MetricCollectionSpec.objects.create(
        name="drip probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        interval_seconds=300,
        config={"url": "https://example.test/", "subject_id": "example", **config},
    )


def _capture_points(monkeypatch) -> list[object]:
    captured: list[object] = []

    def _write(points):
        captured.extend(points)
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _write)
    return captured


def test_page_probe_body_cap_records_failure(db, monkeypatch):
    spec = _probe_spec(max_body_bytes=4)
    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _DripClient([b"abc", b"def"]),
    )
    captured = _capture_points(monkeypatch)

    result = run_metric_collection_specs_once()

    assert result.warning == 1
    assert {point.metric: point.value for point in captured} == {
        "service.http_page_status_code": 0.0,
        "service.http_page_success": 0.0,
    }
    spec.refresh_from_db()
    assert "max_body_bytes=4" in spec.last_error


def _mock_transport_client(monkeypatch, handler) -> None:
    real_client = httpx.Client

    def _factory(**kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(**kwargs)

    monkeypatch.setattr("graphyard.services.httpx.Client", _factory)


def test_page_probe_redirect_body_counts_toward_cap(db, monkeypatch):
    spec = _probe_spec(max_body_bytes=4)

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/":
            return httpx.Response(
                301, headers={"Location": "/final"}, content=b"x" * 20
            )
        return httpx.Response(200, content=b"ok")

    _mock_transport_client(monkeypatch, _handler)
    captured = _capture_points(monkeypatch)

    result = run_metric_collection_specs_once()

    assert result.warning == 1
    assert {point.metric: point.value for point in captured} == {
        "service.http_page_status_code": 0.0,
        "service.http_page_success": 0.0,
    }
    spec.refresh_from_db()
    assert "max_body_bytes=4" in spec.last_error


def test_page_probe_follows_redirects_with_real_client(db, monkeypatch):
    spec = _probe_spec()
    seen: list[str] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path == "/":
            return httpx.Response(302, headers={"Location": "/final"})
        return httpx.Response(200, content=b"<html>")

    _mock_transport_client(monkeypatch, _handler)
    captured = _capture_points(monkeypatch)

    result = run_metric_collection_specs_once()

    assert result.warning == 0
    assert seen == ["/", "/final"]
    by_metric = {point.metric: point.value for point in captured}
    assert by_metric["service.http_page_status_code"] == 200.0
    assert by_metric["service.http_page_redirect_count"] == 1.0
    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK
