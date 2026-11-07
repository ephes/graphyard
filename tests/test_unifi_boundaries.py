from unittest.mock import Mock

import httpx
import pytest

from graphyard import services
from graphyard.models import MetricCollectionSpec

REAL_CLIENT = httpx.Client


def run(monkeypatch, handler, **config):
    monkeypatch.setattr(
        services.httpx,
        "Client",
        lambda **kw: REAL_CLIENT(transport=httpx.MockTransport(handler), **kw),
    )
    monkeypatch.setattr(services, "touch_registry_from_points", Mock())
    write = Mock(return_value=2)
    monkeypatch.setattr(services.influx, "write_points", write)
    spec = MetricCollectionSpec(
        name="synthetic",
        config={
            "base_url": "https://controller.example",
            "username": "invented-user",
            "password": "invented-password",
            "device_name": "synthetic",
            "subject_id": "synthetic",
            "auth_mode": "legacy",
            **config,
        },
    )
    return services._execute_unifi_device_traffic_spec(spec), write


@pytest.mark.parametrize("stage", ["login", "device"])
@pytest.mark.parametrize("status", [307, 308])
@pytest.mark.parametrize(
    "target", ["https://foreign.example/leak", "http://controller.example/leak"]
)
def test_credentials_never_reach_another_origin(monkeypatch, stage, status, target):
    foreign = []

    def handler(request):
        if request.url.host != "controller.example" or request.url.scheme != "https":
            foreign.append(request)
            return httpx.Response(200, json={"data": []})
        if stage == "device" and request.url.path == "/api/login":
            return httpx.Response(
                200, json={}, headers={"x-csrf-token": "invented-csrf"}
            )
        return httpx.Response(status, headers={"location": target})

    outcome, write = run(monkeypatch, handler)
    assert not foreign
    assert outcome[0] == "critical"
    assert "origin" in outcome[3]
    write.assert_not_called()


def test_same_origin_redirects_and_session_headers_work(monkeypatch):
    seen = []

    def handler(request):
        seen.append(request)
        if request.url.path in ["/api/login", "/api/s/default/stat/device"]:
            return httpx.Response(
                307, headers={"location": str(request.url) + "-final"}
            )
        if request.method == "POST":
            assert b"invented-password" in request.content
            return httpx.Response(
                200,
                json={},
                headers={
                    "x-csrf-token": "invented-csrf",
                    "set-cookie": "session=invented; Path=/",
                },
            )
        assert request.headers["x-csrf-token"] == "invented-csrf"
        assert "session=invented" in request.headers["cookie"]
        return httpx.Response(
            200,
            json={
                "data": [
                    {"name": "synthetic", "uplink": {"rx_bytes-r": 1, "tx_bytes-r": 2}}
                ]
            },
        )

    outcome, write = run(monkeypatch, handler)
    assert outcome[:3] == ("ok", 2, 0)
    assert len(seen) == 4
    write.assert_called_once()


@pytest.mark.parametrize("stage", ["login", "device", "redirect", "combined"])
def test_whole_collection_body_budget(monkeypatch, stage):
    def handler(request):
        if stage == "redirect":
            return httpx.Response(
                307, content=b"x" * 129, headers={"location": "/next"}
            )
        if request.method == "POST":
            return httpx.Response(
                200, content=b" " * (129 if stage == "login" else 70) + b"{}"
            )
        return httpx.Response(
            200, content=b" " * (129 if stage == "device" else 70) + b'{"data": []}'
        )

    outcome, write = run(monkeypatch, handler, max_body_bytes=128)
    assert outcome[0] == "critical"
    assert "_PageProbeLimitExceeded" in outcome[3]
    write.assert_not_called()


def test_collection_deadline_includes_fallback(monkeypatch):
    import time

    seen = []

    def handler(request):
        seen.append(request.url.path)
        time.sleep(0.04)
        if request.url.path == "/api/login":
            return httpx.Response(404, json={})
        return httpx.Response(200, json={"data": []})

    outcome, write = run(
        monkeypatch, handler, auth_mode="auto", total_timeout_seconds=0.06
    )
    assert outcome[0] == "critical"
    assert "_PageProbeDeadlineExceeded" in outcome[3]
    write.assert_not_called()
    assert len(seen) == 2


def test_unsolicited_compression_is_refused_before_decode(monkeypatch):
    # Invalid bytes also prove that no gzip decoder was invoked.
    outcome, write = run(
        monkeypatch,
        lambda request: httpx.Response(
            200,
            stream=httpx.ByteStream(b"not compressed"),
            headers={"content-encoding": "gzip"},
        ),
    )
    assert outcome[0] == "critical"
    assert outcome[3] == "UniFi compressed response refused"
    write.assert_not_called()


def test_collection_deadline_also_covers_device_read(monkeypatch):
    import time

    seen = []

    def handler(request):
        seen.append(request.url.path)
        time.sleep(0.04 if request.method == "GET" else 0.01)
        return httpx.Response(
            404 if request.url.path == "/api/login" else 200, json={"data": []}
        )

    outcome, write = run(
        monkeypatch, handler, auth_mode="auto", total_timeout_seconds=0.05
    )
    assert outcome[0] == "critical"
    assert "_PageProbeDeadlineExceeded" in outcome[3]
    assert len(seen) == 3
    write.assert_not_called()
