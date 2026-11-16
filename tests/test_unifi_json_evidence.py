"""UniFi evidence must be unambiguous before either sink is called."""

from unittest.mock import Mock

import httpx
import pytest

from graphyard import services
from graphyard.models import MetricCollectionSpec


def collect(monkeypatch, login=b"{}", device=b"{}"):
    original = httpx.Client
    requests = []

    def respond(request):
        requests.append(request.url.path)
        return httpx.Response(
            200, content=login if request.method == "POST" else device
        )

    monkeypatch.setattr(
        services.httpx,
        "Client",
        lambda **kw: original(**kw, transport=httpx.MockTransport(respond)),
    )
    write = Mock(side_effect=lambda points: len(points))
    registry = Mock()
    monkeypatch.setattr(services.influx, "write_points", write)
    monkeypatch.setattr(services, "touch_registry_from_points", registry)
    result = services._execute_unifi_device_traffic_spec(
        MetricCollectionSpec(
            name="synthetic",
            config={
                "base_url": "http://controller.test",
                "username": "invented",
                "password": "invented",
                "device_name": "synthetic",
                "subject_id": "synthetic",
            },
        )
    )
    return result, write, registry, requests


VALID = b'{"data":[{"name":"synthetic","uplink":{"rx_bytes-r":1,"tx_bytes-r":2}}]}'


@pytest.mark.parametrize(
    "body",
    [
        b'{"meta":{"rc":"error","rc":"ok"}}',
        b'{"meta":{"rc":"error"},"meta":{"rc":"ok"}}',
        b'{"meta":{"rc":"error","r\\u0063":"ok"}}',
    ],
)
def test_duplicate_login_never_uses_legacy_success_fallback(monkeypatch, body):
    result, write, registry, requests = collect(monkeypatch, login=body, device=VALID)
    assert result == ("critical", 0, 0, "collector failure: ValueError")
    assert requests == ["/api/login"]
    write.assert_not_called()
    registry.assert_not_called()


@pytest.mark.parametrize(
    "body",
    [
        b'{"data":[],"data":[{"name":"synthetic","uplink":{"rx_bytes-r":1,"tx_bytes-r":2}}]}',
        VALID.replace(b'"name":"synthetic"', b'"name":"other","name":"synthetic"'),
        VALID.replace(b'"rx_bytes-r":1', b'"rx_bytes-r":999,"rx_bytes-r":1'),
        VALID.replace(
            b'"name":"synthetic"', b'"_id":"other","_id":"synthetic","name":"synthetic"'
        ),
    ],
)
def test_duplicate_device_evidence_is_atomic(monkeypatch, body):
    result, write, registry, _ = collect(monkeypatch, device=body)
    assert result == ("critical", 0, 0, "collector failure: ValueError")
    write.assert_not_called()
    registry.assert_not_called()


@pytest.mark.parametrize(
    "body",
    [
        b'{"data":[' + b"{}," * 1024 + b"{}]}",
        VALID[:-1] + b',"ignored":[' + b"0," * 1024 + b"0]}",
        VALID[:-1] + b',"ignored":' + b"[" * 33 + b"0" + b"]" * 33 + b"}",
        VALID.replace(
            b'"uplink":{', b'"port_table":[' + b"{}," * 1024 + b'{}],"uplink":{'
        ),
        VALID.replace(b'"uplink":{', b'"uplink":{"speed":{"nested":[1,2,3]},'),
        VALID.replace(
            b'"name":"synthetic"', b'"name":' + b"[" * 5 + b'"synthetic"' + b"]" * 5
        ),
        VALID.replace(b'"uplink":{', b'"uplink":{"name":"' + b"x" * 257 + b'",'),
        VALID[:-1]
        + b',"ignored":['
        + b",".join([b"[" + b",".join([b"0"] * 1000) + b"]"] * 101)
        + b"]}",
    ],
)
def test_response_expansion_refuses_before_selection(monkeypatch, body):
    # A refused response must not even reach device selection/string conversion.
    find = Mock(wraps=services._find_unifi_device)
    monkeypatch.setattr(services, "_find_unifi_device", find)
    result, write, registry, _ = collect(monkeypatch, device=body)
    assert result == ("critical", 0, 0, "collector failure: ValueError")
    find.assert_not_called()
    write.assert_not_called()
    registry.assert_not_called()


def test_normal_array_and_tag_boundary_is_accepted(monkeypatch):
    body = VALID.replace(
        b'"uplink":{',
        b'"port_table":['
        + b"{}," * 1023
        + b'{}],"uplink":{"name":"'
        + b"x" * 256
        + b'",',
    )
    result, write, registry, _ = collect(monkeypatch, device=body)
    assert result[:3] == ("ok", 2, 0)
    assert write.call_count == registry.call_count == 1


@pytest.mark.parametrize(
    "rx,tx,valid",
    [
        (-1, 2, 1),
        (1, -2, 1),
        (-1, -2, 0),
        (None, 2, 1),
        ("NaN", 2, 1),
        (1, None, 1),
        (None, None, 0),
        (0, 0, 2),
        (1, 2, 2),
    ],
)
def test_required_directions_and_nonnegative_rate_domain(monkeypatch, rx, tx, valid):
    import json

    body = json.dumps(
        {
            "data": [
                {"name": "synthetic", "uplink": {"rx_bytes-r": rx, "tx_bytes-r": tx}}
            ]
        }
    ).encode()
    result, write, registry, _ = collect(monkeypatch, device=body)
    assert result[:3] == ("ok" if valid == 2 else "warning", valid, 2 - valid)
    if valid:
        assert all(point.value >= 0 for point in write.call_args.args[0])
        assert registry.call_count == 1
    else:
        write.assert_not_called()
        registry.assert_not_called()
