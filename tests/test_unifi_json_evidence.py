"""UniFi evidence must be unambiguous before either sink is called."""

import sys
import tracemalloc
from unittest.mock import Mock

import httpx
import pytest

from graphyard import services
from graphyard.models import MetricCollectionSpec
from graphyard.unifi_json import MAX_ARRAY_ITEMS, MAX_NODES, int_digit_limit_exceeded

REFUSED = ("critical", 0, 0, "UniFi evidence refused")


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
    "value,exceeds_limit",
    [
        ("-" + "9" * 250_000, True),
        ("9" * 250_000 + "e+10", False),
        ('"escaped quote: \\"' + "9" * 250_000 + '"', False),
    ],
    ids=["integer", "exponent", "quoted"],
)
def test_digit_scan_has_bounded_temporary_allocation(monkeypatch, value, exceeds_limit):
    monkeypatch.setattr(sys, "get_int_max_str_digits", lambda: 4300)
    text = f"[{value}]"
    tracemalloc.start()
    try:
        result = int_digit_limit_exceeded(text)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert result is exceeds_limit
    assert peak < 64 * 1024


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
    assert result == REFUSED
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
    assert result == REFUSED
    write.assert_not_called()
    registry.assert_not_called()


@pytest.mark.parametrize(
    "body",
    [
        b'{"data":[' + b"{}," * MAX_ARRAY_ITEMS + b"{}]}",
        VALID[:-1] + b',"ignored":[' + b"0," * MAX_ARRAY_ITEMS + b"0]}",
        VALID[:-1] + b',"ignored":' + b"[" * 33 + b"0" + b"]" * 33 + b"}",
        VALID.replace(
            b'"uplink":{',
            b'"port_table":[' + b"{}," * MAX_ARRAY_ITEMS + b'{}],"uplink":{',
        ),
        VALID.replace(b'"uplink":{', b'"uplink":{"speed":{"nested":[1,2,3]},'),
        VALID.replace(
            b'"name":"synthetic"', b'"name":' + b"[" * 5 + b'"synthetic"' + b"]" * 5
        ),
        VALID.replace(b'"uplink":{', b'"uplink":{"name":"' + b"x" * 257 + b'",'),
        VALID[:-1]
        + b',"ignored":['
        + b",".join([b"[" + b",".join([b"0"] * 4000) + b"]"] * (MAX_NODES // 4000))
        + b"]}",
    ],
)
def test_response_expansion_refuses_before_selection(monkeypatch, body):
    # A refused response must not even reach device selection/string conversion.
    find = Mock(wraps=services._find_unifi_device)
    monkeypatch.setattr(services, "_find_unifi_device", find)
    result, write, registry, _ = collect(monkeypatch, device=body)
    assert result == REFUSED
    find.assert_not_called()
    write.assert_not_called()
    registry.assert_not_called()


def test_normal_array_and_tag_boundary_is_accepted(monkeypatch):
    body = VALID.replace(
        b'"uplink":{',
        b'"port_table":['
        + b"{}," * (MAX_ARRAY_ITEMS - 1)
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


def test_realistic_multi_device_site_is_admitted(monkeypatch):
    # Forty 52-port switches with full counters stay well inside the budget.
    import json

    port = {f"counter_{i}": i for i in range(60)}
    switch = {
        "name": "other",
        "port_table": [dict(port, port_idx=i, name=f"p{i}") for i in range(52)],
    }
    body = json.dumps({"data": [switch] * 40 + [json.loads(VALID)["data"][0]]}).encode()
    result, write, _, _ = collect(monkeypatch, device=body)
    assert result[:3] == ("ok", 2, 0)
    assert write.call_count == 1


def test_metadata_length_uses_the_decoded_value(monkeypatch):
    # 1.000...01 decodes to the float 1.0, as in the Rust collector.
    long_decimal = b"1." + b"0" * 300 + b"1"
    body = VALID.replace(b'"uplink":{', b'"uplink":{"speed":' + long_decimal + b",")
    result, write, _, _ = collect(monkeypatch, device=body)
    assert result[:3] == ("ok", 2, 0)
    assert write.call_args.args[0][0].tags["speed_mbps"] == "1.0"


def test_digit_limit_is_unparseable_wherever_it_appears(monkeypatch):
    # json.loads refuses the integer, so the legacy login fallback applies even
    # though a duplicate member precedes it; Rust scans the digits first too.
    login = b'{"meta":{"rc":"error","rc":"ok"},"n":' + b"9" * 5000 + b"}"
    result, write, _, requests = collect(monkeypatch, login=login, device=VALID)
    assert result[:3] == ("ok", 2, 0)
    assert requests == ["/api/login", "/api/s/default/stat/device"]


def test_device_digit_limit_is_a_decoder_error(monkeypatch):
    device = VALID[:-1] + b',"data2":[],"data2":[],"n":' + b"9" * 5000 + b"}"
    result, write, _, _ = collect(monkeypatch, device=device)
    assert result == ("critical", 0, 0, "collector failure: ValueError")
    write.assert_not_called()


@pytest.mark.parametrize(
    "body,message",
    [
        (b"<html>", "collector failure: JSONDecodeError"),
        # A later syntax error outranks an earlier refusal, as in Rust, even
        # when json.loads itself would exhaust the recursion limit.
        (b"[" * 10000 + b"]" * 9999, "collector failure: JSONDecodeError"),
        (b'{"data":[],"data":[]}x', "collector failure: JSONDecodeError"),
        (
            b'{"data":[' + b"{}," * MAX_ARRAY_ITEMS + b"{}],}",
            "collector failure: JSONDecodeError",
        ),
        (b'{"data":[\xff]}', "collector failure: UnicodeDecodeError"),
        # Accepted by json.loads, but never returned without admission.
        (
            b'{"data":[{"name":"synthetic","uplink":{"rx_bytes-r":NaN,"tx_bytes-r":2}}]}',
            "collector failure: JSONDecodeError",
        ),
        ('{"data":[]}'.encode("utf-16"), "collector failure: UnicodeDecodeError"),
    ],
)
def test_unparseable_device_bodies_keep_decoder_diagnostics(monkeypatch, body, message):
    result, write, _, _ = collect(monkeypatch, device=body)
    assert result == ("critical", 0, 0, message)
    write.assert_not_called()


@pytest.mark.parametrize(
    "login",
    [
        b'{"meta":{"rc":NaN}}',
        '{"meta":{"rc":"error"}}'.encode("utf-16"),
        b'{"meta":{"rc":"error","rc":"ok"}',
        b"[" * 10000 + b"]" * 9999,
    ],
)
def test_non_standard_login_bodies_are_unparseable_like_rust(monkeypatch, login):
    result, write, _, requests = collect(monkeypatch, login=login, device=VALID)
    assert result[:3] == ("ok", 2, 0)
    assert requests == ["/api/login", "/api/s/default/stat/device"]
