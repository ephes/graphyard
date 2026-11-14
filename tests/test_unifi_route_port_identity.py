"""Synthetic controller paths and strict, unique interface identities."""

from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from threading import Thread
from unittest.mock import Mock

import pytest

from graphyard import services
from graphyard.models import MetricCollectionSpec


@contextmanager
def controller(device):
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            seen.append(
                (self.path, self.rfile.read(int(self.headers["Content-Length"])))
            )
            self.reply({})

        def do_GET(self):
            seen.append((self.path, b""))
            self.reply({"data": [device]})

        def reply(self, payload):
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def collect(monkeypatch, base_url, **config):
    write = Mock(return_value=2)
    monkeypatch.setattr(services.influx, "write_points", write)
    monkeypatch.setattr(services, "touch_registry_from_points", Mock())
    outcome = services._execute_unifi_device_traffic_spec(
        MetricCollectionSpec(
            name="synthetic",
            config={
                "base_url": base_url,
                "username": "invented-user",
                "password": "invented-password",
                "device_name": "synthetic",
                "subject_id": "synthetic",
                "auth_mode": "legacy",
                **config,
            },
        )
    )
    return outcome, write


@pytest.mark.parametrize(
    "site",
    ["a/b", "../other", ".", "..", "a#b", "a?b", "%2f", "%2e%2e", "%252f", "a\\b"],
)
def test_invalid_site_sends_no_credentials(monkeypatch, site):
    with controller({}) as (origin, seen):
        outcome, write = collect(monkeypatch, origin, site_id=site)
    assert outcome[0] == "critical"
    assert seen == []
    write.assert_not_called()


@pytest.mark.parametrize("suffix", ["?other=1", "#other", "?", "#"])
def test_ambiguous_base_url_sends_no_credentials(monkeypatch, suffix):
    with controller({}) as (origin, seen):
        outcome, write = collect(monkeypatch, origin + suffix)
    assert outcome[0] == "critical"
    assert seen == []
    write.assert_not_called()


def test_normal_site_reaches_only_its_device_endpoint(monkeypatch):
    with controller(
        {"name": "synthetic", "uplink": {"rx_bytes-r": 1, "tx_bytes-r": 2}}
    ) as (origin, seen):
        outcome, write = collect(monkeypatch, origin, site_id="site_1-a.b")
    assert outcome[:3] == ("ok", 2, 0)
    assert [path for path, _ in seen] == ["/api/login", "/api/s/site_1-a.b/stat/device"]
    write.assert_called_once()


@pytest.mark.parametrize(
    "selector,ports",
    [
        ("port_idx:1", [{"port_idx": 1, "name": "a"}, {"port_idx": 1, "name": "b"}]),
        ("port_name:a", [{"port_idx": 1, "name": "a"}, {"port_idx": 2, "name": "a"}]),
        ("port_idx:1", [{"port_idx": True, "name": "a"}]),
        ("port_idx:1", [{"port_idx": 1.0, "name": "a"}]),
        ("port_name:a", [{"port_idx": True, "name": "a"}]),
        ("port_name:a", [{"port_idx": "1", "name": "a"}]),
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
def test_bad_selected_port_never_writes(monkeypatch, selector, ports, reverse):
    ports = list(reversed(ports)) if reverse else ports
    device = {
        "name": "synthetic",
        "port_table": [
            {**p, "rx_bytes-r": i + 1, "tx_bytes-r": i + 2} for i, p in enumerate(ports)
        ],
    }
    with controller(device) as (origin, _):
        outcome, write = collect(monkeypatch, origin, interface_selector=selector)
    assert outcome[0] == "critical"
    write.assert_not_called()


@pytest.mark.parametrize("selector", ["port_idx:1", "port_name:valid"])
@pytest.mark.parametrize("reverse", [False, True])
def test_typed_identity_selects_integer_not_boolean(selector, reverse):
    ports = [{"port_idx": True, "name": "bool"}, {"port_idx": 1, "name": "valid"}]
    ports = list(reversed(ports)) if reverse else ports
    stats, tags = services._resolve_unifi_interface_stats(
        {"port_table": ports}, interface_selector=selector
    )
    assert stats["name"] == "valid"
    assert tags["port_idx"] == "1"
