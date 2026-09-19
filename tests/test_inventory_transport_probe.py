"""Regressions for diagnostic failures; no Vector binary or application DB needed."""

import importlib.util
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest

spec = importlib.util.spec_from_file_location(
    "inventory_transport_probe",
    Path(__file__).resolve().parents[1] / "scripts/inventory_transport_probe.py",
)
assert spec is not None and spec.loader is not None
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


@contextmanager
def running(server):
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/inventory"
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=3)
        assert not worker.is_alive()


def rejected(url, value, credential=""):
    with httpx.Client(trust_env=False, timeout=5) as client:
        with pytest.raises(httpx.RemoteProtocolError):
            client.post(url, json=value, headers={"Authorization": credential})


def test_multi_report_batch_has_explicit_diagnostic():
    proxy = probe.FaultProxy(0)
    with running(proxy) as url:
        rejected(url, [probe.fixture(), probe.fixture()])
    assert proxy.errors == [
        {
            "stage": "read request",
            "error": "Expected exactly one report per HTTP request",
        }
    ]
    assert not proxy.http_statuses


def test_unresolved_writer_has_explicit_diagnostic():
    proxy = probe.FaultProxy(0)
    proxy.expected_auth = "Bearer expected"
    with running(proxy) as url:
        rejected(url, probe.fixture())
    assert proxy.errors == [
        {
            "stage": "read request",
            "error": "Vector did not resolve the configured writer secret",
        }
    ]


def test_non_json_backend_keeps_status_without_private_response_body():
    class BrokenBackend(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"private backend diagnostic")

    backend = ThreadingHTTPServer(("127.0.0.1", 0), BrokenBackend)
    proxy = probe.FaultProxy(backend.server_port)
    with running(backend), running(proxy) as url:
        rejected(url, probe.fixture())
    assert proxy.http_statuses == [500]
    assert proxy.errors == [
        {
            "stage": "read Graphyard response",
            "error": "Graphyard returned non-JSON HTTP 500",
        }
    ]
    assert "private backend" not in json.dumps(proxy.errors)
