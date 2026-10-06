"""Real-socket tests for the HTTP page probe's whole-probe deadline."""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager

import httpx
import pytest

from graphyard.models import MetricCollectionSpec, MetricCollectionSpecType, StatusLevel
from graphyard.services import run_metric_collection_specs_once
from graphyard.spec_config import validate_spec_config

DRIP_INTERVAL_SECONDS = 0.2
# The server gives up dripping after this long, so a regression fails the
# timing assertion instead of hanging the test run.
DRIP_GIVE_UP_SECONDS = 8.0


@contextmanager
def _dripping_server(*, prefix: bytes, drip: bytes) -> Iterator[str]:
    """Serve one connection: send ``prefix``, then ``drip`` one byte at a time."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    listener.settimeout(DRIP_GIVE_UP_SECONDS)
    port = listener.getsockname()[1]
    stop = threading.Event()

    def _serve() -> None:
        try:
            conn, _ = listener.accept()
        except OSError:
            return
        with conn:
            conn.settimeout(DRIP_GIVE_UP_SECONDS)
            try:
                conn.recv(65536)
                conn.sendall(prefix)
                give_up_at = time.monotonic() + DRIP_GIVE_UP_SECONDS
                index = 0
                while not stop.is_set() and time.monotonic() < give_up_at:
                    conn.sendall(drip[index % len(drip) : index % len(drip) + 1])
                    index += 1
                    stop.wait(DRIP_INTERVAL_SECONDS)
            except OSError:
                return

    server = threading.Thread(target=_serve, daemon=True)
    server.start()
    try:
        yield f"http://127.0.0.1:{port}/slow"
    finally:
        stop.set()
        listener.close()
        server.join(timeout=DRIP_GIVE_UP_SECONDS + 1)


@contextmanager
def _fast_server(body: bytes) -> Iterator[str]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    listener.settimeout(5)
    port = listener.getsockname()[1]

    def _serve() -> None:
        try:
            conn, _ = listener.accept()
        except OSError:
            return
        with conn:
            conn.settimeout(5)
            conn.recv(65536)
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/html\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + body
            )

    server = threading.Thread(target=_serve, daemon=True)
    server.start()
    try:
        yield f"http://127.0.0.1:{port}/"
    finally:
        listener.close()
        server.join(timeout=6)


def _create_spec(url: str, **config: object) -> MetricCollectionSpec:
    return MetricCollectionSpec.objects.create(
        name="dripping probe",
        spec_type=MetricCollectionSpecType.HTTP_PAGE_PROBE,
        interval_seconds=300,
        config={"url": url, "subject_id": "dripping_site", **config},
    )


def _capture_points(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    captured: list[object] = []

    def _capture(points: list[object]) -> int:
        captured.extend(points)
        return len(points)

    monkeypatch.setattr("graphyard.services.influx.write_points", _capture)
    return captured


def _metric_values(points: list[object]) -> dict[str, float]:
    return {point.metric: point.value for point in points}  # type: ignore[attr-defined]


def _assert_deadline_failure(
    spec: MetricCollectionSpec, points: list[object], elapsed: float, deadline: float
) -> None:
    # Deadline plus slack for thread start-up and the bounded close/join.
    assert elapsed < deadline + 1.5, f"probe returned after {elapsed:.1f}s"
    assert _metric_values(points) == {
        "service.http_page_status_code": 0.0,
        "service.http_page_success": 0.0,
    }
    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.WARNING
    assert "total_timeout_seconds" in spec.last_error


def test_page_probe_deadline_bounds_dripping_headers(db, monkeypatch):
    points = _capture_points(monkeypatch)
    with _dripping_server(
        prefix=b"HTTP/1.1 200 OK\r\n", drip=b"X-Drip: aaaaaaaaaaaaaaaaaaaaaaaaaa\r\n"
    ) as url:
        spec = _create_spec(url, request_timeout_seconds=1, total_timeout_seconds=1.5)
        started = time.monotonic()
        result = run_metric_collection_specs_once()
        elapsed = time.monotonic() - started

    assert result.warning == 1
    assert result.failed == 0
    assert result.ingested == 2
    _assert_deadline_failure(spec, points, elapsed, 1.5)


def test_page_probe_deadline_bounds_dripping_body(db, monkeypatch):
    points = _capture_points(monkeypatch)
    with _dripping_server(
        prefix=(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/html\r\n"
            b"Content-Length: 1000000\r\n\r\n"
        ),
        drip=b"<p>slow</p>",
    ) as url:
        spec = _create_spec(url, request_timeout_seconds=1, total_timeout_seconds=1.5)
        started = time.monotonic()
        result = run_metric_collection_specs_once()
        elapsed = time.monotonic() - started

    assert result.warning == 1
    _assert_deadline_failure(spec, points, elapsed, 1.5)


def test_page_probe_default_deadline_is_three_request_timeouts(db, monkeypatch):
    points = _capture_points(monkeypatch)
    with _dripping_server(
        prefix=b"HTTP/1.1 200 OK\r\n", drip=b"X-Drip: aaaaaaaaaaaaaaaaaaaaaaaaaa\r\n"
    ) as url:
        spec = _create_spec(url, request_timeout_seconds=0.5)
        started = time.monotonic()
        run_metric_collection_specs_once()
        elapsed = time.monotonic() - started

    assert elapsed >= 1.4
    _assert_deadline_failure(spec, points, elapsed, 1.5)


def test_page_probe_fast_page_is_unchanged(db, monkeypatch):
    points = _capture_points(monkeypatch)
    with _fast_server(b"<html>ok</html>") as url:
        spec = _create_spec(url, request_timeout_seconds=2, total_timeout_seconds=5)
        result = run_metric_collection_specs_once()

    assert result.warning == 0
    assert result.failed == 0
    values = _metric_values(points)
    assert values["service.http_page_status_code"] == 200.0
    assert values["service.http_page_success"] == 1.0
    assert values["service.http_page_redirect_count"] == 0.0
    assert 0 <= values["service.http_page_total_seconds"] < 2
    spec.refresh_from_db()
    assert spec.last_status == StatusLevel.OK
    assert spec.last_error == ""


@pytest.mark.parametrize("value", [0, -1, "0", "abc", None, True, float("inf")])
def test_total_timeout_seconds_must_be_positive(value):
    errors = validate_spec_config(
        MetricCollectionSpecType.HTTP_PAGE_PROBE,
        {"url": "https://example.com", "total_timeout_seconds": value},
    )
    assert errors == [
        f"config.total_timeout_seconds must be a number greater than 0, got {value!r}"
    ]


def test_total_timeout_seconds_accepts_numbers_and_numeric_strings():
    for value in (1, 2.5, "30"):
        assert (
            validate_spec_config(
                MetricCollectionSpecType.HTTP_PAGE_PROBE,
                {"url": "https://example.com", "total_timeout_seconds": value},
            )
            == []
        )


def _page_probe_workers() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == "graphyard-page-probe"]


def test_page_probe_cancels_connection_that_completes_after_deadline(db, monkeypatch):
    """A worker stuck in DNS past the deadline must not go on to use the server."""
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    listener.settimeout(6)
    port = listener.getsockname()[1]
    received: list[bytes] = []

    def _serve() -> None:
        try:
            conn, _ = listener.accept()
        except OSError:
            return
        with conn:
            conn.settimeout(3)
            try:
                received.append(conn.recv(65536))
                conn.sendall(b"HTTP/1.1 200 OK\r\n")
                for _ in range(15):
                    conn.sendall(b"X")
                    time.sleep(DRIP_INTERVAL_SECONDS)
            except OSError:
                return

    server = threading.Thread(target=_serve, daemon=True)
    server.start()

    real_getaddrinfo = socket.getaddrinfo

    def _slow_getaddrinfo(*args, **kwargs):
        time.sleep(1.8)
        return real_getaddrinfo(*args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", _slow_getaddrinfo)
    points = _capture_points(monkeypatch)
    spec = _create_spec(
        f"http://127.0.0.1:{port}/",
        request_timeout_seconds=2,
        total_timeout_seconds=0.3,
    )
    try:
        started = time.monotonic()
        run_metric_collection_specs_once()
        elapsed = time.monotonic() - started
        _assert_deadline_failure(spec, points, elapsed, 0.3)

        # The DNS lookup outlives the caller's bounded join; once it returns,
        # the worker must drop the new connection instead of sending a request.
        give_up_at = time.monotonic() + 4
        while _page_probe_workers() and time.monotonic() < give_up_at:
            time.sleep(0.05)
        assert _page_probe_workers() == []
        server.join(timeout=4)
        assert received in ([], [b""])
    finally:
        listener.close()
        server.join(timeout=4)


class _BlockingPageProbeClient:
    max_redirects = 20

    def __init__(self, release: threading.Event, sends: list[str]) -> None:
        self._release = release
        self._sends = sends

    def __enter__(self) -> _BlockingPageProbeClient:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    def close(self) -> None:
        # Deliberately ignores close, like a thread stuck in a DNS lookup.
        return None

    def build_request(self, method: str, url: str) -> object:
        del method

        class _Request:
            extensions: dict[str, object] = {}

        request = _Request()
        request.extensions = {"url": url}
        return request

    def send(self, request, *, stream: bool, follow_redirects: bool):
        del request, stream, follow_redirects
        self._sends.append("send")
        self._release.wait(10)
        raise httpx.ConnectError("released")


def test_page_probe_caps_abandoned_workers(db, monkeypatch):
    monkeypatch.setattr("graphyard.services._PAGE_PROBE_CLOSE_JOIN_SECONDS", 0.05)
    monkeypatch.setattr("graphyard.services._PAGE_PROBE_MAX_ABANDONED_WORKERS", 2)
    release = threading.Event()
    sends: list[str] = []
    monkeypatch.setattr(
        "graphyard.services.httpx.Client",
        lambda **kwargs: _BlockingPageProbeClient(release, sends),
    )
    points = _capture_points(monkeypatch)
    spec = _create_spec("https://example.invalid/", total_timeout_seconds=0.1)
    try:
        for _ in range(2):
            run_metric_collection_specs_once(due_only=False)
        assert len(sends) == 2
        assert len(_page_probe_workers()) == 2

        points.clear()
        started = time.monotonic()
        result = run_metric_collection_specs_once(due_only=False)
        elapsed = time.monotonic() - started

        assert len(sends) == 2  # no third worker was started
        assert elapsed < 0.1
        assert result.warning == 1
        assert _metric_values(points)["service.http_page_success"] == 0.0
        spec.refresh_from_db()
        assert "still running" in spec.last_error
    finally:
        release.set()
        for thread in _page_probe_workers():
            thread.join(timeout=5)
    assert _page_probe_workers() == []
