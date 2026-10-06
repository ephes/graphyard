from __future__ import annotations

import socket
import time

import httpcore
import pytest

from graphyard.http_deadline import (
    _WRITE_CHUNK_BYTES,
    _Deadline,
    _DeadlineBackend,
    _DeadlineStream,
)


class _SlowWriteStream(httpcore.NetworkStream):
    def __init__(self) -> None:
        self.writes: list[tuple[int, float | None]] = []

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        # Mimic a peer that accepts data slowly: each call takes a while but
        # still succeeds within its own (per-call) timeout.
        self.writes.append((len(buffer), timeout))
        time.sleep(0.03)


class _RecordingBackend(httpcore.NetworkBackend):
    def __init__(self, failing: set[str]) -> None:
        self.failing = failing
        self.calls: list[tuple[str, int, float | None, str | None]] = []

    def connect_tcp(
        self, host, port, timeout=None, local_address=None, socket_options=None
    ):
        self.calls.append((host, port, timeout, local_address))
        if host in self.failing:
            raise httpcore.ConnectError(f"refused {host}")
        return _SlowWriteStream()


def test_partial_writes_are_bounded_by_deadline():
    inner = _SlowWriteStream()
    stream = _DeadlineStream(inner, _Deadline(time.monotonic() + 0.1, "deadline"))

    started = time.monotonic()
    with pytest.raises(httpcore.WriteTimeout, match="exceeded deadline"):
        stream.write(b"x" * (_WRITE_CHUNK_BYTES * 20), timeout=5.0)

    assert time.monotonic() - started < 0.5
    assert 0 < len(inner.writes) < 20
    assert all(size <= _WRITE_CHUNK_BYTES for size, _ in inner.writes)
    assert all(timeout is not None and timeout <= 0.1 for _, timeout in inner.writes)


def test_dns_resolution_is_bounded_by_deadline(monkeypatch):
    def _stalled_getaddrinfo(*args, **kwargs):
        time.sleep(1.0)
        raise OSError("too late")

    monkeypatch.setattr(
        "graphyard.http_deadline.socket.getaddrinfo", _stalled_getaddrinfo
    )
    backend = _DeadlineBackend(
        _RecordingBackend(set()), _Deadline(time.monotonic() + 0.1, "deadline")
    )

    started = time.monotonic()
    with pytest.raises(httpcore.ConnectTimeout):
        backend.connect_tcp("slow.example", 443, timeout=5.0)
    assert time.monotonic() - started < 0.5


def test_connect_tries_each_resolved_address(monkeypatch):
    monkeypatch.setattr(
        "graphyard.http_deadline.socket.getaddrinfo",
        lambda host, port, type: [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", port, 0, 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port)),
        ],
    )
    inner = _RecordingBackend({"::1"})
    backend = _DeadlineBackend(inner, _Deadline(time.monotonic() + 5, "deadline"))

    stream = backend.connect_tcp(
        "example.test", 80, timeout=2.0, local_address="0.0.0.0"
    )

    assert stream is not None
    assert [call[0] for call in inner.calls] == ["::1", "127.0.0.1"]
    assert all(call[3] == "0.0.0.0" for call in inner.calls)
    assert all(call[2] is not None and call[2] <= 2.0 for call in inner.calls)


def test_connect_reports_last_error_when_all_addresses_fail(monkeypatch):
    monkeypatch.setattr(
        "graphyard.http_deadline.socket.getaddrinfo",
        lambda host, port, type: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port)),
        ],
    )
    backend = _DeadlineBackend(
        _RecordingBackend({"127.0.0.1"}), _Deadline(time.monotonic() + 5, "deadline")
    )

    with pytest.raises(httpcore.ConnectError, match="refused 127.0.0.1"):
        backend.connect_tcp("example.test", 80, timeout=2.0)


class _SocketStream(httpcore.NetworkStream):
    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        raise AssertionError("socket streams are written directly")

    def get_extra_info(self, info: str):
        return self._sock if info == "socket" else None


def test_socket_writes_are_clamped_per_send():
    writer, reader = socket.socketpair()
    try:
        writer.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        stream = _DeadlineStream(
            _SocketStream(writer), _Deadline(time.monotonic() + 0.2, "deadline")
        )
        started = time.monotonic()
        # The reader never drains, so send() eventually blocks.
        with pytest.raises(httpcore.WriteTimeout, match="exceeded deadline"):
            stream.write(b"x" * (8 * 1024 * 1024), timeout=5.0)
        assert time.monotonic() - started < 1.0
    finally:
        writer.close()
        reader.close()


def test_resolve_keeps_ipv6_scope(monkeypatch):
    monkeypatch.setattr(
        "graphyard.http_deadline.socket.getaddrinfo",
        lambda host, port, type: [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("fe80::1", port, 0, 7)),
        ],
    )
    monkeypatch.setattr(
        "graphyard.http_deadline.socket.if_indextoname", lambda index: f"en{index}"
    )
    inner = _RecordingBackend(set())
    backend = _DeadlineBackend(inner, _Deadline(time.monotonic() + 5, "deadline"))

    backend.connect_tcp("router.local", 80, timeout=2.0)

    assert inner.calls[0][0] == "fe80::1%en7"
