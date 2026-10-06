"""An httpx transport whose socket operations never wait past an absolute deadline.

httpx timeouts are per operation: each connect, read and write may wait the full
timeout again, so a server that drips bytes (even incomplete response headers) can
hold a request open indefinitely. This transport clamps every network operation's
timeout to the time left before a fixed ``time.monotonic()`` deadline, and fails
the operation with a timeout once the deadline has passed.
"""

from __future__ import annotations

import functools
import socket
import ssl
import threading
import time
import typing
from collections.abc import Callable, Iterable

import httpcore
import httpx

_SocketOption = (
    tuple[int, int, int]
    | tuple[int, int, bytes | bytearray]
    | tuple[int, int, None, int]
)
_TimeoutError = type[httpcore.TimeoutException]
_T = typing.TypeVar("_T")

# Fallback for streams without a socket: split writes so each piece gets a
# freshly clamped timeout.
_WRITE_CHUNK_BYTES = 4096


class _Deadline:
    def __init__(self, deadline: float, label: str) -> None:
        self._deadline = deadline
        self._label = label

    def call(
        self,
        timeout: float | None,
        error: _TimeoutError,
        operation: Callable[[float], _T],
    ) -> _T:
        """Run ``operation`` with its timeout clamped to the time left."""
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise error(f"exceeded {self._label}")
        try:
            return operation(remaining if timeout is None else min(timeout, remaining))
        except httpcore.TimeoutException:
            if self.expired():
                raise error(f"exceeded {self._label}") from None
            raise

    def expired(self) -> bool:
        return self._deadline - time.monotonic() <= 0


class _DeadlineStream(httpcore.NetworkStream):
    def __init__(self, stream: httpcore.NetworkStream, deadline: _Deadline) -> None:
        self._stream = stream
        self._deadline = deadline

    def read(self, max_bytes: int, timeout: float | None = None) -> bytes:
        return self._deadline.call(
            timeout,
            httpcore.ReadTimeout,
            lambda clamped: self._stream.read(max_bytes, clamped),
        )

    def write(self, buffer: bytes, timeout: float | None = None) -> None:
        sock = self._stream.get_extra_info("socket")
        if not isinstance(sock, socket.socket):
            # Not a plain socket stream: bound each piece instead.
            view = memoryview(buffer)
            for start in range(0, len(view), _WRITE_CHUNK_BYTES):
                self._deadline.call(
                    timeout,
                    httpcore.WriteTimeout,
                    functools.partial(
                        self._stream.write,
                        bytes(view[start : start + _WRITE_CHUNK_BYTES]),
                    ),
                )
            return
        # The inner stream would reuse one timeout for every partial send(), so
        # send directly and clamp each send() to the time left.
        pending = memoryview(buffer)
        while pending:
            sent = self._deadline.call(
                timeout,
                httpcore.WriteTimeout,
                functools.partial(_send_once, sock, pending),
            )
            pending = pending[sent:]

    def close(self) -> None:
        self._stream.close()

    def start_tls(
        self,
        ssl_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> httpcore.NetworkStream:
        stream = self._deadline.call(
            timeout,
            httpcore.ConnectTimeout,
            lambda clamped: self._stream.start_tls(
                ssl_context, server_hostname, clamped
            ),
        )
        return _DeadlineStream(stream, self._deadline)

    def get_extra_info(self, info: str) -> typing.Any:
        return self._stream.get_extra_info(info)


class _DeadlineBackend(httpcore.NetworkBackend):
    def __init__(self, backend: httpcore.NetworkBackend, deadline: _Deadline) -> None:
        self._backend = backend
        self._deadline = deadline

    def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,
        local_address: str | None = None,
        socket_options: Iterable[_SocketOption] | None = None,
    ) -> httpcore.NetworkStream:
        # socket.create_connection() neither bounds name resolution nor
        # shrinks its timeout across addresses, so resolve here and connect
        # to each address literal with a freshly clamped timeout.
        addresses = self._deadline.call(
            timeout,
            httpcore.ConnectTimeout,
            lambda clamped: _resolve(host, port, clamped),
        )
        options = list(socket_options) if socket_options is not None else None
        last_error: httpcore.NetworkError | httpcore.TimeoutException | None = None
        for address in addresses:
            try:
                stream = self._deadline.call(
                    timeout,
                    httpcore.ConnectTimeout,
                    functools.partial(
                        self._connect_address, address, port, local_address, options
                    ),
                )
            except (httpcore.ConnectError, httpcore.ConnectTimeout) as err:
                if self._deadline.expired():
                    raise
                last_error = err
                continue
            return _DeadlineStream(stream, self._deadline)
        assert last_error is not None
        raise last_error

    def _connect_address(
        self,
        address: str,
        port: int,
        local_address: str | None,
        options: list[_SocketOption] | None,
        timeout: float,
    ) -> httpcore.NetworkStream:
        return self._backend.connect_tcp(address, port, timeout, local_address, options)

    def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,
        socket_options: Iterable[_SocketOption] | None = None,
    ) -> httpcore.NetworkStream:
        stream = self._deadline.call(
            timeout,
            httpcore.ConnectTimeout,
            lambda clamped: self._backend.connect_unix_socket(
                path, clamped, socket_options
            ),
        )
        return _DeadlineStream(stream, self._deadline)

    def sleep(self, seconds: float) -> None:
        self._backend.sleep(seconds)


def _send_once(sock: socket.socket, data: memoryview, timeout: float) -> int:
    try:
        sock.settimeout(timeout)
        return sock.send(data)
    except TimeoutError as err:
        raise httpcore.WriteTimeout(str(err) or "write timed out") from err
    except OSError as err:
        raise httpcore.WriteError(str(err)) from err


def _address_literal(sockaddr: tuple[typing.Any, ...]) -> str:
    address = str(sockaddr[0])
    # Keep the zone of scoped (link-local) IPv6 addresses, which getaddrinfo
    # reports separately as scope_id.
    if len(sockaddr) == 4 and sockaddr[3] and "%" not in address:
        try:
            zone = socket.if_indextoname(sockaddr[3])
        except OSError:
            zone = str(sockaddr[3])
        address = f"{address}%{zone}"
    return address


def _resolve(host: str, port: int, timeout: float) -> list[str]:
    """Resolve ``host`` to address literals, giving up after ``timeout``.

    getaddrinfo() cannot be interrupted, so a stalled lookup runs on in a
    daemon thread until the resolver gives up; the caller is not held.
    """
    outcome: dict[str, object] = {}

    def _lookup() -> None:
        try:
            outcome["infos"] = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as err:
            outcome["error"] = err

    worker = threading.Thread(target=_lookup, name="graphyard-dns", daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        raise httpcore.ConnectTimeout(f"resolving {host!r} timed out")
    if "error" in outcome:
        raise httpcore.ConnectError(str(outcome["error"]))
    infos = typing.cast(list[tuple[typing.Any, ...]], outcome["infos"])
    addresses: list[str] = []
    for info in infos:
        address = _address_literal(info[4])
        if address not in addresses:
            addresses.append(address)
    if not addresses:
        raise httpcore.ConnectError(f"no addresses for {host!r}")
    return addresses


class DeadlineTransport(httpx.HTTPTransport):
    """``httpx.HTTPTransport`` (direct connections only) bounded by ``deadline``.

    ``deadline`` is an absolute ``time.monotonic()`` value; ``label`` names it
    in the timeout error message.
    """

    def __init__(self, *, verify: bool, deadline: float, label: str) -> None:
        super().__init__(verify=verify)
        self._pool = httpcore.ConnectionPool(
            ssl_context=httpx.create_ssl_context(verify=verify),
            network_backend=_DeadlineBackend(
                httpcore.SyncBackend(), _Deadline(deadline, label)
            ),
        )
