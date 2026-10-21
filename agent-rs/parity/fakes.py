"""Scripted fake servers of the offline parity harness (agent-rs/PLAN.md 9.4).

Each agent gets its own instances (own ports), so both see the same answers
whatever the scheduling of the two processes:

- ``HttpFake`` serves the collector endpoints (Home Assistant, JSON metrics,
  page probes, UniFi), plain or over TLS. It answers **by request index per
  path**: the n-th request to a path gets the n-th scripted response, the
  last response repeats. A response can be delayed, dripped byte by byte,
  gzip-encoded, oversized or carry several ``Set-Cookie`` headers.
- ``InfluxFake`` stands for InfluxDB: it records ``/api/v2/write`` requests
  and answers Flux (``/api/v2/query``, annotated CSV) and v3 SQL
  (``/api/v3/query_sql``, JSON rows) queries from per-metric sample scripts.
  The samples are placed relative to the ``stop`` of the requested range
  (the agent's own ``now``), so a condition sees the same sample ages in both
  agents although each takes ``now`` from its own wall clock. A query script
  advances by one entry per answered query of that metric (Flux or SQL);
  ``v2_missing`` answers every Flux query with 404 (the ``auto`` fallback).

Every request is logged (method, target, headers, body) for the
comparison. Standard library only.
"""

from __future__ import annotations

import gzip
import json
import re
import ssl
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit


class Script:
    """Thread-safe per-key response scripts with request counters."""

    def __init__(self) -> None:
        self._scripts: dict[str, list[Any]] = {}
        self._served: dict[str, int] = {}
        self._lock = threading.Lock()

    def set(self, key: str, responses: list[Any]) -> None:
        if not responses:
            raise ValueError(f"empty script for {key}")
        if key in self._scripts:
            raise ValueError(f"script for {key} set twice")
        self._scripts[key] = list(responses)

    def __contains__(self, key: str) -> bool:
        return key in self._scripts

    def next(self, key: str) -> Any | None:
        """The response for the next request to ``key`` (``None``: unknown)."""
        with self._lock:
            responses = self._scripts.get(key)
            if responses is None:
                return None
            index = self._served.get(key, 0)
            self._served[key] = index + 1
            return responses[min(index, len(responses) - 1)]

    def served(self) -> dict[str, int]:
        with self._lock:
            return dict(self._served)


@dataclass(frozen=True)
class Request:
    """One request as a fake received it."""

    method: str
    target: str
    headers: tuple[tuple[str, str], ...]
    body: bytes

    @property
    def path(self) -> str:
        return urlsplit(self.target).path

    def header(self, name: str) -> str | None:
        for key, value in self.headers:
            if key.lower() == name.lower():
                return value
        return None


class _Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64
    fake: _Fake


class _Handler(BaseHTTPRequestHandler):
    server: _Server
    protocol_version = "HTTP/1.1"
    # Long redirect chains and reason phrases stay within the defaults.

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        request = Request(
            method=self.command,
            target=self.path,
            headers=tuple((k, v) for k, v in self.headers.items()),
            body=body,
        )
        fake = self.server.fake
        fake.record(request)
        response = fake.respond(request)
        self._send(response)
        self.close_connection = True

    def _send(self, response: dict[str, Any]) -> None:
        fake = self.server.fake
        delay = float(response.get("delay", 0))
        if delay and fake.closing.wait(delay):
            return
        body = response.get("body", b"")
        if not isinstance(body, (str, bytes)):
            body = json.dumps(body).encode()
        if isinstance(body, str):
            body = body.encode()
        headers = [tuple(pair) for pair in response.get("headers", [])]
        if response.get("gzip"):
            body = gzip.compress(body, mtime=0)
            headers.append(("Content-Encoding", "gzip"))
        status = int(response.get("status", 200))
        reason = response.get("reason")
        self.send_response_only(status, reason)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        drip = float(response.get("drip", 0))
        try:
            if drip:
                # One byte per interval: never trips a per-read timeout,
                # only the collector's total deadline.
                for index in range(len(body)):
                    self.wfile.write(body[index : index + 1])
                    self.wfile.flush()
                    if fake.closing.wait(drip):
                        return
            else:
                self.wfile.write(body)
        except OSError:
            pass

    do_GET = _handle
    do_POST = _handle
    do_HEAD = _handle
    do_PUT = _handle
    do_DELETE = _handle


class _Fake:
    """A threaded HTTP server with a request log."""

    def __init__(self, tls: ssl.SSLContext | None = None) -> None:
        self.closing = threading.Event()
        self._log: list[Request] = []
        self._log_lock = threading.Lock()
        server = _Server(("127.0.0.1", 0), _Handler)
        if tls is not None:
            server.socket = tls.wrap_socket(server.socket, server_side=True)
        server.fake = self
        self._server = server
        self.port = server.server_address[1]
        self._thread = threading.Thread(target=server.serve_forever, daemon=True)
        self._thread.start()

    def record(self, request: Request) -> None:
        with self._log_lock:
            self._log.append(request)

    def requests(self) -> list[Request]:
        with self._log_lock:
            return list(self._log)

    def respond(self, request: Request) -> dict[str, Any]:  # pragma: no cover
        raise NotImplementedError

    def close(self) -> None:
        self.closing.set()
        self._server.shutdown()
        self._server.server_close()


class HttpFake(_Fake):
    """Scripted collector endpoints keyed by path (the query is ignored)."""

    def __init__(self, tls: ssl.SSLContext | None = None) -> None:
        self.script = Script()
        super().__init__(tls)

    def respond(self, request: Request) -> dict[str, Any]:
        response = self.script.next(request.path)
        if response is None:
            return {"status": 404, "body": "unknown endpoint"}
        return response


# ------------------------------------------------------------------ InfluxDB

_FLUX_RANGE = re.compile(r"range\(start: (\S+), stop: (\S+)\)")
_FLUX_METRIC = re.compile(r'r\["metric"\] == "((?:[^"\\]|\\.)*)"')
_SQL_START = re.compile(r"time >= '([^']*)'")
_SQL_STOP = re.compile(r"time < '([^']*)'")
_SQL_METRIC = re.compile(r"metric = '((?:[^']|'')*)'")

_FLUX_BASE_COLUMNS = (
    ("result", "string", "false"),
    ("table", "long", "false"),
    ("_start", "dateTime:RFC3339", "true"),
    ("_stop", "dateTime:RFC3339", "true"),
    ("_time", "dateTime:RFC3339", "false"),
    ("_value", "double", "false"),
    ("_field", "string", "true"),
    ("_measurement", "string", "true"),
    ("metric", "string", "true"),
)


def parse_instant(text: str) -> datetime:
    """An RFC 3339 instant as the agents print it (``Z`` or naive UTC)."""
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def rfc3339(ts: datetime, *, zulu: bool = True) -> str:
    text = ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return text + "Z" if zulu else text


def sample_rows(
    samples: list[dict[str, Any]], stop: datetime
) -> list[tuple[datetime, float, dict[str, str]]]:
    """``{"age": seconds before stop, "value": v, "tags": {...}}`` → rows."""
    rows = []
    for sample in samples:
        ts = stop - timedelta(seconds=float(sample["age"]))
        rows.append((ts, float(sample["value"]), dict(sample.get("tags", {}))))
    return rows


def flux_csv(
    measurement: str,
    metric: str,
    start: datetime,
    stop: datetime,
    samples: list[dict[str, Any]],
) -> str:
    """Annotated CSV as InfluxDB 2 answers a Flux query: one table per tag set."""
    rows = sample_rows(samples, stop)
    if not rows:
        return "\r\n"
    tag_keys = sorted({key for _, _, tags in rows for key in tags})
    columns = [*_FLUX_BASE_COLUMNS, *((key, "string", "true") for key in tag_keys)]
    tables: dict[tuple[str, ...], list[tuple[datetime, float]]] = {}
    for ts, value, tags in rows:
        key = tuple(tags.get(name, "") for name in tag_keys)
        tables.setdefault(key, []).append((ts, value))
    lines = [
        "#datatype," + ",".join(kind for _, kind, _ in columns),
        "#group," + ",".join(group for _, _, group in columns),
        "#default,_result" + "," * (len(columns) - 1),
        "," + ",".join(name for name, _, _ in columns),
    ]
    for table, (tag_values, values) in enumerate(tables.items()):
        for ts, value in sorted(values):
            cells = [
                "",
                str(table),
                rfc3339(start),
                rfc3339(stop),
                rfc3339(ts),
                repr(value),
                "value",
                measurement,
                metric,
                *tag_values,
            ]
            lines.append("," + ",".join(cells))
    return "\r\n".join(lines) + "\r\n\r\n"


def sql_rows(
    metric: str, stop: datetime, samples: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """JSON rows as InfluxDB 3 answers ``select *`` (naive UTC ``time``)."""
    return [
        {"time": rfc3339(ts, zulu=False), "metric": metric, "value": value, **tags}
        for ts, value, tags in sorted(
            sample_rows(samples, stop), key=lambda row: row[0]
        )
    ]


class InfluxFake(_Fake):
    """Fake InfluxDB: records writes, answers queries from sample scripts.

    ``queries`` maps a metric name to its script: a list (by answered query
    index, the last entry repeats) of either a list of samples or a
    response dict (``{"status": 500, "body": ...}``) for an error.
    """

    def __init__(self, measurement: str) -> None:
        self.measurement = measurement
        self.queries = Script()
        self.v2_missing = False
        super().__init__()

    def respond(self, request: Request) -> dict[str, Any]:
        path = request.path
        if path == "/api/v2/write":
            return {"status": 204}
        if path == "/api/v2/query":
            if self.v2_missing:
                return {
                    "status": 404,
                    "headers": [("Content-Type", "application/json")],
                    "body": {"code": "not found", "message": "path not found"},
                }
            flux = json.loads(request.body)["query"]
            start_text, stop_text = _FLUX_RANGE.search(flux).groups()  # type: ignore[union-attr]
            metric = _unescape_flux(_FLUX_METRIC.search(flux).group(1))  # type: ignore[union-attr]
            entry = self.queries.next(metric)
            if isinstance(entry, dict):
                return entry
            start, stop = parse_instant(start_text), parse_instant(stop_text)
            return {
                "status": 200,
                "headers": [("Content-Type", "text/csv; charset=utf-8")],
                "body": flux_csv(self.measurement, metric, start, stop, entry or []),
            }
        if path == "/api/v3/query_sql":
            sql = json.loads(request.body)["q"]
            stop = parse_instant(_SQL_STOP.search(sql).group(1))  # type: ignore[union-attr]
            metric = _SQL_METRIC.search(sql).group(1).replace("''", "'")  # type: ignore[union-attr]
            entry = self.queries.next(metric)
            if isinstance(entry, dict):
                return entry
            return {
                "status": 200,
                "headers": [("Content-Type", "application/json")],
                "body": sql_rows(metric, stop, entry or []),
            }
        return {"status": 404, "body": "unknown endpoint"}


def _unescape_flux(text: str) -> str:
    return re.sub(r"\\(.)", r"\1", text)


def query_window(request: Request) -> tuple[datetime, datetime] | None:
    """``(start, stop)`` of a Flux or v3 SQL query request, else ``None``."""
    try:
        payload = json.loads(request.body)
    except ValueError:
        return None
    if request.path == "/api/v2/query" and isinstance(payload, dict):
        match = _FLUX_RANGE.search(str(payload.get("query", "")))
        if match:
            return parse_instant(match.group(1)), parse_instant(match.group(2))
    if request.path == "/api/v3/query_sql" and isinstance(payload, dict):
        sql = str(payload.get("q", ""))
        start, stop = _SQL_START.search(sql), _SQL_STOP.search(sql)
        if start and stop:
            return parse_instant(start.group(1)), parse_instant(stop.group(1))
    return None


def mask_query_bounds(text: str) -> str:
    """Replace the range bounds of a Flux or SQL text by placeholders."""
    text = _FLUX_RANGE.sub("range(start: <start>, stop: <stop>)", text)
    text = _SQL_START.sub("time >= '<start>'", text)
    return _SQL_STOP.sub("time < '<stop>'", text)
