"""Fixture ``line_protocol.json``: InfluxDB writes (slice S4a).

Pins ``graphyard-core::line_protocol`` and ``graphyard-influx::write``
(agent-rs/PLAN.md 6.4) against ``influx.py:write_points`` with the real
``influxdb-client`` 1.50.0 talking to a recording HTTP server on loopback:

* ``lines``: ``Point(measurement).tag(...).field("value", v).time(ts)`` and
  its ``to_line_protocol()`` (escaping, empty-tag dropping, float rendering,
  nanosecond timestamps);
* ``writes``: whole ``write_points`` calls under the given settings, with
  the requests the client sent (method, path with query, headers without
  ``Host``, body), the return value or the exception, and the records of the
  ``graphyard.influx`` logger.

Settings are given as environment values (the Rust side reads them through
``Settings``); ``INFLUX_URL`` may name a placeholder that both sides replace
with a server of their own: ``{server}`` (records the request and answers
with ``respond``), ``{server_noscheme}`` (the same without ``http://``),
``{closed}`` (a port nothing listens on), ``{stall}`` (accepts and never
answers) and ``{raw}`` (reads the request, then plays the byte-level
``respond["raw"]`` script of ``RAW_SCRIPTS``; its requests are not recorded).

Library-derived messages (``ApiException``, urllib3 errors) are stored as
``{"class": ..., "p2": true}``; graphyard's own messages and the
deterministic library ``ValueError``/``OverflowError`` texts in full. Log
messages are stored as Python formats them; ``d10_message`` is the same
record with ``\\n``/``\\r`` in the interpolated values escaped, which is
what Rust logs (D-10), when that differs.
"""

from __future__ import annotations

import logging
import socket
import threading
import time
import warnings
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from django.test import override_settings

from graphyard import influx
from influxdb_client import Point

TS = datetime(2026, 10, 21, 8, 15, 30, 123456, tzinfo=UTC)

# ---------------------------------------------------------------- lines

LINE_TAGS: list[tuple[str, list[tuple[str, str]]]] = [
    ("plain", [("host", "macmini"), ("metric", "host.cpu_usage")]),
    (
        "sorted by key",
        [("b", "2"), ("a", "1"), ("B", "3"), ("_", "4"), ("\u00e4", "5")],
    ),
    ("no tags", []),
    ("escape key chars", [("k,=y z", "v,=w x")]),
    ("escape controls", [("t\tk", "a\nb\rc")]),
    ("backslash not escaped", [("k\\x", "v\\y")]),
    ("trailing backslash gets a space", [("k", "value\\")]),
    ("only backslash", [("k", "\\")]),
    ("quotes kept", [("k", 'say "hi" it\'s')]),
    ("empty key dropped", [("", "v"), ("a", "1")]),
    ("empty value dropped", [("a", ""), ("b", "1")]),
    ("all tags dropped", [("", ""), ("a", "")]),
    ("unicode", [("ort", "K\u00fcche \U0001f321")]),
    ("hash in tag", [("#k", "#v")]),
]

LINE_MEASUREMENTS = [
    "graphyard_metrics",
    "my metrics,x=1",
    "tab\there\nnew\rline",
    "equals=kept",
    "back\\slash",
    "",
    "\u00fcmlaut",
]

LINE_VALUES = [
    21.0,
    0.1,
    0.3,
    -0.0,
    0.0,
    1.5,
    100.0,
    123456789.0,
    1e15,
    1e16,
    1.5e-7,
    1e22,
    5e-324,
    1.7976931348623157e308,
    -2.5,
    1e-05,
    0.0001,
    12345678901234567.0,
    3.141592653589793,
    float("nan"),
    float("inf"),
]

LINE_TIMESTAMPS = [
    TS,
    datetime(1970, 1, 1, tzinfo=UTC),
    datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
    datetime(1, 1, 1, tzinfo=UTC),
    datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
    datetime(2262, 4, 11, 23, 47, 16, 854776, tzinfo=UTC),
]


def _line(
    measurement: str, tags: list[tuple[str, str]], value: float, ts: datetime
) -> dict[str, Any]:
    point = Point(measurement)
    for key, tag_value in tags:
        point = point.tag(key, tag_value)
    point = point.field("value", value).time(ts)
    with warnings.catch_warnings():
        # A measurement starting with "#" warns (SyntaxWarning).
        warnings.simplefilter("ignore")
        line = point.to_line_protocol()
    return {
        "measurement": measurement,
        "tags": [[key, tag_value] for key, tag_value in tags],
        "value": repr(value),
        "ts": ts.isoformat(),
        "line": line,
    }


def _lines() -> list[dict[str, Any]]:
    cases = []
    for _name, tags in LINE_TAGS:
        cases.append(_line("graphyard_metrics", tags, 1.0, TS))
    for measurement in LINE_MEASUREMENTS:
        cases.append(_line(measurement, [("a", "1")], 1.0, TS))
    for value in LINE_VALUES:
        cases.append(_line("m", [("a", "1")], value, TS))
    for ts in LINE_TIMESTAMPS:
        cases.append(_line("m", [("a", "1")], 2.0, ts))
    return cases


# ---------------------------------------------------------------- writes


class _Recorder(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    requests: list[dict[str, Any]] = []
    respond: dict[str, Any] = {}

    def do_POST(self) -> None:  # noqa: N802 (http.server API)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        _Recorder.requests.append(
            {
                "method": self.command,
                "path": self.path,
                "headers": [
                    [name, value]
                    for name, value in self.headers.items()
                    if name.lower() != "host"
                ],
                "body": body.decode("utf-8"),
            }
        )
        status = int(_Recorder.respond.get("status", 204))
        payload = str(_Recorder.respond.get("body", "")).encode("utf-8")
        self.send_response(status)
        for name, value in _Recorder.respond.get("headers", []):
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        del format, args


# Byte-level answers: (head, body chunks, pause between chunks, hold open).
RAW_SCRIPTS: dict[str, tuple[bytes, list[bytes], float, bool]] = {
    # A 2xx whose body ends before its Content-Length.
    "truncated": (
        b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\n",
        [b"abc"],
        0.0,
        False,
    ),
    # A 2xx whose body never arrives.
    "stall_body": (b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\n", [], 0.0, True),
    # An error body dripping slower than nothing but faster than the timeout.
    "drip": (
        b"HTTP/1.1 400 Bad Request\r\nContent-Length: 6\r\n\r\n",
        [b"s", b"l", b"o", b"w", b"l", b"y"],
        0.07,
        False,
    ),
    # A 2xx body that is not UTF-8.
    "not_utf8": (
        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n",
        [b"\xff\xfe"],
        0.0,
        False,
    ),
}


def _read_request(conn: socket.socket) -> None:
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(65536)
        if not chunk:
            return
        data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    length = 0
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            length = int(value.strip())
    while len(body) < length:
        chunk = conn.recv(65536)
        if not chunk:
            return
        body += chunk


class _Raw:
    script = "truncated"

    def __init__(self) -> None:
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.held: list[socket.socket] = []
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._answer, args=(conn,), daemon=True).start()

    def _answer(self, conn: socket.socket) -> None:
        head, chunks, pause, hold = RAW_SCRIPTS[_Raw.script]
        try:
            _read_request(conn)
            conn.sendall(head)
            for chunk in chunks:
                time.sleep(pause)
                conn.sendall(chunk)
        except OSError:
            pass
        if hold:
            self.held.append(conn)
        else:
            conn.close()

    def close(self) -> None:
        self.sock.close()
        for conn in self.held:
            conn.close()


class _Servers:
    def __enter__(self) -> _Servers:
        self.raw = _Raw()
        self.recorder = ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
        threading.Thread(target=self.recorder.serve_forever, daemon=True).start()
        self.stall = socket.socket()
        self.stall.bind(("127.0.0.1", 0))
        self.stall.listen(8)
        closed = socket.socket()
        closed.bind(("127.0.0.1", 0))
        self.closed_port = closed.getsockname()[1]
        closed.close()
        return self

    def __exit__(self, *exc: object) -> None:
        self.recorder.shutdown()
        self.recorder.server_close()
        self.stall.close()
        self.raw.close()

    def url(self, template: str) -> str:
        recorder = f"127.0.0.1:{self.recorder.server_port}"
        return (
            template.replace("{server}", f"http://{recorder}")
            .replace("{server_noscheme}", recorder)
            .replace("{closed}", f"http://127.0.0.1:{self.closed_port}")
            .replace("{stall}", f"http://127.0.0.1:{self.stall.getsockname()[1]}")
            .replace("{raw}", f"http://127.0.0.1:{self.raw.sock.getsockname()[1]}")
        )


ENV_DEFAULTS = {
    "INFLUX_URL": "",
    "INFLUX_TOKEN": "",
    "INFLUX_ORG": "",
    "INFLUX_BUCKET": "graphyard",
    "INFLUX_MEASUREMENT": "graphyard_metrics",
    "INFLUX_TIMEOUT_MS": "10000",
    "INFLUX_API_MODE": "auto",
}

V2 = {"INFLUX_URL": "{server}", "INFLUX_TOKEN": "t0k", "INFLUX_ORG": "graphyard"}


def _p(**changes: Any) -> dict[str, Any]:
    point: dict[str, Any] = {
        "ts": TS,
        "metric": "host.cpu_usage",
        "value": 0.5,
        "subject_type": "host",
        "subject_id": "macmini",
        "source_system": "vector",
        "collector_service": "vector",
        "collector_host": "macmini",
        "source_instance": "vector-macmini",
        "source_entity_id": None,
        "host": None,
        "service": None,
        "tags": None,
    }
    point.update(changes)
    return point


SENSOR = _p(
    metric="ha.sensor.office_temperature",
    value=22.4,
    subject_type="environment_sensor",
    subject_id="office_temperature",
    source_system="homeassistant",
    source_instance="ha-main",
    collector_service="graphyard-agent",
    source_entity_id="sensor.office_temperature",
    tags={"entity_id": "sensor.office_temperature", "unit": "\u00b0C"},
)

INVALID = _p(subject_type="not_a_subject_type")

WRITES: list[tuple[str, dict[str, str], list[dict[str, Any]], dict[str, Any]]] = [
    ("empty list", V2, [], {}),
    ("single host point", V2, [_p()], {}),
    (
        "batch skips invalid point (test_write_points_skips_invalid_points_in_batch)",
        V2,
        [_p(value=0.3, host="macmini"), INVALID],
        {},
    ),
    (
        "non-finite point not counted (test_write_points_does_not_count_non_finite_points)",
        V2,
        [_p(), _p(value=float("nan"))],
        {},
    ),
    (
        "all invalid: no request and no configuration needed",
        {"INFLUX_URL": ""},
        [INVALID, _p(metric=" "), _p(value=float("inf"))],
        {},
    ),
    (
        "validation log escapes newlines (D-10)",
        V2,
        [_p(metric="first\nforged", subject_type="bad\r", subject_id="x\ny"), _p()],
        {},
    ),
    (
        "several points in order",
        V2,
        [_p(), SENSOR, _p(metric="host.load", value=1.0)],
        {},
    ),
    (
        "dimension tags",
        V2,
        [
            _p(
                subject_type="service",
                subject_id="Page Probe",
                source_entity_id=" https://example.com/a b?x=1,y ",
                service=" Web, App ",
                host="ignored",
            ),
            _p(host="MacMini", source_entity_id="", service=""),
        ],
        {},
    ),
    (
        "custom tags: reserved keys ignored, escaping, empty dropped",
        V2,
        [
            _p(
                tags={
                    "spec_name": "My Spec, v=2",
                    "host": "other",
                    "metric": "x",
                    "": "empty key",
                    "empty": "",
                    "trailing": "dir\\",
                    "Zebra": "1",
                    "tab\tkey": "new\nline",
                    "collector_host": "x",
                    "subject_id": "y",
                }
            )
        ],
        {},
    ),
    (
        "reserved key warnings follow point order",
        V2,
        [
            _p(tags={"service": "a"}),
            INVALID,
            _p(tags={"source_system": "b", "service": "c"}),
        ],
        {},
    ),
    (
        "float rendering",
        V2,
        [
            _p(value=21.0),
            _p(value=1e16),
            _p(value=-0.0),
            _p(value=1.5e-7),
            _p(value=123456789.0),
            _p(value=5e-324),
        ],
        {},
    ),
    (
        "timestamps",
        V2,
        [
            _p(ts=datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)),
            _p(ts=datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)),
            _p(ts=datetime(1, 1, 1, tzinfo=UTC)),
        ],
        {},
    ),
    (
        "measurement escaping",
        {**V2, "INFLUX_MEASUREMENT": "my metrics,x=1\tz"},
        [_p()],
        {},
    ),
    ("measurement starting with hash", {**V2, "INFLUX_MEASUREMENT": "#m"}, [_p()], {}),
    ("empty measurement", {**V2, "INFLUX_MEASUREMENT": ""}, [_p()], {}),
    (
        "org and bucket query encoding",
        {**V2, "INFLUX_ORG": "my org~*/\u00fc+&", "INFLUX_BUCKET": "b&c=d e%"},
        [_p()],
        {},
    ),
    ("empty bucket", {**V2, "INFLUX_BUCKET": ""}, [_p()], {}),
    ("url with trailing slash", {**V2, "INFLUX_URL": "{server}/"}, [_p()], {}),
    (
        "url with path prefix",
        {**V2, "INFLUX_URL": "{server}/influx/"},
        [_p()],
        {},
    ),
    ("url without scheme", {**V2, "INFLUX_URL": "{server_noscheme}"}, [_p()], {}),
    ("token kept verbatim", {**V2, "INFLUX_TOKEN": "a b=="}, [_p()], {}),
    ("mode v2 explicit", {**V2, "INFLUX_API_MODE": "v2"}, [_p()], {}),
    ("unknown mode is v2", {**V2, "INFLUX_API_MODE": "v9"}, [_p()], {}),
    (
        "v3 defaults token, writes the empty org",
        {"INFLUX_URL": "{server}", "INFLUX_API_MODE": "v3"},
        [_p()],
        {},
    ),
    (
        "v3 upper case mode, bucket set",
        {"INFLUX_URL": "{server}", "INFLUX_API_MODE": "V3", "INFLUX_BUCKET": "b3"},
        [_p()],
        {},
    ),
    (
        "v3 keeps explicit token and org",
        {
            "INFLUX_URL": "{server}",
            "INFLUX_API_MODE": "v3",
            "INFLUX_TOKEN": "tok3",
            "INFLUX_ORG": "org3",
        },
        [_p()],
        {},
    ),
    ("missing url", {"INFLUX_TOKEN": "t", "INFLUX_ORG": "o"}, [_p()], {}),
    ("missing url in v3", {"INFLUX_API_MODE": "v3"}, [_p()], {}),
    ("v2 missing token", {"INFLUX_URL": "{server}", "INFLUX_ORG": "o"}, [_p()], {}),
    ("v2 missing org", {"INFLUX_URL": "{server}", "INFLUX_TOKEN": "t"}, [_p()], {}),
    (
        "auto missing token and org",
        {"INFLUX_URL": "{server}", "INFLUX_API_MODE": "auto"},
        [_p()],
        {},
    ),
    (
        "configuration error after validation logs",
        {"INFLUX_URL": ""},
        [_p(tags={"host": "x"}), INVALID],
        {},
    ),
    ("status 200 is success", V2, [_p()], {"status": 200, "body": "ok"}),
    (
        "status 400 raises, rejected count not logged",
        V2,
        [_p(), INVALID],
        {
            "status": 400,
            "headers": [["Content-Type", "application/json; charset=utf-8"]],
            "body": '{"code":"invalid","message":"partial write: field type conflict"}',
        },
    ),
    ("status 401 raises", V2, [_p()], {"status": 401, "body": ""}),
    (
        "redirect is not followed",
        V2,
        [_p()],
        {"status": 307, "headers": [["Location", "/elsewhere"]], "body": ""},
    ),
    ("status 500 raises", V2, [_p()], {"status": 500, "body": "boom"}),
    ("timeout zero means none", {**V2, "INFLUX_TIMEOUT_MS": "0"}, [_p()], {}),
    ("small timeout", {**V2, "INFLUX_TIMEOUT_MS": "1500"}, [_p()], {}),
    ("negative timeout", {**V2, "INFLUX_TIMEOUT_MS": "-5"}, [_p()], {}),
    ("huge timeout", {**V2, "INFLUX_TIMEOUT_MS": "1" + "0" * 400}, [_p()], {}),
    (
        "timeout beyond the nanosecond clock",
        {**V2, "INFLUX_TIMEOUT_MS": "100000000000000000"},
        [_p()],
        {},
    ),
    ("connection refused", {**V2, "INFLUX_URL": "{closed}"}, [_p()], {}),
    (
        "read timeout",
        {**V2, "INFLUX_URL": "{stall}", "INFLUX_TIMEOUT_MS": "200"},
        [_p()],
        {},
    ),
    ("unsupported scheme", {**V2, "INFLUX_URL": "ftp://127.0.0.1:9"}, [_p()], {}),
    (
        "truncated 2xx body fails the write",
        {**V2, "INFLUX_URL": "{raw}"},
        [_p(), INVALID],
        {"raw": "truncated"},
    ),
    (
        "stalled 2xx body times out",
        {**V2, "INFLUX_URL": "{raw}", "INFLUX_TIMEOUT_MS": "300"},
        [_p()],
        {"raw": "stall_body"},
    ),
    (
        "slow body within the read timeout",
        {**V2, "INFLUX_URL": "{raw}", "INFLUX_TIMEOUT_MS": "250"},
        [_p()],
        {"raw": "drip"},
    ),
    (
        "2xx body that is not UTF-8",
        {**V2, "INFLUX_URL": "{raw}"},
        [_p()],
        {"raw": "not_utf8"},
    ),
]

# Exceptions whose text comes from a library and depends on the environment.
P2_CLASSES = {
    "ApiException",
    "NewConnectionError",
    "ReadTimeoutError",
    "ConnectTimeoutError",
    "URLSchemeUnknown",
    "LocationParseError",
    "ProtocolError",
    "UnicodeDecodeError",
}


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[dict[str, str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        entry = {"level": record.levelname, "message": record.getMessage()}
        escaped = record.msg % tuple(_d10(arg) for arg in record.args or ())
        if escaped != entry["message"]:
            entry["d10_message"] = escaped
        self.records.append(entry)


def _d10(arg: object) -> object:
    if isinstance(arg, str):
        return arg.replace("\n", "\\n").replace("\r", "\\r")
    return arg


def _encode_point(point: dict[str, Any]) -> dict[str, Any]:
    encoded = dict(point)
    encoded["ts"] = point["ts"].isoformat()
    encoded["value"] = repr(point["value"])
    tags = point["tags"]
    encoded["tags"] = None if tags is None else [[k, v] for k, v in tags.items()]
    return encoded


def _settings(env: dict[str, str], servers: _Servers) -> dict[str, Any]:
    values = {**ENV_DEFAULTS, **env}
    return {
        "INFLUX_URL": servers.url(values["INFLUX_URL"]),
        "INFLUX_TOKEN": values["INFLUX_TOKEN"],
        "INFLUX_ORG": values["INFLUX_ORG"],
        "INFLUX_BUCKET": values["INFLUX_BUCKET"],
        "INFLUX_MEASUREMENT": values["INFLUX_MEASUREMENT"],
        "INFLUX_TIMEOUT_MS": int(values["INFLUX_TIMEOUT_MS"]),
        "INFLUX_API_MODE": values["INFLUX_API_MODE"].lower(),
    }


def _write(
    servers: _Servers,
    env: dict[str, str],
    points: list[dict[str, Any]],
    respond: dict[str, Any],
) -> dict[str, Any]:
    _Recorder.requests = []
    _Recorder.respond = respond
    _Raw.script = respond.get("raw", "truncated")
    capture = _Capture()
    logger = logging.getLogger(influx.__name__)
    logger.addHandler(capture)
    previous_level = logger.level
    logger.setLevel(logging.DEBUG)
    result: dict[str, Any]
    try:
        with override_settings(**_settings(env, servers)), warnings.catch_warnings():
            warnings.simplefilter("ignore")
            written = influx.write_points([influx.MetricPoint(**p) for p in points])
        result = {"written": written}
    except Exception as exc:  # noqa: BLE001 (the fixture records every failure)
        name = type(exc).__name__
        if name in P2_CLASSES:
            result = {"error": {"class": name, "p2": True}}
        else:
            result = {"error": {"class": name, "message": str(exc)}}
    finally:
        logger.removeHandler(capture)
        logger.setLevel(previous_level)
    return {
        "result": result,
        "requests": list(_Recorder.requests),
        "logs": capture.records,
    }


def _writes() -> list[dict[str, Any]]:
    cases = []
    with _Servers() as servers:
        for name, env, points, respond in WRITES:
            outcome = _write(servers, env, points, respond)
            cases.append(
                {
                    "name": name,
                    "env": env,
                    "respond": respond,
                    "points": [_encode_point(point) for point in points],
                    **outcome,
                }
            )
    return cases


def build_line_protocol() -> dict[str, Any]:
    return {"lines": _lines(), "writes": _writes()}


GENERATORS = {"line_protocol.json": build_line_protocol}
