"""Fixtures ``flux.json``, ``sql_v3.json`` and ``isoformat.json`` (slice S4b).

Pin ``graphyard-influx``'s condition queries (agent-rs/PLAN.md 6.5) and the
``datetime.fromisoformat`` port in ``graphyard-core::pycompat``:

* ``isoformat.json``: CPython 3.12's C ``datetime.fromisoformat`` on a string
  corpus (fields, UTC offset, ``str()``, the UTC conversion of
  ``influx._ensure_utc``) or its ``ValueError``.
* ``flux.json``: ``query_condition_window`` with ``query_range`` stubbed (the
  lookback window, the tag filters, the host-filter warning, errors), the
  Flux text of ``_query_range_v2_flux`` with a capturing client, and
  end-to-end cases of ``query_range`` in mode ``v2`` against a loopback fake
  InfluxDB: the request the real ``influxdb_client`` sends and the samples it
  decodes from canned annotated CSV (``FluxCsvParser``), or the error.
* ``sql_v3.json``: the SQL text and request payload of ``_query_range_v3_sql``
  with ``_read_v3_sql_rows`` stubbed, end-to-end cases in mode ``v3`` (row
  decoding, the error texts of ``_read_v3_sql_rows``, the httpx request log
  line) and mode ``auto`` (fallback to v3 on a v2 404 only).

The fake server answers each connection with one scripted response (raw
bytes, ``Connection: close``), records the requests, and is started per case.
The Rust tests replay the same scripts against their own fake server.
``{server}`` in recorded text stands for ``127.0.0.1:<port>``. Errors whose
text comes from a library (urllib3, ``ApiException`` header dumps) are stored
as ``{"class": ..., "p2": true}``.
"""

from __future__ import annotations

import base64
import contextlib
import gzip
import io
import json
import logging
import socket
import threading
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

from django.test import override_settings

from graphyard import influx
from graphyard.models import ConditionDefinition

NOW = datetime(2026, 10, 21, 12, 0, 0, 123456, tzinfo=UTC)
NOW_WHOLE = datetime(2026, 10, 21, 12, 0, tzinfo=UTC)

# Python classes whose ``str()`` is library text (P2, section 2.3).
P2_CLASSES = {
    "ApiException",
    "NewConnectionError",
    "ReadTimeoutError",
    "ConnectTimeoutError",
    "ProtocolError",
    "MaxRetryError",
    "UnicodeDecodeError",
}

# --------------------------------------------------------------------------
# datetime.fromisoformat
# --------------------------------------------------------------------------

ISOFORMAT_CASES = [
    # InfluxDB renderings (RFC 3339, nanoseconds) and the v3 rendering.
    "2026-10-21T10:00:00Z",
    "2026-10-21T10:00:00.123456789Z",
    "2026-10-21T10:00:00.1Z",
    "2026-10-21T10:00:00.000000001Z",
    "2026-10-21T10:00:00+00:00",
    "2026-10-21T10:00:00.123456789+00:00",
    "2026-10-21T10:00:00",
    "2026-10-21T10:00:00.5",
    # Date forms.
    "2026-10-21",
    "20261021",
    "2026-W43-3",
    "2026W433",
    "2026-W43",
    "2026W43",
    "2026-W53",
    "2020-W53-7",
    "2026-W54-1",
    "2026-W00-1",
    "2026-W01-8",
    "2026-W01-0",
    "2026-1021",
    "202610-21",
    "2026-13-01",
    "2026-00-10",
    "2026-02-30",
    "2024-02-29",
    "2025-02-29",
    "0000-01-01",
    "0001-01-01",
    "9999-12-31",
    "10000-01-01",
    # Separators.
    "2026-10-21 10:00:00",
    "2026-10-21x10:00:00",
    "2026-10-21Ä10:00",
    "2026-10-21€10:00",
    "2026-10-21\U0001f60010:00",
    "2026-10-2110:00",
    "2026102110:00",
    "20261021T1000",
    "2026-W43-3T10:00",
    "2026-W43T10:00",
    "2026W43T10:00",
    "2026W433T10:00",
    "2026-W43-10:00",
    "2026-10-21T",
    "2026-10-21Ä",
    # Time forms.
    "2026-10-21T10",
    "2026-10-21T10:00",
    "2026-10-21T1000",
    "2026-10-21T100000",
    "2026-10-21T10:0000",
    "2026-10-21T1000:00",
    "2026-10-21T10:00:00,5",
    "2026-10-21T10:00:00.",
    "2026-10-21T10:00:00.1234567890123",
    "2026-10-21T10:00:00.12a",
    "2026-10-21T10.5",
    "2026-10-21T10:00.5",
    "2026-10-21T10:00:00 ",
    "2026-10-21T1",
    "2026-10-21T10:0",
    "2026-10-21T10:00:00:00",
    "2026-10-21T24:00:00",
    "2026-10-21T25:00",
    "2026-10-21T10:60",
    "2026-10-21T10:00:60",
    "2026-10-21T10:00:00\u0000",
    # Offsets.
    "2026-10-21T10:00:00+0530",
    "2026-10-21T10:00:00+05",
    "2026-10-21T10:00:00+05:30:15",
    "2026-10-21T10:00:00+05:30:15.5",
    "2026-10-21T10:00:00-05:30:15.000001",
    "2026-10-21T10:00:00-23:59",
    "2026-10-21T10:00:00+23:59:59.999999",
    "2026-10-21T10:00:00+24:00",
    "2026-10-21T10:00:00-24:00",
    "2026-10-21T10:00:00+99:00",
    "2026-10-21T10:00:00+00:00:00",
    "2026-10-21T10:00:00+00:00:00.5",
    "2026-10-21T10:00:00-00:00",
    "2026-10-21T10:00:00.123+01:00",
    "2026-10-21T10:00:00z",
    "2026-10-21T10:00:00Z ",
    "2026-10-21T10:00:00ZZ",
    "2026-10-21T10:00:00+5",
    "2026-10-21T10:00:00+05:3",
    "2026-10-21T10:00:00+05:30Z",
    "2026-10-21T10+05",
    "2026-10-21T-05",
    "2026-10-21TZ",
    "9999-12-31T23:59:59.999999-01:00",
    "9999-12-31T23:59:59+00:00",
    "0001-01-01T00:00:00+01:00",
    "0001-01-01T00:00:00-01:00",
    # Not ISO at all.
    "",
    " 2026-10-21",
    "2026",
    "2026-10",
    "abcdefgh",
    "1700000000",
    "None",
    "True",
    "2026-10-21T10:00:00+00:00+00:00",
]


def _offset_us(dt: datetime) -> int | None:
    offset = dt.utcoffset()
    if offset is None:
        return None
    return (offset.days * 86400 + offset.seconds) * 1_000_000 + offset.microseconds


def _error(err: BaseException) -> dict[str, Any]:
    name = type(err).__name__
    if name in P2_CLASSES:
        return {"class": name, "p2": True}
    return {"class": name, "message": str(err)}


def build_isoformat() -> list[dict[str, Any]]:
    cases = []
    for text in ISOFORMAT_CASES:
        case: dict[str, Any] = {"input": text}
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as err:
            case["error"] = str(err)
            cases.append(case)
            continue
        case["fields"] = [
            dt.year,
            dt.month,
            dt.day,
            dt.hour,
            dt.minute,
            dt.second,
            dt.microsecond,
        ]
        case["offset_us"] = _offset_us(dt)
        case["str"] = str(dt)
        try:
            case["utc"] = influx._ensure_utc(dt).isoformat()
        except OverflowError as err:
            case["utc_error"] = _error(err)
        cases.append(case)
    return cases


# --------------------------------------------------------------------------
# Fake InfluxDB
# --------------------------------------------------------------------------

RECORDED_HEADERS = (
    "accept",
    "accept-encoding",
    "authorization",
    "content-type",
    "user-agent",
)


def _response_bytes(script: dict[str, Any]) -> tuple[bytes, list[bytes]]:
    """The head and the body chunks a script sends."""
    if "body_b64" in script:
        body = base64.b64decode(script["body_b64"])
    else:
        body = script.get("body", "").encode("utf-8")
    headers = [tuple(header) for header in script.get("headers", [])]
    if script.get("gzip"):
        body = gzip.compress(body, mtime=0)
        headers.append(("Content-Encoding", "gzip"))
    chunks = [body]
    if "split" in script:
        cut = script["split"]
        chunks = [body[:cut], body[cut:]]
    if "drip" in script:
        chunks = [body[i : i + 1] for i in range(len(body))]
    length = script.get("content_length", len(body))
    headers.append(("Content-Length", str(length)))
    headers.append(("Connection", "close"))
    head = f"HTTP/1.1 {script['status']} {script['reason']}\r\n"
    head += "".join(f"{name}: {value}\r\n" for name, value in headers)
    return (head + "\r\n").encode("latin-1"), chunks


class FakeInflux:
    """Loopback server: one scripted response per connection, in order."""

    def __init__(self, scripts: list[dict[str, Any]]) -> None:
        self.scripts = scripts
        self.requests: list[dict[str, Any]] = []
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.sock.settimeout(0.2)
        self.port = self.sock.getsockname()[1]
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._serve, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> FakeInflux:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop.set()
        self.thread.join(5)
        self.sock.close()

    def _serve(self) -> None:
        index = 0
        while not self.stop.is_set() and index < len(self.scripts):
            try:
                conn, _ = self.sock.accept()
            except TimeoutError:
                continue
            script = self.scripts[index]
            index += 1
            try:
                self._answer(conn, script)
            except OSError:
                pass
            finally:
                conn.close()

    def _answer(self, conn: socket.socket, script: dict[str, Any]) -> None:
        conn.settimeout(5)
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = conn.recv(65536)
            if not chunk:
                return
            data += chunk
        head, _, body = data.partition(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        method, target, _ = lines[0].split(" ", 2)
        headers = [line.split(": ", 1) for line in lines[1:]]
        length = next((int(v) for k, v in headers if k.lower() == "content-length"), 0)
        while len(body) < length:
            chunk = conn.recv(65536)
            if not chunk:
                break
            body += chunk
        self.requests.append(
            {
                "method": method,
                "target": target,
                "headers": [
                    [k.lower(), v] for k, v in headers if k.lower() in RECORDED_HEADERS
                ],
                "body": body.decode("utf-8"),
            }
        )
        action = script.get("action", "respond")
        if action == "close":
            return
        _sleep(self.stop, script.get("header_delay", 0))
        head_bytes, chunks = _response_bytes(script)
        conn.sendall(head_bytes)
        for position, chunk in enumerate(chunks):
            if position:
                _sleep(self.stop, script.get("chunk_delay", 0))
            conn.sendall(chunk)


def _sleep(stop: threading.Event, seconds: float) -> None:
    if seconds:
        stop.wait(seconds)


def _closed_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class _Records(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.lines: list[list[str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append([record.levelname, record.getMessage()])


@contextmanager
def _captured_logs() -> Iterator[_Records]:
    handler = _Records()
    loggers = [logging.getLogger("graphyard.influx"), logging.getLogger("httpx")]
    saved = [(lg, lg.level, lg.propagate) for lg in loggers]
    for lg in loggers:
        lg.addHandler(handler)
        lg.setLevel(logging.INFO)
        lg.propagate = False
    try:
        yield handler
    finally:
        for lg, level, propagate in saved:
            lg.removeHandler(handler)
            lg.setLevel(level)
            lg.propagate = propagate


def _mask(text: str, port: int) -> str:
    return text.replace(f"127.0.0.1:{port}", "{server}")


def _sample(sample: influx.MetricSample) -> dict[str, Any]:
    return {
        "ts": sample.ts.isoformat(),
        "value": sample.value,
        "host": sample.host,
        "metric": sample.metric,
        "service": sample.service,
        "subject_type": sample.subject_type,
        "subject_id": sample.subject_id,
        "source_system": sample.source_system,
        "source_instance": sample.source_instance,
        "source_entity_id": sample.source_entity_id,
        "collector_service": sample.collector_service,
        "collector_host": sample.collector_host,
        "tags": sample.tags,
    }


BASE_SETTINGS = {
    "INFLUX_TOKEN": "test-token",
    "INFLUX_ORG": "graphyard",
    "INFLUX_BUCKET": "graphyard",
    "INFLUX_MEASUREMENT": "graphyard_metrics",
    "INFLUX_TIMEOUT_MS": 10000,
}

RANGE_START = datetime(2026, 10, 21, 11, 0, tzinfo=UTC)
RANGE_STOP = datetime(2026, 10, 21, 12, 0, 0, 500000, tzinfo=UTC)


def _run_e2e(case: dict[str, Any]) -> dict[str, Any]:
    """Run ``query_range`` against a fake server with the case's scripts."""
    settings = {**BASE_SETTINGS, **case.get("settings", {})}
    call = case.get("call", {})
    # FluxCsvParser prints its duplicated-columns warning to stdout and
    # warns through ``warnings``; neither belongs in the generator output.
    with (
        FakeInflux(case["responses"]) as server,
        _captured_logs() as logs,
        contextlib.redirect_stdout(io.StringIO()),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("ignore")
        if case.get("refused"):
            url = f"http://127.0.0.1:{_closed_port()}"
        elif "url" in case:
            url = case["url"].replace("{server}", f"127.0.0.1:{server.port}")
        else:
            url = server.url
        patches = [
            mock.patch.object(influx, name, value)
            for name, value in case.get("patch", {}).items()
        ]
        for patch in patches:
            patch.start()
        try:
            with override_settings(INFLUX_URL=url, **settings):
                samples = influx.query_range(
                    call.get("metric", "host.disk_usage"),
                    RANGE_START,
                    RANGE_STOP,
                    host=call.get("host"),
                    service=call.get("service"),
                    subject_type=call.get("subject_type"),
                    subject_id=call.get("subject_id"),
                    tags=call.get("tags"),
                )
            result: dict[str, Any] = {"samples": [_sample(s) for s in samples]}
        except Exception as err:  # noqa: BLE001
            result = {"error": _error(err)}
            if "message" in result["error"]:
                result["error"]["message"] = _mask(
                    result["error"]["message"], server.port
                )
        finally:
            for patch in patches:
                patch.stop()
    out = dict(case)
    out["settings"] = settings
    out["requests"] = server.requests
    out["logs"] = [[level, _mask(line, server.port)] for level, line in logs.lines]
    out.update(result)
    return out


# --------------------------------------------------------------------------
# flux.json
# --------------------------------------------------------------------------


def _condition(**fields: Any) -> ConditionDefinition:
    defaults: dict[str, Any] = {
        "id": 7,
        "name": "disk",
        "metric_name": "host.filesystem_used_ratio",
        "host_filter": "",
        "service_filter": "",
        "subject_type_filter": "",
        "subject_id_filter": "",
        "tags_filter": {},
        "operator": "gte",
        "warning_threshold": 0.8,
        "critical_threshold": 0.9,
        "window_minutes": 30,
        "breach_minutes": 5,
        "stale_after_seconds": None,
    }
    defaults.update(fields)
    return ConditionDefinition(**defaults)


WINDOW_FIELDS = (
    "id",
    "name",
    "metric_name",
    "host_filter",
    "service_filter",
    "subject_type_filter",
    "subject_id_filter",
    "tags_filter",
    "window_minutes",
    "breach_minutes",
    "stale_after_seconds",
)

WINDOW_CASES: list[dict[str, Any]] = [
    {"name": "defaults", "condition": {}},
    {"name": "whole_second_now", "condition": {}, "now": "whole"},
    # test_condition_evaluation.py::test_query_condition_window_looks_back_far_enough_for_the_anchor
    {
        "name": "anchor_longer_than_window",
        "condition": {"window_minutes": 10, "breach_minutes": 10},
    },
    {
        "name": "window_longer_than_anchor",
        "condition": {"window_minutes": 30, "breach_minutes": 10},
    },
    {"name": "stale_allowance_longer", "condition": {"stale_after_seconds": 7200}},
    {"name": "stale_allowance_rounds_up", "condition": {"stale_after_seconds": 3601}},
    {
        "name": "stale_allowance_zero_uses_global",
        "condition": {"stale_after_seconds": 0},
    },
    {"name": "stale_global_rounds_up", "condition": {}, "stale_warning": 601},
    {
        "name": "stale_global_zero",
        "condition": {"window_minutes": 0, "breach_minutes": 0},
        "stale_warning": 0,
    },
    {
        "name": "stale_global_negative",
        "condition": {"window_minutes": 0, "breach_minutes": 0},
        "stale_warning": -150,
    },
    {
        "name": "negative_columns",
        "condition": {
            "window_minutes": -10,
            "breach_minutes": -20,
            "stale_after_seconds": -600,
        },
        "stale_warning": -6000,
    },
    {"name": "large_lookback_in_range", "condition": {"window_minutes": 1_000_000_000}},
    {"name": "lookback_before_year_1", "condition": {"window_minutes": 1_500_000_000}},
    {
        "name": "lookback_after_year_9999",
        "condition": {"window_minutes": -5_000_000_000},
    },
    {"name": "lookback_beyond_timedelta", "condition": {}, "stale_warning": 10**14},
    {"name": "lookback_beyond_c_int_days", "condition": {}, "stale_warning": 10**20},
    {"name": "lookback_float_rounding", "condition": {}, "stale_warning": 10**30 + 7},
    {"name": "lookback_division_overflow", "condition": {}, "stale_warning": 10**400},
    {
        "name": "huge_negative_global_ignored",
        "condition": {"stale_after_seconds": 60},
        "stale_warning": -(10**30),
    },
    # test_metric_dimensions.py::test_query_condition_window_applies_subject_filters_and_host_compat
    {
        "name": "subject_filters_drop_host_filter",
        "condition": {
            "name": "network sensor",
            "metric_name": "ha.sensor.fritz_box_7590_ax_cpu_temperature",
            "subject_type_filter": "network_device",
            "subject_id_filter": "Fritz Box 7590 AX",
            "host_filter": "macmini",
            "service_filter": "homeassistant",
            "tags_filter": {"device_class": "temperature"},
        },
    },
    # test_metric_dimensions.py::test_query_condition_window_keeps_host_filter_for_host_subject
    {
        "name": "host_subject_keeps_host_filter",
        "condition": {
            "name": "host disk",
            "subject_type_filter": "host",
            "host_filter": "macmini",
        },
    },
    {
        "name": "host_filter_without_subject_type",
        "condition": {"host_filter": "macmini", "tags_filter": {"mountpoint": "/"}},
    },
    {
        "name": "subject_type_normalised",
        "condition": {"subject_type_filter": "  HOST \t", "host_filter": "macmini"},
    },
    {
        "name": "host_filter_warning_escapes_name",
        "condition": {
            "name": "multi\nline\rname",
            "subject_type_filter": "Service",
            "host_filter": "macmini",
        },
    },
    {
        "name": "tags_filter_values_are_str",
        "condition": {
            "tags_filter": {
                "s": "x",
                "i": 5,
                "f": 1.5,
                "f0": 2.0,
                "big": 10**30,
                "b": True,
                "n": None,
                "l": [1, "a"],
                "d": {"k": False},
            }
        },
    },
    {
        "name": "tags_filter_subject_keys_overridden_in_place",
        "condition": {
            "tags_filter": {"subject_id": "raw", "a": "1", "subject_type": "raw"},
            "subject_type_filter": "Host",
            "subject_id_filter": "Mac Mini",
        },
    },
    {"name": "tags_filter_list", "condition": {"tags_filter": ["a"]}},
    {"name": "tags_filter_null", "condition": {"tags_filter": None}},
    {"name": "tags_filter_string", "condition": {"tags_filter": "a"}},
    {"name": "tags_filter_number", "condition": {"tags_filter": 3}},
    {"name": "invalid_subject_id_filter", "condition": {"subject_id_filter": "__!!__"}},
    {
        "name": "subject_id_filter_unicode",
        "condition": {"subject_id_filter": "Wohnzimmer Äußen K"},
    },
    {"name": "service_filter_kept", "condition": {"service_filter": "unifi"}},
    {
        "name": "error_order_overflow_before_tags",
        "condition": {"tags_filter": ["a"]},
        "stale_warning": 10**400,
    },
]


def _window_case(case: dict[str, Any]) -> dict[str, Any]:
    condition = _condition(**case["condition"])
    stale_warning = case.get("stale_warning", 600)
    now = NOW_WHOLE if case.get("now") == "whole" else NOW
    captured: dict[str, Any] = {}

    def fake_query_range(metric_name, start, stop, **kwargs):  # type: ignore[no-untyped-def]
        captured.update(metric=metric_name, start=start, stop=stop, **kwargs)
        return []

    out: dict[str, Any] = {
        "name": case["name"],
        "condition": {field: getattr(condition, field) for field in WINDOW_FIELDS},
        "stale_warning": str(stale_warning),
        "now": now.isoformat(),
    }
    with (
        override_settings(CONDITION_DATA_STALE_WARNING_SECONDS=stale_warning),
        mock.patch.object(influx, "query_range", fake_query_range),
        _captured_logs() as logs,
    ):
        try:
            influx.query_condition_window(condition, now=now)
        except Exception as err:  # noqa: BLE001
            out["error"] = _error(err)
    out["logs"] = logs.lines
    if "error" not in out:
        out["range"] = {
            "metric": captured["metric"],
            "start": captured["start"].isoformat(),
            "stop": captured["stop"].isoformat(),
            "host": captured["host"],
            "service": captured["service"],
            "tags": captured["tags"],
        }
        out["flux"] = _flux_text(
            captured["metric"],
            captured["start"],
            captured["stop"],
            host=captured["host"],
            service=captured["service"],
            tags=captured["tags"],
        )
    return out


def _flux_text(metric: str, start: datetime, stop: datetime, **kwargs: Any) -> str:
    captured: list[str] = []

    class _Client:
        def __enter__(self) -> _Client:
            return self

        def __exit__(self, *args: object) -> None:
            pass

        def query_api(self) -> Any:
            class _Api:
                def query(self, query: str, org: str) -> list[Any]:
                    captured.append(query)
                    return []

            return _Api()

    with mock.patch.object(influx, "_build_client", _Client):
        influx._query_range_v2_flux(metric, start, stop, **kwargs)
    return captured[0]


SPECIAL = 'a"b\\c${d}$e{f\n\tgä\U0001f600'

FLUX_TEXT_CASES: list[dict[str, Any]] = [
    {"name": "metric_only", "metric": "host.disk_usage"},
    {
        "name": "all_filters",
        "metric": "m",
        "host": "macmini",
        "service": "unifi",
        "subject_type": "host",
        "subject_id": "macmini",
        "tags": {"mountpoint": "/", "device": "sda"},
    },
    {
        "name": "empty_filters_dropped",
        "metric": "m",
        "host": "",
        "service": "",
        "subject_type": "",
        "subject_id": "",
        "tags": {},
    },
    # test_influx_modes.py::test_query_range_v2_flux_uses_safe_bracket_access_for_tag_keys
    {"name": "bracket_access_escapes_keys", "metric": "m1", "tags": {'bad"tag': 'v"1'}},
    # test_query_and_ingest_log_escaping.py::test_flux_interpolation_literal_in_query
    {
        "name": "interpolation_literal",
        "metric": SPECIAL,
        "host": SPECIAL,
        "service": SPECIAL,
        "tags": {SPECIAL: SPECIAL},
        "bucket": SPECIAL,
        "measurement": SPECIAL,
    },
    {
        "name": "dollar_brace_only",
        "metric": "${x}",
        "bucket": "$${",
        "measurement": "\\${",
    },
    {
        "name": "microsecond_bounds",
        "metric": "m",
        "start": "2026-10-21T11:00:00.000001+00:00",
        "stop": "2026-10-21T12:00:00.999999+00:00",
    },
    {
        "name": "early_years",
        "metric": "m",
        "start": "0001-01-01T00:00:00+00:00",
        "stop": "0999-12-31T23:59:59+00:00",
    },
    {
        "name": "start_after_stop",
        "metric": "m",
        "start": "2026-10-21T13:00:00+00:00",
        "stop": "2026-10-21T12:00:00+00:00",
    },
]


def _flux_text_case(case: dict[str, Any]) -> dict[str, Any]:
    start = datetime.fromisoformat(case.get("start", RANGE_START.isoformat()))
    stop = datetime.fromisoformat(case.get("stop", RANGE_STOP.isoformat()))
    kwargs = {
        k: case[k]
        for k in ("host", "service", "subject_type", "subject_id", "tags")
        if k in case
    }
    bucket = case.get("bucket", "graphyard")
    measurement = case.get("measurement", "graphyard_metrics")
    with override_settings(INFLUX_BUCKET=bucket, INFLUX_MEASUREMENT=measurement):
        text = _flux_text(case["metric"], start, stop, **kwargs)
    return {
        **case,
        "start": start.isoformat(),
        "stop": stop.isoformat(),
        "bucket": bucket,
        "measurement": measurement,
        "flux": text,
    }


CSV_TYPES = "#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string,string,string"
CSV_GROUP = "#group,false,false,true,true,false,false,true,true,true,true,true,true"
CSV_DEFAULT = "#default,_result,,,,,,,,,,,"
CSV_HEADER = ",result,table,_start,_stop,_time,_value,_field,_measurement,host,metric,mountpoint,subject_type"


def _csv(
    *rows: str,
    header: str = CSV_HEADER,
    types: str = CSV_TYPES,
    group: str = CSV_GROUP,
    default: str = CSV_DEFAULT,
) -> str:
    return "\r\n".join([types, group, default, header, *rows]) + "\r\n\r\n"


def _row(
    table: int, time_: str, value: str, mountpoint: str = "/", host: str = "macmini"
) -> str:
    return (
        f",,{table},2026-10-21T11:00:00Z,2026-10-21T12:00:00.5Z,{time_},{value},value,"
        f"graphyard_metrics,{host},host.disk_usage,{mountpoint},host"
    )


def _ok(body: str, **extra: Any) -> dict[str, Any]:
    return {
        "status": 200,
        "reason": "OK",
        "headers": [["Content-Type", "text/csv; charset=utf-8"]],
        "body": body,
        **extra,
    }


CSV_CASES: list[dict[str, Any]] = [
    {"name": "empty_response", "responses": [_ok("\r\n")]},
    {"name": "no_body", "responses": [_ok("")]},
    {
        "name": "one_table",
        "responses": [
            _ok(
                _csv(
                    _row(0, "2026-10-21T11:30:00Z", "0.5"),
                    _row(0, "2026-10-21T11:31:00.123456789Z", "0.75"),
                )
            )
        ],
    },
    {
        "name": "several_tables_and_series",
        "responses": [
            _ok(
                _csv(
                    _row(0, "2026-10-21T11:30:00Z", "0.5", "/"),
                    _row(1, "2026-10-21T11:30:00Z", "0.25", "/data"),
                    _row(1, "2026-10-21T11:31:00Z", "1e-3", "/data"),
                )
            )
        ],
    },
    {
        "name": "second_annotation_block",
        "responses": [
            _ok(
                _csv(_row(0, "2026-10-21T11:30:00Z", "1"))
                + "#datatype,string,long,dateTime:RFC3339,long,string,boolean,duration\r\n"
                + "#group,false,false,false,false,true,false,false\r\n"
                + "#default,_result,,,,,,\r\n"
                + ",result,table,_time,_value,mountpoint,flag,age\r\n"
                + ",,2,2026-10-21T11:40:00+02:00,42,/boot,true,15\r\n"
                + ",,2,2026-10-21T11:41:00,7,/boot,false,-3\r\n\r\n"
            )
        ],
    },
    {
        "name": "defaults_fill_empty_cells",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double,string,string,string\r\n"
                "#group,false,false,false,false,true,true,true\r\n"
                "#default,_result,3,,2.5,dflt,,\r\n"
                ",result,table,_time,_value,host,service,device\r\n"
                ",,0,2026-10-21T11:30:00Z,,,,\r\n"
                ",,0,2026-10-21T11:31:00Z,4,h,s,d\r\n"
            )
        ],
    },
    {
        "name": "empty_table_id_ignores_default",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,3,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,,2026-10-21T11:30:00Z,1\r\n"
            )
        ],
    },
    {
        "name": "empty_value_and_time_skipped",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z,\r\n"
                ",,0,,1.5\r\n"
                ",,0,2026-10-21T11:32:00Z,2\r\n"
            )
        ],
    },
    {
        "name": "typed_tag_values",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double,long,unsignedLong,double,boolean,dateTime:RFC3339Nano,duration,base64Binary,unknownType,string\r\n"
                "#group,false,false,false,false,false,false,false,false,false,false,false,false,false\r\n"
                "#default,_result,,,,,,,,,,,,\r\n"
                ",result,table,_time,_value,l,u,d,b,t,du,bin,x,empty\r\n"
                ",,0,2026-10-21T11:30:00Z,1,-5,18446744073709551615,1e16,true,2026-10-21T11:30:00.5-01:30,1000,aGk=,whatever,\r\n"
                ",,0,2026-10-21T11:31:00Z,1,1_000, 7 ,+Inf,TRUE,0001-01-01T00:00:00Z,0,AAEC/w==,,x\r\n"
            )
        ],
    },
    {
        "name": "value_types",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,long\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z,12345678901234567890\r\n"
            ),
        ],
    },
    {
        "name": "string_value_parsed_as_float",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,string\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z, 1_5.5 \r\n"
            ),
        ],
    },
    # test_influx_query_safety.py::test_nonfinite_query_evidence_is_an_evaluation_error (v2)
    *[
        {
            "name": f"non_finite_{label}",
            "responses": [
                _ok(
                    _csv(
                        _row(0, "2026-10-21T11:30:00Z", "20"),
                        _row(0, "2026-10-21T11:31:00Z", value),
                    )
                )
            ],
        }
        for label, value in [
            ("nan", "NaN"),
            ("inf", "+Inf"),
            ("neg_inf", "-Inf"),
            ("overflow", "1e999"),
        ]
    ],
    {
        "name": "string_value_not_a_number",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,string\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z,abc\r\n"
            ),
        ],
    },
    {
        "name": "bool_value",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,boolean\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z,true\r\n"
            ),
        ],
    },
    {
        "name": "time_not_a_datetime",
        "responses": [
            _ok(
                "#datatype,string,long,string,double\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z,1\r\n"
            ),
        ],
    },
    {
        "name": "time_out_of_range_after_utc",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,0001-01-01T00:00:00+01:00,1\r\n"
            ),
        ],
    },
    {
        "name": "missing_value_column",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339\r\n"
                "#group,false,false,false\r\n"
                "#default,_result,,\r\n"
                ",result,table,_time\r\n"
                ",,0,2026-10-21T11:30:00Z\r\n"
            ),
        ],
    },
    {
        "name": "missing_time_column",
        "responses": [
            _ok(
                "#datatype,string,long,double\r\n"
                "#group,false,false,false\r\n"
                "#default,_result,,\r\n"
                ",result,table,_value\r\n"
                ",,0,1\r\n"
            ),
        ],
    },
    {
        "name": "invalid_double",
        "responses": [_ok(_csv(_row(0, "2026-10-21T11:30:00Z", '"1,5"')))],
    },
    {
        "name": "invalid_long",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,long\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z,1.5\r\n"
            ),
        ],
    },
    {
        "name": "invalid_datetime",
        "responses": [_ok(_csv(_row(0, "yesterday", "1")))],
    },
    {
        "name": "invalid_table_id",
        "responses": [
            _ok(_csv(_row(0, "2026-10-21T11:30:00Z", "1").replace(",,0,", ",,x,", 1)))
        ],
    },
    {
        "name": "invalid_base64",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double,base64Binary\r\n"
                "#group,false,false,false,false,false\r\n"
                "#default,_result,,,,\r\n"
                ",result,table,_time,_value,bin\r\n"
                ",,0,2026-10-21T11:30:00Z,1,abcde\r\n"
            ),
        ],
    },
    {
        "name": "query_error_table",
        "responses": [
            _ok(
                "#datatype,string,string\r\n"
                "#group,true,true\r\n"
                "#default,,\r\n"
                ",error,reference\r\n"
                ',"failed to initialize execute state: type error 3:1-3:30: ""x"" not found",897\r\n'
            ),
        ],
    },
    {
        "name": "query_error_after_records",
        "responses": [
            _ok(
                _csv(_row(0, "2026-10-21T11:30:00Z", "1"))
                + "#datatype,string,string\r\n#group,true,true\r\n#default,,\r\n,error,reference\r\n,'quoted' message,\r\n"
            ),
        ],
    },
    {
        "name": "no_annotations",
        "responses": [
            _ok(",result,table,_time,_value\r\n,,0,2026-10-21T11:30:00Z,1\r\n")
        ],
    },
    {
        "name": "no_group_annotation",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z,1\r\n"
            ),
        ],
    },
    {
        "name": "default_before_datatype",
        "responses": [
            _ok(
                "#default,_result,,,\r\n"
                "#datatype,string,long,dateTime:RFC3339,double\r\n"
                "#group,false,false,false,false\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z,\r\n"
                ",,0,2026-10-21T11:31:00Z,3\r\n"
            ),
        ],
    },
    {
        "name": "short_data_row",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z\r\n"
            ),
        ],
    },
    {
        "name": "single_cell_row",
        "responses": [_ok(_csv(_row(0, "2026-10-21T11:30:00Z", "1")) + "x\r\n")],
    },
    {
        "name": "two_cell_row",
        "responses": [_ok(_csv(_row(0, "2026-10-21T11:30:00Z", "1")) + ",y\r\n")],
    },
    {
        "name": "short_group_row",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double\r\n"
                "#group,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
            ),
        ],
    },
    {
        "name": "duplicate_column_names",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double,string,string\r\n"
                "#group,false,false,false,false,true,true\r\n"
                "#default,_result,,,,,\r\n"
                ",result,table,_time,_value,tag,tag\r\n"
                ",,0,2026-10-21T11:30:00Z,1,first,second\r\n"
            ),
        ],
    },
    {
        "name": "quoted_cells",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double,string,string,string\r\n"
                "#group,false,false,false,false,true,true,true\r\n"
                "#default,_result,,,,,,\r\n"
                ",result,table,_time,_value,a,b,c\r\n"
                ',,0,2026-10-21T11:30:00Z,1,"x,y","say ""hi""","multi\nline"\r\n'
                ',,0,2026-10-21T11:31:00Z,2,plain"quote,"",unicode ä€\r\n'
            ),
        ],
    },
    {
        "name": "lf_line_endings",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double,string\n"
                "#group,false,false,false,false,true\n"
                "#default,_result,,,,\n"
                ",result,table,_time,_value,host\n"
                ",,0,2026-10-21T11:30:00Z,1,h1\n\n"
            ),
        ],
    },
    {
        "name": "extra_cells_ignored",
        "responses": [
            _ok(
                "#datatype,string,long,dateTime:RFC3339,double\r\n"
                "#group,false,false,false,false\r\n"
                "#default,_result,,,\r\n"
                ",result,table,_time,_value\r\n"
                ",,0,2026-10-21T11:30:00Z,1,extra,cells\r\n"
            ),
        ],
    },
    {
        "name": "invalid_utf8",
        "responses": [
            {
                "status": 200,
                "reason": "OK",
                "headers": [["Content-Type", "text/csv"]],
                "body_b64": base64.b64encode(b"#datatype,string\r\n,\xff\r\n").decode(),
            }
        ],
    },
]

REQUEST_CASES: list[dict[str, Any]] = [
    {
        "name": "request_shape",
        "responses": [_ok("")],
        "call": {"metric": "m", "host": "h", "tags": {"k": "v"}},
    },
    {
        "name": "request_non_ascii_query_and_org",
        "responses": [_ok("")],
        "settings": {"INFLUX_ORG": "my org/ä&=+~*", "INFLUX_TOKEN": "tok en"},
        "call": {"metric": "tempä\U0001f600 \x7f\x01"},
    },
    {
        "name": "request_url_trailing_slash",
        "responses": [_ok("")],
        "url": "http://{server}/",
    },
    {
        "name": "request_url_double_trailing_slash",
        "responses": [_ok("")],
        "url": "http://{server}//",
    },
    {
        "name": "request_url_path_prefix",
        "responses": [_ok("")],
        "url": "http://{server}/influx",
    },
    {
        "name": "mode_other_is_v2",
        "responses": [_ok("")],
        "settings": {"INFLUX_API_MODE": "whatever"},
    },
]

V2_ERROR_CASES: list[dict[str, Any]] = [
    {"name": "missing_url", "responses": [], "url": ""},
    {"name": "missing_token", "responses": [], "settings": {"INFLUX_TOKEN": ""}},
    {"name": "missing_org", "responses": [], "settings": {"INFLUX_ORG": ""}},
    {
        "name": "http_404",
        "responses": [
            {
                "status": 404,
                "reason": "Not Found",
                "headers": [["Content-Type", "application/json"]],
                "body": '{"code":"not found","message":"path not found"}',
            }
        ],
    },
    {
        "name": "http_500",
        "responses": [
            {
                "status": 500,
                "reason": "Internal Server Error",
                "headers": [],
                "body": "boom",
            }
        ],
    },
    {
        "name": "http_302_not_followed",
        "responses": [
            {
                "status": 302,
                "reason": "Found",
                "headers": [["Location", "/elsewhere"]],
                "body": "",
            }
        ],
    },
    {"name": "connection_refused", "responses": [], "refused": True},
    {"name": "closed_without_response", "responses": [{"action": "close"}]},
    {
        "name": "read_timeout",
        "responses": [_ok("", header_delay=1.5)],
        "settings": {"INFLUX_TIMEOUT_MS": 200},
    },
    {
        "name": "truncated_body",
        "responses": [
            _ok(_csv(_row(0, "2026-10-21T11:30:00Z", "1")), content_length=1000)
        ],
    },
    {
        "name": "http_404_truncated_body",
        "responses": [
            {
                "status": 404,
                "reason": "Not Found",
                "headers": [],
                "body": "x",
                "content_length": 100,
            }
        ],
    },
    {
        "name": "gzip_body_decoded",
        "responses": [_ok(_csv(_row(0, "2026-10-21T11:30:00Z", "1")), gzip=True)],
    },
    {
        "name": "timeout_zero_means_none",
        "responses": [_ok("")],
        "settings": {"INFLUX_TIMEOUT_MS": 0},
    },
    {
        "name": "timeout_negative",
        "responses": [],
        "settings": {"INFLUX_TIMEOUT_MS": -5},
    },
    {
        "name": "timeout_overflow",
        "responses": [],
        "settings": {"INFLUX_TIMEOUT_MS": 10**400},
    },
]

for _case in V2_ERROR_CASES:
    _case.setdefault("settings", {})
    _case["settings"].setdefault("INFLUX_API_MODE", "v2")
for _case in CSV_CASES + REQUEST_CASES:
    _case.setdefault("settings", {})
    _case["settings"].setdefault("INFLUX_API_MODE", "v2")


B64_CASES = [
    "",
    "aGk=",
    "aGk",
    "aGk==",
    "aGk=ignored",
    "aG=k=",
    "a!G k\n=",
    "abcde",
    "a",
    "ab",
    "abc",
    "ab==cd",
    "AAEC/w==",
    "-_8=",
    "+/8=",
    "====",
    "a===",
    "YWJj\u00e4",
    "'\"\\\t\r\x7f",
    "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+P0BBQkNERUZHSElKS0xNTk9QUVJTVFVWV1hZWltcXV5fYGFiY2RlZmdoaWprbG1ub3BxcnN0dXZ3eHl6e3x9fn+AgYKD",
]


def _b64_case(text: str) -> dict[str, Any]:
    try:
        decoded = base64.b64decode(text)
    except (ValueError, TypeError) as err:
        return {
            "input": text,
            "error": {"class": type(err).__name__, "message": str(err)},
        }
    return {"input": text, "str": str(decoded)}


def build_flux() -> dict[str, Any]:
    return {
        "b64decode": [_b64_case(text) for text in B64_CASES],
        "window": [_window_case(case) for case in WINDOW_CASES],
        "flux_text": [_flux_text_case(case) for case in FLUX_TEXT_CASES],
        "csv": [_run_e2e(case) for case in CSV_CASES],
        "requests": [_run_e2e(case) for case in REQUEST_CASES],
        "errors": [_run_e2e(case) for case in V2_ERROR_CASES],
    }


# --------------------------------------------------------------------------
# sql_v3.json
# --------------------------------------------------------------------------

SQL_TEXT_CASES: list[dict[str, Any]] = [
    {"name": "metric_only", "metric": "host.disk_usage"},
    {
        "name": "all_filters",
        "metric": "m",
        "host": "macmini",
        "service": "unifi",
        "subject_type": "host",
        "subject_id": "macmini",
        "tags": {"mountpoint": "/", "device": "sda"},
    },
    {
        "name": "empty_filters_dropped",
        "metric": "m",
        "host": "",
        "service": "",
        "tags": {},
    },
    {
        "name": "quotes_doubled",
        "metric": "it's",
        "host": "o'h",
        "tags": {"k": "a''b"},
        "measurement": "m_1",
    },
    {"name": "unicode_literals", "metric": "ä\U0001f600\n", "tags": {"_x9": "\\"}},
    {"name": "invalid_tag_key", "metric": "m", "tags": {"ok": "1", "bad-key": "2"}},
    {"name": "invalid_tag_key_unicode", "metric": "m", "tags": {"ä": "1"}},
    {"name": "invalid_tag_key_newline", "metric": "m", "tags": {"a\n": "1"}},
    {"name": "invalid_measurement", "metric": "m", "measurement": "graphyard.metrics"},
    {
        "name": "invalid_key_before_measurement",
        "metric": "m",
        "tags": {"1x": "1"},
        "measurement": "bad name",
    },
    {"name": "token_absent", "metric": "m", "token": ""},
    {"name": "bucket_unicode", "metric": "m", "bucket": "bä"},
    {
        "name": "microsecond_bounds",
        "metric": "m",
        "start": "2026-10-21T11:00:00.000001+00:00",
        "stop": "2026-10-21T12:00:00.999999+00:00",
    },
]


def _sql_text_case(case: dict[str, Any]) -> dict[str, Any]:
    start = datetime.fromisoformat(case.get("start", RANGE_START.isoformat()))
    stop = datetime.fromisoformat(case.get("stop", RANGE_STOP.isoformat()))
    kwargs = {
        k: case[k]
        for k in ("host", "service", "subject_type", "subject_id", "tags")
        if k in case
    }
    measurement = case.get("measurement", "graphyard_metrics")
    bucket = case.get("bucket", "graphyard")
    token = case.get("token", "test-token")
    captured: dict[str, Any] = {}

    async def fake_read(payload: dict[str, str], headers: dict[str, str]) -> Any:
        captured["payload"] = payload
        captured["headers"] = headers
        return []

    out = {
        **case,
        "start": start.isoformat(),
        "stop": stop.isoformat(),
        "measurement": measurement,
        "bucket": bucket,
        "token": token,
    }
    with (
        override_settings(
            INFLUX_MEASUREMENT=measurement, INFLUX_BUCKET=bucket, INFLUX_TOKEN=token
        ),
        mock.patch.object(influx, "_read_v3_sql_rows", fake_read),
    ):
        try:
            influx._query_range_v3_sql(case["metric"], start, stop, **kwargs)
        except Exception as err:  # noqa: BLE001
            out["error"] = _error(err)
            return out
    out["payload"] = captured["payload"]
    out["headers"] = captured["headers"]
    return out


def _json_ok(body: str, **extra: Any) -> dict[str, Any]:
    return {
        "status": 200,
        "reason": "OK",
        "headers": [["Content-Type", "application/json"]],
        "body": body,
        **extra,
    }


def _v3_rows(*values: Any) -> str:
    rows = [
        {
            "time": (
                NOW_WHOLE - timedelta(minutes=len(values) - index - 1)
            ).isoformat(),
            "value": value,
            "metric": "host.disk_usage",
            "host": "h1",
            "mountpoint": "/" if index else "/data",
        }
        for index, value in enumerate(values)
    ]
    return json.dumps(rows)


_EXACT_BODY = json.dumps(
    [
        {"time": NOW_WHOLE.isoformat(), "value": 20, "mountpoint": "/"},
        {"time": NOW_WHOLE.isoformat(), "value": 30, "mountpoint": "/data"},
    ]
)

V3_ROW_CASES: list[dict[str, Any]] = [
    # test_influx_query_safety.py::test_finite_query_keeps_multiple_series (v3)
    {"name": "finite_rows_keep_series", "responses": [_json_ok(_v3_rows(20.0, 25.0))]},
    # test_influx_modes.py::test_query_range_v3_sql_selects_and_returns_dimension_columns
    {
        "name": "dimension_columns",
        "responses": [
            _json_ok(
                json.dumps(
                    [
                        {
                            "time": "2026-03-04T12:00:00Z",
                            "value": 22.4,
                            "host": "macmini",
                            "metric": "ha.sensor.office_temperature",
                            "service": "homeassistant",
                            "subject_type": "environment_sensor",
                            "subject_id": "office_temperature",
                            "source_system": "homeassistant",
                            "source_instance": "ha-main",
                            "source_entity_id": "sensor.office_temperature",
                            "collector_service": "graphyard-agent",
                            "collector_host": "macmini",
                            "mountpoint": "/",
                            "device": None,
                        }
                    ]
                )
            )
        ],
    },
    # test_influx_query_safety.py::test_nonfinite_query_evidence_is_an_evaluation_error (v3)
    *[
        {
            "name": f"non_finite_{label}",
            "responses": [_json_ok(_v3_rows(20.0, 25.0, value))],
        }
        for label, value in [
            ("nan", "NaN"),
            ("inf", "Infinity"),
            ("neg_inf", "-Infinity"),
            ("overflow", "1e999"),
        ]
    ],
    {
        "name": "overflow_number_token",
        "responses": [_json_ok('[{"time": "2026-10-21T11:00:00Z", "value": 1e999}]')],
    },
    {
        "name": "value_types",
        "responses": [
            _json_ok(
                '[{"time": "2026-10-21T11:00:00Z", "value": 3},'
                ' {"time": "2026-10-21T11:01:00Z", "value": "4.5"},'
                ' {"time": "2026-10-21T11:02:00Z", "value": " 1_0 "},'
                ' {"time": "2026-10-21T11:03:00Z", "value": 123456789012345678901234567890},'
                ' {"time": "2026-10-21T11:04:00Z", "value": -0.0}]'
            )
        ],
    },
    {
        "name": "bool_value",
        "responses": [_json_ok('[{"time": "2026-10-21T11:00:00Z", "value": true}]')],
    },
    {
        "name": "list_value",
        "responses": [_json_ok('[{"time": "2026-10-21T11:00:00Z", "value": [1]}]')],
    },
    {
        "name": "rows_skipped",
        "responses": [
            _json_ok(
                '[1, "x", null, [], {"value": 1}, {"time": "2026-10-21T11:00:00Z"},'
                ' {"time": null, "value": 1}, {"time": "2026-10-21T11:00:00Z", "value": null},'
                ' {"time": "not a time", "value": 1}, {"time": 1700000000, "value": 1},'
                ' {"time": true, "value": 1}, {"time": {"a": 1}, "value": 1},'
                ' {"time": "2026-10-21T11:05:00", "value": 5}]'
            )
        ],
    },
    {
        "name": "time_forms",
        "responses": [
            _json_ok(
                '[{"time": "2026-10-21T11:00:00.123456789Z", "value": 1},'
                ' {"time": "2026-10-21T13:00:00+02:00", "value": 2},'
                ' {"time": "2026-10-21", "value": 3},'
                ' {"time": "2026-10-21T11:00:00ZZ", "value": 4},'
                ' {"time": "2026-10-21 11:00:00.5", "value": 5}]'
            )
        ],
    },
    {
        "name": "time_out_of_range_after_utc",
        "responses": [_json_ok('[{"time": "0001-01-01T00:00:00+01:00", "value": 1}]')],
    },
    {
        "name": "tag_values_are_str",
        "responses": [
            _json_ok(
                '[{"time": "2026-10-21T11:00:00Z", "value": 1, "host": null, "service": 5,'
                ' "metric": null, "a": 1.0, "b": true, "c": [1, {"x": null}], "d": {"k": "v"},'
                ' "e": null, "f": 1e400, "g": 10, "subject_id": false}]'
            )
        ],
    },
    {
        "name": "repeated_keys",
        "responses": [
            _json_ok(
                '[{"time": "2026-10-21T11:00:00Z", "a": "1", "value": 1, "b": "x", "a": "2", "value": 2}]'
            )
        ],
    },
    {
        "name": "missing_columns_defaults",
        "responses": [_json_ok('[{"time": "2026-10-21T11:00:00Z", "value": 1}]')],
    },
    {"name": "empty_list", "responses": [_json_ok("[]")]},
]

V3_ERROR_CASES: list[dict[str, Any]] = [
    # test_influx_query_safety.py::test_sql_failures_close_response
    {
        "name": "http_404",
        "responses": [
            {"status": 404, "reason": "Not Found", "headers": [], "body": ""}
        ],
    },
    {
        "name": "http_500",
        "responses": [
            {
                "status": 500,
                "reason": "Internal Server Error",
                "headers": [],
                "body": "",
            }
        ],
    },
    {
        "name": "http_302",
        "responses": [
            {
                "status": 302,
                "reason": "Found",
                "headers": [["Location", "/x"]],
                "body": "",
            }
        ],
    },
    {
        "name": "http_101_like_199",
        "responses": [
            {"status": 299, "reason": "Odd Success", "headers": [], "body": "[]"}
        ],
    },
    {"name": "invalid_json", "responses": [_json_ok("invalid")]},
    {"name": "not_a_list", "responses": [_json_ok("{}")]},
    {
        "name": "nan_token",
        "responses": [_json_ok('[{"time": "2026-10-21T11:00:00Z", "value": NaN}]')],
        "divergence": "D-4",
    },
    {"name": "bom", "responses": [_json_ok("﻿[]")]},
    {"name": "trailing_garbage", "responses": [_json_ok("[] x")]},
    {"name": "int_digit_limit", "responses": [_json_ok("[" + "1" * 4301 + "]")]},
    {
        "name": "invalid_utf8",
        "responses": [
            {
                "status": 200,
                "reason": "OK",
                "headers": [],
                "body_b64": base64.b64encode(b'["\xff"]').decode(),
            }
        ],
    },
    # test_influx_query_safety.py::test_sql_oversize_stops_reading_and_closes
    {
        "name": "oversize",
        "responses": [_json_ok("[12345]", split=1)],
        "patch": {"SQL_QUERY_MAX_BODY_BYTES": 5},
    },
    # test_influx_query_safety.py::test_sql_size_limit_counts_decoded_bytes
    {
        "name": "oversize_decoded",
        "responses": [_json_ok("[" + " " * 1000 + "]", gzip=True)],
        "patch": {"SQL_QUERY_MAX_BODY_BYTES": 100},
    },
    # test_influx_query_safety.py::test_sql_exact_byte_limit_and_split_json_keep_series
    {
        "name": "exact_byte_limit_split",
        "responses": [_json_ok(_EXACT_BODY, split=10)],
        "patch": {"SQL_QUERY_MAX_BODY_BYTES": len(_EXACT_BODY)},
    },
    {"name": "gzip_body", "responses": [_json_ok(_v3_rows(1.0), gzip=True)]},
    # test_influx_query_safety.py::test_sql_deadline_includes_headers
    {
        "name": "deadline_headers",
        "responses": [_json_ok("[]", header_delay=1.5)],
        "patch": {"SQL_QUERY_TOTAL_TIMEOUT_SECONDS": 0.3},
    },
    # test_influx_query_safety.py::test_sql_deadline_cancels_drip_or_blocked_read
    {
        "name": "deadline_drip",
        "responses": [_json_ok("[" + " " * 40 + "]", drip=True, chunk_delay=0.05)],
        "patch": {"SQL_QUERY_TOTAL_TIMEOUT_SECONDS": 0.3},
    },
    {
        "name": "deadline_blocked_read",
        "responses": [_json_ok("[" + " " * 10 + "]", split=1, chunk_delay=1.5)],
        "patch": {"SQL_QUERY_TOTAL_TIMEOUT_SECONDS": 0.3},
    },
    # test_influx_query_safety.py::test_sql_transport_error_preserves_safe_category
    {"name": "connection_refused", "responses": [], "refused": True},
    {"name": "closed_without_response", "responses": [{"action": "close"}]},
    {"name": "truncated_body", "responses": [_json_ok("[]", content_length=10)]},
    {
        "name": "invalid_gzip",
        "responses": [_json_ok("not gzip", headers=[["Content-Encoding", "gzip"]])],
    },
    {
        "name": "read_timeout",
        "responses": [_json_ok("[]", header_delay=1.6)],
        "settings": {"INFLUX_TIMEOUT_MS": 100},
    },
    # test_influx_query_safety.py::test_sql_invalid_header_is_not_reported_as_invalid_json
    {
        "name": "token_not_ascii",
        "responses": [],
        "settings": {"INFLUX_TOKEN": "pasted-smart-quote-’"},
    },
    {
        "name": "token_not_latin1_free",
        "responses": [],
        "settings": {"INFLUX_TOKEN": "café"},
    },
    {
        "name": "token_control_character",
        "responses": [],
        "settings": {"INFLUX_TOKEN": "a\nb"},
    },
    {
        "name": "token_absent_no_header",
        "responses": [_json_ok("[]")],
        "settings": {"INFLUX_TOKEN": ""},
    },
    {"name": "url_empty", "responses": [], "url": ""},
    {"name": "url_unsupported_scheme", "responses": [], "url": "ftp://{server}"},
    {"name": "url_no_scheme", "responses": [], "url": "{server}"},
    {
        "name": "url_trailing_slashes",
        "responses": [_json_ok("[]")],
        "url": "http://{server}///",
    },
    {
        "name": "url_path_prefix",
        "responses": [_json_ok("[]")],
        "url": "http://{server}/v3/",
    },
    {
        "name": "timeout_overflow",
        "responses": [],
        "settings": {"INFLUX_TIMEOUT_MS": 10**400},
    },
    {
        "name": "timeout_floor_is_one_second",
        "responses": [_json_ok("[]", header_delay=0.5)],
        "settings": {"INFLUX_TIMEOUT_MS": 100},
    },
    {
        "name": "timeout_negative_uses_floor",
        "responses": [_json_ok("[]")],
        "settings": {"INFLUX_TIMEOUT_MS": -5},
    },
]

for _case in V3_ROW_CASES + V3_ERROR_CASES:
    _case.setdefault("settings", {})
    _case["settings"].setdefault("INFLUX_API_MODE", "v3")

_V2_404 = {
    "status": 404,
    "reason": "Not Found",
    "headers": [["Content-Type", "application/json"]],
    "body": '{"code":"not found","message":"path not found"}',
}

AUTO_CASES: list[dict[str, Any]] = [
    # test_influx_modes.py::test_query_range_auto_falls_back_to_v3_on_v2_404
    {
        "name": "v2_404_falls_back_to_v3",
        "responses": [_V2_404, _json_ok(_v3_rows(42.0))],
    },
    {
        "name": "v2_and_v3_404",
        "responses": [
            _V2_404,
            {"status": 404, "reason": "Not Found", "headers": [], "body": ""},
        ],
    },
    # test_influx_modes.py::test_query_range_auto_does_not_hide_non_404_errors
    {
        "name": "v2_500_propagates",
        "responses": [
            {
                "status": 500,
                "reason": "Internal Server Error",
                "headers": [],
                "body": "server error",
            }
        ],
    },
    {
        "name": "v2_ok_no_fallback",
        "responses": [_ok(_csv(_row(0, "2026-10-21T11:30:00Z", "1")))],
    },
    {
        "name": "v2_config_error_no_fallback",
        "responses": [],
        "settings": {"INFLUX_TOKEN": ""},
    },
    {
        "name": "v2_parse_error_no_fallback",
        "responses": [_ok(_csv(_row(0, "2026-10-21T11:30:00Z", "NaN")))],
    },
    {"name": "v2_refused_no_fallback", "responses": [], "refused": True},
    {
        "name": "v2_truncated_404_no_fallback",
        "responses": [
            {
                "status": 404,
                "reason": "Not Found",
                "headers": [],
                "body": "x",
                "content_length": 100,
            }
        ],
    },
]

for _case in AUTO_CASES:
    _case.setdefault("settings", {})
    _case["settings"].setdefault("INFLUX_API_MODE", "auto")


def build_sql_v3() -> dict[str, Any]:
    return {
        "sql_text": [_sql_text_case(case) for case in SQL_TEXT_CASES],
        "rows": [_run_e2e(case) for case in V3_ROW_CASES],
        "errors": [_run_e2e(case) for case in V3_ERROR_CASES],
        "auto": [_run_e2e(case) for case in AUTO_CASES],
    }


GENERATORS = {
    "flux.json": build_flux,
    "isoformat.json": build_isoformat,
    "sql_v3.json": build_sql_v3,
}
