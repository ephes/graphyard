"""Fixtures ``page_probe.json`` and ``unifi.json``: the page probe and UniFi
executors (slice S7b).

Pins ``graphyard-collect::executors::{page_probe, unifi}`` (agent-rs/PLAN.md
6.3) against ``services.py``: ``_execute_http_page_probe_spec``,
``_execute_unifi_device_traffic_spec`` and the UniFi helpers
``_find_unifi_device``, ``_resolve_unifi_interface_stats``,
``_unifi_response_summary``, ``_unifi_response_error`` and
``_unifi_login_response_ok``. Nothing touches the network: the executors run
with the real ``httpx.Client`` on ``httpx.MockTransport``, and
``influx.write_points``/``touch_registry_from_points`` are replaced by a
recording sink, so the Rust tests replay each scenario on a fake server with
a fake sink.

Executor scenarios (``executor`` in both files):

* ``config``: the spec's config; URLs use the origin ``ORIGIN``, which the
  Rust tests replace with their fake server's origin everywhere (config,
  replies, expected texts);
* ``routes``: request target → replies in order (the last repeats), 404 for
  anything else; ``sink``: ``write_error``/``registry_error`` make the sink
  raise ``RuntimeError(<text>)``;
* ``requests``: method, target, the headers in ``RECORDED_HEADERS`` (lower
  case, as sent) and the body text;
* ``calls``: the sink calls in order with their points (every field except
  ``ts``; the page probe's timing values are ``null``, they are measured);
* ``outcome``: ``[status, ingested, skipped, message]``, or ``raised``: the
  exception the executor raised (``class``, ``message``); ``p2`` marks a
  library-derived message whose text may differ (PLAN.md 2.3);
* ``log``: the ``HTTP Request:`` lines.

``MockTransport`` checks no URL scheme, so URLs that httpcore rejects
(``UnsupportedProtocol``) are pinned by ``http_errors.json`` and Rust tests,
not here.
"""

from __future__ import annotations

import gzip
import json
import logging
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any
from unittest import mock

import httpx

from graphyard import influx, services
from graphyard.spec_config import SpecConfigError

ORIGIN = "http://probe.test"
RECORDED_HEADERS = (
    "accept",
    "content-type",
    "content-length",
    "x-csrf-token",
    "cookie",
    "authorization",
)
TIMING_METRICS = {
    "service.http_page_ttfb_seconds",
    "service.http_page_total_seconds",
}

Reply = dict[str, Any]
Routes = dict[str, Reply | list[Reply]]


def _reply(status: int, body: bytes = b"", **headers: str) -> Reply:
    return {"status": status, "headers": headers, "body": body}


def _json_reply(status: int, payload: object, **headers: str) -> Reply:
    return _reply(status, json.dumps(payload).encode(), **headers)


def _gzip_reply(status: int, body: bytes) -> Reply:
    # The OS byte of the gzip header is pinned to Unix (3) so the file is the
    # same on every platform (zlib writes 19 on macOS).
    compressed = bytearray(gzip.compress(body, mtime=0))
    compressed[9] = 3
    return {
        "status": status,
        "headers": {"Content-Encoding": "gzip"},
        "body": bytes(compressed),
    }


def _encode_reply(reply: Reply) -> dict[str, Any]:
    return {
        "status": reply["status"],
        "headers": [[key, value] for key, value in reply["headers"].items()],
        "body_hex": reply["body"].hex(),
    }


def _encode_point(point: influx.MetricPoint) -> dict[str, Any]:
    return {
        "metric": point.metric,
        "value": None if point.metric in TIMING_METRICS else point.value,
        "subject_type": point.subject_type,
        "subject_id": point.subject_id,
        "source_system": point.source_system,
        "collector_service": point.collector_service,
        "collector_host": point.collector_host,
        "source_instance": point.source_instance,
        "source_entity_id": point.source_entity_id,
        "host": point.host,
        "service": point.service,
        "tags": None
        if point.tags is None
        else [[key, value] for key, value in point.tags.items()],
    }


class _Sink:
    """Stands in for ``influx.write_points`` and ``touch_registry_from_points``."""

    def __init__(self, write_error: str | None, registry_error: str | None) -> None:
        self.write_error = write_error
        self.registry_error = registry_error
        self.calls: list[dict[str, Any]] = []

    def write_points(self, points: list[influx.MetricPoint]) -> int:
        self.calls.append(
            {"call": "write", "points": [_encode_point(p) for p in points]}
        )
        if self.write_error is not None:
            raise RuntimeError(self.write_error)
        return len(points)

    def touch_registry(self, points: list[influx.MetricPoint]) -> None:
        self.calls.append(
            {"call": "touch", "points": [_encode_point(p) for p in points]}
        )
        if self.registry_error is not None:
            raise RuntimeError(self.registry_error)


class _LogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


def _record_request(request: httpx.Request) -> dict[str, Any]:
    headers = [
        [name, request.headers[name]]
        for name in RECORDED_HEADERS
        if name in request.headers
    ]
    return {
        "method": request.method,
        "target": request.url.raw_path.decode("ascii"),
        "headers": headers,
        "body": request.read().decode("utf-8"),
    }


def _run_executor(
    executor: Callable[[Any], tuple[str, int, int, str]],
    *,
    name: str,
    config: object,
    routes: Routes,
    write_error: str | None = None,
    registry_error: str | None = None,
    p2: bool = False,
) -> dict[str, Any]:
    requests: list[dict[str, Any]] = []
    counters: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(_record_request(request))
        target = request.url.raw_path.decode("ascii")
        route = routes.get(target, _reply(404))
        if isinstance(route, list):
            index = counters.get(target, 0)
            counters[target] = index + 1
            route = route[min(index, len(route) - 1)]
        return httpx.Response(
            route["status"], headers=route["headers"], content=route["body"]
        )

    real_client = httpx.Client

    def client_factory(**kwargs: Any) -> httpx.Client:
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    sink = _Sink(write_error, registry_error)
    capture = _LogCapture()
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(capture)
    spec = SimpleNamespace(name=name, config=config)
    try:
        with (
            mock.patch.object(services.httpx, "Client", client_factory),
            mock.patch.object(services.influx, "write_points", sink.write_points),
            mock.patch.object(
                services, "touch_registry_from_points", sink.touch_registry
            ),
        ):
            try:
                status, ingested, skipped, message = executor(spec)
                outcome: dict[str, Any] = {
                    "outcome": [str(status), ingested, skipped, message]
                }
            except Exception as err:  # noqa: BLE001 - recorded as the oracle
                outcome = {"raised": {"class": type(err).__name__, "message": str(err)}}
    finally:
        logger.removeHandler(capture)
        logger.setLevel(previous_level)
    return {
        "spec_name": name,
        "config": config,
        "routes": {
            target: [
                _encode_reply(item)
                for item in (route if isinstance(route, list) else [route])
            ]
            for target, route in routes.items()
        },
        "sink": {"write_error": write_error, "registry_error": registry_error},
        **outcome,
        "p2": p2,
        "requests": requests,
        "calls": sink.calls,
        "log": capture.lines,
    }


# --- Page probe -------------------------------------------------------------

_OK_PAGE = {"/": _reply(200, b"<html>ok</html>")}


def _probe(url: str = ORIGIN + "/", **config: object) -> dict[str, object]:
    return {"url": url, "subject_id": "example_site", **config}


PAGE_PROBE_SCENARIOS: list[dict[str, Any]] = [
    {
        "name": "success with the production dimensions",
        "config": _probe(
            service_id="example_service",
            source_system="http_probe",
            source_instance="public_web",
            collector_service="graphyard-agent",
            collector_host="macmini",
            request_timeout_seconds=15,
        ),
        "routes": _OK_PAGE,
    },
    {"name": "defaults", "config": _probe(), "routes": _OK_PAGE},
    {
        "name": "values are str() and stripped, empty dimensions fall back",
        "config": _probe(
            url=f"  {ORIGIN}/  ",
            subject_id="  Example_Site ",
            service_id=" ",
            source_system="",
            source_instance="  ",
            collector_service="",
            collector_host=" ",
        ),
        "routes": _OK_PAGE,
    },
    {
        "name": "non-string values are str()",
        "config": {
            "url": ORIGIN + "/",
            "subject_id": 42,
            "service_id": True,
            "source_system": None,
            "source_instance": 1.5,
            "collector_host": ["a", 1],
        },
        "routes": _OK_PAGE,
    },
    {
        "name": "configured tags are str() and override the fixed ones",
        "config": _probe(
            tags={
                "env": "prod",
                "count": 3,
                "ratio": 1.0,
                "flag": False,
                "none": None,
                "nested": {"a": [1, "b"]},
                "http_method": "HEAD",
                "spec_name": "override",
            }
        ),
        "routes": _OK_PAGE,
    },
    {
        "name": "tags that are not an object are ignored",
        "config": _probe(tags=["env", "prod"]),
        "routes": _OK_PAGE,
    },
    {
        "name": "redirect followed",
        "config": _probe(),
        "routes": {
            "/": _reply(301, b"moved", Location="/final"),
            "/final": _reply(200),
        },
    },
    {
        "name": "redirect not followed is a success",
        "config": _probe(follow_redirects=False),
        "routes": {"/": _reply(302, b"moved", Location="/final")},
    },
    {
        "name": "server error is a warning with five points",
        "config": _probe(),
        "routes": {"/": _reply(503, b"down")},
    },
    {
        "name": "not found is a warning",
        "config": _probe(),
        "routes": {"/": _reply(404)},
    },
    {
        "name": "no content is a success",
        "config": _probe(),
        "routes": {"/": _reply(204)},
    },
    {
        "name": "body over the cap is the warning path",
        "config": _probe(max_body_bytes=4),
        "routes": {"/": _reply(200, b"abcdef")},
    },
    {
        "name": "decoded bytes count towards the cap",
        "config": _probe(max_body_bytes="64"),
        "routes": {"/": _gzip_reply(200, b" " * 4096)},
    },
    {
        "name": "redirect body counts towards the cap",
        "config": _probe(max_body_bytes=4),
        "routes": {
            "/": _reply(301, b"x" * 20, Location="/final"),
            "/final": _reply(200, b"ok"),
        },
    },
    {
        "name": "twenty-one redirects",
        "config": _probe(url=ORIGIN + "/r0"),
        "routes": {
            **{f"/r{i}": _reply(302, Location=f"/r{i + 1}") for i in range(21)},
            "/r21": _reply(200),
        },
    },
    {
        "name": "invalid location is the warning path",
        "config": _probe(),
        "routes": {"/": _reply(302, Location="https://[::1")},
    },
    {
        "name": "invalid url is critical",
        "config": _probe(url="http://[::1"),
        "routes": {},
    },
    {"name": "config not an object", "config": ["url"], "routes": {}},
    {
        "name": "url is required",
        "config": {"url": "  ", "subject_id": "x"},
        "routes": {},
    },
    {
        "name": "subject_id is required",
        "config": {"url": ORIGIN + "/"},
        "routes": {},
    },
    {
        "name": "config errors come before the required fields",
        "config": {"request_timeout_seconds": 0},
        "routes": {},
    },
    {
        "name": "total timeout config error",
        "config": _probe(total_timeout_seconds="soon"),
        "routes": {},
    },
    {
        "name": "max_body_bytes config error",
        "config": _probe(max_body_bytes=0),
        "routes": {},
    },
    {
        "name": "verify_tls config error",
        "config": _probe(verify_tls="false"),
        "routes": {},
    },
    {
        "name": "follow_redirects config error",
        "config": _probe(follow_redirects=1),
        "routes": {},
    },
    {
        "name": "write failure on success is critical",
        "config": _probe(),
        "routes": _OK_PAGE,
        "write_error": "influx write failed",
    },
    {
        "name": "registry failure on success is critical",
        "config": _probe(),
        "routes": _OK_PAGE,
        "registry_error": "registry failed",
    },
    {
        "name": "write failure on the warning path is raised",
        "config": _probe(max_body_bytes=1),
        "routes": _OK_PAGE,
        "write_error": "influx write failed",
    },
    {
        "name": "registry failure on the warning path is raised",
        "config": _probe(max_body_bytes=1),
        "routes": _OK_PAGE,
        "registry_error": "registry failed",
    },
]


def _page_probe_cases() -> list[dict[str, Any]]:
    return [
        {
            "name": scenario["name"],
            **_run_executor(
                services._execute_http_page_probe_spec,
                name="page probe",
                config=scenario["config"],
                routes=scenario["routes"],
                write_error=scenario.get("write_error"),
                registry_error=scenario.get("registry_error"),
            ),
        }
        for scenario in PAGE_PROBE_SCENARIOS
    ]


def build_page_probe() -> dict[str, Any]:
    return {"origin": ORIGIN, "executor": _page_probe_cases()}


# --- UniFi helpers ----------------------------------------------------------

DEVICES: list[object] = [
    "not a device",
    {"name": " Switch A ", "mac": "AA:BB:CC:00:00:01"},
    {"name": "Switch B", "mac": " aa:bb:cc:00:00:02 "},
    {"mac": "aa:bb:cc:00:00:03"},
    {"name": 7, "mac": None},
    {"name": "Switch B", "mac": "aa:bb:cc:00:00:05"},
]

FIND_CASES: list[tuple[str, str]] = [
    ("Switch A", ""),
    ("  Switch B ", ""),
    ("", "AA:BB:CC:00:00:02"),
    ("", " aa:bb:cc:00:00:03 "),
    ("missing", "aa:bb:cc:00:00:03"),
    ("Switch B", "aa:bb:cc:00:00:01"),
    ("7", ""),
    ("None", ""),
    ("", "none"),
    ("", ""),
    ("switch a", ""),
    ("missing", "missing"),
]


def _find_cases() -> list[dict[str, Any]]:
    cases = []
    for device_name, device_mac in FIND_CASES:
        found = services._find_unifi_device(
            DEVICES, device_name=device_name, device_mac=device_mac
        )
        cases.append(
            {
                "device_name": device_name,
                "device_mac": device_mac,
                "index": None if found is None else DEVICES.index(found),
            }
        )
    return cases


PORT_TABLE: list[object] = [
    "junk",
    {"port_idx": 1, "name": "Port 1", "speed": 1000},
    {"port_idx": 2.0, "name": " SFP+ 1 ", "speed": 10000.0},
    {"port_idx": 10, "name": "", "speed": None},
    {"port_idx": 10, "name": "late duplicate"},
    {"port_idx": True, "name": "bool index"},
    {"port_idx": "5", "name": "string index", "speed": "2500"},
    {"name": "no index", "speed": [1, 2]},
    {"port_idx": 12345678901234567890, "name": "huge"},
    {"port_idx": 7, "name": "it's"},
]

INTERFACE_DEVICES: dict[str, dict[str, object]] = {
    "full": {
        "uplink": {
            "name": " eth0 ",
            "port_idx": 10,
            "speed": 10000,
            "rx_bytes-r": 1.5,
            "tx_bytes-r": 2,
        },
        "port_table": PORT_TABLE,
    },
    "uplink via num_port": {
        "uplink": {"port_idx": 0, "num_port": 2, "speed": True},
        "port_table": PORT_TABLE,
    },
    "uplink port_idx is falsy and num_port missing": {
        "uplink": {"port_idx": "", "name": ""},
        "port_table": PORT_TABLE,
    },
    "uplink float index matches an int": {
        "uplink": {"port_idx": 1.0, "speed": 1000.5},
        "port_table": PORT_TABLE,
    },
    "uplink bool index": {"uplink": {"port_idx": True}, "port_table": PORT_TABLE},
    "uplink string index": {"uplink": {"port_idx": "5"}, "port_table": PORT_TABLE},
    "uplink without a port table": {"uplink": {"port_idx": 3, "name": 4}},
    "port table not a list": {
        "uplink": {"port_idx": 1},
        "port_table": {"1": {"port_idx": 1}},
    },
    "uplink not an object": {"uplink": [1], "port_table": PORT_TABLE},
    "no uplink": {"port_table": PORT_TABLE},
}

SELECTORS = [
    "uplink",
    " UpLink ",
    "",
    "   ",
    "port_idx:1",
    "port_idx: 2 ",
    "PORT_IDX:10",
    "port_idx:007",
    "port_idx:5",
    "port_idx:7",
    "port_idx:12345678901234567890",
    "port_idx:99999999999999999999999999",
    "port_idx:" + "1" * 4301,
    "port_idx:x",
    "port_idx:",
    "port_idx:-1",
    "port_idx:1:2",
    "port_idx:+1",
    "port_name:Port 1",
    "port_name:SFP+ 1",
    "Port_Name: SFP+ 1 ",
    "port_name:no index",
    "port_name:missing",
    "port_name:it's",
    "port_name:",
    "port_name:  ",
    "port_name:a:b",
    "port",
    "eth0",
]


def _interface_cases() -> list[dict[str, Any]]:
    cases = []
    for device_name, device in INTERFACE_DEVICES.items():
        for selector in SELECTORS:
            try:
                stats, tags = services._resolve_unifi_interface_stats(
                    device, interface_selector=selector
                )
                result: dict[str, Any] = {
                    "stats": stats,
                    "tags": [[key, value] for key, value in tags.items()],
                }
            except ValueError as err:
                result = {"error": {"class": type(err).__name__, "message": str(err)}}
            cases.append({"device": device_name, "selector": selector, **result})
    return cases


SUMMARY_CASES: list[tuple[int, bytes, tuple[str, ...], dict[str, str]]] = [
    (400, b"", ("user", "secret"), {}),
    (
        400,
        b'{"meta":{"rc":"error","msg":"api.err.Invalid"},"data":[]}',
        ("ab", "secret"),
        {},
    ),
    (
        401,
        b'{"username":"homeassistant","password":"secret"}',
        ("homeassistant", "secret"),
        {},
    ),
    (401, b'{"msg":"x","username":"a","password":"secret"}', ("a", "secret"), {}),
    (401, b'{"PassWord" :\n "x y z", "password":"", "password": "b"}', (), {}),
    (401, b'{"password": "unterminated', (), {}),
    (401, b"secretsecret secret", ("secret",), {}),
    (401, b"abc", ("", "abc"), {}),
    (401, b"abcd", ("abcd",), {}),
    (500, b"  line one\n\tline two\r\n\x1cthree\x0b\x0c  ", (), {}),
    (500, b"x" * 300, (), {}),
    (500, "\u00e4".encode() * 250, (), {}),
    (500, b"bad \xff\xfe utf-8 \xe2\x82", (), {}),
    (500, b"\xef\xbb\xbfbom", (), {}),
    (500, b"\xed\xa0\x80 \xf0\x9f\x98 \xc0\xaf \xf4\x90\x80\x80", (), {}),
    (500, "caf\u00e9 \u2028 nbsp\u00a0end".encode(), ("caf\u00e9",), {}),
    (
        500,
        b"<html><body>Error</body></html>",
        (),
        {"Content-Type": "text/html;charset=utf-8"},
    ),
    (502, b'{"password"\t:\t"tab"}', (), {}),
    (502, '{"pa\u017f\u017fword": "long s"}'.encode(), (), {}),
]


def _summary_cases() -> list[dict[str, Any]]:
    cases = []
    for status, body, redactions, headers in SUMMARY_CASES:
        response = httpx.Response(status, content=body, headers=headers)
        cases.append(
            {
                "status": status,
                "headers": [[key, value] for key, value in headers.items()],
                "body_hex": body.hex(),
                "redactions": list(redactions),
                "summary": services._unifi_response_summary(
                    response, redactions=redactions
                ),
                "error": services._unifi_response_error(
                    response, endpoint="/api/login", redactions=redactions
                ),
            }
        )
    return cases


LOGIN_OK_CASES: list[tuple[int, bytes]] = [
    (200, b'{"meta":{"rc":"ok"},"data":[]}'),
    (200, b'{"meta":{"rc":"OK"}}'),
    (200, b'{"meta":{"rc":"error","msg":"api.err.Invalid"}}'),
    (200, b'{"meta":{"rc":null}}'),
    (200, b'{"meta":{"rc":true}}'),
    (200, b'{"meta":{"rc":" ok"}}'),
    (200, b'{"meta":{}}'),
    (200, b'{"meta":[]}'),
    (200, b'{"data":[]}'),
    (200, b"[]"),
    (200, b'"ok"'),
    (200, b""),
    (200, b"<html>"),
    (200, b"\xff"),
    (200, b'\xef\xbb\xbf{"meta":{"rc":"error"}}'),
    (200, b'{"meta":{"rc":"error"},"n":' + b"1" * 4301 + b"}"),
    (204, b""),
    (299, b'{"meta":{"rc":"ok"}}'),
    (300, b'{"meta":{"rc":"ok"}}'),
    (199, b""),
    (400, b'{"meta":{"rc":"ok"}}'),
    (401, b""),
]


def _login_ok_cases() -> list[dict[str, Any]]:
    return [
        {
            "status": status,
            "body_hex": body.hex(),
            "ok": services._unifi_login_response_ok(
                httpx.Response(status, content=body)
            ),
        }
        for status, body in LOGIN_OK_CASES
    ]


# --- UniFi executor ---------------------------------------------------------

_DEVICE = {
    "name": "USW Pro XG 8 PoE",
    "mac": "70:49:A2:21:53:45",
    "uplink": {
        "name": "eth0",
        "port_idx": 10,
        "speed": 10000,
        "rx_bytes-r": 22790.32633644922,
        "tx_bytes-r": 9224.609959748512,
    },
    "port_table": [
        {"port_idx": 10, "name": "SFP+ 2"},
        {"port_idx": 3, "name": "Port 3", "rx_bytes-r": 5, "tx_bytes-r": "6.5"},
    ],
}
_DEVICES = {
    "meta": {"rc": "ok"},
    "data": [{"name": "other", "mac": "00:00:00:00:00:01"}, _DEVICE],
}
_LOGIN_OK = _json_reply(200, {"meta": {"rc": "ok"}, "data": []})
_LEGACY_DEVICES = "/api/s/default/stat/device"
_OS_DEVICES = "/proxy/network/api/s/default/stat/device"


def _unifi(**config: object) -> dict[str, object]:
    base: dict[str, object] = {
        "base_url": ORIGIN,
        "username": "homeassistant",
        "password": "secret",
        "site_id": "default",
        "device_name": "USW Pro XG 8 PoE",
        "interface_selector": "uplink",
        "subject_id": "usw_pro_xg_8_poe",
        "service_id": "unifi",
        "collector_host": "macmini",
        "verify_tls": False,
    }
    base.update(config)
    return {key: value for key, value in base.items() if value is not _DROP}


_DROP: Any = object()


def _legacy_routes(
    devices: Reply | None = None, **extra: Reply | list[Reply]
) -> Routes:
    routes: Routes = {
        "/api/login": _LOGIN_OK,
        _LEGACY_DEVICES: devices if devices is not None else _json_reply(200, _DEVICES),
    }
    routes.update(extra)
    return routes


_LEGACY_FAILS = _reply(
    400, b'{"meta":{"rc":"error","msg":"api.err.Invalid"},"data":[]}'
)

UNIFI_SCENARIOS: list[dict[str, Any]] = [
    {
        "name": "legacy login and uplink rates",
        "config": _unifi(),
        "routes": _legacy_routes(),
    },
    {
        "name": "falls back to UniFi OS auth with a CSRF token",
        "config": _unifi(),
        "routes": {
            "/api/login": _LEGACY_FAILS,
            "/api/auth/login": _json_reply(
                200, {"meta": {"rc": "ok"}}, **{"X-CSRF-Token": "  csrf-123 "}
            ),
            _OS_DEVICES: _json_reply(200, _DEVICES),
        },
    },
    {
        "name": "unifi_os mode only",
        "config": _unifi(auth_mode=" UniFi_OS "),
        "routes": {
            "/api/auth/login": _reply(200, b"not json"),
            _OS_DEVICES: _json_reply(200, _DEVICES),
        },
    },
    {
        "name": "legacy mode reports the login response body",
        "config": _unifi(auth_mode="legacy"),
        "routes": {"/api/login": _LEGACY_FAILS},
    },
    {
        "name": "both attempts fail",
        "config": _unifi(),
        "routes": {
            "/api/login": _reply(
                200, b'{"meta":{"rc":"error","msg":"api.err.LoginRequired"}}'
            ),
            "/api/auth/login": _reply(
                401, b'{"username":"homeassistant","password":"secret"}'
            ),
        },
    },
    {
        "name": "short values are not redacted",
        "config": _unifi(username="a", auth_mode="legacy"),
        "routes": {
            "/api/login": _reply(
                400, b'{"msg":"api.err.Invalid","username":"a","password":"secret"}'
            )
        },
    },
    {
        "name": "empty login body has no summary",
        "config": _unifi(auth_mode="legacy"),
        "routes": {"/api/login": _reply(403)},
    },
    {
        "name": "unsupported auth mode",
        "config": _unifi(auth_mode="token"),
        "routes": {},
    },
    {
        "name": "empty auth mode is auto",
        "config": _unifi(auth_mode="  "),
        "routes": _legacy_routes(),
    },
    {
        "name": "login redirect is followed with cookies",
        "config": _unifi(),
        "routes": _legacy_routes(
            **{
                "/api/login": _reply(
                    302,
                    b"moved",
                    Location="/api/login2",
                    **{"Set-Cookie": "first=1; Path=/"},
                ),
                "/api/login2": _json_reply(
                    200,
                    {"meta": {"rc": "ok"}},
                    **{"Set-Cookie": "unifises=abc; Path=/"},
                ),
            }
        ),
    },
    {
        "name": "temporary redirect keeps the login body",
        "config": _unifi(auth_mode="legacy"),
        "routes": _legacy_routes(
            **{
                "/api/login": _reply(307, Location=ORIGIN + "/api/v2/login"),
                "/api/v2/login": _LOGIN_OK,
            }
        ),
    },
    {
        "name": "device list redirect is followed",
        "config": _unifi(),
        "routes": _legacy_routes(
            devices=_reply(301, b"elsewhere", Location="/v2/devices"),
            **{"/v2/devices": _json_reply(200, _DEVICES)},
        ),
    },
    {
        "name": "twenty-one redirects",
        "config": _unifi(auth_mode="legacy"),
        "routes": {
            "/api/login": _reply(302, Location="/r1"),
            **{f"/r{i}": _reply(302, Location=f"/r{i + 1}") for i in range(1, 22)},
        },
    },
    {
        "name": "device list error status",
        "config": _unifi(username="homeassistant"),
        "routes": _legacy_routes(
            devices=_reply(
                401,
                b'{"meta":{"rc":"error","msg":"api.err.LoginRequired"},"user":"homeassistant"}',
            )
        ),
    },
    {
        "name": "device list redirect without location is an error status",
        "config": _unifi(),
        "routes": _legacy_routes(devices=_reply(302, b"")),
    },
    {
        "name": "device list is not JSON",
        "config": _unifi(),
        "routes": _legacy_routes(devices=_reply(200, b"<html>")),
        "p2": True,
    },
    {
        "name": "device response is not an object",
        "config": _unifi(),
        "routes": _legacy_routes(devices=_json_reply(200, [_DEVICE])),
    },
    {
        "name": "device response without a data list",
        "config": _unifi(),
        "routes": _legacy_routes(devices=_json_reply(200, {"data": {"0": _DEVICE}})),
    },
    {
        "name": "device not found",
        "config": _unifi(device_name="missing"),
        "routes": _legacy_routes(),
    },
    {
        "name": "device by MAC",
        "config": _unifi(device_name=_DROP, device_mac=" 70:49:a2:21:53:45 "),
        "routes": _legacy_routes(),
    },
    {
        "name": "port selector with custom dimensions",
        "config": _unifi(
            interface_selector="port_idx:3",
            site_id=" lab ",
            subject_type=" Service ",
            source_system="",
            source_instance="",
            service_id="",
            collector_service=" ",
            collector_host="",
            receive_metric_name=" rx.custom ",
            transmit_metric_name="tx.custom",
        ),
        "routes": {
            "/api/login": _LOGIN_OK,
            "/api/s/lab/stat/device": _json_reply(200, _DEVICES),
        },
    },
    {
        "name": "selector error is critical",
        "config": _unifi(interface_selector="port_name:missing"),
        "routes": _legacy_routes(),
    },
    {
        "name": "non-numeric rates are skipped",
        "config": _unifi(),
        "routes": _legacy_routes(
            devices=_json_reply(
                200,
                {
                    "data": [
                        {
                            "name": "USW Pro XG 8 PoE",
                            "uplink": {"rx_bytes-r": "NaN", "tx_bytes-r": "12"},
                        }
                    ]
                },
            )
        ),
    },
    {
        "name": "no numeric rates",
        "config": _unifi(),
        "routes": _legacy_routes(
            devices=_json_reply(
                200,
                {
                    "data": [
                        {"name": "USW Pro XG 8 PoE", "uplink": {"rx_bytes-r": None}}
                    ]
                },
            )
        ),
    },
    {
        "name": "write failure is critical",
        "config": _unifi(),
        "routes": _legacy_routes(),
        "write_error": "influx write failed",
    },
    {
        "name": "registry failure is critical",
        "config": _unifi(),
        "routes": _legacy_routes(),
        "registry_error": "registry failed",
    },
    {
        "name": "base_url keeps whitespace and loses trailing slashes",
        "config": _unifi(base_url=ORIGIN + "///"),
        "routes": _legacy_routes(),
    },
    {
        "name": "invalid base_url",
        "config": _unifi(base_url="http://[::1"),
        "routes": {},
    },
    {
        "name": "login body escapes like json.dumps",
        "config": _unifi(username='h\u00e4 "q"\\', password="p\u00e4ss\nword\x01"),
        "routes": _legacy_routes(),
    },
    {"name": "config not an object", "config": "nope", "routes": {}},
    {"name": "base_url is required", "config": _unifi(base_url="///"), "routes": {}},
    {"name": "username is required", "config": _unifi(username=" "), "routes": {}},
    {"name": "password is required", "config": _unifi(password=_DROP), "routes": {}},
    {
        "name": "device is required",
        "config": _unifi(device_name="", device_mac=" "),
        "routes": {},
    },
    {"name": "subject_id is required", "config": _unifi(subject_id="  "), "routes": {}},
    {
        "name": "config errors come first",
        "config": {"request_timeout_seconds": "x"},
        "routes": {},
    },
    {
        "name": "verify_tls config error",
        "config": _unifi(verify_tls="no"),
        "routes": {},
    },
]


def _unifi_cases() -> list[dict[str, Any]]:
    return [
        {
            "name": scenario["name"],
            **_run_executor(
                services._execute_unifi_device_traffic_spec,
                name="unifi usw traffic",
                config=scenario["config"],
                routes=scenario["routes"],
                write_error=scenario.get("write_error"),
                registry_error=scenario.get("registry_error"),
                p2=scenario.get("p2", False),
            ),
        }
        for scenario in UNIFI_SCENARIOS
    ]


def build_unifi() -> dict[str, Any]:
    assert issubclass(SpecConfigError, ValueError)
    return {
        "origin": ORIGIN,
        "devices": DEVICES,
        "find_device": _find_cases(),
        "interface_devices": INTERFACE_DEVICES,
        "interface_stats": _interface_cases(),
        "response_summary": _summary_cases(),
        "login_ok": _login_ok_cases(),
        "executor": _unifi_cases(),
    }


GENERATORS = {
    "page_probe.json": build_page_probe,
    "unifi.json": build_unifi,
}
