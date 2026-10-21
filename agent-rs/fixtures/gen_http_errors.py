"""Fixture ``http_errors.json``: httpx semantics of the HTTP layer (slice S6).

Pins ``graphyard-collect::http`` (agent-rs/PLAN.md 6.1, 6.2) against the
httpx 0.28.1 client the collectors use and against the graphyard helpers
around it (``_read_http_json``, ``_run_http_page_probe``,
``_run_http_with_deadline``, ``_PageProbeCancellation``). Nothing here
touches the network: URL cases stop at a network backend stub that records
where httpcore would connect, and the request scenarios run on
``httpx.MockTransport``.

Sections:

* ``urls``: ``httpx.URL(text)`` → ``str()`` or the ``InvalidURL`` message,
  then what sending it does before any I/O (``UnsupportedProtocol``, or the
  host and port httpcore connects to, the request target and ``Host``).
* ``reason_phrases``: ``httpx.codes.get_reason_phrase`` for 100..999, the
  phrase httpx shows when a server sends an empty reason.
* ``raise_for_status``: ``Response.raise_for_status()`` texts.
* ``redirects``: ``Client._build_redirect_request`` (method, URL,
  ``Authorization`` and body handling, invalid ``Location``).
* ``exception_classes``: whether each exception class is an
  ``httpx.HTTPError`` and whether the page probe's ``except`` takes it as a
  warning.
* ``messages``: graphyard-authored texts (deadline, abandoned workers,
  body cap).
* ``page_probe`` / ``read_json``: scripted request sequences through the
  real helpers: requests sent, ``HTTP Request:`` log lines, result or
  error. ``MockTransport`` checks no URL scheme and can return a 1xx as a
  final response, so those cases live in ``urls``/``raise_for_status``
  only.
"""

from __future__ import annotations

import gzip
import json
import logging
from collections.abc import Callable
from typing import Any

import httpx

from graphyard import services

# --- URLs -------------------------------------------------------------------

URLS = [
    "https://example.test/",
    "https://example.test",
    "http://example.test:80",
    "http://example.test:8080/p",
    "https://EXAMPLE.test/A/B",
    "https://example.test/a/./b/../c?x=1#frag",
    "HTTPS://Example.TEST:443/x",
    "https://example.test:443/x",
    "http://example.test/a b?c d#e f",
    "http://example.test/a|b{c}",
    "http://example.test/%41%zz",
    "http://example.test/p?q='x'",
    "http://example.test/a\\b",
    "http://example.test/%2e%2e/x",
    "http://example.test/café?q=é",
    "https://example.test?q",
    "https://example.test#frag",
    "http://user:pw@example.test/",
    "http://us er@example.test/",
    "http://us%40er:p%3Ass@example.test/",
    "http://example.test:/x",
    "http://example.test: 80/",
    "http://example.test:+8080/",
    "http://example.test:8_0/",
    "http://127.0.0.1:99999/",
    "http://HOST.example./",
    "http://127.0.0.1:8123/api/states",
    "https://127.0.0.1:8443/",
    "https://[::1]:8443/",
    "https://[::FFFF:1.2.3.4]/",
    "http://127.1/",
    "http://bücher.example/",
    "http://exa mple.test/",
    "http://ho%41st/",
    "http://[fe80::1%25eth0]/",
    "ws://example.test/socket",
    "wss://example.test/",
    "ftp://example.test/",
    "mailto:someone@example.test",
    "example.test",
    "example.test/path",
    "//example.test/x",
    "/relative/path",
    "http:///x",
    "http:foo",
    ":foo",
    "",
    "https://[::1",
    "http://example.test:abc/",
    "http://example.test::80/",
    "http://256.1.1.1/",
    "http://01.1.1.1/",
    "https://[::g]/",
    "https://[::1%]/",
    "http://example.test/\nx",
    "http://example.test/\tx",
    "http://example.test/\x7f",
    "\x00",
    "http://example.test/a b",
    "http:example.test/x",
    "https:/example.test/x",
    "//[::1]/",
    "//",
    "a:b",
    "1http://example.test/",
]


class _NoNetwork(Exception):
    """Raised by the stub network backend where httpcore would connect."""

    def __init__(self, host: str, port: int) -> None:
        super().__init__(host, port)
        self.host = host
        self.port = port


class _StubBackend:
    def connect_tcp(
        self, host, port, timeout=None, local_address=None, socket_options=None
    ):  # noqa: ANN001
        raise _NoNetwork(host, port)

    def connect_unix_socket(self, *args, **kwargs):  # noqa: ANN002, ANN003
        raise AssertionError("unexpected unix socket")

    def sleep(self, seconds: float) -> None:
        del seconds


def _send_without_network(text: str) -> dict[str, Any]:
    client = httpx.Client()
    client._transport._pool._network_backend = _StubBackend()  # type: ignore[attr-defined]
    try:
        request = client.build_request("GET", text)
        client.send(request)
    except _NoNetwork as stop:
        return {
            "connect": [stop.host, stop.port],
            "target": request.url.raw_path.decode("ascii"),
            "host_header": request.headers["Host"],
            # httpx's auth flow sends URL credentials as basic auth.
            "authorization": request.headers.get("Authorization"),
        }
    except httpx.UnsupportedProtocol as err:
        return {"class": "UnsupportedProtocol", "message": str(err)}
    finally:
        client.close()
    raise AssertionError(f"request to {text!r} was not stopped")


def _url_cases() -> list[dict[str, Any]]:
    cases = []
    for text in URLS:
        case: dict[str, Any] = {"url": text}
        try:
            url = httpx.URL(text)
        except httpx.InvalidURL as err:
            case["invalid"] = str(err)
        else:
            case["display"] = str(url)
            case["absolute"] = url.is_absolute_url
            case["send"] = _send_without_network(text)
        cases.append(case)
    return cases


LONG_URL_PREFIX = "https://example.test/"


def _long_url_cases() -> list[dict[str, Any]]:
    """``MAX_URL_LENGTH`` at its boundary (the URL is rebuilt from its length)."""
    cases = []
    for length in (65536, 65537):
        text = LONG_URL_PREFIX + "a" * (length - len(LONG_URL_PREFIX))
        try:
            httpx.URL(text)
            invalid = None
        except httpx.InvalidURL as err:
            invalid = str(err)
        cases.append(
            {
                "prefix": LONG_URL_PREFIX,
                "fill": "a",
                "length": length,
                "invalid": invalid,
            }
        )
    return cases


# --- Status texts -----------------------------------------------------------

STATUSES = [
    100, 101, 103, 199, 200, 201, 204, 299, 300, 301, 302, 303, 304, 305, 306,
    307, 308, 399, 400, 401, 403, 404, 405, 408, 410, 413, 414, 418, 421, 422,
    425, 426, 429, 451, 499, 500, 501, 502, 503, 504, 505, 507, 511, 599, 600,
    999,
]  # fmt: skip


def _reason_phrases() -> dict[str, str]:
    return {
        str(code): phrase
        for code in range(100, 1000)
        if (phrase := httpx.codes.get_reason_phrase(code))
    }


def _raise_for_status_cases() -> list[dict[str, Any]]:
    cases = []
    url = "https://example.test/api/states/sensor.temperature"
    for status in STATUSES:
        for reason in ("", "Custom Reason"):
            for location in (None, "/next", ""):
                headers = {} if location is None else {"Location": location}
                response = httpx.Response(
                    status,
                    headers=headers,
                    request=httpx.Request("GET", url),
                    extensions={"reason_phrase": reason.encode("ascii")},
                )
                try:
                    response.raise_for_status()
                    message = None
                except httpx.HTTPStatusError as err:
                    message = str(err)
                cases.append(
                    {
                        "status": status,
                        "server_reason": reason,
                        "location": location,
                        "url": url,
                        "reason_phrase": response.reason_phrase,
                        "message": message,
                    }
                )
    return cases


# --- Redirect requests ------------------------------------------------------

REDIRECT_BASES = [
    "https://example.test/a/b?x=1",
    "http://example.test/start#top",
    "https://127.0.0.1:8443/api/login",
]
REDIRECT_LOCATIONS = [
    "/final",
    "final",
    "../up",
    "?page=2",
    "#frag",
    "",
    "//other.test/p",
    "https://other.test/",
    "http://example.test:80/same",
    "https://example.test/upgrade",
    "https://example.test:443/upgrade",
    "HTTP://EXAMPLE.TEST/x",
    "http:no-host",
    "/a b",
    "/café",
    "ftp://example.test/file",
    "https://[::1",
    "http://example.test:abc/",
    "/\x7f",
]


def _redirect_cases() -> list[dict[str, Any]]:
    client = httpx.Client()
    cases = []
    for base in REDIRECT_BASES:
        for location in REDIRECT_LOCATIONS:
            for status, method in (
                (302, "GET"),
                (301, "POST"),
                (303, "HEAD"),
                (307, "POST"),
            ):
                request = client.build_request(
                    method,
                    base,
                    headers={"Authorization": "Bearer secret"},
                    content=b"body" if method == "POST" else None,
                )
                # Servers send bytes; httpx decodes them (ASCII, then UTF-8).
                response = httpx.Response(
                    status,
                    headers={"Location": location.encode("utf-8")},
                    request=request,
                )
                case: dict[str, Any] = {
                    "base": base,
                    "method": method,
                    "status": status,
                    "location": location,
                }
                try:
                    redirect = client._build_redirect_request(request, response)
                except Exception as err:  # noqa: BLE001 - recorded as the oracle
                    case["error"] = {"class": type(err).__name__, "message": str(err)}
                else:
                    case["redirect"] = {
                        "method": redirect.method,
                        "url": str(redirect.url),
                        "authorization": "Authorization" in redirect.headers,
                        "body": redirect.stream is request.stream and method == "POST",
                    }
                cases.append(case)
    client.close()
    return cases


URLJOIN_CASES = [
    ("https://example.test/a/b/c?x=1#f", "d"),
    ("https://example.test/a/b/c", "../../d"),
    ("https://example.test/a/b/c", "../../../../d"),
    ("https://example.test/a/b/", "./"),
    ("https://example.test/a/b/c", ".."),
    ("https://example.test/a//b/c", "d"),
    ("https://example.test/a/b;p", "c;q"),
    ("https://example.test/a/b;p?x", ";q"),
    ("https://example.test", "a"),
    ("https://example.test?x=1", "?y=2"),
    ("https://example.test/a", "//other.test"),
    ("https://example.test/a", "#f"),
    ("https://example.test/a?q", ""),
    ("http://example.test/a/b", "c/./d/../e"),
]


def _urljoin_cases() -> list[dict[str, str]]:
    from urllib.parse import urljoin

    return [
        {"base": base, "url": url, "joined": urljoin(base, url)}
        for base, url in URLJOIN_CASES
    ]


# --- Exception classes ------------------------------------------------------

_PAGE_PROBE_WARNING = (
    httpx.HTTPError,
    services._PageProbeLimitExceeded,
    services._PageProbeDeadlineExceeded,
)


def _exception_classes() -> list[dict[str, Any]]:
    request = httpx.Request("GET", "https://example.test/")
    response = httpx.Response(500, request=request)
    instances: list[BaseException] = [
        httpx.ConnectError("x", request=request),
        httpx.ConnectTimeout("x", request=request),
        httpx.ReadTimeout("x", request=request),
        httpx.WriteTimeout("x", request=request),
        httpx.PoolTimeout("x", request=request),
        httpx.ReadError("x", request=request),
        httpx.WriteError("x", request=request),
        httpx.RemoteProtocolError("x", request=request),
        httpx.LocalProtocolError("x", request=request),
        httpx.ProxyError("x", request=request),
        httpx.UnsupportedProtocol("x", request=request),
        httpx.DecodingError("x", request=request),
        httpx.TooManyRedirects("x", request=request),
        httpx.HTTPStatusError("x", request=request, response=response),
        httpx.InvalidURL("x"),
        services._PageProbeLimitExceeded("x"),
        services._PageProbeDeadlineExceeded("x"),
        json.JSONDecodeError("x", "", 0),
        UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte"),
        ValueError("x"),
        OverflowError("x"),
    ]
    return [
        {
            "class": type(err).__name__,
            "http_error": isinstance(err, httpx.HTTPError),
            "page_probe_warning": isinstance(err, _PAGE_PROBE_WARNING),
        }
        for err in instances
    ]


# --- graphyard messages -----------------------------------------------------

DEADLINE_TOTALS = [
    30.0,
    30,
    1.5,
    0.3,
    0.1,
    1e-05,
    86400.0,
    0.30000000000000004,
    45.5,
    2.0,
]


def _deadline_message(total: float) -> str:
    cancellation = services._PageProbeCancellation(total)
    cancellation._deadline = float("-inf")  # already passed
    try:
        cancellation.check()
    except services._PageProbeDeadlineExceeded as err:
        return str(err)
    raise AssertionError("deadline did not fire")


class _ClosingClient:
    def close(self) -> None:
        return None


def _abandoned_message(count: int) -> str:
    original = services._live_abandoned_page_probe_workers
    services._live_abandoned_page_probe_workers = lambda: count  # type: ignore[assignment]
    try:
        services._run_http_with_deadline(
            _ClosingClient(),  # type: ignore[arg-type]
            url="https://example.test/",
            operation=lambda cancellation: None,
            total_timeout_seconds=1.0,
        )
    except services._PageProbeDeadlineExceeded as err:
        return str(err)
    finally:
        services._live_abandoned_page_probe_workers = original  # type: ignore[assignment]
    raise AssertionError("abandoned cap did not fire")


def _messages() -> dict[str, Any]:
    return {
        "max_abandoned_workers": services._PAGE_PROBE_MAX_ABANDONED_WORKERS,
        "close_join_seconds": repr(services._PAGE_PROBE_CLOSE_JOIN_SECONDS),
        "max_redirects": httpx.Client().max_redirects,
        "deadline": [
            {"total": repr(float(total)), "message": _deadline_message(total)}
            for total in DEADLINE_TOTALS
        ],
        "abandoned": [
            {"count": count, "message": _abandoned_message(count)}
            for count in (4, 5, 17)
        ],
    }


# --- Scripted request sequences ---------------------------------------------

ORIGIN = "http://probe.test"

Reply = dict[str, Any]


def _reply(status: int, body: bytes = b"", **headers: str) -> Reply:
    return {"status": status, "headers": headers, "body": body}


def _gzip_reply(status: int, body: bytes) -> Reply:
    # zlib writes the platform into the header's OS byte (3 on Linux, 19 on
    # macOS); decoders ignore it, so it is pinned to Unix to keep the fixture
    # the same on every platform.
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


class _LogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


def _run_scripted(
    routes: dict[str, Reply | list[Reply]],
    run: Callable[[httpx.Client, str], Any],
    path: str,
) -> dict[str, Any]:
    requests: list[str] = []
    counters: dict[str, int] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        target = request.url.raw_path.decode("ascii")
        requests.append(f"{request.method} {target}")
        route = routes.get(target, _reply(404))
        if isinstance(route, list):
            index = counters.get(target, 0)
            counters[target] = index + 1
            route = route[min(index, len(route) - 1)]
        return httpx.Response(
            route["status"], headers=route["headers"], content=route["body"]
        )

    client = httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    )
    capture = _LogCapture()
    logger = logging.getLogger("httpx")
    previous_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(capture)
    try:
        outcome: dict[str, Any] = {"result": run(client, ORIGIN + path)}
    except Exception as err:  # noqa: BLE001 - recorded as the oracle
        outcome = {"error": {"class": type(err).__name__, "message": str(err)}}
    finally:
        logger.removeHandler(capture)
        logger.setLevel(previous_level)
    if "error" in outcome and outcome["error"]["class"] == "JSONDecodeError":
        outcome["error"]["p2"] = True
    return {
        "routes": {
            target: [
                _encode_reply(item)
                for item in (route if isinstance(route, list) else [route])
            ]
            for target, route in routes.items()
        },
        "path": path,
        **outcome,
        "requests": requests,
        "log": capture.lines,
    }


def _probe(
    max_body_bytes: int, follow_redirects: bool
) -> Callable[[httpx.Client, str], Any]:
    def run(client: httpx.Client, url: str) -> dict[str, int]:
        result = services._run_http_page_probe(
            client,
            url=url,
            max_body_bytes=max_body_bytes,
            follow_redirects=follow_redirects,
            cancellation=services._PageProbeCancellation(None),
        )
        return {
            "status_code": result.status_code,
            "redirect_count": result.redirect_count,
        }

    return run


def _chain(count: int, final: Reply) -> dict[str, Reply | list[Reply]]:
    routes: dict[str, Reply | list[Reply]] = {
        f"/r{i}": _reply(302, Location=f"/r{i + 1}") for i in range(count)
    }
    routes[f"/r{count}"] = final
    return routes


PROBE_SCENARIOS: list[tuple[str, dict[str, Reply | list[Reply]], str, int, bool]] = [
    ("ok", {"/": _reply(200, b"<html>ok</html>")}, "/", 10485760, True),
    (
        "redirect followed",
        {"/": _reply(302, Location="/final"), "/final": _reply(200, b"<html>")},
        "/",
        10485760,
        True,
    ),
    (
        "redirect not followed",
        {"/": _reply(302, b"moved", Location="/final")},
        "/",
        10485760,
        False,
    ),
    (
        "303 to get",
        {"/a/b": _reply(303, Location="next?x=1"), "/a/next?x=1": _reply(204)},
        "/a/b",
        10485760,
        True,
    ),
    (
        "query only location",
        {
            "/list?page=1": _reply(307, Location="?page=2"),
            "/list?page=2": _reply(200, b"[]"),
        },
        "/list?page=1",
        10485760,
        True,
    ),
    (
        "304 is not a redirect",
        {"/": _reply(304, Location="/elsewhere")},
        "/",
        10485760,
        True,
    ),
    ("301 without location", {"/": _reply(301)}, "/", 10485760, True),
    ("error status", {"/": _reply(503, b"down")}, "/", 10485760, True),
    ("twenty redirects", _chain(20, _reply(200, b"done")), "/r0", 10485760, True),
    ("twenty-one redirects", _chain(21, _reply(200, b"done")), "/r0", 10485760, True),
    ("empty location loops", {"/": _reply(302, Location="")}, "/", 10485760, True),
    ("body at cap", {"/": _reply(200, b"abcd")}, "/", 4, True),
    ("body over cap", {"/": _reply(200, b"abcde")}, "/", 4, True),
    (
        "redirect body counts toward cap",
        {"/": _reply(301, b"x" * 20, Location="/final"), "/final": _reply(200, b"ok")},
        "/",
        4,
        True,
    ),
    (
        "bodies of all hops add up",
        {"/": _reply(302, b"abc", Location="/final"), "/final": _reply(200, b"de")},
        "/",
        4,
        True,
    ),
    ("decoded bytes count", {"/": _gzip_reply(200, b" " * 4096)}, "/", 64, True),
    (
        "compressed body within cap",
        {"/": _gzip_reply(200, b"<html>ok</html>")},
        "/",
        64,
        True,
    ),
    (
        "invalid location",
        {"/": _reply(302, Location="https://[::1")},
        "/",
        10485760,
        True,
    ),
    (
        "invalid location not followed",
        {"/": _reply(302, Location="http://probe.test:abc/")},
        "/",
        10485760,
        False,
    ),
]


def _page_probe_cases() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "max_body_bytes": max_body_bytes,
            "follow_redirects": follow,
            **_run_scripted(routes, _probe(max_body_bytes, follow), path),
        }
        for name, routes, path, max_body_bytes, follow in PROBE_SCENARIOS
    ]


def _read_json(max_body_bytes: int) -> Callable[[httpx.Client, str], Any]:
    def run(client: httpx.Client, url: str) -> str:
        payload = services._read_http_json(
            client, url, max_body_bytes, services._PageProbeCancellation(None)
        )
        return json.dumps(payload)

    return run


JSON_SCENARIOS: list[tuple[str, dict[str, Reply | list[Reply]], int]] = [
    ("object", {"/": _reply(200, b'{"value": 7}')}, 10485760),
    (
        "list",
        {"/": _reply(200, b'[{"entity_id": "sensor.t", "state": "7"}]')},
        10485760,
    ),
    ("body at cap", {"/": _reply(200, b'{"value": 7}')}, 12),
    ("body over cap", {"/": _reply(200, b'{"value": 7} ')}, 12),
    ("decoded bytes count", {"/": _gzip_reply(200, b'{"value": 7}' + b" " * 4096)}, 64),
    ("compressed within cap", {"/": _gzip_reply(200, b'{"value": 7}')}, 64),
    ("not found", {"/": _reply(404, b'{"message": "Entity not found."}')}, 10485760),
    ("server error", {"/": _reply(500, b"boom")}, 10485760),
    (
        "redirect is not followed",
        {"/": _reply(302, b"", Location="/elsewhere")},
        10485760,
    ),
    ("invalid location", {"/": _reply(301, Location="https://[::1")}, 10485760),
    ("304 without location", {"/": _reply(304)}, 10485760),
    ("invalid json", {"/": _reply(200, b'{"value": ')}, 10485760),
    ("empty body", {"/": _reply(200, b"")}, 10485760),
    ("utf-8 bom", {"/": _reply(200, b'\xef\xbb\xbf{"value": 7}')}, 10485760),
    (
        "big integer",
        {"/": _reply(200, b'{"value": 123456789012345678901234567890}')},
        10485760,
    ),
    ("no content", {"/": _reply(204)}, 10485760),
    (
        "integer at the digit limit",
        {"/": _reply(200, b'{"value": ' + b"7" * 4300 + b"}")},
        10485760,
    ),
    (
        "integer beyond the digit limit",
        {
            "/": _reply(
                200, b'{"text": "' + b"1" * 5000 + b'", "value": -' + b"7" * 4301 + b"}"
            )
        },
        10485760,
    ),
]


def _read_json_cases() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "max_body_bytes": max_body_bytes,
            **_run_scripted(routes, _read_json(max_body_bytes), "/"),
        }
        for name, routes, max_body_bytes in JSON_SCENARIOS
    ]


def build_http_errors() -> dict[str, Any]:
    return {
        "httpx_version": httpx.__version__,
        "urls": _url_cases(),
        "long_urls": _long_url_cases(),
        "reason_phrases": _reason_phrases(),
        "raise_for_status": _raise_for_status_cases(),
        "redirects": _redirect_cases(),
        "urljoin": _urljoin_cases(),
        "exception_classes": _exception_classes(),
        "messages": _messages(),
        "page_probe": _page_probe_cases(),
        "read_json": _read_json_cases(),
    }


GENERATORS = {"http_errors.json": build_http_errors}
