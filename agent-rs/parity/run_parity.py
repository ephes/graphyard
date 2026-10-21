"""Offline parity harness: the Python and Rust agents on identical inputs.

agent-rs/PLAN.md 9.4. Two copies of a database built from
``fixtures/schema/schema.sql`` are seeded with the same corpus (``CORPUS``:
every spec type, every condition branch, a config error, an unknown spec
type, a disabled spec and condition, a non-host ``host_filter``). Each agent
gets its own fakes (``fakes.py``): collector endpoints over HTTP and TLS and
a fake InfluxDB. Per round, ``manage.py start_agent --run-once`` (settings
``settings_parity.py``) runs on copy A and ``graphyard-agent --run-once --db``
on copy B, at the same time and with the same environment; the rounds vary
``INFLUX_API_MODE`` (``v2``, ``v3``, ``auto`` with the Flux endpoint
answering 404) and end with a round without ``INFLUX_TOKEN`` (the lazy
configuration errors). Three more rounds run ``manage.py
evaluate_conditions`` against ``graphyard-agent evaluate-conditions --db``
(slice S15a, PLAN.md 11.4) on the same databases: all enabled conditions,
one condition by ``--condition-id`` (v3, so the SQL request line is
logged) and the id of the disabled condition (nothing evaluated, the
heartbeat still written). The fakes answer by request index, so round n
sees the n-th scripted answer in both agents.

Compared after every round, with only these masks:

- spec, condition, heartbeat and registry rows, every column;
  ``last_run_at``, ``last_evaluated``, ``last_success``, ``updated_at`` of
  heartbeats and ``last_seen_at`` as null/non-null, ``next_run_time`` as its
  distance to ``last_run_at`` (both are ``now``-based);
- the exit code (each round's ``expect_code``), stdout lines, and the
  operator log lines of PLAN.md 3.6 on stderr (``OPERATOR_LINE``; ``Metric
  collection spec … raised`` up to ``raised``; the ``CommandError`` line of
  ``evaluate_conditions``);
- the requests every fake received, in order: method, target, the headers
  of ``COMPARED_HEADERS`` and the body. Written lines are parsed: the
  timestamp is masked when it lies within the agent's run (a ``now()``
  point), the value of the page-probe timings is masked. Flux and v3 SQL
  texts have their ``range``/``time`` bounds masked and carry the window
  length ``stop - start`` instead, compared exactly;
- each agent's port numbers are replaced by placeholders first.

The Python results must also match the corpus expectations (the status of
every spec and condition per round, the exit code and the summary line of
the ``evaluate_conditions`` rounds), so the comparison cannot pass on a
corpus that stopped exercising its branches. Every unmasked difference fails
the run (exit 1). Run with ``just rs-parity`` (builds the Rust binary first)
or::

    uv run python agent-rs/parity/run_parity.py --rust-binary PATH
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fakes import (  # noqa: E402
    HttpFake,
    InfluxFake,
    Request,
    mask_query_bounds,
    query_window,
)

PARITY_DIR = Path(__file__).resolve().parent
REPO = PARITY_DIR.parents[1]
SCHEMA_SQL = REPO / "agent-rs" / "fixtures" / "schema" / "schema.sql"
DJANGO_DIR = REPO / "src" / "django"
MEASUREMENT = "graphyard_metrics"
#: Creation time of the seeded rows (never written by either agent).
SEEDED_AT = "2026-01-01 00:00:00"
#: Home Assistant ``last_updated`` of the scripted states: a fixed instant,
#: so these points keep their exact timestamp in the comparison.
HA_UPDATED = "2025-06-01T12:00:00.250000+00:00"
#: Points whose value is a measured duration (P2).
TIMING_METRICS = {"service.http_page_ttfb_seconds", "service.http_page_total_seconds"}
#: Request headers compared (lower case). Others are P2 (PLAN.md 6.1).
COMPARED_HEADERS = (
    "host",
    "accept",
    "accept-encoding",
    "user-agent",
    "authorization",
    "content-type",
    "content-length",
    "cookie",
    "x-csrf-token",
)
#: ``Content-Length`` of write and query bodies holds now-based values.
INFLUX_UNCOMPARED_HEADERS = {"content-length"}
#: Operator log lines of PLAN.md 3.6 compared on stderr.
OPERATOR_LINE = re.compile(
    r"^(HTTP Request: |Spec |Ignoring |Metric point validation failed |"
    r"metrics_write_rejected |Metric collection spec .* raised|CommandError: )"
)
PROCESS_TIMEOUT = 120.0


# ------------------------------------------------------------------- corpus


@dataclass(frozen=True)
class Round:
    name: str
    env: dict[str, str]
    #: The fake InfluxDB answers every Flux query with 404.
    v2_missing: bool = False
    #: ``False``: the agents' ``--run-once``; ``True``: ``evaluate_conditions``
    #: / ``evaluate-conditions`` (S15a), with ``--condition-id`` of the
    #: condition named ``condition`` when that is set.
    evaluate: bool = False
    condition: str | None = None
    #: The exit code both commands must end with.
    expect_code: int = 0
    #: The Python command's stdout (checked for the ``evaluate`` rounds).
    expect_stdout: str | None = None


ROUNDS = (
    Round("v2", {"INFLUX_API_MODE": "v2"}),
    Round("v2 again", {"INFLUX_API_MODE": "v2"}),
    Round("v3", {"INFLUX_API_MODE": "v3"}),
    Round("auto, Flux 404", {"INFLUX_API_MODE": "auto"}, v2_missing=True),
    Round("v2 without INFLUX_TOKEN", {"INFLUX_API_MODE": "v2", "INFLUX_TOKEN": ""}),
    Round(
        "evaluate-conditions",
        {"INFLUX_API_MODE": "v2"},
        evaluate=True,
        expect_code=1,
        expect_stdout="Condition evaluation completed with failures (2/15).\n",
    ),
    Round(
        "evaluate-conditions --condition-id, v3",
        {"INFLUX_API_MODE": "v3"},
        evaluate=True,
        condition="cpu by round",
        expect_stdout="Condition evaluation completed (1).\n",
    ),
    Round(
        "evaluate-conditions --condition-id of a disabled condition",
        {"INFLUX_API_MODE": "v2"},
        evaluate=True,
        condition="disabled condition",
        expect_stdout="Condition evaluation completed (0).\n",
    ),
)
#: Rounds of the agents' ``--run-once`` (the spec expectations cover these).
AGENT_ROUNDS = sum(not round_.evaluate for round_ in ROUNDS)

OK, WARNING, CRITICAL = "ok", "warning", "critical"
#: Expected status in the config-error round for specs that write.
NO_TOKEN = CRITICAL


@dataclass
class Spec:
    name: str
    spec_type: str
    #: The stored ``config`` (any JSON value) or ``raw`` text.
    config: Any
    #: Expected Python status per round (``None``: the spec never runs).
    expect: list[str] | None
    enabled: bool = True
    interval: int = 60


@dataclass
class Condition:
    name: str
    metric: str
    expect: list[str] | None
    #: Per answered query: samples (``age`` seconds before the range stop)
    #: or an error response.
    samples: list[Any] = field(default_factory=lambda: [[]])
    operator: str = "gte"
    warning: float | None = 80.0
    critical: float | None = 90.0
    window: int = 30
    breach: int = 5
    host_filter: str = ""
    service_filter: str = ""
    tags_filter: Any = field(default_factory=dict)
    subject_type_filter: str = ""
    subject_id_filter: str = ""
    alert_when_stale: bool = True
    stale_after: int | None = None
    enabled: bool = True


def reply(
    status: int = 200,
    body: Any = "",
    headers: list[tuple[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    if not isinstance(body, (str, bytes)):
        headers = [("Content-Type", "application/json"), *(headers or [])]
    return {"status": status, "body": body, "headers": headers or [], **extra}


def redirect(status: int, location: str) -> dict[str, Any]:
    return reply(status, "", [("Location", location)])


def ha_state(
    entity_id: str, state: str, device_class: str | None = None, **extra: Any
) -> dict[str, Any]:
    attributes: dict[str, Any] = {}
    if device_class is not None:
        attributes["device_class"] = device_class
    return {
        "entity_id": entity_id,
        "state": state,
        "attributes": {**attributes, **extra.pop("attributes", {})},
        **extra,
    }


ENV_SCAN_STATES = [
    ha_state(
        "sensor.fritz_box_7590_ax_download_throughput",
        "1234.56",
        "data_rate",
        attributes={"unit_of_measurement": "kB/s"},
        last_updated=HA_UPDATED,
    ),
    ha_state(
        "sensor.fritz_box_7590_ax_upload_throughput",
        "98.7",
        "data_rate",
        attributes={"unit_of_measurement": "kB/s"},
        last_updated="2025-06-01T12:00:00Z",
    ),
    ha_state(
        "sensor.fritz_box_7590_ax_cpu_temperature",
        "61",
        "temperature",
        attributes={"unit_of_measurement": "°C"},
        last_updated=HA_UPDATED,
    ),
    ha_state(
        "sensor.usw_pro_xg_8_poe_temperature",
        "48.5",
        "temperature",
        last_updated="2025-06-01 12:00:01",
    ),
    ha_state("sensor.office_humidity", "41", "humidity"),
    ha_state("sensor.wohnzimmer_luftfeuchtigkeit", "unavailable", "humidity"),
    ha_state("sensor.fritz_box_7590_ax_link_uptime", "123456", "duration"),
    ha_state("binary_sensor.bad_humidity_alarm", "off", "moisture"),
    {"entity_id": "light.kitchen", "state": "on", "attributes": {}},
    "not an object",
]

NON_NUMERIC_STATES = [
    ha_state("sensor.office_temperature", "unknown", "temperature"),
    ha_state("sensor.cellar_humidity", "unavailable", "humidity"),
]

ENV_SCAN_CONFIG = {
    "access_token": "ha-token",
    "host_id": "homeassistant",
    "service_id": "homeassistant",
    "source_instance": "ha-main",
    "collector_service": "graphyard-agent",
    "collector_host": "macmini",
    "subject_mapping": {
        "default": {
            "subject_type": "environment_sensor",
            "subject_id_from": "entity_name_slug",
        },
        "rules": [
            {
                "match_entity_id_regex": r"^sensor\.fritz_box_7590_ax_(cpu_temperature|upload_throughput|download_throughput)$",
                "subject_type": "network_device",
                "subject_id_template": "fritz_box_7590_ax",
            },
            {
                "match_entity_id_regex": r"^sensor\.usw_pro_xg_8_poe_temperature$",
                "subject_type": "network_device",
                "subject_id_template": "usw_pro_xg_8_poe",
            },
        ],
    },
    "metric_mapping": {
        "rules": [
            {
                "match_entity_id_regex": r"^sensor\.fritz_box_7590_ax_download_throughput$",
                "metric_name": "network_device.network_receive_bytes_per_second",
                "value_multiplier": 1000,
                "extra_tags": {"traffic_direction": "receive", "traffic_scope": "wan"},
            },
            {
                "match_entity_id_regex": r"^sensor\.fritz_box_7590_ax_upload_throughput$",
                "metric_name": "network_device.network_transmit_bytes_per_second",
                "value_multiplier": 1000,
                "extra_tags": {"traffic_direction": "transmit", "traffic_scope": "wan"},
            },
        ]
    },
    "metric_prefix": "ha.",
    "include_device_classes": ["temperature", "humidity"],
    "entity_id_regex": "(^sensor\\.fritz_box_7590_ax_(upload_throughput|download_throughput)$|temperature|humidity)",
    "request_timeout_seconds": 10,
    "verify_tls": True,
}

UNIFI_DEVICE = {
    "name": "USW Pro",
    "mac": "AA:BB:CC:DD:EE:01",
    "uplink": {
        "name": "eth8",
        "port_idx": 8,
        "speed": 10000,
        "rx_bytes-r": 1234.5,
        "tx_bytes-r": 678,
    },
    "port_table": [
        {
            "port_idx": 2,
            "name": "Port 2",
            "speed": 1000,
            "rx_bytes-r": 11,
            "tx_bytes-r": 22.5,
        },
        {"port_idx": 8, "name": "Uplink", "speed": 10000},
    ],
}
OTHER_DEVICE = {"name": "UDM", "mac": "aa:bb:cc:dd:ee:99", "uplink": {}}


def devices(*items: Any) -> dict[str, Any]:
    return reply(200, {"meta": {"rc": "ok"}, "data": list(items)})


def unifi_login_ok(csrf: str, cookie: str) -> dict[str, Any]:
    return reply(
        200,
        {"meta": {"rc": "ok"}, "data": []},
        [
            ("Set-Cookie", f"{cookie}; Path=/"),
            ("Set-Cookie", "parity_tracking=1; Path=/api"),
            ("X-CSRF-Token", csrf),
        ],
    )


def samples(*items: tuple[float, float], **tags: str) -> list[dict[str, Any]]:
    return [{"age": age, "value": value, "tags": dict(tags)} for age, value in items]


@dataclass
class Corpus:
    specs: list[Spec]
    conditions: list[Condition]


def build_corpus(http: HttpFake, https: HttpFake, influx: InfluxFake) -> Corpus:
    """The corpus of one agent, with its fakes' scripts."""
    origin = f"http://127.0.0.1:{http.port}"
    tls_origin = f"https://127.0.0.1:{https.port}"

    def script(fake: HttpFake, path: str, *responses: dict[str, Any]) -> str:
        fake.script.set(path, list(responses))
        return path

    # --- Home Assistant -----------------------------------------------------
    script(
        http,
        "/ha-env/api/states",
        reply(200, ENV_SCAN_STATES),
        reply(200, {"message": "not a list"}),
        reply(200, ENV_SCAN_STATES, gzip=True),
        reply(200, NON_NUMERIC_STATES),
        reply(200, ENV_SCAN_STATES),
    )
    script(
        http,
        "/ha-env2/api/states",
        reply(
            200,
            [
                ha_state("sensor.wohnzimmer_luftfeuchtigkeit", "52.5", "humidity"),
                ha_state("sensor.keller_luftfeuchtigkeit", "60", None),
                ha_state("sensor.office_temperature", "21", "temperature"),
            ],
        ),
    )
    sensor = "/ha/api/states/sensor.office_temperature"
    script(
        http,
        sensor,
        reply(
            200,
            ha_state(
                "sensor.office_temperature",
                "21.5",
                "temperature",
                attributes={"unit_of_measurement": "°C"},
                last_updated=HA_UPDATED,
            ),
        ),
        reply(200, ha_state("sensor.office_temperature", "unavailable", "temperature")),
        reply(401, {"message": "Unauthorized"}),
        reply(200, ha_state("sensor.office_temperature", "22", "temperature")),
        reply(200, ha_state("sensor.office_temperature", "22.5", "temperature")),
    )

    # --- HTTP JSON metrics --------------------------------------------------
    script(
        http,
        "/json/storage",
        reply(200, {"pools": [{"used_ratio": 0.5}, {"used_ratio": 0.25}]}),
        reply(200, {"pools": []}),
        reply(200, {"pools": [{"used_ratio": "NaN"}]}),
        reply(500, "boom"),
        reply(200, {"pools": [{"used_ratio": 7}]}),
    )
    script(
        http,
        "/json/mail",
        reply(200, {"queue": {"depth": 12}}, gzip=True),
        reply(200, {"queue": {"depth": 1}, "padding": "x" * 300}),
        reply(200, {"queue": {"depth": 2}}, delay=3),
        redirect(302, "/json/elsewhere"),
        reply(200, {"queue": {"depth": "13"}}),
    )
    script(http, "/json/toaster", reply(200, {"value": 1}))
    script(http, "/json/disabled", reply(200, {"value": 1}))

    # --- page probes --------------------------------------------------------
    script(
        http,
        "/page/home",
        redirect(302, "/page/landing"),
        reply(404, "<html>gone</html>", [("Content-Type", "text/html")]),
        reply(200, "<html>" + "d" * 40 + "</html>", drip=0.1),
        reply(200, "<html>" + "x" * 4096 + "</html>"),
        redirect(301, "landing"),
    )
    script(http, "/page/landing", reply(200, "<html>landing</html>"))
    for index in range(22):
        script(http, f"/loop/{index}", redirect(302, f"/loop/{index + 1}"))
    script(http, "/nofollow", redirect(301, "/elsewhere"))

    # --- UniFi (TLS, verify_tls false) ----------------------------------------
    script(https, "/unifi1/api/login", unifi_login_ok("csrf-legacy", "unifises=abc"))
    script(
        https,
        "/unifi1/api/s/default/stat/device",
        redirect(302, "/unifi1/api/s/default/stat/device-moved"),
        devices(OTHER_DEVICE),
        reply(401, {"meta": {"rc": "error", "msg": "api.err.LoginRequired"}}),
        devices(
            OTHER_DEVICE,
            {**UNIFI_DEVICE, "uplink": {**UNIFI_DEVICE["uplink"], "rx_bytes-r": "n/a"}},
        ),
        devices(UNIFI_DEVICE),
    )
    script(https, "/unifi1/api/s/default/stat/device-moved", devices(UNIFI_DEVICE))
    script(
        https,
        "/unifi2/api/login",
        reply(404, "<html>\n  Not   Found\n</html>", [("Content-Type", "text/html")]),
    )
    script(
        https,
        "/unifi2/api/auth/login",
        unifi_login_ok("csrf-os", "TOKEN=jwt"),
        unifi_login_ok("csrf-os", "TOKEN=jwt"),
        unifi_login_ok("csrf-os", "TOKEN=jwt"),
        reply(
            401,
            {"error": "bad credentials", "password": "s3cret-pass", "user": "admin"},
        ),
        unifi_login_ok("csrf-os", "TOKEN=jwt"),
    )
    script(
        https,
        "/unifi2/proxy/network/api/s/default/stat/device",
        devices(UNIFI_DEVICE),
        devices({**UNIFI_DEVICE, "port_table": [{"port_idx": 8, "name": "Uplink"}]}),
        devices(UNIFI_DEVICE),
        devices(UNIFI_DEVICE),
    )

    specs = [
        Spec(
            "ha env scan",
            "home_assistant_env_scan",
            {**ENV_SCAN_CONFIG, "base_url": origin + "/ha-env/"},
            [OK, CRITICAL, OK, WARNING, NO_TOKEN],
        ),
        Spec(
            "ha env scan default mapping",
            "home_assistant_env_scan",
            {
                "base_url": origin + "/ha-env2",
                "access_token": "ha-token",
                "metric_prefix": "env.",
                "include_device_classes": "temperature",
                "entity_id_regex": "LUFTFEUCHTIGKEIT",
                "source_system": " HomeAssistant ",
            },
            [OK, OK, OK, OK, NO_TOKEN],
            interval=0,
        ),
        Spec(
            "ha sensor office",
            "home_assistant_sensor",
            {
                "base_url": origin + "/ha",
                "access_token": "ha-token",
                "entity_id": " sensor.office_temperature ",
                "host_id": "macmini",
                "subject_mapping": {
                    "default": {
                        "subject_type": "host",
                        "subject_id_from": "static",
                        "subject_id": "macmini",
                    },
                    "rules": [
                        {
                            "match_entity_id_regex": "sensor\\.office_.*",
                            "subject_type": "host",
                            "subject_id_template": "macmini",
                        }
                    ],
                },
                "metric_mapping": {
                    "rules": [
                        {
                            "match_entity_id_regex": "temperature",
                            "metric_name": "host.room_temperature_celsius",
                            "extra_tags": {"room": "office", "host": "evil"},
                        }
                    ]
                },
            },
            [OK, WARNING, CRITICAL, OK, NO_TOKEN],
        ),
        Spec(
            "ha sensor config not an object",
            "home_assistant_sensor",
            [],
            [CRITICAL] * 5,
        ),
        Spec(
            "json config error",
            "http_json_metric",
            {"url": origin + "/json/none", "request_timeout_seconds": "soon"},
            [CRITICAL] * 5,
        ),
        Spec(
            "json disabled",
            "http_json_metric",
            {
                "url": origin + "/json/disabled",
                "metric_path": "$.value",
                "metric_name": "x",
            },
            None,
            enabled=False,
        ),
        Spec(
            "json fractal storage",
            "http_json_metric",
            {
                "url": origin + "/json/storage",
                "metric_path": "$.pools.0.used_ratio",
                "metric_name": "host.storage_used_ratio",
                "host_id": "fractal",
                "subject_type": "host",
                "subject_id": "fractal",
                "basic_username": "monitor",
                "basic_password": "pw:secret",
                "tags": {"pool": "tank", "service": "ignored", "n": 1},
            },
            [OK, WARNING, WARNING, CRITICAL, NO_TOKEN],
            interval=300,
        ),
        Spec(
            "json mail queue",
            "http_json_metric",
            {
                "url": origin + "/json/mail?format=json",
                "metric_path": "$.queue.depth",
                "metric_name": "service.queue_depth",
                "service_id": "mail",
                "host_id": "macmini",
                "bearer_token": " mail-token ",
                "max_body_bytes": "256",
                "request_timeout_seconds": 5,
                "total_timeout_seconds": 1,
            },
            [OK, CRITICAL, CRITICAL, CRITICAL, NO_TOKEN],
        ),
        Spec(
            "json unknown subject type",
            "http_json_metric",
            {
                "url": origin + "/json/toaster",
                "metric_path": "$.value",
                "metric_name": "service.toast",
                "subject_type": "toaster",
            },
            [CRITICAL] * 5,
        ),
        Spec(
            "page probe home",
            "http_page_probe",
            {
                "url": origin + "/page/home",
                "subject_id": "homepage",
                "max_body_bytes": 1024,
                "request_timeout_seconds": 5,
                "total_timeout_seconds": 1,
                "tags": {"site": "home"},
            },
            [OK, WARNING, WARNING, WARNING, NO_TOKEN],
        ),
        Spec(
            "page probe missing url",
            "http_page_probe",
            {"subject_id": "nothing"},
            [CRITICAL] * 5,
        ),
        Spec(
            "page probe no follow",
            "http_page_probe",
            {
                "url": origin + "/nofollow",
                "subject_id": "nofollow",
                "service_id": "nofollow-svc",
                "follow_redirects": False,
                "collector_host": "macmini",
            },
            [OK, OK, OK, OK, NO_TOKEN],
        ),
        Spec(
            "page probe redirect loop",
            "http_page_probe",
            {"url": origin + "/loop/0", "subject_id": "loop"},
            # The warning path's write failure escapes the executor.
            [WARNING, WARNING, WARNING, WARNING, CRITICAL],
        ),
        Spec(
            "unifi legacy uplink",
            "unifi_device_traffic",
            {
                "base_url": tls_origin + "/unifi1/",
                "username": "admin",
                "password": "s3cret-pass",
                "auth_mode": "legacy",
                "device_name": "USW Pro",
                "subject_id": "usw_pro",
                "verify_tls": False,
            },
            [OK, WARNING, CRITICAL, OK, NO_TOKEN],
        ),
        Spec(
            "unifi os port",
            "unifi_device_traffic",
            {
                "base_url": tls_origin + "/unifi2",
                "username": "admin",
                "password": "s3cret-pass",
                "device_mac": "aa:bb:cc:dd:ee:01",
                "interface_selector": "port_idx:2",
                "subject_id": "usw_pro",
                "collector_host": "",
                "verify_tls": False,
            },
            [OK, CRITICAL, OK, CRITICAL, NO_TOKEN],
        ),
        Spec(
            "unknown spec type",
            "legacy_poller",
            {"url": origin + "/json/none"},
            [CRITICAL] * 5,
        ),
    ]

    ok_cpu = samples((30, 10.0), (90, 20.0), (400, 5.0))
    conditions = [
        Condition(
            "cpu by round",
            "host.cpu_percent",
            [OK, WARNING, CRITICAL, OK, CRITICAL, OK, OK, OK],
            [
                ok_cpu,
                samples((30, 85.0), (120, 85.0), (330, 81.0)),
                samples((30, 95.0), (200, 99.5), (320, 92.0)),
                ok_cpu,
            ],
        ),
        Condition(
            "disk series",
            "host.filesystem_used_ratio",
            [CRITICAL, OK, WARNING, WARNING, CRITICAL, WARNING, WARNING, WARNING],
            [
                samples((60, 0.5), (240, 0.5), mountpoint="/")
                + samples((60, 0.95), (200, 0.97), (330, 0.96), mountpoint="/data"),
                samples((60, 0.5), mountpoint="/")
                + samples((30, 0.6), mountpoint="/data"),
                samples((60, 0.85), (240, 0.85), mountpoint="/")
                + samples((60, 0.86), (280, 0.81), mountpoint="/data"),
                samples((60, 0.5), mountpoint="/")
                + samples((700, 0.6), mountpoint="/data"),
            ],
            warning=0.8,
            critical=0.9,
            tags_filter={"mountpoint_kind": "local"},
            subject_type_filter=" Host ",
        ),
        Condition(
            "stale sample",
            "service.stale_metric",
            [WARNING, WARNING, WARNING, WARNING, CRITICAL, WARNING, WARNING, WARNING],
            [samples((900, 1.0), (960, 1.0))],
        ),
        Condition(
            "stale sample intermittent",
            "service.stale_metric_intermittent",
            [OK, OK, OK, OK, CRITICAL, OK, OK, OK],
            [samples((900, 1.0))],
            alert_when_stale=False,
        ),
        Condition(
            "no samples",
            "service.no_samples",
            [WARNING, WARNING, WARNING, WARNING, CRITICAL, WARNING, WARNING, WARNING],
        ),
        Condition(
            "no samples intermittent",
            "service.no_samples_intermittent",
            [OK, OK, OK, OK, CRITICAL, OK, OK, OK],
            alert_when_stale=False,
        ),
        Condition(
            "non-host host filter",
            "service.http_latency",
            [OK, OK, OK, OK, CRITICAL, OK, OK, OK],
            [samples((30, 0.2), (150, 0.3))],
            host_filter="macmini",
            service_filter="mail",
            subject_type_filter="Service",
            subject_id_filter="Mail Server",
        ),
        Condition(
            "host filter and tags",
            "host.load",
            [WARNING, WARNING, WARNING, WARNING, CRITICAL, WARNING, WARNING, WARNING],
            [samples((30, 85.0), (100, 86.0), (200, 87.0), (290, 88.0))],
            host_filter="macmini",
            subject_type_filter="host",
            tags_filter={"mountpoint": "/", "fs": 1, "quote": "a\"b'c${x}"},
        ),
        Condition(
            "unknown operator",
            "host.unknown_operator",
            [CRITICAL] * 8,
            [samples((30, 1.0))],
            operator="bogus",
        ),
        Condition(
            "tags filter not an object",
            "host.tags_list",
            [CRITICAL] * 8,
            tags_filter=[],
        ),
        Condition(
            "stale allowance",
            "service.hourly",
            [OK, OK, OK, OK, CRITICAL, OK, OK, OK],
            [samples((1800, 3.0))],
            stale_after=3600,
        ),
        Condition(
            "window start sample",
            "host.free_ratio",
            [CRITICAL] * 8,
            [samples((360, 50.0), (300, 3.0), (120, 2.0), (30, 1.0))],
            operator="lt",
            warning=10.0,
            critical=5.0,
        ),
        Condition(
            "anchor carry",
            "host.temperature",
            [CRITICAL, OK, OK, CRITICAL, CRITICAL, CRITICAL, CRITICAL, CRITICAL],
            [
                samples((400, 95.0), (100, 95.0)),
                samples((1000, 95.0), (200, 95.0)),
                samples((200, 95.0), (100, 95.0)),
                samples((300, 91.5)),
            ],
            warning=None,
        ),
        Condition(
            "v3 error",
            "service.v3_error",
            [OK, OK, CRITICAL, OK, CRITICAL, OK, OK, OK],
            [
                samples((30, 1.0)),
                samples((30, 1.0)),
                {"status": 500, "body": "boom"},
                samples((30, 1.0)),
            ],
        ),
        Condition(
            "flux only tag key",
            "service.flux_only",
            [OK, OK, CRITICAL, CRITICAL, CRITICAL, OK, OK, OK],
            [samples((30, 1.0))],
            tags_filter={"mount-point": "/"},
        ),
        Condition(
            "disabled condition",
            "service.disabled",
            None,
            [samples((30, 99.0))],
            enabled=False,
        ),
    ]
    for condition in conditions:
        influx.queries.set(condition.metric, condition.samples)
    return Corpus(specs, conditions)


# ------------------------------------------------------------------ database


def seed(db: Path, corpus: Corpus) -> None:
    connection = sqlite3.connect(db)
    connection.executescript(SCHEMA_SQL.read_text())
    for index, spec in enumerate(corpus.specs, start=1):
        connection.execute(
            "INSERT INTO metric_collection_spec (id, name, enabled, spec_type,"
            " interval_seconds, next_run_time, config, last_run_at, last_status,"
            " last_error, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, 0, ?, NULL, 'ok', '', ?, ?)",
            (
                index,
                spec.name,
                int(spec.enabled),
                spec.spec_type,
                spec.interval,
                json.dumps(spec.config),
                SEEDED_AT,
                SEEDED_AT,
            ),
        )
    for index, condition in enumerate(corpus.conditions, start=1):
        connection.execute(
            "INSERT INTO condition_definition (id, name, enabled, metric_name,"
            " host_filter, service_filter, tags_filter, operator, warning_threshold,"
            " critical_threshold, window_minutes, breach_minutes, status,"
            " last_evaluated, message, last_value, created_at, updated_at,"
            " subject_id_filter, subject_type_filter, alert_when_stale,"
            " stale_after_seconds)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ok', NULL, '', NULL, ?, ?,"
            " ?, ?, ?, ?)",
            (
                index,
                condition.name,
                int(condition.enabled),
                condition.metric,
                condition.host_filter,
                condition.service_filter,
                json.dumps(condition.tags_filter),
                condition.operator,
                condition.warning,
                condition.critical,
                condition.window,
                condition.breach,
                SEEDED_AT,
                SEEDED_AT,
                condition.subject_id_filter,
                condition.subject_type_filter,
                int(condition.alert_when_stale),
                condition.stale_after,
            ),
        )
    # An existing heartbeat (update path; metric_collectors is inserted) and
    # the stale legacy row nothing writes.
    connection.executemany(
        "INSERT INTO pipeline_heartbeat (name, status, last_success, last_error,"
        " details, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                "condition_evaluator",
                "ok",
                SEEDED_AT,
                "",
                '{"evaluated": 0, "failed": 0}',
                SEEDED_AT,
            ),
            (
                "ha_poller",
                "ok",
                "2026-03-01 10:00:00.5",
                "",
                "{}",
                "2026-03-01 10:00:00.5",
            ),
        ],
    )
    # Registry rows: a stale host, a host seen "in the future" (never stale),
    # a service to re-parent and a subject whose source system changes.
    connection.executemany(
        "INSERT INTO host_registry (id, host_id, display_name, grafana_url, enabled,"
        " last_seen_at) VALUES (?, ?, ?, '', 1, ?)",
        [
            (1, "macmini", "Mac mini", SEEDED_AT),
            (2, "fractal", "fractal", "2999-01-01 00:00:00"),
        ],
    )
    connection.execute(
        "INSERT INTO service_registry (id, service_id, display_name, grafana_url,"
        " enabled, last_seen_at, host_id) VALUES (1, 'homeassistant', 'HA', '', 1, ?, 2)",
        (SEEDED_AT,),
    )
    connection.execute(
        "INSERT INTO subject_registry (id, subject_type, subject_id, display_name,"
        " source_system, last_seen_at) VALUES (1, 'network_device',"
        " 'fritz_box_7590_ax', 'Fritz!Box', 'legacy', ?)",
        (SEEDED_AT,),
    )
    connection.commit()
    connection.close()


def _rows(db: Path, table: str) -> list[dict[str, Any]]:
    connection = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    try:
        return [
            dict(row)
            for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")
        ]
    finally:
        connection.close()


def _epoch(text: str) -> int:
    return int(datetime.fromisoformat(text).replace(tzinfo=UTC).timestamp())


NULLABLE_MASKED = {
    "metric_collection_spec": ("last_run_at",),
    "condition_definition": ("last_evaluated",),
    "pipeline_heartbeat": ("last_success", "updated_at"),
    "subject_registry": ("last_seen_at",),
    "host_registry": ("last_seen_at",),
    "service_registry": ("last_seen_at",),
}


def table_view(
    db: Path, table: str, tokens: list[tuple[str, str]]
) -> list[dict[str, Any]]:
    """The rows of ``table`` with the documented masks applied."""
    view = []
    for row in _rows(db, table):
        if table == "metric_collection_spec" and row["last_run_at"] is not None:
            # now_ts + max(1, interval): compared as the distance to last_run_at.
            row["next_run_time"] = (
                f"last_run_at + {row['next_run_time'] - _epoch(row['last_run_at'])}"
            )
        if not (table == "pipeline_heartbeat" and row["name"] == "ha_poller"):
            # (The legacy heartbeat nothing writes is compared exactly.)
            for column in NULLABLE_MASKED[table]:
                if row[column] is not None:
                    row[column] = "<set>"
        view.append({key: normalise(value, tokens) for key, value in row.items()})
    return view


TABLES = tuple(NULLABLE_MASKED)


# ---------------------------------------------------------------- comparison


def normalise(value: Any, tokens: list[tuple[str, str]]) -> Any:
    if isinstance(value, str):
        for token, placeholder in tokens:
            value = value.replace(token, placeholder)
    return value


def split_unescaped(
    text: str, separator: str, *, quotes: bool = False, maxsplit: int = -1
) -> list[str]:
    """Split line protocol on ``separator`` outside backslash escapes and,
    with ``quotes`` (the field set), outside double-quoted string values."""
    parts: list[str] = []
    current: list[str] = []
    escaped = quoted = False
    for char in text:
        if escaped:
            current.append(char)
            escaped = False
        elif char == "\\":
            current.append(char)
            escaped = True
        elif quotes and char == '"':
            current.append(char)
            quoted = not quoted
        elif char == separator and not quoted and len(parts) != maxsplit:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


def line_view(
    line: str, tokens: list[tuple[str, str]], window: tuple[int, int]
) -> dict[str, Any]:
    """One written line: series key and field exact, ``now()`` parts masked."""
    # The series key ends at the first unescaped space (quotes are literal
    # there); field string values may hold quoted spaces and commas.
    head = split_unescaped(line, " ", maxsplit=1)
    parts = (
        [head[0], *split_unescaped(head[1], " ", quotes=True)]
        if len(head) == 2
        else head
    )
    if len(parts) != 3:
        return {"unparsed": line}
    series, fields, timestamp = parts
    tags = dict(
        tag.split("=", 1) for tag in split_unescaped(series, ",")[1:] if "=" in tag
    )
    if tags.get("metric") in TIMING_METRICS:
        # Only the duration of the `value` field; any other field stays.
        fields = ",".join(
            "value=<duration>" if field.startswith("value=") else field
            for field in split_unescaped(fields, ",", quotes=True)
        )
    ts: int | str = int(timestamp) if timestamp.isdigit() else timestamp
    if isinstance(ts, int) and window[0] <= ts <= window[1]:
        ts = "<now>"
    return {"series": normalise(series, tokens), "fields": fields, "ts": ts}


def request_view(
    request: Request, tokens: list[tuple[str, str]], window: tuple[int, int]
) -> dict[str, Any]:
    influx = request.path.startswith("/api/v")
    headers = {
        name: normalise(request.header(name), tokens)
        for name in COMPARED_HEADERS
        if request.header(name) is not None
        and not (influx and name in INFLUX_UNCOMPARED_HEADERS)
    }
    view: dict[str, Any] = {
        "method": request.method,
        "target": request.target,
        "headers": headers,
    }
    text = request.body.decode("utf-8", errors="replace")
    if request.path == "/api/v2/write":
        view["lines"] = [
            line_view(line, tokens, window) for line in text.split("\n") if line
        ]
    elif request.path in ("/api/v2/query", "/api/v3/query_sql"):
        bounds = query_window(request)
        payload = json.loads(text)
        for key in ("query", "q"):
            if key in payload:
                payload[key] = mask_query_bounds(payload[key])
        view["body"] = json.dumps(payload)
        view["window"] = (
            str(bounds[1] - bounds[0]) if bounds is not None else "<no window>"
        )
    else:
        view["body"] = normalise(text, tokens)
    return view


def operator_lines(stderr: str, tokens: list[tuple[str, str]]) -> list[str]:
    lines = []
    for line in stderr.splitlines():
        if OPERATOR_LINE.match(line):
            if line.startswith("Metric collection spec "):
                line = line[: line.index(" raised") + len(" raised")]
            lines.append(normalise(line, tokens))
    return lines


def _dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def diff_lists(label: str, python: list[Any], rust: list[Any]) -> list[str]:
    """Item by item (key order included); dicts report the differing keys."""
    differences = []
    for index in range(max(len(python), len(rust))):
        a = python[index] if index < len(python) else "<missing>"
        b = rust[index] if index < len(rust) else "<missing>"
        if _dump(a) == _dump(b):
            continue
        if isinstance(a, dict) and isinstance(b, dict) and list(a) == list(b):
            name = a.get("name") or a.get("target") or ""
            details = "".join(
                f"\n    {key}: python={_dump(a[key])}\n    {' ' * len(key)}  rust  ={_dump(b[key])}"
                for key in a
                if _dump(a[key]) != _dump(b[key])
            )
            differences.append(f"{label} [{index}] {name}:{details}")
        else:
            differences.append(
                f"{label} [{index}]:\n    python={_dump(a)}\n    rust  ={_dump(b)}"
            )
    return differences


# -------------------------------------------------------------------- agents


def _openssl_context(directory: Path) -> ssl.SSLContext:
    key, cert = directory / "tls.key", directory / "tls.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "30",
            "-subj",
            "/CN=parity.test",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    return context


@dataclass
class Run:
    """One ``--run-once`` process: its output and wall-clock window (ns)."""

    code: int
    stdout: str
    stderr: str
    window: tuple[int, int]


class Agent:
    """One implementation: its fakes, corpus, database and runs."""

    def __init__(self, label: str, workdir: Path, tls: ssl.SSLContext) -> None:
        self.label = label
        self.http = HttpFake()
        self.https = HttpFake(tls)
        self.influx = InfluxFake(MEASUREMENT)
        self.corpus = build_corpus(self.http, self.https, self.influx)
        self.db = workdir / f"{label}.sqlite3"
        seed(self.db, self.corpus)
        self.tokens = [
            (f"127.0.0.1:{self.http.port}", "127.0.0.1:<http>"),
            (f"127.0.0.1:{self.https.port}", "127.0.0.1:<https>"),
            (f"127.0.0.1:{self.influx.port}", "127.0.0.1:<influx>"),
        ]
        self._seen = {"http": 0, "https": 0, "influx": 0}

    def fakes(self) -> dict[str, Any]:
        return {"http": self.http, "https": self.https, "influx": self.influx}

    def new_requests(self) -> dict[str, list[Request]]:
        """The requests each fake received since the last call."""
        result = {}
        for name, fake in self.fakes().items():
            requests = fake.requests()
            result[name] = requests[self._seen[name] :]
            self._seen[name] = len(requests)
        return result

    def env(self, round_: Round) -> dict[str, str]:
        env = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("DJANGO_", "GRAPHYARD_", "INFLUX_", "PARITY_"))
            and key.upper()
            not in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "PYTHONPATH"}
        }
        env.update(
            {
                "PYTHONUNBUFFERED": "1",
                "PYTHONHASHSEED": "0",
                "INFLUX_URL": f"http://127.0.0.1:{self.influx.port}/",
                "INFLUX_TOKEN": "parity-token",
                "INFLUX_ORG": "parity org",
                "INFLUX_BUCKET": "parity-bucket",
                "INFLUX_MEASUREMENT": MEASUREMENT,
                "INFLUX_TIMEOUT_MS": "5000",
                "GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS": "600",
            }
        )
        env.update(round_.env)
        return env

    def condition_args(self, round_: Round) -> list[str]:
        """``--condition-id <id>`` of the round's condition (seeded ids
        follow the corpus order)."""
        if round_.condition is None:
            return []
        names = [condition.name for condition in self.corpus.conditions]
        return ["--condition-id", str(names.index(round_.condition) + 1)]

    def command(
        self, rust_binary: Path, round_: Round
    ) -> tuple[list[str], dict[str, str], Path]:
        raise NotImplementedError

    def close(self) -> None:
        for fake in self.fakes().values():
            fake.close()


class PythonAgent(Agent):
    def command(
        self, rust_binary: Path, round_: Round
    ) -> tuple[list[str], dict[str, str], Path]:
        del rust_binary
        extra = {
            "DJANGO_SETTINGS_MODULE": "settings_parity",
            "PARITY_DATABASE": str(self.db),
            "PYTHONPATH": os.pathsep.join(
                str(path) for path in (PARITY_DIR, REPO / "src", DJANGO_DIR)
            ),
        }
        command = (
            ["evaluate_conditions", *self.condition_args(round_)]
            if round_.evaluate
            else ["start_agent", "--run-once"]
        )
        return ([sys.executable, "manage.py", *command], extra, DJANGO_DIR)


class RustAgent(Agent):
    def command(
        self, rust_binary: Path, round_: Round
    ) -> tuple[list[str], dict[str, str], Path]:
        command = (
            ["evaluate-conditions", "--db", str(self.db), *self.condition_args(round_)]
            if round_.evaluate
            else ["--run-once", "--db", str(self.db)]
        )
        return ([str(rust_binary), *command], {}, self.db.parent)


def run_round(agents: list[Agent], round_: Round, rust_binary: Path) -> dict[str, Run]:
    runs: dict[str, Run] = {}

    def run(agent: Agent) -> None:
        agent.influx.v2_missing = round_.v2_missing
        argv, extra, cwd = agent.command(rust_binary, round_)
        env = {**agent.env(round_), **extra}
        start = time.time_ns()
        try:
            process = subprocess.run(
                argv,
                env=env,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=PROCESS_TIMEOUT,
            )
            code, stdout, stderr = process.returncode, process.stdout, process.stderr
        except subprocess.TimeoutExpired as err:
            code = -1
            stdout = (
                (err.stdout or b"").decode()
                if isinstance(err.stdout, bytes)
                else (err.stdout or "")
            )
            stderr = f"timed out after {PROCESS_TIMEOUT}s"
        end = time.time_ns()
        runs[agent.label] = Run(code, stdout, stderr, (start - 10**9, end + 10**9))

    threads = [threading.Thread(target=run, args=(agent,)) for agent in agents]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return runs


def compare_round(
    python: Agent, rust: Agent, runs: dict[str, Run], round_no: int
) -> list[str]:
    differences: list[str] = []
    a, b = runs[python.label], runs[rust.label]
    round_ = ROUNDS[round_no - 1]
    prefix = f"round {round_no} ({round_.name})"
    # Taken first, so the next round compares only its own requests.
    requests_a, requests_b = python.new_requests(), rust.new_requests()
    if (a.code, b.code) != (round_.expect_code, round_.expect_code):
        return [
            f"{prefix}: exit codes python={a.code} rust={b.code}\n"
            f"--- python stderr (tail)\n{a.stderr[-3000:]}\n"
            f"--- rust stderr (tail)\n{b.stderr[-3000:]}"
        ]
    differences += diff_lists(
        f"{prefix} stdout",
        [normalise(line, python.tokens) for line in a.stdout.splitlines()],
        [normalise(line, rust.tokens) for line in b.stdout.splitlines()],
    )
    differences += diff_lists(
        f"{prefix} operator log lines",
        operator_lines(a.stderr, python.tokens),
        operator_lines(b.stderr, rust.tokens),
    )
    for table in TABLES:
        differences += diff_lists(
            f"{prefix} {table}",
            table_view(python.db, table, python.tokens),
            table_view(rust.db, table, rust.tokens),
        )
    for name in requests_a:
        differences += diff_lists(
            f"{prefix} requests to the {name} fake",
            [request_view(r, python.tokens, a.window) for r in requests_a[name]],
            [request_view(r, rust.tokens, b.window) for r in requests_b[name]],
        )
    return differences


def expectation_problems(agent: Agent, round_no: int) -> list[str]:
    """The Python oracle must take the branch the corpus was written for."""
    problems = []
    # The specs keep the status of the last agent round through the
    # evaluate_conditions rounds.
    spec_round = sum(not round_.evaluate for round_ in ROUNDS[:round_no])
    specs = {row["name"]: row for row in _rows(agent.db, "metric_collection_spec")}
    for spec in agent.corpus.specs:
        row = specs[spec.name]
        if spec.expect is None:
            if row["last_run_at"] is not None:
                problems.append(f"spec {spec.name!r} ran but should never run")
        elif row["last_status"] != spec.expect[spec_round - 1]:
            problems.append(
                f"round {round_no}: spec {spec.name!r} is {row['last_status']}"
                f" ({row['last_error']!r}), the corpus expects {spec.expect[spec_round - 1]}"
            )
    conditions = {row["name"]: row for row in _rows(agent.db, "condition_definition")}
    for condition in agent.corpus.conditions:
        row = conditions[condition.name]
        if condition.expect is None:
            if row["last_evaluated"] is not None or row["message"]:
                problems.append(f"condition {condition.name!r} was evaluated")
        elif row["status"] != condition.expect[round_no - 1]:
            problems.append(
                f"round {round_no}: condition {condition.name!r} is {row['status']}"
                f" ({row['message']!r}), the corpus expects {condition.expect[round_no - 1]}"
            )
    return problems


def run(args: argparse.Namespace) -> int:
    rust_binary = Path(args.rust_binary).resolve()
    if not os.access(rust_binary, os.X_OK):
        print(f"not an executable: {rust_binary}", file=sys.stderr)
        return 2
    workdir = Path(tempfile.mkdtemp(prefix="graphyard-parity-"))
    tls = _openssl_context(workdir)
    python = PythonAgent("python", workdir, tls)
    rust = RustAgent("rust", workdir, tls)
    differences: list[str] = []
    problems: list[str] = []
    totals = {"requests": 0, "writes": 0, "queries": 0}
    try:
        for round_no, round_ in enumerate(ROUNDS, start=1):
            started = time.monotonic()
            runs = run_round([python, rust], round_, rust_binary)
            if args.keep:
                for label, result in runs.items():
                    (workdir / f"round{round_no}-{label}.stdout").write_text(
                        result.stdout
                    )
                    (workdir / f"round{round_no}-{label}.stderr").write_text(
                        result.stderr
                    )
            counts = {
                name: len(fake.requests()) for name, fake in python.fakes().items()
            }
            round_differences = compare_round(python, rust, runs, round_no)
            differences += round_differences
            problems += expectation_problems(python, round_no)
            python_run = runs[python.label]
            if round_.expect_stdout is not None and (
                python_run.code,
                python_run.stdout,
            ) != (round_.expect_code, round_.expect_stdout):
                problems.append(
                    f"round {round_no}: the Python command exited {python_run.code}"
                    f" with {python_run.stdout!r}, the corpus expects"
                    f" {round_.expect_code} with {round_.expect_stdout!r}"
                )
            print(
                f"round {round_no} ({round_.name}): {time.monotonic() - started:.1f}s,"
                f" {len(round_differences)} difference(s)",
                flush=True,
            )
        for request in python.influx.requests():
            totals["writes"] += request.path == "/api/v2/write"
            totals["queries"] += request.path in ("/api/v2/query", "/api/v3/query_sql")
        totals["requests"] = counts["http"] + counts["https"]
        if not (totals["requests"] and totals["writes"] and totals["queries"]):
            problems.append(f"the corpus reached too few endpoints: {totals}")
    finally:
        python.close()
        rust.close()
    if args.keep:
        print(f"work directory kept: {workdir}")
    else:
        shutil.rmtree(workdir, ignore_errors=True)
    if problems:
        print(f"{len(problems)} corpus expectation(s) not met by the Python agent:")
        for problem in problems:
            print(f"- {problem}")
    if differences:
        print(f"{len(differences)} unmasked difference(s):")
        for difference in differences:
            print(f"- {difference}")
    if problems or differences:
        return 1
    print(
        f"parity OK: {len(ROUNDS)} rounds ({AGENT_ROUNDS} agent runs,"
        f" {len(ROUNDS) - AGENT_ROUNDS} evaluate-conditions), {len(python.corpus.specs)} specs,"
        f" {len(python.corpus.conditions)} conditions, {totals['requests']} collector"
        f" requests, {totals['writes']} writes, {totals['queries']} queries;"
        " zero unmasked differences"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "--rust-binary", required=True, help="path of the graphyard-agent binary"
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="keep the work directory (databases, per-round stdout/stderr)",
    )
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
