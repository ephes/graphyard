"""Fixture ``ha_json_executors.json``: the HA and JSON executors end to end (S7a).

Pins ``graphyard-collect::executors`` (agent-rs/PLAN.md 6.3) against
``services.py``: ``_execute_home_assistant_sensor_spec``,
``_execute_home_assistant_env_scan_spec`` and
``_execute_http_json_metric_spec``, each run on a scripted
``httpx.MockTransport`` with ``influx.write_points`` and
``touch_registry_from_points`` replaced by recorders (optionally failing
with ``write_error``/``touch_error``)
and ``datetime.now`` pinned. Rust replays every scenario against a fake
server and a recording sink.

Per scenario: the spec (name, type, config as JSON text with the origin
``http://fixture.test``), the scripted replies per request target, the
outcome ``[status, ingested, skipped, message]`` or the exception that
escaped the executor (``SpecConfigError``), the requests sent (method,
target, ``Authorization``, ``Accept``), the points handed to the writer and
to the registry touch (``null`` when not called; the encoding of
``gen_normalize.py``), and the ``graphyard.services``/``httpx`` log lines.
An outcome message ending in a library text (P2: a JSON decode position,
a ``re`` compile error) carries its graphyard-authored
``outcome_p2_prefix``.
"""

from __future__ import annotations

import gzip
import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest import mock

import httpx

from graphyard import services
from graphyard.spec_config import SpecConfigError

ORIGIN = "http://fixture.test"
NOW = datetime(2026, 10, 21, 9, 45, 0, 250000, tzinfo=UTC)


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: ANN001, ANN206 - datetime signature
        return NOW if tz is not None else NOW.replace(tzinfo=None)


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


def _reply(status: int, body: Any = b"", **headers: str) -> dict[str, Any]:
    if not isinstance(body, bytes):
        body = json.dumps(body).encode()
    return {"status": status, "headers": headers, "body": body}


def _gzip_reply(status: int, body: bytes) -> dict[str, Any]:
    # The OS byte of the gzip header is platform-dependent; pin it to Unix.
    compressed = bytearray(gzip.compress(body, mtime=0))
    compressed[9] = 3
    return {
        "status": status,
        "headers": {"Content-Encoding": "gzip"},
        "body": bytes(compressed),
    }


def _encode_point(point: Any) -> dict[str, Any]:
    encoded = asdict(point)
    ordered = {
        key: encoded[key]
        for key in (
            "ts",
            "metric",
            "value",
            "subject_type",
            "subject_id",
            "source_system",
            "collector_service",
            "collector_host",
            "source_instance",
            "source_entity_id",
            "host",
            "service",
            "tags",
        )
    }
    ordered["ts"] = point.ts.isoformat()
    ordered["value"] = repr(point.value)
    ordered["tags"] = (
        None if point.tags is None else [[k, v] for k, v in point.tags.items()]
    )
    return ordered


EXECUTORS = {
    "home_assistant_sensor": services._execute_home_assistant_sensor_spec,
    "home_assistant_env_scan": services._execute_home_assistant_env_scan_spec,
    "http_json_metric": services._execute_http_json_metric_spec,
}


def scenario(
    name: str,
    spec_type: str,
    config: Any,
    routes: dict[str, dict[str, Any]] | None = None,
    *,
    write_error: str | None = None,
    touch_error: str | None = None,
    spec_name: str | None = None,
    p2_prefix: str | None = None,
) -> dict[str, Any]:
    config_text = config if isinstance(config, str) else json.dumps(config)
    routes = routes or {}
    spec = SimpleNamespace(
        name=spec_name or name, spec_type=spec_type, config=json.loads(config_text)
    )
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        target = request.url.raw_path.decode("ascii")
        requests.append(
            {
                "method": request.method,
                "target": target,
                "authorization": request.headers.get("Authorization"),
                "accept": request.headers.get("Accept"),
            }
        )
        reply = routes.get(target, _reply(404))
        return httpx.Response(
            reply["status"], headers=reply["headers"], content=reply["body"]
        )

    original_client = httpx.Client

    def factory(**kwargs: Any) -> httpx.Client:
        return original_client(**kwargs, transport=httpx.MockTransport(handler))

    written: list[Any] | None = None
    touched: list[Any] | None = None

    def write_points(points: list[Any]) -> int:
        nonlocal written
        written = [_encode_point(point) for point in points]
        if write_error is not None:
            raise RuntimeError(write_error)
        return len(points)

    def touch_registry(points: list[Any]) -> None:
        nonlocal touched
        touched = [_encode_point(point) for point in points]
        if touch_error is not None:
            raise RuntimeError(touch_error)

    services._subject_mapping_warning_keys.clear()
    capture = _Capture()
    loggers = [logging.getLogger("graphyard.services"), logging.getLogger("httpx")]
    levels = [logger.level for logger in loggers]
    for logger in loggers:
        logger.setLevel(logging.INFO)
        logger.addHandler(capture)
    try:
        with (
            mock.patch.object(services.httpx, "Client", factory),
            mock.patch.object(services, "datetime", _FixedDatetime),
            mock.patch.object(services.influx, "write_points", write_points),
            mock.patch.object(services, "touch_registry_from_points", touch_registry),
        ):
            try:
                status, ingested, skipped, message = EXECUTORS[spec_type](spec)
                outcome: dict[str, Any] = {
                    "outcome": [str(status), ingested, skipped, message]
                }
                if p2_prefix is not None:
                    assert message.startswith(p2_prefix)
                    outcome["outcome_p2_prefix"] = p2_prefix
            except SpecConfigError as exc:
                outcome = {"error": {"class": type(exc).__name__, "message": str(exc)}}
    finally:
        for logger, level in zip(loggers, levels):
            logger.removeHandler(capture)
            logger.setLevel(level)
    return {
        "name": name,
        "spec_name": spec.name,
        "spec_type": spec_type,
        "config": config_text,
        "routes": {
            target: {
                "status": reply["status"],
                "headers": [[k, v] for k, v in reply["headers"].items()],
                "body_hex": reply["body"].hex(),
            }
            for target, reply in routes.items()
        },
        "write_error": write_error,
        "touch_error": touch_error,
        **outcome,
        "requests": requests,
        "written": written,
        "touched": touched,
        "log": capture.lines,
    }


# --- corpus -------------------------------------------------------------------

HA = {"base_url": ORIGIN, "access_token": "token"}
STATE = {
    "entity_id": "sensor.office_temperature",
    "state": "21.2",
    "last_updated": "2026-03-04T12:00:00Z",
    "attributes": {"device_class": "temperature"},
}
SENSOR_PATH = "/api/states/sensor.office_temperature"
SENSOR = {
    **HA,
    "entity_id": "sensor.office_temperature",
    "metric_name": "ha.sensor.office_temperature",
    "host_id": "homeassistant",
    "service_id": "homeassistant",
}

ENV_STATES = [
    {
        "entity_id": "sensor.living_room_temperature",
        "state": "22.5",
        "last_updated": "2026-03-04T12:00:00Z",
        "attributes": {"device_class": "temperature"},
    },
    {
        "entity_id": "sensor.living_room_humidity",
        "state": "52.4",
        "last_updated": "2026-03-04T12:00:00Z",
        "attributes": {"device_class": "humidity"},
    },
    {
        "entity_id": "sensor.kitchen_humidity",
        "state": "unknown",
        "last_updated": "2026-03-04T12:00:00Z",
        "attributes": {"device_class": "humidity"},
    },
    {"entity_id": "binary_sensor.door", "state": "on", "attributes": {}},
]

PRODUCTION_ENV_SCAN = {
    "base_url": ORIGIN,
    "access_token": "redacted",
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

PRODUCTION_STATES = [
    {
        "entity_id": "sensor.fritz_box_7590_ax_download_throughput",
        "state": "1234.56",
        "attributes": {"unit_of_measurement": "kB/s", "device_class": "data_rate"},
        "last_updated": "2026-10-21T09:44:58.5+00:00",
    },
    {
        "entity_id": "sensor.fritz_box_7590_ax_upload_throughput",
        "state": "98.7",
        "attributes": {"unit_of_measurement": "kB/s", "device_class": "data_rate"},
        "last_updated": "2026-10-21T09:44:58.5+00:00",
    },
    {
        "entity_id": "sensor.fritz_box_7590_ax_cpu_temperature",
        "state": "61",
        "attributes": {"unit_of_measurement": "°C", "device_class": "temperature"},
        "last_updated": "2026-10-21T09:40:00+00:00",
    },
    {
        "entity_id": "sensor.usw_pro_xg_8_poe_temperature",
        "state": "48.5",
        "attributes": {"unit_of_measurement": "°C", "device_class": "temperature"},
        "last_updated": "2026-10-21T09:41:00+00:00",
    },
    {
        "entity_id": "sensor.fritz_box_7590_ax_link_uptime",
        "state": "123456",
        "attributes": {"device_class": "duration"},
    },
    {
        "entity_id": "sensor.wohnzimmer_luftfeuchtigkeit",
        "state": "unavailable",
        "attributes": {"unit_of_measurement": "%", "device_class": "humidity"},
    },
    {
        "entity_id": "binary_sensor.bad_humidity_alarm",
        "state": "off",
        "attributes": {"device_class": "moisture"},
    },
    {"entity_id": "light.kitchen", "state": "on", "attributes": {}},
    {"entity_id": "sensor.sun_next_dawn", "state": "2026-10-22T05:30:00+00:00"},
]

JSON = {
    "url": ORIGIN + "/health",
    "metric_path": "$.queue.depth",
    "metric_name": "service.queue_depth",
    "host_id": "macmini",
    "service_id": "mail",
}
HEALTH = {"/health": _reply(200, {"queue": {"depth": 12}})}


def _scenarios() -> list[dict[str, Any]]:
    sensor_ok = {SENSOR_PATH: _reply(200, STATE)}
    return [
        # --- home_assistant_sensor ------------------------------------------
        scenario(
            "sensor ingests one entity (test_home_assistant_sensor_spec_ingests_single_entity)",
            "home_assistant_sensor",
            SENSOR,
            sensor_ok,
            spec_name="ha single sensor",
        ),
        scenario(
            "sensor default metric name and trailing slashes",
            "home_assistant_sensor",
            {
                **HA,
                "base_url": ORIGIN + "///",
                "entity_id": "  sensor.office_temperature ",
            },
            sensor_ok,
        ),
        scenario("sensor config not an object", "home_assistant_sensor", "[1, 2]"),
        scenario("sensor config null", "home_assistant_sensor", "null"),
        scenario(
            "sensor base_url required", "home_assistant_sensor", {"base_url": "///"}
        ),
        scenario(
            "sensor access_token required",
            "home_assistant_sensor",
            {"base_url": ORIGIN, "access_token": ""},
        ),
        scenario(
            "sensor entity_id required",
            "home_assistant_sensor",
            {**HA, "entity_id": "  "},
        ),
        scenario(
            "sensor required fields before knobs",
            "home_assistant_sensor",
            {"verify_tls": "no"},
        ),
        scenario(
            "sensor knob error escapes",
            "home_assistant_sensor",
            {**SENSOR, "verify_tls": "no"},
        ),
        scenario(
            "sensor total timeout knob error escapes",
            "home_assistant_sensor",
            {**SENSOR, "request_timeout_seconds": 30000},
        ),
        scenario(
            "sensor payload not an object",
            "home_assistant_sensor",
            SENSOR,
            {SENSOR_PATH: _reply(200, [STATE])},
        ),
        scenario(
            "sensor state not numeric",
            "home_assistant_sensor",
            SENSOR,
            {SENSOR_PATH: _reply(200, {**STATE, "state": "unavailable"})},
        ),
        scenario(
            "sensor entity not found",
            "home_assistant_sensor",
            SENSOR,
            {SENSOR_PATH: _reply(404, {"message": "Entity not found."})},
        ),
        scenario(
            "sensor redirect is not followed",
            "home_assistant_sensor",
            SENSOR,
            {SENSOR_PATH: _reply(302, b"", Location="/elsewhere")},
        ),
        scenario(
            "sensor invalid JSON",
            "home_assistant_sensor",
            SENSOR,
            {SENSOR_PATH: _reply(200, b'{"state": ')},
            p2_prefix="",
        ),
        scenario(
            "sensor body over cap",
            "home_assistant_sensor",
            {**SENSOR, "max_body_bytes": len(json.dumps(STATE))},
            {SENSOR_PATH: _reply(200, json.dumps(STATE).encode() + b" " * 4096)},
        ),
        scenario(
            "sensor body exactly at cap",
            "home_assistant_sensor",
            {**SENSOR, "max_body_bytes": len(json.dumps(STATE))},
            sensor_ok,
        ),
        scenario(
            "sensor gzip body counted decoded",
            "home_assistant_sensor",
            {**SENSOR, "max_body_bytes": len(json.dumps(STATE))},
            {SENSOR_PATH: _gzip_reply(200, json.dumps(STATE).encode() + b" " * 4096)},
        ),
        scenario(
            "sensor write failure",
            "home_assistant_sensor",
            SENSOR,
            sensor_ok,
            write_error="influx write failed",
        ),
        scenario(
            "sensor registry failure after the write",
            "home_assistant_sensor",
            SENSOR,
            sensor_ok,
            touch_error="database is locked",
        ),
        scenario(
            "sensor slug failure is critical",
            "home_assistant_sensor",
            {**HA, "entity_id": "sensor."},
            {"/api/states/sensor.": _reply(200, {**STATE, "entity_id": "sensor."})},
        ),
        scenario(
            "sensor subject rule without template",
            "home_assistant_sensor",
            {
                **SENSOR,
                "subject_mapping": {
                    "rules": [
                        {
                            "match_entity_id_regex": "sensor\\..*",
                            "subject_type": "service",
                        }
                    ]
                },
            },
            sensor_ok,
        ),
        scenario(
            "sensor invalid URL",
            "home_assistant_sensor",
            {**HA, "base_url": "http://[::1", "entity_id": "sensor.x"},
        ),
        # --- home_assistant_env_scan ----------------------------------------
        scenario(
            "env scan collects temperature and humidity (test_home_assistant_env_scan_collects_temp_and_humidity)",
            "home_assistant_env_scan",
            {**HA, "host_id": "homeassistant", "service_id": "homeassistant"},
            {"/api/states": _reply(200, ENV_STATES)},
            spec_name="ha env scan",
        ),
        scenario(
            "env scan production config",
            "home_assistant_env_scan",
            PRODUCTION_ENV_SCAN,
            {"/api/states": _reply(200, PRODUCTION_STATES)},
            spec_name="Home Assistant Environment Scan",
        ),
        scenario(
            "env scan states not a list",
            "home_assistant_env_scan",
            HA,
            {"/api/states": _reply(200, {"states": []})},
        ),
        scenario(
            "env scan invalid entity_id_regex",
            "home_assistant_env_scan",
            {**HA, "entity_id_regex": "(unclosed"},
            p2_prefix="invalid entity_id_regex: ",
        ),
        scenario(
            "env scan knob error before the regex",
            "home_assistant_env_scan",
            {**HA, "entity_id_regex": "(unclosed", "max_body_bytes": 0},
        ),
        scenario(
            "env scan base_url required",
            "home_assistant_env_scan",
            {"access_token": "t"},
        ),
        scenario(
            "env scan access_token required",
            "home_assistant_env_scan",
            {"base_url": ORIGIN},
        ),
        scenario(
            "env scan nothing matches",
            "home_assistant_env_scan",
            HA,
            {
                "/api/states": _reply(
                    200,
                    [
                        {
                            "entity_id": "sensor.power",
                            "state": "5",
                            "attributes": {"device_class": "power"},
                        },
                        {"entity_id": "sensor.outdoor_temperature", "state": "unknown"},
                    ],
                )
            },
        ),
        scenario(
            "env scan item shapes",
            "home_assistant_env_scan",
            {**HA, "entity_id_regex": "TEMP"},
            {
                "/api/states": _reply(
                    200,
                    [
                        "not an object",
                        7,
                        {"entity_id": 5, "state": "1"},
                        {"entity_id": None, "state": "1"},
                        {"state": "1"},
                        {"entity_id": "Sensor.upper_temp", "state": "1"},
                        {
                            "entity_id": "sensor.Outdoor_TEMP",
                            "state": "3.5",
                            "attributes": "x",
                        },
                        {"entity_id": "binary_sensor.window_temp", "state": "1"},
                        {
                            "entity_id": "sensor.hum",
                            "state": "40",
                            "attributes": {"device_class": "Humidity"},
                        },
                        {
                            "entity_id": "sensor.hum2",
                            "state": "40",
                            "attributes": {"device_class": "humidity"},
                        },
                    ],
                )
            },
        ),
        scenario(
            "env scan device classes and null device class",
            "home_assistant_env_scan",
            {
                **HA,
                "include_device_classes": [" Power ", None, 5],
                "entity_id_regex": "^$",
                "metric_prefix": "home.",
            },
            {
                "/api/states": _reply(
                    200,
                    [
                        {
                            "entity_id": "sensor.power",
                            "state": "5",
                            "attributes": {"device_class": "POWER"},
                        },
                        {
                            "entity_id": "sensor.unclassified",
                            "state": "6",
                            "attributes": {"device_class": None},
                        },
                        {
                            "entity_id": "sensor.five",
                            "state": "7",
                            "attributes": {"device_class": 5},
                        },
                        {
                            "entity_id": "sensor.temperature",
                            "state": "8",
                            "attributes": {},
                        },
                    ],
                )
            },
        ),
        scenario(
            "env scan include_device_classes not a list",
            "home_assistant_env_scan",
            {**HA, "include_device_classes": "power", "entity_id_regex": "^$"},
            {"/api/states": _reply(200, ENV_STATES)},
        ),
        scenario(
            "env scan normalisation error stops the scan",
            "home_assistant_env_scan",
            {**HA, "subject_mapping": {"default": {}}},
            {
                "/api/states": _reply(
                    200,
                    [
                        {"entity_id": "sensor.ok_temperature", "state": "1"},
                        {
                            "entity_id": "sensor.",
                            "state": "2",
                            "attributes": {"device_class": "temperature"},
                        },
                    ],
                )
            },
        ),
        scenario(
            "env scan write failure",
            "home_assistant_env_scan",
            HA,
            {"/api/states": _reply(200, ENV_STATES)},
            write_error="write failed",
        ),
        scenario(
            "env scan server error",
            "home_assistant_env_scan",
            HA,
            {"/api/states": _reply(503, b"busy")},
        ),
        # --- http_json_metric ------------------------------------------------
        scenario(
            "json ingests value (test_http_json_metric_spec_ingests_value)",
            "http_json_metric",
            JSON,
            HEALTH,
            spec_name="mail queue depth",
        ),
        scenario(
            "json missing path (test_http_json_metric_spec_missing_path_sets_warning)",
            "http_json_metric",
            {**JSON, "metric_path": "$.queue.missing"},
            HEALTH,
        ),
        scenario(
            "json nested value (test_http_json_metric_spec_ingests_three_level_nested_value)",
            "http_json_metric",
            {
                "url": ORIGIN + "/thermal",
                "metric_path": "$.lm_sensors.cpu_tctl.value",
                "metric_name": "host.temperature_celsius",
                "host_id": "fractal",
                "subject_type": "host",
                "subject_id": "fractal",
                "source_system": "fractal_thermal_endpoint",
                "source_instance": "fractal-lm-sensors",
                "source_entity_id": "lm_sensors:k10temp:Tctl",
            },
            {"/thermal": _reply(200, {"lm_sensors": {"cpu_tctl": {"value": 55.625}}})},
        ),
        scenario(
            "json non-finite value (test_http_json_metric_spec_skips_non_finite_value)",
            "http_json_metric",
            JSON,
            {"/health": _reply(200, {"queue": {"depth": "NaN"}})},
        ),
        scenario(
            "json canonical dimensions (test_http_json_metric_spec_emits_canonical_dimensions)",
            "http_json_metric",
            {
                **JSON,
                "subject_type": "service",
                "subject_id": "graphyard_web",
                "source_system": "graphyard",
                "source_instance": "default",
                "source_entity_id": "fractal_thermal_endpoint:queue_depth",
                "collector_service": "graphyard-agent",
                "collector_host": "macmini",
            },
            HEALTH,
        ),
        scenario(
            "json bearer token",
            "http_json_metric",
            {**JSON, "bearer_token": " test-bearer ", "verify_tls": False},
            HEALTH,
        ),
        scenario(
            "json basic auth",
            "http_json_metric",
            {**JSON, "basic_username": "user", "basic_password": "pass"},
            HEALTH,
        ),
        scenario(
            "json basic auth needs both fields",
            "http_json_metric",
            {**JSON, "basic_username": "user", "basic_password": "   "},
            HEALTH,
        ),
        scenario(
            "json bearer and basic auth",
            "http_json_metric",
            {**JSON, "bearer_token": "b", "basic_username": "u", "basic_password": "p"},
            HEALTH,
        ),
        scenario(
            "json credentials in the URL",
            "http_json_metric",
            {**JSON, "url": "http://nyx:secret@fixture.test/health"},
            HEALTH,
        ),
        scenario(
            "json fractal storage endpoint (production shape)",
            "http_json_metric",
            {
                "url": ORIGIN + "/storage",
                "basic_username": "nyxmon",
                "basic_password": "redacted",
                "metric_path": "$.pools.fast.cap_ratio",
                "metric_name": "host.zfs_pool_capacity_ratio",
                "host_id": "fractal",
                "subject_type": "host",
                "subject_id": "fractal",
                "source_system": "fractal_storage_endpoint",
                "source_instance": "fractal-zfs",
                "source_entity_id": "zfs:fast:cap_ratio",
                "collector_service": "graphyard-agent",
                "collector_host": "macmini",
                "tags": {"pool": "fast"},
                "request_timeout_seconds": 10,
            },
            {"/storage": _reply(200, {"pools": {"fast": {"cap_ratio": 0.27}}})},
            spec_name="fractal fast pool capacity",
        ),
        scenario(
            "json host subject keeps the raw host id",
            "http_json_metric",
            {
                **JSON,
                "subject_type": " Host ",
                "host_id": " Mac Mini ",
                "subject_id": "macmini",
            },
            HEALTH,
        ),
        scenario(
            "json default chain without service",
            "http_json_metric",
            {
                "url": ORIGIN + "/health",
                "metric_path": "$.queue.depth",
                "metric_name": "m",
            },
            HEALTH,
        ),
        scenario(
            "json default chain with empty host id",
            "http_json_metric",
            {**JSON, "host_id": "", "service_id": ""},
            HEALTH,
        ),
        scenario(
            "json blank fields fall back",
            "http_json_metric",
            {
                **JSON,
                "source_instance": "  ",
                "source_entity_id": "  ",
                "collector_service": " ",
                "collector_host": " ",
                "service_id": "  ",
                "host_id": "",
            },
            HEALTH,
        ),
        scenario(
            "json null fields are text",
            "http_json_metric",
            {
                **JSON,
                "subject_type": None,
                "subject_id": None,
                "source_system": None,
                "host_id": None,
                "service_id": None,
            },
            HEALTH,
        ),
        scenario(
            "json tags",
            "http_json_metric",
            {
                **JSON,
                "tags": {
                    "pool": "fast",
                    "n": 1,
                    "flag": True,
                    "none": None,
                    "spec_name": "override",
                },
            },
            HEALTH,
        ),
        scenario(
            "json tags not an object",
            "http_json_metric",
            {**JSON, "tags": ["a"]},
            HEALTH,
        ),
        scenario("json config not an object", "http_json_metric", '"text"'),
        scenario("json url required", "http_json_metric", {**JSON, "url": "  "}),
        scenario(
            "json metric_path required", "http_json_metric", {**JSON, "metric_path": ""}
        ),
        scenario(
            "json metric_name required",
            "http_json_metric",
            {**JSON, "metric_name": " "},
        ),
        scenario(
            "json knobs before required fields",
            "http_json_metric",
            {"request_timeout_seconds": 0},
        ),
        scenario(
            "json max_body_bytes knob error",
            "http_json_metric",
            {**JSON, "max_body_bytes": "lots"},
        ),
        scenario(
            "json server error",
            "http_json_metric",
            JSON,
            {"/health": _reply(500, b"boom")},
        ),
        scenario(
            "json root path on a scalar",
            "http_json_metric",
            {**JSON, "metric_path": "$"},
            {"/health": _reply(200, b"7")},
        ),
        scenario(
            "json null value is not found",
            "http_json_metric",
            JSON,
            {"/health": _reply(200, {"queue": {"depth": None}})},
        ),
        scenario(
            "json bool value",
            "http_json_metric",
            JSON,
            {"/health": _reply(200, {"queue": {"depth": True}})},
        ),
        scenario(
            "json string value",
            "http_json_metric",
            JSON,
            {"/health": _reply(200, {"queue": {"depth": " 42 "}})},
        ),
        scenario(
            "json list index",
            "http_json_metric",
            {**JSON, "metric_path": "$.queue.1"},
            {"/health": _reply(200, {"queue": [1, 2.5]})},
        ),
        scenario(
            "json index beyond the digit limit",
            "http_json_metric",
            {**JSON, "metric_path": "$.queue." + "1" * 4301},
            {"/health": _reply(200, {"queue": [1, 2.5]})},
        ),
        scenario(
            "json integer beyond the digit limit in the body",
            "http_json_metric",
            JSON,
            {"/health": _reply(200, b'{"queue": {"depth": ' + b"9" * 4301 + b"}}")},
        ),
        scenario(
            "json huge integer value",
            "http_json_metric",
            JSON,
            {"/health": _reply(200, {"queue": {"depth": 10**400}})},
        ),
        scenario(
            "json invalid URL",
            "http_json_metric",
            {**JSON, "url": "http://[::1"},
        ),
        scenario(
            "json write failure",
            "http_json_metric",
            JSON,
            HEALTH,
            write_error="(400)\nReason: Bad Request\n",
        ),
        scenario(
            "json registry failure",
            "http_json_metric",
            JSON,
            HEALTH,
            touch_error="Invalid subject_id '': expected lowercase snake_case identifier",
        ),
    ]


def build_ha_json_executors() -> dict[str, Any]:
    return {"origin": ORIGIN, "now": NOW.isoformat(), "scenarios": _scenarios()}


GENERATORS = {"ha_json_executors.json": build_ha_json_executors}
