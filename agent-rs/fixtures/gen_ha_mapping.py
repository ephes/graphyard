"""Fixture ``ha_mapping.json``: Home Assistant state normalisation (S7a).

Pins ``graphyard-core::home_assistant`` (agent-rs/PLAN.md 6.3) against
``services.py``: ``_resolve_subject_mapping``,
``_resolve_home_assistant_metric_mapping`` and
``_normalize_home_assistant_sensor_state``, with the warn-once keys of
``_warn_subject_mapping_once``. One case is a sequence of calls on one spec
sharing the warn-once set (cleared at the start of the case, as at the start
of a metrics run); the production env scan config of macmini is one of them.

Encoding:

* spec configs and state payloads are JSON texts (decoded with
  ``json.loads`` here and ``pycompat::json_loads`` in Rust);
* ``datetime.now`` inside ``services`` is pinned to ``now``;
* points use the encoding of ``gen_normalize.py`` (``ts`` isoformat,
  ``value`` as ``repr``, tags as ``[key, value]`` pairs);
* floats in calls and results are ``repr`` text;
* log records are ``{"level", "message"}``; a message ending in a ``re``
  compile error is ``{"level", "prefix", "p2": true}`` (the error text is
  the regex engine's, D-5);
* errors are ``{"class": <exception class>, "message": str(exc)}``;
* ``warned_keys`` is the sorted warn-once set after the last call.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest import mock

from graphyard import services

NOW = datetime(2026, 10, 21, 9, 30, 15, 654321, tzinfo=UTC)


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):  # noqa: ANN001, ANN206 - datetime signature
        return NOW if tz is not None else NOW.replace(tzinfo=None)


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.records: list[dict[str, Any]] = []

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        args = record.args if isinstance(record.args, tuple) else ()
        errors = [arg for arg in args if isinstance(arg, re.error)]
        if errors:
            text = str(errors[-1])
            assert message.endswith(text)
            self.records.append(
                {
                    "level": record.levelname,
                    "prefix": message[: len(message) - len(text)],
                    "p2": True,
                }
            )
        else:
            self.records.append({"level": record.levelname, "message": message})


def _encode_point(point: Any) -> dict[str, Any] | None:
    if point is None:
        return None
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
    tags = point.tags
    ordered["tags"] = None if tags is None else [[k, v] for k, v in tags.items()]
    return ordered


def _error(exc: BaseException) -> dict[str, str]:
    return {"class": type(exc).__name__, "message": str(exc)}


# --- calls --------------------------------------------------------------------


def normalize(
    payload: Any,
    *,
    override: str = "",
    extra_tags: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "fn": "normalize",
        "payload": payload if isinstance(payload, str) else json.dumps(payload),
        "metric_name_override": override,
        "extra_tags": [[k, v] for k, v in (extra_tags or {}).items()],
    }


def subject(entity_id: str) -> dict[str, Any]:
    return {"fn": "subject", "entity_id": entity_id}


def metric(entity_id: str, metric_name: str, value: float) -> dict[str, Any]:
    return {
        "fn": "metric",
        "entity_id": entity_id,
        "metric_name": metric_name,
        "value": repr(value),
    }


def _run_call(spec: SimpleNamespace, call: dict[str, Any]) -> dict[str, Any]:
    if call["fn"] == "normalize":
        point = services._normalize_home_assistant_sensor_state(
            json.loads(call["payload"]),
            spec=spec,
            metric_name_override=call["metric_name_override"],
            extra_tags=dict(call["extra_tags"]) or None,
        )
        return {"point": _encode_point(point)}
    if call["fn"] == "subject":
        resolved = services._resolve_subject_mapping(
            spec=spec, entity_id=call["entity_id"]
        )
        return {"subject": None if resolved is None else list(resolved)}
    name, value, tags = services._resolve_home_assistant_metric_mapping(
        spec=spec,
        entity_id=call["entity_id"],
        metric_name=call["metric_name"],
        value=float(call["value"]),
    )
    return {"metric": [name, repr(value), [[k, v] for k, v in tags.items()]]}


def _case(
    name: str, config: Any, calls: list[dict[str, Any]], spec_name: str = ""
) -> dict[str, Any]:
    spec_name = spec_name or name
    config_text = config if isinstance(config, str) else json.dumps(config)
    spec = SimpleNamespace(name=spec_name, config=json.loads(config_text))
    services._subject_mapping_warning_keys.clear()
    capture = _Capture()
    logger = logging.getLogger("graphyard.services")
    previous_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(capture)
    results = []
    try:
        with mock.patch.object(services, "datetime", _FixedDatetime):
            for call in calls:
                capture.records = []
                try:
                    outcome = _run_call(spec, call)
                except Exception as exc:  # noqa: BLE001 - recorded as the oracle
                    outcome = {"error": _error(exc)}
                results.append({**call, **outcome, "logs": capture.records})
    finally:
        logger.removeHandler(capture)
        logger.setLevel(previous_level)
    return {
        "name": name,
        "spec_name": spec_name,
        "config": config_text,
        "calls": results,
        "warned_keys": sorted(services._subject_mapping_warning_keys),
    }


# --- corpus -------------------------------------------------------------------

PRODUCTION_CONFIG = {
    "base_url": "http://macmini.local:10020",
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

PRODUCTION_NAME = "Home Assistant Environment Scan"


def _ha_state(entity_id: str, state: Any, **attributes: Any) -> dict[str, Any]:
    return {
        "entity_id": entity_id,
        "state": state,
        "attributes": attributes,
        "last_changed": "2026-10-21T09:29:58.123456+00:00",
        "last_updated": "2026-10-21T09:29:58.123456+00:00",
    }


def _production_calls() -> list[dict[str, Any]]:
    states = [
        _ha_state(
            "sensor.fritz_box_7590_ax_download_throughput",
            "1234.56",
            unit_of_measurement="kB/s",
            device_class="data_rate",
            friendly_name="FRITZ!Box 7590 AX Download Throughput",
        ),
        _ha_state(
            "sensor.fritz_box_7590_ax_upload_throughput",
            "98.7",
            unit_of_measurement="kB/s",
            device_class="data_rate",
        ),
        _ha_state(
            "sensor.fritz_box_7590_ax_cpu_temperature",
            "61",
            unit_of_measurement="°C",
            device_class="temperature",
        ),
        _ha_state(
            "sensor.usw_pro_xg_8_poe_temperature",
            "48.5",
            unit_of_measurement="°C",
            device_class="temperature",
        ),
        _ha_state(
            "sensor.wohnzimmer_temperatur",
            "21.4",
            unit_of_measurement="°C",
            device_class="temperature",
        ),
        _ha_state(
            "sensor.bad_luftfeuchtigkeit",
            "unavailable",
            unit_of_measurement="%",
            device_class="humidity",
        ),
        _ha_state(
            "sensor.keller_humidity",
            "67.0",
            unit_of_measurement="%",
            device_class="humidity",
        ),
    ]
    return [
        normalize(
            state,
            override=f"ha.{state['entity_id']}",
            extra_tags={"spec_name": PRODUCTION_NAME},
        )
        for state in states
    ]


STATE = {
    "entity_id": "sensor.office_temperature",
    "state": "21.2",
    "last_updated": "2026-03-04T12:00:00Z",
    "attributes": {"device_class": "temperature", "unit_of_measurement": "°C"},
}


def _state(**changes: Any) -> dict[str, Any]:
    state = dict(STATE)
    for key, value in changes.items():
        if value is _DROP:
            state.pop(key, None)
        else:
            state[key] = value
    return state


_DROP = object()

BASE = {"base_url": "https://ha.local", "access_token": "token"}

DEFAULT_MAPPING = {
    "subject_mapping": {
        "default": {
            "subject_type": "environment_sensor",
            "subject_id_from": "entity_name_slug",
        }
    }
}


def _cfg(**changes: Any) -> dict[str, Any]:
    return {**BASE, **DEFAULT_MAPPING, **changes}


def _cases() -> list[dict[str, Any]]:
    cases = [
        _case(PRODUCTION_NAME, PRODUCTION_CONFIG, _production_calls()),
        _case(
            "single sensor (test_home_assistant_sensor_spec_ingests_single_entity)",
            {
                **BASE,
                "entity_id": "sensor.office_temperature",
                "metric_name": "ha.sensor.office_temperature",
                "host_id": "homeassistant",
                "service_id": "homeassistant",
            },
            [
                normalize(
                    STATE,
                    override="ha.sensor.office_temperature",
                    extra_tags={"spec_name": "ha single sensor"},
                ),
                normalize(STATE, extra_tags={"spec_name": "ha single sensor"}),
            ],
            spec_name="ha single sensor",
        ),
        _case(
            "missing mapping warns once per run",
            BASE,
            [
                normalize(_state(entity_id="sensor.Wohnzimmer Sensor Temperature")),
                normalize(_state()),
                subject("binary_sensor.door"),
            ],
        ),
        _case(
            "null mapping is missing",
            {**BASE, "subject_mapping": None},
            [subject("sensor.a")],
        ),
        _case(
            "invalid mapping types",
            {**BASE, "subject_mapping": ["rules"]},
            [subject("sensor.a"), subject("sensor.b")],
        ),
        _case(
            "string mapping type",
            {**BASE, "subject_mapping": "x"},
            [subject("sensor.a")],
        ),
        _case(
            "rules: fullmatch, unknown type, missing template, bad template, bad regex",
            _cfg(
                subject_mapping={
                    "default": {"subject_type": "environment_sensor"},
                    "rules": [
                        "not an object",
                        {"subject_type": "host"},
                        {"match_entity_id_regex": "   "},
                        {"match_entity_id_regex": "(unclosed", "subject_type": "host"},
                        {
                            "match_entity_id_regex": "sensor\\.unknown_.*",
                            "subject_type": "Not_A_Type",
                            "subject_id_template": "x",
                        },
                        {
                            "match_entity_id_regex": " sensor\\.host_.* ",
                            "subject_type": " HOST ",
                            "subject_id_template": " Mac Mini ",
                        },
                        {
                            "match_entity_id_regex": "sensor\\.notemplate_.*",
                            "subject_type": "service",
                        },
                        {
                            "match_entity_id_regex": "sensor\\.blanktemplate_.*",
                            "subject_type": "service",
                            "subject_id_template": "   ",
                        },
                        {
                            "match_entity_id_regex": "sensor\\.numtemplate_.*",
                            "subject_type": "service",
                            "subject_id_template": 5,
                        },
                        {
                            "match_entity_id_regex": "sensor\\.badtemplate_.*",
                            "subject_type": "service",
                            "subject_id_template": "!!!",
                        },
                        {
                            "match_entity_id_regex": "sensor\\.partial",
                            "subject_type": "service",
                            "subject_id_template": "partial",
                        },
                        {
                            "match_entity_id_regex": "a|ab",
                            "subject_type": "network_device",
                            "subject_id_template": "alternation",
                        },
                        {
                            "match_entity_id_regex": "(?x) sensor \\. verbose_.*  # comment",
                            "subject_type": "network_device",
                            "subject_id_template": "verbose",
                        },
                        {
                            "match_entity_id_regex": "(?i)(?x) sensor \\. multiflag_.*  # comment",
                            "subject_type": "network_device",
                            "subject_id_template": "multiflag",
                        },
                        {
                            "match_entity_id_regex": "sensor\\.(?=look)lookahead",
                            "subject_type": "network_device",
                            "subject_id_template": "lookaround",
                        },
                        {
                            "match_entity_id_regex": "sensor\\.(\\w)\\1x",
                            "subject_type": "network_device",
                            "subject_id_template": "backref",
                        },
                    ],
                }
            ),
            [
                subject("sensor.unknown_one"),
                subject("sensor.unknown_two"),
                subject("sensor.host_macmini"),
                subject("sensor.notemplate_a"),
                subject("sensor.blanktemplate_a"),
                subject("sensor.numtemplate_a"),
                subject("sensor.badtemplate_a"),
                subject("sensor.partial_more"),
                subject("sensor.partial"),
                subject("ab"),
                subject("sensor.verbose_x"),
                subject("SENSOR.MultiFlag_x"),
                subject("sensor.lookahead"),
                subject("sensor.aax"),
                subject("sensor.abx"),
                normalize(_state(entity_id="sensor.host_homeassistant")),
                normalize(_state(entity_id="sensor.notemplate_b")),
            ],
        ),
        _case(
            "rules not a list are ignored",
            {**BASE, "subject_mapping": {"rules": {"a": 1}, "default": {}}},
            [subject("sensor.a")],
        ),
        _case(
            "default mapping variants",
            {
                **BASE,
                "subject_mapping": {
                    "default": {
                        "subject_type": " Network_Device ",
                        "subject_id_from": "friendly_name",
                    }
                },
            },
            [
                subject("sensor.Router CPU"),
                subject("binary_sensor.Router"),
                subject("Sensor.Upper"),
            ],
        ),
        _case(
            "default mapping null",
            {**BASE, "subject_mapping": {"default": None}},
            [subject("sensor.a")],
        ),
        _case(
            "default mapping unknown type",
            {**BASE, "subject_mapping": {"default": {"subject_type": "gadget"}}},
            [subject("sensor.a"), subject("sensor.b")],
        ),
        _case(
            "default mapping subject_id_from null",
            {**BASE, "subject_mapping": {"default": {"subject_id_from": None}}},
            [subject("sensor.a")],
        ),
        _case(
            "slug failures raise",
            _cfg(),
            [
                subject("sensor."),
                subject("binary_sensor.!!"),
                normalize(_state(entity_id="sensor.___")),
            ],
        ),
        _case(
            "metric mapping (test_home_assistant_env_scan_metric_mapping_rewrites_metric_and_value)",
            _cfg(
                subject_mapping={
                    "default": {"subject_type": "environment_sensor"},
                    "rules": [
                        {
                            "match_entity_id_regex": "^sensor\\.fritz_box_.*_upload_throughput$",
                            "subject_type": "network_device",
                            "subject_id_template": "fritz_box_7590_ax",
                        }
                    ],
                },
                metric_mapping={
                    "rules": [
                        {
                            "match_entity_id_regex": "^sensor\\.fritz_box_.*_upload_throughput$",
                            "metric_name": "network_device.network_transmit_bytes_per_second",
                            "value_multiplier": 1000,
                            "extra_tags": {
                                "traffic_direction": "transmit",
                                "traffic_scope": "wan",
                            },
                        }
                    ]
                },
            ),
            [
                normalize(
                    _state(
                        entity_id="sensor.fritz_box_7590_ax_upload_throughput",
                        state="6.1",
                        attributes={
                            "device_class": "data_rate",
                            "unit_of_measurement": "kB/s",
                        },
                    ),
                    override="ha.sensor.fritz_box_7590_ax_upload_throughput",
                    extra_tags={"spec_name": "ha traffic env scan"},
                )
            ],
        ),
        _case(
            "metric mapping rules and multipliers",
            _cfg(
                metric_mapping={
                    "rules": [
                        7,
                        {"metric_name": "no regex"},
                        {"match_entity_id_regex": "[bad"},
                        {
                            "match_entity_id_regex": "^sensor\\.bool$",
                            "value_multiplier": True,
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.none$",
                            "value_multiplier": None,
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.list$",
                            "value_multiplier": [2],
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.text$",
                            "value_multiplier": " 2.5 ",
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.badtext$",
                            "value_multiplier": "two",
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.float$",
                            "value_multiplier": 0.001,
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.huge$",
                            "value_multiplier": 10**400,
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.bigfloat$",
                            "value_multiplier": "1e999",
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.zero$",
                            "value_multiplier": 0,
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.tags$",
                            "extra_tags": {
                                "n": 1,
                                "b": True,
                                "x": None,
                                "entity_id": "override",
                            },
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.badtags$",
                            "extra_tags": ["a"],
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.nulltags$",
                            "extra_tags": None,
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.rename$",
                            "metric_name": "  ",
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.nullname$",
                            "metric_name": None,
                        },
                        {
                            "match_entity_id_regex": "^sensor\\.spaced$",
                            "metric_name": "  new.name  ",
                        },
                        {"match_entity_id_regex": "UPPER"},
                        {"match_entity_id_regex": "^sensor\\.default$"},
                    ]
                }
            ),
            [
                metric("sensor.bool", "m", 2.0),
                metric("sensor.bool", "m", 3.0),
                metric("sensor.none", "m", 2.0),
                metric("sensor.list", "m", 2.0),
                metric("sensor.text", "m", 2.0),
                metric("sensor.badtext", "m", 2.0),
                metric("sensor.float", "m", 1234.5),
                metric("sensor.huge", "m", 2.0),
                metric("sensor.bigfloat", "m", 2.0),
                metric("sensor.zero", "m", 2.0),
                metric("sensor.tags", "m", 2.0),
                metric("sensor.badtags", "m", 2.0),
                metric("sensor.nulltags", "m", 2.0),
                metric("sensor.rename", "m", 2.0),
                metric("sensor.nullname", "m", 2.0),
                metric("sensor.spaced", "m", 2.0),
                metric("sensor.upper", "  m  ", 2.0),
                metric("sensor.UPPER", "  m  ", 2.0),
                metric("sensor.default", "m", 2.0),
                metric("sensor.unmatched", "m", 2.0),
                normalize(_state(entity_id="sensor.zero", state="5")),
                normalize(_state(entity_id="sensor.bigfloat", state="5")),
                normalize(_state(entity_id="sensor.huge", state="5")),
                normalize(_state(entity_id="sensor.tags", state="5")),
            ],
        ),
        _case(
            "metric mapping rules not a list",
            _cfg(metric_mapping={"rules": "x"}),
            [metric("sensor.a", "m", 1.0), metric("sensor.b", "m", 1.0)],
        ),
        _case(
            "metric mapping rules null",
            _cfg(metric_mapping={"rules": None}),
            [metric("sensor.a", "m", 1.0)],
        ),
        _case(
            "metric mapping not an object",
            _cfg(metric_mapping=["rules"]),
            [metric("sensor.a", "m", 1.0)],
        ),
        _case(
            "overflowing mapped value (test_home_assistant_env_scan_skips_overflowing_mapped_value)",
            _cfg(
                metric_mapping={
                    "rules": [
                        {
                            "match_entity_id_regex": "_upload_throughput$",
                            "value_multiplier": 1000,
                        }
                    ]
                }
            ),
            [
                normalize(
                    _state(entity_id="sensor.overflow_upload_throughput", state="1e308")
                ),
                normalize(
                    _state(entity_id="sensor.normal_upload_throughput", state="6.1")
                ),
            ],
        ),
        _case(
            "state values",
            _cfg(),
            [
                normalize(_state(state="22.5")),
                normalize(_state(state=" 22.5 ")),
                normalize(_state(state=7)),
                normalize(_state(state=7.25)),
                normalize(_state(state=True)),
                normalize(_state(state=None)),
                normalize(_state(state=_DROP)),
                normalize(_state(state="unknown")),
                normalize(_state(state="unavailable")),
                normalize(_state(state="nan")),
                normalize(_state(state="inf")),
                normalize(_state(state="1e999")),
                normalize(_state(state="1_000")),
                normalize(_state(state=[1])),
                normalize(_state(state={"v": 1})),
                normalize('{"entity_id": "sensor.big", "state": ' + "9" * 400 + "}"),
                normalize(_state(state="-0.0")),
            ],
        ),
        _case(
            "entity ids",
            _cfg(),
            [
                normalize(_state(entity_id="  sensor.Office Temp  ")),
                normalize(_state(entity_id="")),
                normalize(_state(entity_id="   ")),
                normalize(_state(entity_id=_DROP)),
                normalize(_state(entity_id=None)),
                normalize(_state(entity_id=42)),
                normalize(_state(entity_id="light.kitchen")),
            ],
        ),
        _case(
            "timestamps",
            _cfg(),
            [
                normalize(_state(last_updated="2026-03-04T12:00:00Z")),
                normalize(_state(last_updated="2026-03-04T12:00:00.123456+02:00")),
                normalize(_state(last_updated="2026-03-04T12:00:00")),
                normalize(_state(last_updated="2026-03-04 12:00:00.5-05:30")),
                normalize(_state(last_updated="2026-03-04")),
                normalize(_state(last_updated="20260304T120000Z")),
                normalize(_state(last_updated="2026-03-04T12:00:00ZZ")),
                normalize(_state(last_updated="2026-03-04T12:00:00+00:00Z")),
                normalize(_state(last_updated="not a date")),
                normalize(_state(last_updated="")),
                normalize(_state(last_updated=None)),
                normalize(_state(last_updated=_DROP)),
                normalize(_state(last_updated=0)),
                normalize(_state(last_updated=0.0)),
                normalize(_state(last_updated=False)),
                normalize(_state(last_updated=[])),
                normalize(_state(last_updated=5)),
                normalize(_state(last_updated=True)),
                normalize(_state(last_updated=["2026"])),
                normalize(_state(last_updated="0001-01-01T00:30:00+01:00")),
                normalize(_state(last_updated="9999-12-31T23:30:00-01:00")),
                normalize(_state(last_updated="0001-01-01T00:30:00")),
                normalize(_state(last_updated="2026-02-30T00:00:00")),
            ],
        ),
        _case(
            "attributes and tags",
            _cfg(),
            [
                normalize(_state(attributes=None), extra_tags={"spec_name": "s"}),
                normalize(_state(attributes=[1]), extra_tags={"spec_name": "s"}),
                normalize(_state(attributes={}), extra_tags={"spec_name": "s"}),
                normalize(
                    _state(
                        attributes={"unit_of_measurement": None, "device_class": None}
                    ),
                ),
                normalize(
                    _state(attributes={"unit_of_measurement": 5, "device_class": False})
                ),
                normalize(
                    _state(
                        attributes={
                            "device_class": "temperature",
                            "unit_of_measurement": "°C",
                        }
                    ),
                    extra_tags={
                        "spec_name": "s",
                        "entity_id": "replaced",
                        "unit": "pre",
                    },
                ),
            ],
        ),
        _case(
            "collector fields",
            _cfg(
                source_system=" Home Assistant ",
                source_instance="   ",
                host_id="  ",
                service_id=" ",
                collector_service="",
                collector_host="   ",
            ),
            [normalize(_state())],
        ),
        _case(
            "collector fields null",
            _cfg(
                source_system=None,
                source_instance=None,
                host_id=None,
                service_id=None,
                collector_service=None,
                collector_host=None,
            ),
            [normalize(_state())],
        ),
        _case(
            "collector host defaults to host id",
            _cfg(host_id=" Edge Host ", service_id="svc"),
            [normalize(_state())],
        ),
        _case(
            "host compatibility tag",
            {
                **BASE,
                "host_id": "Mac Mini",
                "subject_mapping": {
                    "default": {"subject_type": "host"},
                    "rules": [
                        {
                            "match_entity_id_regex": "sensor\\.other",
                            "subject_type": "host",
                            "subject_id_template": "fractal",
                        }
                    ],
                },
            },
            [
                normalize(_state(entity_id="sensor.mac_mini")),
                normalize(_state(entity_id="sensor.other")),
                normalize(_state(entity_id="sensor.mac-mini")),
            ],
        ),
        _case(
            "host compatibility with invalid host id",
            {
                **BASE,
                "host_id": "!!!",
                "subject_mapping": {"default": {"subject_type": "host"}},
            },
            [normalize(_state(entity_id="sensor.x"))],
        ),
        _case(
            "metric names",
            _cfg(),
            [
                normalize(_state(entity_id="sensor.a b  c")),
                normalize(_state(), override="  custom  "),
            ],
        ),
        _case(
            "log values with line breaks (D-10)",
            {**BASE, "subject_mapping": {"default": {"subject_type": "a\nb"}}},
            [subject("sensor.x")],
            spec_name="evil\nspec",
        ),
    ]
    return cases


def build_ha_mapping() -> dict[str, Any]:
    return {"now": NOW.isoformat(), "cases": _cases()}


GENERATORS = {"ha_mapping.json": build_ha_mapping}
