"""Fixture ``normalize.json``: metric point normalisation (slice S4a).

Pins ``graphyard-core::normalize`` (agent-rs/PLAN.md 6.4) against
``influx.py``: ``normalize_subject_id``, ``_normalize_dimension_value``,
``parse_finite_float`` and ``normalize_metric_point``, plus the reserved tag
keys of ``write_points``.

Encoding:

* floats (point values, results) are ``repr`` text, so ``nan``/``inf`` and
  ``-0.0`` survive the JSON file;
* ``parse_finite_float`` inputs are JSON texts decoded with ``json.loads``,
  as the collectors receive them from HTTP bodies, so Rust decodes the same
  text with ``pycompat::json_loads``;
* timestamps are ``datetime.isoformat()`` of aware UTC datetimes;
* tags are lists of ``[key, value]`` pairs (insertion order is kept);
* errors are ``{"class": <exception class>, "message": str(exc)}``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from graphyard import influx

SUBJECT_IDS = [
    "macmini",
    "  MacMini  ",
    "Mac Mini!!",
    "__a__b__",
    "a__b",
    "a-b.c",
    "123",
    "a\tb",
    "x\ny",
    "\x1cabc\x1f",
    " nbsp ",
    "ÄÖÜ host",
    "K",
    "İstanbul",
    "ΣΑΣ",
    "café \U0001f389 host",
    "o'neil",
    'say "hi"',
    "back\\slash",
    "",
    "   ",
    "!!!",
    "_",
    "\x7f",
]

DIMENSION_VALUES = [
    ("source_system", None, "Vector"),
    ("source_system", None, " home assistant "),
    ("source_system", None, "a  b"),
    ("source_system", None, "a\tb\nc"),
    ("source_system", None, "ok-1.2_3"),
    ("source_system", None, "UPPER"),
    ("source_system", None, "-leading"),
    ("source_system", None, ".dot"),
    ("source_system", None, "_under"),
    ("source_system", None, "9lives"),
    ("source_system", None, "a/b"),
    ("source_system", None, "a b"),
    ("source_system", None, " x "),
    ("source_system", None, "ünïcode"),
    ("source_system", None, "K"),
    ("source_system", None, "\x1c"),
    ("source_system", None, ""),
    ("source_system", None, "   "),
    ("source_system", None, "o'neil"),
    ("collector_service", None, "graphyard-agent"),
    ("collector_service", None, ""),
    ("collector_host", None, "macmini"),
    ("collector_host", None, "Mac Mini"),
    ("source_instance", "default", ""),
    ("source_instance", "default", "   "),
    ("source_instance", "default", "ha-main"),
    ("source_instance", "default", "HA Main"),
    ("source_instance", "default", "bad/instance"),
    ("source_instance", "has space", ""),
    ("source_instance", "Bad!", ""),
]

# JSON texts as HTTP bodies hand them to parse_finite_float.
FINITE_FLOAT_INPUTS = [
    "1.5",
    "3",
    "-0.0",
    "0",
    "5e-324",
    "1e308",
    "1e999",
    "-1e999",
    "12345678901234567890",
    "1" + "0" * 400,
    '"1.5"',
    '" 42 "',
    '"1_000"',
    '"1__0"',
    '"  -1.25e-3  "',
    '"\\u00a01.5\\u2003"',
    '"0x10"',
    '"1e309"',
    '"nan"',
    '"NaN"',
    '"inf"',
    '"-Infinity"',
    '"+inf"',
    '"abc"',
    '""',
    '"None"',
    "true",
    "false",
    "null",
    "[1]",
    '{"a": 1}',
    '"it\'s"',
]


def _error(exc: BaseException) -> dict[str, str]:
    return {"class": type(exc).__name__, "message": str(exc)}


def _subject_ids() -> list[dict[str, Any]]:
    cases = []
    for raw in SUBJECT_IDS:
        try:
            cases.append({"raw": raw, "ok": influx.normalize_subject_id(raw)})
        except ValueError as exc:
            cases.append({"raw": raw, "error": _error(exc)})
    return cases


def _dimension_values() -> list[dict[str, Any]]:
    cases = []
    for field_name, fallback, raw in DIMENSION_VALUES:
        case: dict[str, Any] = {
            "field_name": field_name,
            "fallback": fallback,
            "raw": raw,
        }
        try:
            case["ok"] = influx._normalize_dimension_value(
                raw, field_name=field_name, fallback=fallback
            )
        except ValueError as exc:
            case["error"] = _error(exc)
        cases.append(case)
    return cases


def _finite_floats() -> list[dict[str, Any]]:
    cases = []
    for text in FINITE_FLOAT_INPUTS:
        raw = json.loads(text)
        try:
            cases.append({"json": text, "ok": repr(influx.parse_finite_float(raw))})
        except (TypeError, ValueError, OverflowError) as exc:
            cases.append({"json": text, "error": _error(exc)})
    return cases


TS = datetime(2026, 10, 21, 8, 15, 30, 123456, tzinfo=UTC)

BASE_POINT: dict[str, Any] = {
    "ts": TS,
    "metric": "host.cpu_usage",
    "value": 0.3,
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


def _point(**changes: Any) -> dict[str, Any]:
    return {**BASE_POINT, **changes}


POINTS: list[tuple[str, dict[str, Any]]] = [
    ("baseline host point", _point()),
    ("host defaults to subject id", _point(host=None)),
    ("blank host defaults to subject id", _point(host="   ")),
    ("host normalised and equal", _point(host=" MacMini ")),
    (
        "host mismatch (test_normalize_metric_point_rejects_host_mismatch)",
        _point(host="other-host"),
    ),
    ("invalid host", _point(host="!!!")),
    (
        "non-host drops host (test_normalize_metric_point_drops_host_for_non_host_subject)",
        _point(
            metric="ha.sensor.office_temperature",
            value=22.4,
            subject_type="environment_sensor",
            subject_id="office_temperature",
            source_system="homeassistant",
            source_instance="ha-main",
            collector_service="graphyard-agent",
            collector_host="macmini",
            host="legacy-host",
        ),
    ),
    ("subject type stripped and lowered", _point(subject_type=" HOST ")),
    (
        "subject type mixed case",
        _point(subject_type="Network_Device", subject_id="USG Gateway"),
    ),
    ("service subject", _point(subject_type="service", subject_id="unifi")),
    ("unknown subject type", _point(subject_type="not_a_subject_type")),
    ("unknown subject type with quote", _point(subject_type="it's\n")),
    ("empty subject type", _point(subject_type="")),
    ("metric stripped", _point(metric="  host.cpu usage\t")),
    ("blank metric", _point(metric="   ")),
    ("empty metric", _point(metric="")),
    ("metric with newline kept", _point(metric="first\nforged")),
    ("invalid subject id", _point(subject_id="!!!")),
    ("subject id normalised", _point(subject_id=" Mac-Mini ", host="mac_mini")),
    ("source system required", _point(source_system="")),
    ("source system invalid", _point(source_system="home/assistant")),
    ("source system normalised", _point(source_system=" Home Assistant ")),
    ("source instance defaults", _point(source_instance="")),
    ("blank source instance defaults", _point(source_instance=" \t")),
    ("source instance invalid", _point(source_instance="a,b")),
    ("collector service required", _point(collector_service="  ")),
    ("collector host invalid", _point(collector_host="mac mini!")),
    ("source entity id stripped", _point(source_entity_id="  sensor.Office  ")),
    ("blank source entity id dropped", _point(source_entity_id="   ")),
    ("empty source entity id dropped", _point(source_entity_id="")),
    ("service stripped not lowered", _point(service="  Web App, Front  ")),
    ("blank service dropped", _point(service=" ")),
    ("tags kept in order", _point(tags={"z": "1", "a": "2", "spec_name": "My Spec"})),
    ("empty tags", _point(tags={})),
    (
        "value nan (test_normalize_metric_point_rejects_non_finite_value)",
        _point(value=float("nan")),
    ),
    ("value inf", _point(value=float("inf"))),
    ("value -inf", _point(value=float("-inf"))),
    ("value negative zero", _point(value=-0.0)),
    ("value tiny", _point(value=5e-324)),
    ("value huge", _point(value=1.7976931348623157e308)),
    (
        "first error wins: metric",
        _point(metric="", subject_type="bad", value=float("nan")),
    ),
    ("first error wins: subject type", _point(subject_type="bad", subject_id="!!!")),
    ("first error wins: subject id", _point(subject_id="", source_system="")),
    ("first error wins: host before value", _point(host="other", value=float("nan"))),
    (
        "pre-epoch timestamp",
        _point(ts=datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)),
    ),
]


def _encode_point(point: dict[str, Any]) -> dict[str, Any]:
    encoded = dict(point)
    encoded["ts"] = point["ts"].isoformat()
    encoded["value"] = repr(point["value"])
    tags = point["tags"]
    encoded["tags"] = None if tags is None else [[k, v] for k, v in tags.items()]
    return encoded


def _encode_normalized(point: influx.MetricPoint) -> dict[str, Any]:
    return {
        "ts": point.ts.isoformat(),
        "metric": point.metric,
        "value": repr(point.value),
        "subject_type": point.subject_type,
        "subject_id": point.subject_id,
        "source_system": point.source_system,
        "source_instance": point.source_instance,
        "source_entity_id": point.source_entity_id,
        "collector_service": point.collector_service,
        "collector_host": point.collector_host,
        "host": point.host,
        "service": point.service,
        "tags": [[k, v] for k, v in (point.tags or {}).items()],
    }


def _points() -> list[dict[str, Any]]:
    cases = []
    for name, fields in POINTS:
        case: dict[str, Any] = {"name": name, "point": _encode_point(fields)}
        try:
            normalized = influx.normalize_metric_point(influx.MetricPoint(**fields))
            case["ok"] = _encode_normalized(normalized)
        except ValueError as exc:
            case["error"] = _error(exc)
        cases.append(case)
    return cases


def build_normalize() -> dict[str, Any]:
    return {
        "reserved_tag_keys": sorted(influx._RESERVED_TAG_KEYS),
        "subject_ids": _subject_ids(),
        "dimension_values": _dimension_values(),
        "parse_finite_float": _finite_floats(),
        "points": _points(),
    }


GENERATORS = {"normalize.json": build_normalize}
