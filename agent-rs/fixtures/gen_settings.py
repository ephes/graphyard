"""Fixture ``settings.json``: the agent's environment variables (slice S2).

Pins ``graphyard-core::settings`` (agent-rs/PLAN.md 3.3). Every case runs the
real ``config/settings/base.py`` with exactly the case's environment
(django-environ 0.13.0 semantics: defaults, ``$`` proxies, ``int()``
parsing, the inner ``GRAPHYARD_HA_POLL_INTERVAL_SECONDS`` default) and
records the resulting agent settings or the exception of the import. For
successful cases it also runs ``apps.py:_configure_sqlite_connection`` on a
recording connection with those settings and records the pragma statements
and the warnings.

A case marked ``"divergence": "D-7"`` sets a variable only the web reads:
Python fails the import, the Rust agent does not read the variable.
"""

from __future__ import annotations

import logging
import os
import runpy
from pathlib import Path
from typing import Any
from unittest import mock

from django.test import override_settings

from graphyard import apps

BASE_SETTINGS = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "django"
    / "config"
    / "settings"
    / "base.py"
)

# Agent settings in Rust field order: (Rust field, Python setting name).
AGENT_SETTINGS = [
    ("sqlite_timeout_seconds", "GRAPHYARD_SQLITE_TIMEOUT_SECONDS"),
    ("sqlite_busy_timeout_ms", "GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS"),
    ("sqlite_journal_mode", "GRAPHYARD_SQLITE_JOURNAL_MODE"),
    ("sqlite_synchronous", "GRAPHYARD_SQLITE_SYNCHRONOUS"),
    ("influx_url", "INFLUX_URL"),
    ("influx_token", "INFLUX_TOKEN"),
    ("influx_org", "INFLUX_ORG"),
    ("influx_bucket", "INFLUX_BUCKET"),
    ("influx_measurement", "INFLUX_MEASUREMENT"),
    ("influx_timeout_ms", "INFLUX_TIMEOUT_MS"),
    ("influx_api_mode", "INFLUX_API_MODE"),
    (
        "metric_collection_interval_seconds",
        "GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS",
    ),
    ("condition_eval_interval_seconds", "GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS"),
    (
        "condition_data_stale_warning_seconds",
        "CONDITION_DATA_STALE_WARNING_SECONDS",
    ),
]

PRODUCTION = {
    "DJANGO_SETTINGS_MODULE": "config.settings.production",
    "INFLUX_URL": "http://127.0.0.1:8086",
    "INFLUX_TOKEN": "influx-token-value",
    "INFLUX_ORG": "graphyard",
    "INFLUX_BUCKET": "graphyard",
    "INFLUX_MEASUREMENT": "graphyard_metrics",
    "INFLUX_TIMEOUT_MS": "10000",
    "INFLUX_API_MODE": "v2",
    "GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS": "60",
    "GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS": "60",
    "GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS": "600",
    "GRAPHYARD_SQLITE_TIMEOUT_SECONDS": "30",
    "GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS": "30000",
    "GRAPHYARD_SQLITE_JOURNAL_MODE": "WAL",
    "GRAPHYARD_SQLITE_SYNCHRONOUS": "FULL",
}

CASES: list[dict[str, Any]] = [
    {"name": "defaults", "env": {}},
    {"name": "production values (macmini, PLAN.md section 1)", "env": PRODUCTION},
    {
        "name": "integers with whitespace, sign, underscores and leading zeros",
        "env": {
            "GRAPHYARD_SQLITE_TIMEOUT_SECONDS": " 15 ",
            "GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS": "-5",
            "INFLUX_TIMEOUT_MS": "1_000",
            "GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS": "+0030",
            "GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS": "\t900\n",
        },
    },
    {
        "name": "integers beyond 64 bits",
        "env": {
            "GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS": "99999999999999999999",
            "GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS": "-99999999999999999999",
        },
    },
    {
        "name": "metric interval from the legacy HA poll interval",
        "env": {"GRAPHYARD_HA_POLL_INTERVAL_SECONDS": "45"},
    },
    {
        "name": "metric interval set wins over the HA poll interval",
        "env": {
            "GRAPHYARD_HA_POLL_INTERVAL_SECONDS": "45",
            "GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS": "90",
        },
    },
    {
        "name": "invalid HA poll interval fails although the metric interval is set",
        "env": {
            "GRAPHYARD_HA_POLL_INTERVAL_SECONDS": "often",
            "GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS": "90",
        },
    },
    {
        "name": "proxy to another variable",
        "env": {
            "GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS": "$EVAL_EVERY",
            "EVAL_EVERY": "120",
            "INFLUX_TOKEN": "$SHARED_TOKEN",
            "SHARED_TOKEN": "proxied-token",
        },
    },
    {
        "name": "proxy chain with repeated dollar signs",
        "env": {
            "INFLUX_TIMEOUT_MS": "$$FIRST",
            "FIRST": "$SECOND",
            "SECOND": "2500",
            "INFLUX_ORG": "$$$ORG_SOURCE",
            "ORG_SOURCE": "proxied-org",
        },
    },
    {
        "name": "proxy to a missing variable takes the default",
        "env": {
            "INFLUX_TIMEOUT_MS": "$MISSING_TIMEOUT",
            "INFLUX_BUCKET": "$MISSING_BUCKET",
            "GRAPHYARD_SQLITE_JOURNAL_MODE": "$",
        },
    },
    {
        "name": "proxy of the metric interval to a missing variable takes the HA poll interval",
        "env": {
            "GRAPHYARD_HA_POLL_INTERVAL_SECONDS": "33",
            "GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS": "$NOT_SET",
        },
    },
    {
        "name": "proxy to an invalid integer",
        "env": {"INFLUX_TIMEOUT_MS": "$INFLUX_URL", "INFLUX_URL": "http://influx:8086"},
    },
    {
        "name": "proxy cycle",
        "env": {"INFLUX_URL": "$LOOP_A", "LOOP_A": "$LOOP_B", "LOOP_B": "$LOOP_A"},
    },
    {
        "name": "dollar sign inside a value is not a proxy",
        "env": {"INFLUX_TOKEN": "tok$en", "INFLUX_URL": " http://spaced/ "},
    },
    {"name": "empty integer", "env": {"GRAPHYARD_SQLITE_TIMEOUT_SECONDS": ""}},
    {"name": "float integer", "env": {"INFLUX_TIMEOUT_MS": "10000.0"}},
    {
        "name": "suffixed integer",
        "env": {"GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS": "600s"},
    },
    {
        "name": "first invalid variable in settings order wins",
        "env": {
            "GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS": "x",
            "GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS": "y",
        },
    },
    {
        "name": "long invalid integer is cut in the message",
        "env": {"INFLUX_TIMEOUT_MS": "z" * 250},
    },
    {
        "name": "integer beyond the 4300 digit limit",
        "env": {"INFLUX_TIMEOUT_MS": "7" * 4301},
    },
    {
        "name": "empty strings",
        "env": {
            "INFLUX_URL": "",
            "INFLUX_BUCKET": "",
            "INFLUX_MEASUREMENT": "",
            "INFLUX_API_MODE": "",
        },
    },
    {
        "name": "API mode is lower-cased",
        "env": {"INFLUX_API_MODE": "V3"},
    },
    {"name": "API mode auto in capitals", "env": {"INFLUX_API_MODE": "AUTO"}},
    {
        "name": "API mode with spaces is not stripped",
        "env": {"INFLUX_API_MODE": " v3 "},
    },
    {
        "name": "SQLite modes are stripped and upper-cased",
        "env": {
            "GRAPHYARD_SQLITE_JOURNAL_MODE": "  delete\n",
            "GRAPHYARD_SQLITE_SYNCHRONOUS": "\textra ",
        },
    },
    {
        "name": "invalid SQLite modes fall back with warnings",
        "env": {
            "GRAPHYARD_SQLITE_JOURNAL_MODE": "wal2",
            "GRAPHYARD_SQLITE_SYNCHRONOUS": "fast",
        },
    },
    {
        "name": "empty SQLite modes fall back",
        "env": {
            "GRAPHYARD_SQLITE_JOURNAL_MODE": "   ",
            "GRAPHYARD_SQLITE_SYNCHRONOUS": "",
        },
    },
    {
        "name": "web-only setting is not validated by the agent",
        "env": {"GRAPHYARD_LOGIN_FAILURE_WINDOW_SECONDS": "0"},
        "divergence": "D-7",
    },
]


class _RecordingCursor:
    def __init__(self, statements: list[str]) -> None:
        self._statements = statements

    def __enter__(self) -> _RecordingCursor:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str) -> None:
        self._statements.append(sql)


class _RecordingConnection:
    vendor = "sqlite"

    def __init__(self) -> None:
        self.statements: list[str] = []

    def cursor(self) -> _RecordingCursor:
        return _RecordingCursor(self.statements)


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def _pragmas(values: dict[str, Any]) -> dict[str, list[str]]:
    connection = _RecordingConnection()
    capture = _Capture()
    logger = logging.getLogger(apps.__name__)
    logger.addHandler(capture)
    try:
        with override_settings(**values):
            apps._configure_sqlite_connection(sender=None, connection=connection)
    finally:
        logger.removeHandler(capture)
    return {"statements": connection.statements, "warnings": capture.messages}


def _encode(value: object) -> object:
    # Integers as text: they may exceed 64 bits.
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return value


def _case(case: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {"name": case["name"], "env": case["env"]}
    if "divergence" in case:
        result["divergence"] = case["divergence"]
    with mock.patch.dict(os.environ, case["env"], clear=True):
        try:
            module = runpy.run_path(str(BASE_SETTINGS))
        except RecursionError as exc:
            result["error"] = {"class": type(exc).__name__, "p2": True}
            return result
        except Exception as exc:  # noqa: BLE001 - every import failure is recorded
            result["error"] = {"class": type(exc).__name__, "message": str(exc)}
            return result
    values = {python: module[python] for _field, python in AGENT_SETTINGS}
    result["settings"] = {
        field: _encode(values[python]) for field, python in AGENT_SETTINGS
    }
    result["pragmas"] = _pragmas(values)
    return result


def build_settings() -> dict[str, Any]:
    return {"cases": [_case(case) for case in CASES]}


GENERATORS = {"settings.json": build_settings}
