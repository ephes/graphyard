"""Fixtures ``store_ops.json`` and ``datetime_codec.json`` (slice S3a).

``store_ops.json`` pins the SQL effects of the agent's non-registry store
operations (PLAN.md 5.3, 5.4): every case starts from the given rows, runs the
Python operation on a temporary database migrated with the test settings
(``timezone.now`` injected) and records every row of the touched table
afterwards (all columns, raw SQLite values) or the exception. The operations:

* ``load_specs``: ``MetricCollectionSpec.objects.filter(enabled=True)`` (plus
  ``next_run_time__lte=now_ts`` for ``due_only``), as
  ``run_metric_collection_specs_once`` loads them, with the loaded values;
* ``update_spec``: ``spec.save(update_fields=[...])`` of the run result;
* ``load_conditions``: ``evaluate_conditions_once``'s queryset (optionally
  filtered by ``condition_id``) with the loaded values;
* ``update_condition_success`` / ``update_condition_failure``: the two
  ``condition.save(update_fields=...)`` calls of ``evaluate_conditions_once``;
* ``record_heartbeat``: ``services.record_heartbeat``.

``datetime_codec.json`` pins Django's datetime column conversion: stored text
through the ``sqlite3`` converter (``parse_datetime``) and
``convert_datetimefield_value``, then back through
``adapt_datetimefield_value``, for a curated and a seeded random corpus.

Integers in row images are SQLite INTEGER values, floats (always written with
a fraction or exponent by ``json.dumps``) are REAL values.
"""

from __future__ import annotations

import contextlib
import random
import sqlite3
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

from django.core.management import call_command
from django.db import connection, connections
from django.db.backends.sqlite3.base import decoder
from django.utils.dateparse import parse_datetime

from graphyard import services
from graphyard.models import ConditionDefinition, MetricCollectionSpec

NOW = datetime(2026, 10, 21, 8, 15, 30, 123456, tzinfo=UTC)
NOW_WHOLE_SECOND = datetime(2026, 10, 21, 8, 15, 30, tzinfo=UTC)
CREATED = "2026-01-02 03:04:05.678901"

SPEC_TABLE = "metric_collection_spec"
CONDITION_TABLE = "condition_definition"
HEARTBEAT_TABLE = "pipeline_heartbeat"


def _error(err: Exception, *, p2: bool) -> dict[str, Any]:
    error: dict[str, Any] = {"class": type(err).__name__, "message": str(err)}
    if p2:
        error["p2"] = True
    return error


def _db_text(value: datetime | None) -> str | None:
    """A loaded datetime as Django writes it (naive UTC text)."""
    if value is None:
        return None
    return str(connection.ops.adapt_datetimefield_value(value))


# ---------------------------------------------------------------------------
# A temporary migrated database.


@contextlib.contextmanager
def temp_database() -> Iterator[sqlite3.Connection]:
    """Point Django's default connection at a fresh migrated database.

    Yields a plain ``sqlite3`` connection (no converters, autocommit) for
    seeding and reading raw rows.
    """
    django_connection = connections["default"]
    django_connection.close()
    original = django_connection.settings_dict["NAME"]
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "db.sqlite3"
        django_connection.settings_dict["NAME"] = str(path)
        raw = None
        try:
            call_command("migrate", verbosity=0, interactive=False)
            raw = sqlite3.connect(path, isolation_level=None)
            yield raw
        finally:
            if raw is not None:
                raw.close()
            django_connection.close()
            django_connection.settings_dict["NAME"] = original


def _rows(raw: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    cursor = raw.execute(f'SELECT * FROM "{table}" ORDER BY id')
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _seed(raw: sqlite3.Connection, table: str, rows: list[dict[str, Any]]) -> None:
    raw.execute(f'DELETE FROM "{table}"')
    # Every case starts like a fresh database: AUTOINCREMENT continues
    # after the highest seeded id.
    raw.execute("DELETE FROM sqlite_sequence WHERE name = ?", [table])
    for row in rows:
        columns = ", ".join(f'"{column}"' for column in row)
        marks = ", ".join("?" for _ in row)
        raw.execute(
            f'INSERT INTO "{table}" ({columns}) VALUES ({marks})', list(row.values())
        )


# ---------------------------------------------------------------------------
# Rows.


def spec_row(row_id: int, name: str, **overrides: Any) -> dict[str, Any]:
    row = {
        "id": row_id,
        "name": name,
        "enabled": 1,
        "spec_type": "http_json_metric",
        "interval_seconds": 300,
        "next_run_time": 0,
        "config": '{"url": "http://127.0.0.1:9/metrics", "metric_path": "$.value"}',
        "last_run_at": None,
        "last_status": "warning",
        "last_error": "",
        "created_at": CREATED,
        "updated_at": CREATED,
    }
    row.update(overrides)
    return row


def condition_row(row_id: int, name: str, **overrides: Any) -> dict[str, Any]:
    row = {
        "id": row_id,
        "name": name,
        "enabled": 1,
        "metric_name": "host.filesystem_used_ratio",
        "host_filter": "",
        "service_filter": "",
        "tags_filter": "{}",
        "operator": "gte",
        "warning_threshold": 0.8,
        "critical_threshold": 0.9,
        "window_minutes": 30,
        "breach_minutes": 5,
        "status": "ok",
        "last_evaluated": None,
        "message": "",
        "last_value": None,
        "created_at": CREATED,
        "updated_at": CREATED,
        "subject_id_filter": "",
        "subject_type_filter": "",
        "alert_when_stale": 1,
        "stale_after_seconds": None,
    }
    row.update(overrides)
    return row


def heartbeat_row(row_id: int, name: str, **overrides: Any) -> dict[str, Any]:
    row = {
        "id": row_id,
        "name": name,
        "status": "ok",
        "last_success": "2026-10-20 07:00:00.250000",
        "last_error": "",
        "details": '{"total": 1}',
        "updated_at": "2026-10-20 07:00:00.250001",
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Operations.


def _spec_values(spec: MetricCollectionSpec) -> dict[str, Any]:
    return {
        "id": spec.id,
        "name": spec.name,
        "enabled": spec.enabled,
        "spec_type": spec.spec_type,
        "interval_seconds": spec.interval_seconds,
        "next_run_time": spec.next_run_time,
        "last_run_at": _db_text(spec.last_run_at),
        "last_status": spec.last_status,
        "last_error": spec.last_error,
        "created_at": _db_text(spec.created_at),
        "updated_at": _db_text(spec.updated_at),
    }


def _condition_values(condition: ConditionDefinition) -> dict[str, Any]:
    return {
        "id": condition.id,
        "name": condition.name,
        "enabled": condition.enabled,
        "metric_name": condition.metric_name,
        "host_filter": condition.host_filter,
        "service_filter": condition.service_filter,
        "subject_type_filter": condition.subject_type_filter,
        "subject_id_filter": condition.subject_id_filter,
        "operator": condition.operator,
        "warning_threshold": condition.warning_threshold,
        "critical_threshold": condition.critical_threshold,
        "window_minutes": condition.window_minutes,
        "breach_minutes": condition.breach_minutes,
        "stale_after_seconds": condition.stale_after_seconds,
        "alert_when_stale": condition.alert_when_stale,
        "status": condition.status,
        "message": condition.message,
        "last_value": condition.last_value,
        "last_evaluated": _db_text(condition.last_evaluated),
        "created_at": _db_text(condition.created_at),
        "updated_at": _db_text(condition.updated_at),
    }


def _now(op: dict[str, Any]) -> datetime:
    return datetime.fromisoformat(op["now"]).replace(tzinfo=UTC)


def _load_specs(raw: sqlite3.Connection, op: dict[str, Any]) -> dict[str, Any]:
    queryset = MetricCollectionSpec.objects.filter(enabled=True)
    if op["due_before"] is not None:
        queryset = queryset.filter(next_run_time__lte=op["due_before"])
    specs = list(queryset)
    configs = dict(raw.execute(f'SELECT id, config FROM "{SPEC_TABLE}"').fetchall())
    loaded = []
    for spec in specs:
        values = _spec_values(spec)
        # The executor parses the stored text (spec_config, D-11); the store
        # hands it over unchanged.
        values["config"] = configs[spec.id]
        loaded.append(values)
    return {"loaded": loaded}


def _update_spec(op: dict[str, Any]) -> None:
    spec = MetricCollectionSpec.objects.get(id=op["id"])
    if op.get("delete_first"):
        MetricCollectionSpec.objects.filter(id=op["id"]).delete()
    spec.last_run_at = _now(op)
    spec.last_status = op["last_status"]
    spec.last_error = op["last_error"]
    spec.next_run_time = op["next_run_time"]
    spec.save(
        update_fields=["last_run_at", "last_status", "last_error", "next_run_time"]
    )


def _load_conditions(raw: sqlite3.Connection, op: dict[str, Any]) -> dict[str, Any]:
    queryset = ConditionDefinition.objects.filter(enabled=True)
    if op["condition_id"] is not None:
        queryset = queryset.filter(id=op["condition_id"])
    conditions = list(queryset)
    filters = dict(
        raw.execute(f'SELECT id, tags_filter FROM "{CONDITION_TABLE}"').fetchall()
    )
    loaded = []
    for condition in conditions:
        values = _condition_values(condition)
        values["tags_filter"] = filters[condition.id]
        loaded.append(values)
    return {"loaded": loaded}


def _update_condition(op: dict[str, Any], *, success: bool) -> None:
    condition = ConditionDefinition.objects.get(id=op["id"])
    if op.get("delete_first"):
        ConditionDefinition.objects.filter(id=op["id"]).delete()
    condition.status = op["status"]
    condition.message = op["message"]
    if success:
        condition.last_value = op["last_value"]
        condition.last_evaluated = _now(op)
        condition.save(
            update_fields=["status", "message", "last_value", "last_evaluated"]
        )
    else:
        condition.save(update_fields=["status", "message"])


def _record_heartbeat(op: dict[str, Any]) -> None:
    services.record_heartbeat(
        op["name"],
        status=op["status"],
        last_error=op["last_error"],
        details=op["details"],
        success=op["success"],
    )


def _run_case(raw: sqlite3.Connection, case: dict[str, Any]) -> dict[str, Any]:
    table = case["table"]
    _seed(raw, table, case["before"])
    op = case["op"]
    result: dict[str, Any] = {}
    with mock.patch("django.utils.timezone.now", return_value=_now(op)):
        try:
            kind = op["kind"]
            if kind == "load_specs":
                result.update(_load_specs(raw, op))
            elif kind == "update_spec":
                _update_spec(op)
            elif kind == "load_conditions":
                result.update(_load_conditions(raw, op))
            elif kind == "update_condition_success":
                _update_condition(op, success=True)
            elif kind == "update_condition_failure":
                _update_condition(op, success=False)
            elif kind == "record_heartbeat":
                _record_heartbeat(op)
            else:
                raise AssertionError(f"unknown operation {kind}")
        except Exception as err:  # noqa: BLE001
            # The 0-row rule's message is graphyard-visible (P1); the others
            # are library texts (P2).
            p1 = str(err) == "Save with update_fields did not affect any rows."
            result["error"] = _error(err, p2=not p1)
    return {**case, **result, "after": _rows(raw, table)}


NOW_TEXT = "2026-10-21 08:15:30.123456"
NOW_WHOLE_TEXT = "2026-10-21 08:15:30"

SPEC_ROWS = [
    spec_row(1, "b-spec", next_run_time=100),
    spec_row(2, "a-spec", next_run_time=200, spec_type="http_page_probe"),
    spec_row(3, "A-spec", next_run_time=0, interval_seconds=60),
    spec_row(4, "disabled", enabled=0),
    spec_row(5, "z-truthy-enabled", enabled=2, next_run_time=50),
    spec_row(
        6,
        "\u00e9-spec",
        next_run_time=150,
        last_run_at="not a datetime",
        last_status="ok",
    ),
    spec_row(
        7,
        "a spec",
        next_run_time=1000,
        last_run_at="2026-01-01T00:00:00+02:00",
        config='{"b": 1, "a": [1.50, "\\u00fc"]}',
        last_error="line one\nline two",
    ),
    spec_row(
        8,
        "_under",
        next_run_time=150,
        last_run_at="2026-10-21 08:15:30.5",
        last_status="critical",
        updated_at=20261021,
    ),
    spec_row(9, "a10", next_run_time=151),
    spec_row(10, "a9", next_run_time=149, interval_seconds=0),
]

CONDITION_ROWS = [
    condition_row(1, "disk-b"),
    condition_row(
        2,
        "disk-a",
        tags_filter='{"mountpoint": "/", "device": "sda1"}',
        warning_threshold=None,
        critical_threshold=3,
        stale_after_seconds=900,
        alert_when_stale=0,
        last_value=0.25,
        last_evaluated="2026-10-21 08:00:00",
        status="warning",
        message="No samples available in condition window",
        host_filter="macmini",
        service_filter="vector",
        subject_type_filter="host",
        subject_id_filter="macmini",
    ),
    condition_row(3, "disabled", enabled=0),
    condition_row(4, "truthy-alert-flag", alert_when_stale=2, enabled=2),
    condition_row(5, "text-alert-flag", alert_when_stale="1", operator="lt"),
    condition_row(6, "Upper", last_evaluated="garbage"),
]


def _spec_cases() -> list[dict[str, Any]]:
    base = {"table": SPEC_TABLE, "before": SPEC_ROWS}
    cases = [
        {
            **base,
            "name": "load all enabled specs ordered by name",
            "op": {"kind": "load_specs", "now": NOW_TEXT, "due_before": None},
        },
        {
            **base,
            "name": "load due specs",
            "op": {"kind": "load_specs", "now": NOW_TEXT, "due_before": 150},
        },
        {
            **base,
            "name": "load due specs at time zero",
            "op": {"kind": "load_specs", "now": NOW_TEXT, "due_before": 0},
        },
        {
            "table": SPEC_TABLE,
            "before": [spec_row(1, "only-disabled", enabled=0)],
            "name": "load with nothing enabled",
            "op": {"kind": "load_specs", "now": NOW_TEXT, "due_before": None},
        },
        {
            "table": SPEC_TABLE,
            "before": [
                spec_row(1, "fine"),
                spec_row(2, "out-of-range", last_run_at="2026-13-01 00:00:00"),
                spec_row(
                    3, "disabled-out-of-range", enabled=0, created_at="2026-1-32 0:0"
                ),
            ],
            "name": "an out-of-range stored datetime fails the whole load",
            "op": {"kind": "load_specs", "now": NOW_TEXT, "due_before": None},
        },
        {
            "table": SPEC_TABLE,
            "before": [
                spec_row(1, "fine"),
                spec_row(
                    2, "disabled-out-of-range", enabled=0, created_at="2026-1-32 0:0"
                ),
            ],
            "name": "a disabled row is never converted",
            "op": {"kind": "load_specs", "now": NOW_TEXT, "due_before": None},
        },
    ]
    update_before = [
        spec_row(1, "first", last_run_at="2026-10-21 08:10:00", last_status="ok"),
        spec_row(2, "second", last_error="old error"),
    ]
    for name, now, status, error, next_run in [
        ("update the run result", NOW_TEXT, "critical", "boom", 1_792_829_730),
        ("update with a whole-second now", NOW_WHOLE_TEXT, "ok", "", 1),
        (
            "update keeps the error text verbatim",
            NOW_TEXT,
            "warning",
            '\u00fcnicode "quoted"\nnext line\ttab',
            1_792_829_790,
        ),
    ]:
        cases.append(
            {
                "table": SPEC_TABLE,
                "before": update_before,
                "name": name,
                "op": {
                    "kind": "update_spec",
                    "now": now,
                    "id": 1,
                    "last_status": status,
                    "last_error": error,
                    "next_run_time": next_run,
                },
            }
        )
    cases.append(
        {
            "table": SPEC_TABLE,
            "before": update_before,
            "name": "update of a deleted spec affects no rows",
            "op": {
                "kind": "update_spec",
                "now": NOW_TEXT,
                "id": 1,
                "delete_first": True,
                "last_status": "ok",
                "last_error": "",
                "next_run_time": 5,
            },
        }
    )
    return cases


def _condition_cases() -> list[dict[str, Any]]:
    base = {"table": CONDITION_TABLE, "before": CONDITION_ROWS}
    cases = [
        {
            **base,
            "name": "load enabled conditions ordered by name",
            "op": {"kind": "load_conditions", "now": NOW_TEXT, "condition_id": None},
        },
        {
            **base,
            "name": "load one condition",
            "op": {"kind": "load_conditions", "now": NOW_TEXT, "condition_id": 2},
        },
        {
            **base,
            "name": "load a disabled condition by id",
            "op": {"kind": "load_conditions", "now": NOW_TEXT, "condition_id": 3},
        },
    ]
    update_before = [condition_row(1, "first"), condition_row(2, "second")]
    for name, now, status, message, last_value in [
        (
            "condition success",
            NOW_TEXT,
            "warning",
            "Value 0.85 >= warning threshold 0.8",
            0.85,
        ),
        (
            "condition success without a value",
            NOW_WHOLE_TEXT,
            "warning",
            "No samples",
            None,
        ),
        ("condition success with an integral value", NOW_TEXT, "critical", "x", 3.0),
    ]:
        cases.append(
            {
                "table": CONDITION_TABLE,
                "before": update_before,
                "name": name,
                "op": {
                    "kind": "update_condition_success",
                    "now": now,
                    "id": 2,
                    "status": status,
                    "message": message,
                    "last_value": last_value,
                },
            }
        )
    cases.append(
        {
            "table": CONDITION_TABLE,
            "before": [
                condition_row(1, "first"),
                condition_row(
                    2, "second", last_value=0.5, last_evaluated="2026-10-21 08:00:00"
                ),
            ],
            "name": "condition failure writes status and message only",
            "op": {
                "kind": "update_condition_failure",
                "now": NOW_TEXT,
                "id": 2,
                "status": "critical",
                "message": "evaluation error: Influx setting missing: INFLUX_URL is required",
            },
        }
    )
    for kind in ("update_condition_success", "update_condition_failure"):
        cases.append(
            {
                "table": CONDITION_TABLE,
                "before": update_before,
                "name": f"{kind} of a deleted condition affects no rows",
                "op": {
                    "kind": kind,
                    "now": NOW_TEXT,
                    "id": 1,
                    "delete_first": True,
                    "status": "critical",
                    "message": "evaluation error: x",
                    "last_value": 1.5,
                },
            }
        )
    return cases


def _heartbeat_cases() -> list[dict[str, Any]]:
    metrics = {
        "name": "metric_collectors",
        "status": "ok",
        "last_error": "",
        "details": {"total": 3, "failed": 0, "warning": 0, "ingested": 7, "skipped": 1},
        "success": True,
    }
    others = [heartbeat_row(1, "metric_ingest")]

    def case(
        title: str,
        before: list[dict[str, Any]],
        now: str = NOW_TEXT,
        **op: Any,
    ) -> dict[str, Any]:
        return {
            "table": HEARTBEAT_TABLE,
            "before": before,
            "name": title,
            "op": {"kind": "record_heartbeat", "now": now, **metrics, **op},
        }

    existing = [*others, heartbeat_row(2, "metric_collectors")]
    return [
        case("insert after a success", others),
        case(
            "insert after a failure has no last_success",
            others,
            status="critical",
            last_error="a: boom; b: \u00fcber\nzwei",
            success=False,
        ),
        case("insert with a whole-second now", [], now=NOW_WHOLE_TEXT),
        case("update after a success", existing),
        case(
            "update after a failure rewrites the loaded last_success",
            existing,
            status="warning",
            last_error="a: boom",
            success=False,
        ),
        case(
            "update rewrites an unparseable last_success as NULL",
            [*others, heartbeat_row(2, "metric_collectors", last_success="yesterday")],
            success=False,
        ),
        case(
            "update rewrites an INTEGER last_success through its text form",
            [*others, heartbeat_row(2, "metric_collectors", last_success=20261020)],
            success=False,
        ),
        case(
            "update rewrites an offset last_success in UTC",
            [
                *others,
                heartbeat_row(
                    2, "metric_collectors", last_success="2026-10-20T09:00:00.5+02:00"
                ),
            ],
            success=False,
        ),
        case(
            "update rewrites a T-separated last_success with the space form",
            [
                *others,
                heartbeat_row(
                    2, "metric_collectors", last_success="2026-10-20T07:00:00.000000"
                ),
            ],
            success=False,
        ),
        case(
            "update replaces a NULL last_success after a success",
            [*others, heartbeat_row(2, "metric_collectors", last_success=None)],
        ),
        case(
            "update keeps a NULL last_success after a failure",
            [*others, heartbeat_row(2, "metric_collectors", last_success=None)],
            success=False,
        ),
        case(
            "an out-of-range stored last_success fails the update",
            [
                *others,
                heartbeat_row(
                    2, "metric_collectors", last_success="2026-02-30 00:00:00"
                ),
            ],
        ),
        case(
            "an out-of-range stored updated_at fails the update",
            [
                *others,
                heartbeat_row(2, "metric_collectors", updated_at="2026-10-20 25:00"),
            ],
            success=False,
        ),
        case(
            "an unparseable stored updated_at is overwritten",
            [*others, heartbeat_row(2, "metric_collectors", updated_at="soon")],
        ),
        case(
            "a last_success that leaves the Python range in UTC fails the rewrite",
            [
                *others,
                heartbeat_row(
                    2, "metric_collectors", last_success="0001-01-01T00:30:00+01:00"
                ),
            ],
            success=False,
        ),
        case(
            "empty details",
            existing,
            name="condition_evaluator",
            details={},
            success=False,
            status="warning",
            last_error="1 condition(s) failed",
        ),
        case(
            "condition evaluator details",
            existing,
            name="condition_evaluator",
            details={"evaluated": 2, "failed": 0},
        ),
        case(
            "an invalid stored details document is replaced",
            [*others, heartbeat_row(2, "metric_collectors", details='"text"')],
        ),
    ]


def build_store_ops() -> dict[str, Any]:
    with temp_database() as raw:
        cases = [
            _run_case(raw, case)
            for case in [*_spec_cases(), *_condition_cases(), *_heartbeat_cases()]
        ]
    return {"now_default": NOW_TEXT, "cases": cases}


# ---------------------------------------------------------------------------
# Datetime codec corpus.

CURATED_DATETIMES = [
    "2026-10-21 08:15:30",
    "2026-10-21 08:15:30.123456",
    "2026-10-21 08:15:30.000001",
    "2026-10-21 08:15:30.000000",
    "2026-10-21T08:15:30",
    "2026-10-21T08:15:30Z",
    "2026-10-21T08:15:30+00:00",
    "2026-10-21T08:15:30-00:00",
    "2026-10-21T08:15:30+02:00",
    "2026-10-21T08:15:30.5-05:30",
    "2026-10-21 08:15:30+0530",
    "2026-10-21 08:15:30+05",
    "2026-10-21 08:15:30 +05:00",
    "2026-10-21 08:15:30  Z",
    "2026-10-21 08:15:30\n",
    "2026-10-21 08:15:30Z\n",
    "2026-10-21 08:15:30\n\n",
    "2026-10-21 08:15:30 \t",
    "2026-10-21 08:15:30\x1c",
    "2026-10-21 08:15:30\u3000",
    "2026-10-21 08:15:30,5",
    "2026-10-21 08:15:30.1234567",
    "2026-10-21 08:15:30.123456789012",
    "2026-10-21 08:15:30.1234567890123",
    "2026-10-21 08:15",
    "2026-10-21 08",
    "2026-10-21",
    "2026-10-21T",
    "20261021",
    "20261021T081530",
    "20261021T0815",
    "2026-W43-3",
    "2026W433",
    "2026-W43",
    "2026W43",
    "2026W43T08",
    "2026W433T08:15",
    "2026-W43-3T08:15:30",
    "2026-W01-1",
    "2026-W53-1",
    "2020-W53-7",
    "2026-W00-1",
    "2026-W43-0",
    "2026-W43-8",
    "9999-W52-6",
    "0001-W01-1",
    "2026-W01-0000",
    "2026-W01-",
    "2026-294",
    "2026-1-1 1:2:3",
    "2026-1-1 1:2",
    "2026-01-01 1:2:3.4",
    "2026-1-1T1:2:3Z",
    "2026-1-1 1:2:3+02",
    "2026-1-1 1:2:3+0230",
    "2026-1-1 1:2:3+02:30\n",
    "2026-1-1 1:2:3 Z",
    "2026-1-1 1:2:3+24:00",
    "2026-1-1 1:2:3-24:00",
    "2026-1-1 1:2:3+23:59",
    "2026-1-1 1:2:3+99:99",
    "2026-13-01 00:00:00",
    "2026-00-10 00:00:00",
    "2026-02-29 00:00:00",
    "2024-02-29 00:00:00",
    "2026-02-30 00:00",
    "2026-1-32 0:0",
    "0000-01-01 00:00:00",
    "0001-01-01 00:00:00",
    "0001-01-01T00:30:00+01:00",
    "9999-12-31 23:59:59.999999",
    "9999-12-31T23:59:59-01:00",
    "2026-10-21 24:00:00",
    "2026-10-21 23:60:00",
    "2026-10-21 23:59:60",
    "2026-10-21 24:00",
    "2026-10-21 99:99",
    "2026-10-21T24:00:00",
    "2026-10-21T08:15:30:12",
    "2026-10-21T081530.5",
    "2026-10-21T08:15:30.",
    "2026-10-21T08:15:30.12a",
    "2026-10-21T08:1530",
    "2026-10-21T08:15:3",
    "2026-10-21T1230451",
    "2026-10-21T08:15:30+00:00:00.5",
    "2026-10-21T08:15:30+05:30:15",
    "2026-10-21T08:15:30+05:30:15.25",
    "2026-10-21T08:15:30-23:59:59.999999",
    "2026-10-21T08:15:30+0",
    "2026-10-21T08:15:30+",
    "2026-10-21T08:15:30Zx",
    "2026-10-21T08:15:30z",
    "2026-10-21T08:15:30 Z",
    "2026-10-21x08:15:30",
    "2026-10-21\u00e908:15:30",
    "2026-10-21\u20ac08:15:30",
    "2026-10-21\U0001f60008:15:30",
    "2026-10-21\x0008:15:30",
    "2026-10-21 08:15:30\x00",
    "2026-10-2108:15:30",
    "2026/10/21 08:15:30",
    "\uff12\uff10\uff12\uff16-10-21 08:15:30",
    "2026-10-21 \uff10\uff18:15:30",
    " 2026-10-21 08:15:30",
    "",
    "2026",
    "2026-10",
    "202610",
    "2026-W4",
    "garbage",
    "20261021",
    "1760000000",
    "1.5",
]

_DATE_PIECES = [
    "2026-10-21",
    "20261021",
    "2026-W43-3",
    "2026W433",
    "2026-W43",
    "2026W43",
    "2024-02-29",
    "2026-02-29",
    "2026-1-1",
    "2026-01-1",
    "0000-01-01",
    "9999-12-31",
    "2026-13-01",
    "2020-W53-7",
    "2026-10-2",
    "2026-10",
]
_SEPARATORS = [
    " ",
    "T",
    "t",
    "x",
    "\u00e9",
    "\u20ac",
    "\U0001f600",
    "_",
    "",
    "-",
    "1",
    "\n",
]
_TIMES = [
    "08:15:30",
    "08:15:30.123456",
    "08:15:30.1",
    "08:15:30,5",
    "08:15",
    "08",
    "081530",
    "0815",
    "08:15:30.1234567",
    "8:15:30",
    "08:5:30",
    "24:00:00",
    "23:59:60",
    "08:15:30:12",
    "081530.5",
    "08:15:30.",
    "8:1",
    "08:1530",
    "1:2:3.4",
    "",
]
_ZONES = [
    "",
    "",
    "Z",
    "z",
    "+00:00",
    "-00:00",
    "+02:00",
    "-05:30",
    "+0530",
    "+05",
    "+5",
    "+05:3",
    "+23:59",
    "+24:00",
    "-23:59:59.999999",
    "+00:00:00.5",
    "+05:30:15",
    " +02:00",
    "  Z",
    "+99:99",
    "-1",
]
_TAILS = ["", "", "", "\n", " ", "x", "\x00", "0"]

RANDOM_DATETIMES = 1500


def _random_datetimes() -> list[str]:
    rng = random.Random(20261021)
    seen = set(CURATED_DATETIMES)
    corpus = []
    while len(corpus) < RANDOM_DATETIMES:
        text = (
            rng.choice(_DATE_PIECES)
            + rng.choice(_SEPARATORS)
            + rng.choice(_TIMES)
            + rng.choice(_ZONES)
            + rng.choice(_TAILS)
        )
        if text not in seen:
            seen.add(text)
            corpus.append(text)
    return corpus


def _offset_micros(value: datetime) -> int:
    offset = value.utcoffset()
    assert offset is not None
    return (offset.days * 86400 + offset.seconds) * 1_000_000 + offset.microseconds


def _codec_case(text: str) -> dict[str, Any]:
    case: dict[str, Any] = {"text": text}
    try:
        # Python's sqlite3 converter receives the stored bytes.
        value = decoder(parse_datetime)(text.encode())
        value = connection.ops.convert_datetimefield_value(value, None, connection)
    except Exception as err:  # noqa: BLE001
        case["error"] = _error(err, p2=True)
        return case
    if value is None:
        case["loaded"] = None
        return case
    case["loaded"] = {
        "local": str(value.replace(tzinfo=None)),
        "offset_micros": _offset_micros(value),
    }
    try:
        case["stored"] = str(connection.ops.adapt_datetimefield_value(value))
    except Exception as err:  # noqa: BLE001
        case["stored_error"] = _error(err, p2=True)
    return case


def _format_cases() -> list[dict[str, Any]]:
    values = [
        NOW,
        NOW_WHOLE_SECOND,
        datetime(1, 1, 1, tzinfo=UTC),
        datetime(999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        datetime(9999, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        datetime(2026, 10, 21, 8, 15, 30, 1, tzinfo=UTC),
        datetime(2026, 10, 21, 8, 15, 30, 100000, tzinfo=UTC),
        NOW + timedelta(days=365),
    ]
    return [
        {
            "timestamp_micros": (value - datetime(1970, 1, 1, tzinfo=UTC))
            // timedelta(microseconds=1),
            "stored": str(connection.ops.adapt_datetimefield_value(value)),
        }
        for value in values
    ]


def build_datetime_codec() -> dict[str, Any]:
    return {
        "parse": [
            _codec_case(text) for text in CURATED_DATETIMES + _random_datetimes()
        ],
        "format": _format_cases(),
    }


GENERATORS = {
    "datetime_codec.json": build_datetime_codec,
    "store_ops.json": build_store_ops,
}
