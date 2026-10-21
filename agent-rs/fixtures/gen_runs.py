"""Fixture ``runs.json`` (slice S8): the metrics and conditions runs.

Pins ``services.run_metric_collection_specs_once`` and
``services.evaluate_conditions_once`` (PLAN.md 3.4, 3.5): spec selection and
order, executor isolation, the per-spec row update, the run counters, the
error list and the ``metric_collectors``/``condition_evaluator`` heartbeats,
and the run aborts (no matching specs, a row deleted during the run).

The executors are stubbed: every spec type maps to one function that plays
the spec's script by name (an outcome tuple, or an exception it raises, and
optionally a row it deletes first). The condition samples come from a stub of
``influx.query_condition_window`` scripted the same way; the evaluation
itself is the real ``evaluate_condition``. Each scenario seeds a temporary
migrated database (``gen_store.temp_database``), runs the operation with the
clocks fixed to ``now`` and records every row of the three tables afterwards.
"""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest import mock

from graphyard import influx, services
from graphyard.spec_config import SpecConfigError


def _load_store_builder() -> ModuleType:
    path = Path(__file__).resolve().parent / "gen_store.py"
    spec = importlib.util.spec_from_file_location("agent_rs_gen_runs_store", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_store = _load_store_builder()
spec_row = _store.spec_row
condition_row = _store.condition_row
heartbeat_row = _store.heartbeat_row

NOW = "2026-10-21T08:15:30.123456+00:00"
NOW_TS = 1792570530
SPEC_TABLE = "metric_collection_spec"
CONDITION_TABLE = "condition_definition"
HEARTBEAT_TABLE = "pipeline_heartbeat"
TABLES = (SPEC_TABLE, CONDITION_TABLE, HEARTBEAT_TABLE)

_EXCEPTIONS = {
    "SpecConfigError": SpecConfigError,
    "RuntimeError": RuntimeError,
    "ValueError": ValueError,
    "InfluxConfigurationError": influx.InfluxConfigurationError,
}


def _raise(script: dict[str, Any]) -> None:
    raised = script["raise"]
    raise _EXCEPTIONS[raised["class"]](raised["message"])


def _delete(raw: Any, script: dict[str, Any]) -> None:
    if "delete" in script:
        table, name = script["delete"]
        raw.execute(f'DELETE FROM "{table}" WHERE name = ?', [name])


def _spec_executor(raw: Any, scripts: dict[str, Any]) -> Any:
    def execute(spec: Any) -> tuple[str, int, int, str]:
        script = scripts[spec.name]
        _delete(raw, script)
        if "raise" in script:
            _raise(script)
        status, ingested, skipped, message = script["outcome"]
        return status, ingested, skipped, message

    return execute


def _sample(sample: dict[str, Any]) -> influx.MetricSample:
    return influx.MetricSample(
        ts=datetime.fromisoformat(sample["ts"]),
        value=sample["value"],
        host=sample["host"],
        metric=sample["metric"],
        service=None,
    )


def _query_stub(raw: Any, scripts: dict[str, Any]) -> Any:
    def query(condition: Any, *, now: datetime) -> list[influx.MetricSample]:
        assert now == datetime.fromisoformat(NOW)
        script = scripts[condition.name]
        _delete(raw, script)
        if "raise" in script:
            _raise(script)
        return [_sample(sample) for sample in script["samples"]]

    return query


class _FixedDatetime(datetime):
    @classmethod
    def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
        assert tz is UTC
        return datetime.fromisoformat(NOW)


def _run(raw: Any, scenario: dict[str, Any]) -> dict[str, Any]:
    for table in TABLES:
        _store._seed(raw, table, scenario["seed"].get(table, []))
    now = datetime.fromisoformat(NOW)
    scripts = scenario["scripts"]
    result: dict[str, Any] = {}
    with (
        mock.patch("django.utils.timezone.now", return_value=now),
        mock.patch.object(services, "datetime", _FixedDatetime),
        mock.patch.dict(
            services._SPEC_EXECUTORS,
            {kind: _spec_executor(raw, scripts) for kind in services._SPEC_EXECUTORS},
        ),
        mock.patch.object(influx, "query_condition_window", _query_stub(raw, scripts)),
    ):
        try:
            if scenario["kind"] == "metrics":
                run = services.run_metric_collection_specs_once(
                    due_only=scenario["due_only"]
                )
                result["result"] = {
                    "total": run.total,
                    "failed": run.failed,
                    "warning": run.warning,
                    "ingested": run.ingested,
                    "skipped": run.skipped,
                }
            else:
                run = services.evaluate_conditions_once()
                result["result"] = {"total": run.total, "failed": run.failed}
        except Exception as err:  # noqa: BLE001
            result["error"] = {"class": type(err).__name__, "message": str(err)}
    after = {table: _store._rows(raw, table) for table in TABLES}
    return {**scenario, **result, "after": after}


def _ok(ingested: int = 1, skipped: int = 0) -> dict[str, Any]:
    return {"outcome": ["ok", ingested, skipped, ""]}


def _metric_scenarios() -> list[dict[str, Any]]:
    due = {"next_run_time": NOW_TS}
    later = {"next_run_time": NOW_TS + 1}
    return [
        {
            # Order by name (binary collation), isolation of every failure
            # kind, the due filter, a truthy `enabled = 2`, interval 0.
            "name": "mixed_due_run",
            "kind": "metrics",
            "due_only": True,
            "seed": {
                SPEC_TABLE: [
                    spec_row(1, "c ok", **due),
                    spec_row(2, "a bad timeout", **due),
                    spec_row(3, "b crashing", **due),
                    spec_row(4, "d warning", **due),
                    spec_row(5, "e quiet warning", **due),
                    spec_row(6, "f unknown", spec_type="legacy_type", **due),
                    spec_row(7, "g disabled", enabled=0, **due),
                    spec_row(8, "h not due", **later),
                    spec_row(9, "i interval zero", interval_seconds=0, **due),
                    spec_row(10, "Z upper", **due),
                    spec_row(11, "j truthy enabled", enabled=2, **due),
                ],
                HEARTBEAT_TABLE: [heartbeat_row(1, "metric_collectors")],
            },
            "scripts": {
                "c ok": _ok(2, 1),
                "a bad timeout": {
                    "raise": {
                        "class": "SpecConfigError",
                        "message": "config.request_timeout_seconds must be a positive number",
                    }
                },
                "b crashing": {
                    "raise": {"class": "RuntimeError", "message": "collector bug"}
                },
                "d warning": {"outcome": ["warning", 0, 3, "no numeric values found"]},
                "e quiet warning": {"outcome": ["warning", 1, 0, ""]},
                "i interval zero": _ok(),
                "Z upper": _ok(),
                "j truthy enabled": _ok(4),
            },
        },
        {
            "name": "all_failed_keeps_last_success",
            "kind": "metrics",
            "due_only": False,
            "seed": {
                SPEC_TABLE: [spec_row(1, "one"), spec_row(2, "two", **later)],
                HEARTBEAT_TABLE: [heartbeat_row(4, "metric_collectors")],
            },
            "scripts": {
                "one": {"outcome": ["critical", 0, 0, "HTTP 500"]},
                "two": {"raise": {"class": "ValueError", "message": "bad\nvalue"}},
            },
        },
        {
            # Only the first five messages reach the heartbeat.
            "name": "error_list_is_capped_at_five",
            "kind": "metrics",
            "due_only": True,
            "seed": {
                SPEC_TABLE: [
                    spec_row(index, f"spec {index}", **due) for index in range(1, 9)
                ],
            },
            "scripts": {
                **{
                    f"spec {index}": {"outcome": ["critical", 0, 0, f"failure {index}"]}
                    for index in range(1, 5)
                },
                "spec 5": {"outcome": ["warning", 0, 1, "warned 5"]},
                "spec 6": {"outcome": ["warning", 2, 0, "warned 6"]},
                "spec 7": {"outcome": ["critical", 0, 0, "failure 7"]},
                "spec 8": _ok(5, 2),
            },
        },
        {
            "name": "nothing_due_writes_nothing",
            "kind": "metrics",
            "due_only": True,
            "seed": {SPEC_TABLE: [spec_row(1, "later", **later)]},
            "scripts": {},
        },
        {
            "name": "run_once_without_enabled_specs_fails",
            "kind": "metrics",
            "due_only": False,
            "seed": {SPEC_TABLE: [spec_row(1, "off", enabled=0)]},
            "scripts": {},
        },
        {
            # A spec deleted after the load: its save matches no row and the
            # run aborts without a heartbeat (the earlier rows stay written).
            "name": "spec_deleted_during_run_aborts",
            "kind": "metrics",
            "due_only": False,
            "seed": {
                SPEC_TABLE: [spec_row(1, "a first"), spec_row(2, "b second")],
            },
            "scripts": {
                "a first": {**_ok(), "delete": [SPEC_TABLE, "b second"]},
                "b second": _ok(),
            },
        },
    ]


def _samples(value: float, *times: str) -> list[dict[str, Any]]:
    return [
        {
            "ts": f"2026-10-21T{time}+00:00",
            "value": value,
            "host": "macmini",
            "metric": "host.filesystem_used_ratio",
        }
        for time in times or ("08:14:30",)
    ]


def _condition_scenarios() -> list[dict[str, Any]]:
    return [
        {
            "name": "mixed_conditions",
            "kind": "conditions",
            "seed": {
                CONDITION_TABLE: [
                    condition_row(1, "c ok", status="warning"),
                    condition_row(2, "b query fails"),
                    condition_row(3, "a no samples"),
                    condition_row(4, "d critical"),
                    condition_row(5, "e disabled", enabled=0),
                    condition_row(6, "f intermittent", alert_when_stale=0),
                ],
                HEARTBEAT_TABLE: [heartbeat_row(2, "condition_evaluator")],
            },
            "scripts": {
                "c ok": {"samples": _samples(0.5)},
                "b query fails": {
                    "raise": {
                        "class": "InfluxConfigurationError",
                        "message": "Influx settings missing: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG are required",
                    }
                },
                "a no samples": {"samples": []},
                "d critical": {"samples": _samples(0.95, "08:09:00", "08:14:30")},
                "f intermittent": {"samples": []},
            },
        },
        {
            "name": "all_conditions_ok_inserts_heartbeat",
            "kind": "conditions",
            "seed": {CONDITION_TABLE: [condition_row(1, "only")]},
            "scripts": {"only": {"samples": _samples(0.1)}},
        },
        {
            "name": "no_conditions",
            "kind": "conditions",
            "seed": {},
            "scripts": {},
        },
        {
            # The deleted condition's success save and then its failure save
            # match no row: the run aborts without a heartbeat.
            "name": "condition_deleted_during_run_aborts",
            "kind": "conditions",
            "seed": {
                CONDITION_TABLE: [
                    condition_row(1, "a first"),
                    condition_row(2, "b second"),
                ],
                HEARTBEAT_TABLE: [heartbeat_row(1, "condition_evaluator")],
            },
            "scripts": {
                "a first": {
                    "samples": _samples(0.2),
                    "delete": [CONDITION_TABLE, "b second"],
                },
                "b second": {"samples": _samples(0.2)},
            },
        },
    ]


def build_runs() -> dict[str, Any]:
    scenarios = []
    with _store.temp_database() as raw:
        for scenario in [*_metric_scenarios(), *_condition_scenarios()]:
            scenarios.append(_run(raw, scenario))
    return {"now": NOW, "now_ts": NOW_TS, "scenarios": scenarios}


GENERATORS = {"runs.json": build_runs}
