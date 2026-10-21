"""Fixture ``model.json``: the shared value tables and result types (slice S1).

Pins what ``graphyard-core::model`` mirrors: the stored values of
``StatusLevel``, ``SubjectType``, ``MetricCollectionSpecType`` and
``ComparisonOperator``, the status severities, the spec types that have an
executor, ``_operator_fn`` on a few value pairs, and the fields and defaults
of the dataclasses the agent passes around.
"""

from __future__ import annotations

import dataclasses
from typing import Any

from graphyard import influx, services
from graphyard.models import (
    ComparisonOperator,
    MetricCollectionSpecType,
    StatusLevel,
    SubjectType,
)

COMPARISON_PAIRS = [
    (1.0, 2.0),
    (2.0, 2.0),
    (3.0, 2.0),
    (-0.0, 0.0),
    (0.1 + 0.2, 0.3),
    (5e-324, 0.0),
    (-1.7976931348623157e308, 1.7976931348623157e308),
]


def _values(choices: list[tuple[str, str]]) -> list[str]:
    return [value for value, _label in choices]


def _fields(cls: type) -> list[dict[str, Any]]:
    fields = []
    for field in dataclasses.fields(cls):
        if field.default is not dataclasses.MISSING:
            required, default = False, field.default
        elif field.default_factory is not dataclasses.MISSING:
            required, default = False, field.default_factory()
        else:
            required, default = True, None
        fields.append({"name": field.name, "required": required, "default": default})
    return fields


def _comparisons() -> list[dict[str, Any]]:
    cases = []
    for operator in _values(ComparisonOperator.CHOICES):
        compare = services._operator_fn(operator)
        for left, right in COMPARISON_PAIRS:
            cases.append(
                {
                    "operator": operator,
                    "left": left,
                    "right": right,
                    "result": compare(left, right),
                }
            )
    return cases


def _unknown_operator() -> dict[str, Any]:
    try:
        services._operator_fn("between")
    except KeyError as exc:
        return {"operator": "between", "class": type(exc).__name__, "message": str(exc)}
    raise AssertionError("_operator_fn accepted an unknown operator")


def build_model() -> dict[str, Any]:
    return {
        "status_levels": [
            {"value": value, "severity": services._STATUS_SEVERITY[value]}
            for value in _values(StatusLevel.CHOICES)
        ],
        "subject_types": _values(SubjectType.CHOICES),
        "subject_types_all_sorted": sorted(SubjectType.ALL),
        "spec_types": _values(MetricCollectionSpecType.CHOICES),
        "spec_executors": list(services._SPEC_EXECUTORS),
        "comparison_operators": _values(ComparisonOperator.CHOICES),
        "comparisons": _comparisons(),
        "unknown_operator": _unknown_operator(),
        "dataclasses": {
            "MetricPoint": _fields(influx.MetricPoint),
            "MetricSample": _fields(influx.MetricSample),
            "ConditionEvaluation": _fields(services.ConditionEvaluation),
            "ConditionEvaluationRun": _fields(services.ConditionEvaluationRun),
            "MetricCollectionSpecRun": _fields(services.MetricCollectionSpecRun),
        },
    }


GENERATORS = {"model.json": build_model}
