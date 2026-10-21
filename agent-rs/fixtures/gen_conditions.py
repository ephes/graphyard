"""Fixture ``conditions.json``: condition evaluation (slice S5).

Pins ``graphyard-core::conditions`` (agent-rs/PLAN.md section 7) against
``services.evaluate_condition`` and its helpers (``_series_key``,
``_group_samples_by_series``, ``_series_labels``, ``_evaluate_series``,
``_is_breached_for_duration``, ``ConditionDefinition.stale_limit_seconds``).

Every case runs the real ``evaluate_condition`` on an unsaved
``ConditionDefinition`` with ``influx.query_condition_window`` stubbed to
return the case's samples in the listed order (the stub records the ``now``
it was called with) and ``settings.CONDITION_DATA_STALE_WARNING_SECONDS``
overridden. Cases named ``test_*`` reproduce the scenarios of
``tests/test_condition_evaluation.py`` (TESTMAP.md, S5 rows); the others
cover the branches and corner cases those tests leave open.

Encoding: datetimes as ``isoformat()``, floats as ``repr`` text (``nan`` and
``inf`` included), thresholds as the floats Django reads from the ``REAL``
columns. A result is ``{"status", "message", "last_value", "evaluated_at"}``;
an exception is ``{"class", "message"}``, or ``{"class", "p2": true}`` for
library-derived (P2) texts. ``total_seconds`` pins ``int()`` and ``repr()``
of ``timedelta.total_seconds()``, which the staleness and anchor checks use.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

from django.test import override_settings

from graphyard import influx, services
from graphyard.models import ComparisonOperator, ConditionDefinition

NOW = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
DEFAULT_STALE = 600

# Exceptions whose text comes from CPython itself rather than graphyard.
_P2_CLASSES = {"OverflowError"}


def _condition(
    *,
    warning_threshold: float | None = 60.0,
    critical_threshold: float | None = 75.0,
    breach_minutes: int = 5,
    metric_name: str = "ha.sensor.office_humidity",
    operator: str = ComparisonOperator.GT,
    stale_after_seconds: int | None = None,
    alert_when_stale: bool = True,
) -> ConditionDefinition:
    return ConditionDefinition(
        name="test-condition",
        enabled=True,
        metric_name=metric_name,
        host_filter="",
        service_filter="",
        tags_filter={},
        operator=operator,
        warning_threshold=warning_threshold,
        critical_threshold=critical_threshold,
        window_minutes=30,
        breach_minutes=breach_minutes,
        stale_after_seconds=stale_after_seconds,
        alert_when_stale=alert_when_stale,
    )


def _disk_condition(**overrides: Any) -> ConditionDefinition:
    values: dict[str, Any] = {
        "warning_threshold": 0.80,
        "critical_threshold": 0.90,
        "metric_name": "host.filesystem_used_ratio",
        "operator": ComparisonOperator.GTE,
    }
    values.update(overrides)
    return _condition(**values)


def _sample_at(
    now: datetime, *, seconds_ago: float, value: float, **fields: Any
) -> influx.MetricSample:
    values: dict[str, Any] = {
        "host": "homeassistant",
        "metric": "ha.sensor.office_humidity",
        "service": "homeassistant",
        "tags": {},
    }
    values.update(fields)
    return influx.MetricSample(
        ts=now - timedelta(seconds=seconds_ago), value=value, **values
    )


def _sample(now: datetime, *, minutes_ago: int, value: float) -> influx.MetricSample:
    return _sample_at(now, seconds_ago=60 * minutes_ago, value=value)


def _series_sample(
    now: datetime, *, minutes_ago: int, value: float, mountpoint: str
) -> influx.MetricSample:
    return influx.MetricSample(
        ts=now - timedelta(minutes=minutes_ago),
        value=value,
        host="macmini",
        metric="host.filesystem_used_ratio",
        service=None,
        subject_type="host",
        subject_id="macmini",
        tags={"mountpoint": mountpoint},
    )


def _by_time(samples: list[influx.MetricSample]) -> list[influx.MetricSample]:
    return sorted(samples, key=lambda item: item.ts)


def _flux_order(*series: list[influx.MetricSample]) -> list[influx.MetricSample]:
    """v2/Flux: each table sorted by time, tables concatenated."""
    return [sample for table in series for sample in _by_time(table)]


def _global_order(*series: list[influx.MetricSample]) -> list[influx.MetricSample]:
    """v3/SQL: one result sorted globally by time."""
    return _by_time([sample for table in series for sample in table])


def _five_minute_series(
    now: datetime, *, offset_seconds: float, count: int, value: float
) -> list[influx.MetricSample]:
    return [
        _sample_at(now, seconds_ago=offset_seconds + 300 * i, value=value)
        for i in reversed(range(count))
    ]


# --- encoding -----------------------------------------------------------------


def _float(value: float | None) -> str | None:
    return None if value is None else repr(float(value))


def _encode_condition(condition: ConditionDefinition) -> dict[str, Any]:
    return {
        "metric_name": condition.metric_name,
        "operator": condition.operator,
        "warning_threshold": _float(condition.warning_threshold),
        "critical_threshold": _float(condition.critical_threshold),
        "breach_minutes": condition.breach_minutes,
        "stale_after_seconds": condition.stale_after_seconds,
        "alert_when_stale": condition.alert_when_stale,
    }


def _encode_sample(sample: influx.MetricSample) -> dict[str, Any]:
    return {
        "ts": sample.ts.isoformat(),
        "value": repr(sample.value),
        "host": sample.host,
        "metric": sample.metric,
        "service": sample.service,
        "subject_type": sample.subject_type,
        "subject_id": sample.subject_id,
        "source_system": sample.source_system,
        "source_instance": sample.source_instance,
        "source_entity_id": sample.source_entity_id,
        "collector_service": sample.collector_service,
        "collector_host": sample.collector_host,
        "tags": dict(sample.tags),
    }


def _encode_error(exc: BaseException) -> dict[str, Any]:
    name = type(exc).__name__
    if name in _P2_CLASSES:
        return {"class": name, "p2": True}
    return {"class": name, "message": str(exc)}


def _case(
    name: str,
    condition: ConditionDefinition,
    samples: list[influx.MetricSample],
    *,
    now: datetime = NOW,
    stale_warning_seconds: int = DEFAULT_STALE,
    query_error: Exception | None = None,
) -> dict[str, Any]:
    # Thresholds come from REAL columns: Django hands the evaluation floats.
    for field in ("warning_threshold", "critical_threshold"):
        value = getattr(condition, field)
        if value is not None:
            setattr(condition, field, float(value))
    seen: list[datetime] = []

    def stub(_condition: ConditionDefinition, *, now: datetime):
        seen.append(now)
        if query_error is not None:
            raise query_error
        return list(samples)

    case: dict[str, Any] = {
        "name": name,
        "condition": _encode_condition(condition),
        "stale_warning_seconds": stale_warning_seconds,
        "now": now.isoformat(),
        "samples": [_encode_sample(sample) for sample in samples],
    }
    if query_error is not None:
        case["query_error"] = str(query_error)
    with (
        override_settings(CONDITION_DATA_STALE_WARNING_SECONDS=stale_warning_seconds),
        mock.patch.object(influx, "query_condition_window", stub),
    ):
        try:
            result = services.evaluate_condition(condition, now=now)
        except Exception as exc:  # noqa: BLE001 - the error is the fixture
            case["query_now"] = seen[0].isoformat()
            case["error"] = _encode_error(exc)
            return case
    case["query_now"] = seen[0].isoformat()
    case["result"] = {
        "status": str(result.status),
        "message": result.message,
        "last_value": _float(result.last_value),
        "evaluated_at": result.evaluated_at.isoformat(),
    }
    return case


# --- the Python tests ---------------------------------------------------------


def _python_test_cases() -> list[dict[str, Any]]:
    now = NOW
    cases = [
        _case("test_evaluate_condition_no_samples_returns_warning", _condition(), []),
        _case(
            "test_evaluate_condition_stale_data_returns_warning",
            _condition(),
            [_sample(now, minutes_ago=2, value=80.0)],
            stale_warning_seconds=60,
        ),
        _case(
            "test_evaluate_condition_critical_threshold_path",
            _condition(),
            _by_time(
                [_sample(now, minutes_ago=i, value=80.0) for i in [5, 4, 3, 2, 1, 0]]
            ),
        ),
        _case(
            "test_evaluate_condition_warning_threshold_path",
            _condition(),
            _by_time(
                [_sample(now, minutes_ago=i, value=65.0) for i in [5, 4, 3, 2, 1, 0]]
            ),
        ),
        _case(
            "test_evaluate_condition_grace_window_prevents_false_breach",
            _condition(critical_threshold=None),
            _by_time([_sample(now, minutes_ago=i, value=80.0) for i in [3, 2, 1, 0]]),
        ),
    ]

    def full_and_healthy():
        full = [
            _series_sample(now, minutes_ago=i, value=0.95, mountpoint="/data")
            for i in [6, 5, 4, 3, 2, 1, 0]
        ]
        healthy = [
            _series_sample(now, minutes_ago=i, value=0.40, mountpoint="/")
            for i in [6, 5, 4, 3, 2, 1, 0]
        ]
        return full, healthy

    full, healthy = full_and_healthy()
    for suffix, ordered in (
        ("flux_full_first", _flux_order(full, healthy)),
        ("flux_healthy_first", _flux_order(healthy, full)),
        ("global", _global_order(full, healthy)),
    ):
        cases.append(
            _case(
                f"test_one_breaching_series_next_to_healthy_one_is_critical[{suffix}]",
                _disk_condition(),
                ordered,
            )
        )

    fresh = [
        _series_sample(now, minutes_ago=i, value=0.40, mountpoint="/")
        for i in [3, 2, 1, 0]
    ]
    stale = [
        _series_sample(now, minutes_ago=i, value=0.50, mountpoint="/backup")
        for i in [20, 15, 10]
    ]
    for suffix, ordered in (
        ("flux_stale_first", _flux_order(stale, fresh)),
        ("flux_fresh_first", _flux_order(fresh, stale)),
        ("global", _global_order(stale, fresh)),
    ):
        cases.append(
            _case(
                f"test_stale_series_next_to_fresh_one_is_warning[{suffix}]",
                _disk_condition(),
                ordered,
                stale_warning_seconds=120,
            )
        )

    first = [
        _series_sample(now, minutes_ago=i, value=0.30, mountpoint="/")
        for i in [3, 2, 1]
    ]
    second = [
        _series_sample(now, minutes_ago=i, value=0.20, mountpoint="/data")
        for i in [3, 2, 1, 0]
    ]
    cases.append(
        _case(
            "test_healthy_multi_series_reports_newest_value",
            _disk_condition(),
            _flux_order(second, first),
        )
    )

    def collector_sample(minutes_ago: int, value: float, collector: str):
        return _sample_at(
            now, seconds_ago=60 * minutes_ago, value=value, collector_host=collector
        )

    cases.append(
        _case(
            "test_series_split_by_collector_dimension",
            _condition(),
            _global_order(
                [collector_sample(i, 80.0, "macmini") for i in [5, 4, 3, 2, 1, 0]],
                [collector_sample(i, 40.0, "pi") for i in [5, 4, 3, 2, 1, 0]],
            ),
        )
    )

    for offset in [0, 30, 60, 90, 120, 150, 180, 210, 240, 270]:
        cases.append(
            _case(
                f"test_five_minute_series_breaches_for_every_phase_offset[{offset}]",
                _condition(critical_threshold=None, breach_minutes=10),
                _five_minute_series(now, offset_seconds=offset, count=6, value=80.0),
            )
        )
    for offset in (120, 0, 210):
        cases.append(
            _case(
                f"test_five_minute_series_critical_for_every_evaluation[{offset}]",
                _condition(breach_minutes=10),
                _five_minute_series(now, offset_seconds=offset, count=3, value=80.0),
            )
        )
    dip = _five_minute_series(now, offset_seconds=120, count=4, value=80.0)
    dip[-2] = _sample_at(now, seconds_ago=420, value=50.0)
    cases.append(
        _case(
            "test_five_minute_series_with_one_in_window_sample_below_is_ok",
            _condition(critical_threshold=None, breach_minutes=10),
            _by_time(dip),
        )
    )
    cases.append(
        _case(
            "test_anchor_below_threshold_means_breach_not_held_yet",
            _condition(critical_threshold=None, breach_minutes=10),
            [
                _sample_at(now, seconds_ago=720, value=50.0),
                _sample_at(now, seconds_ago=420, value=80.0),
                _sample_at(now, seconds_ago=120, value=80.0),
            ],
        )
    )
    cases.append(
        _case(
            "test_series_younger_than_breach_window_is_not_breached",
            _condition(critical_threshold=None, breach_minutes=10),
            [
                _sample_at(now, seconds_ago=420, value=80.0),
                _sample_at(now, seconds_ago=120, value=80.0),
            ],
        )
    )
    cases.append(
        _case(
            "test_anchor_older_than_staleness_limit_is_not_carried_forward",
            _condition(critical_threshold=None, breach_minutes=10),
            [
                _sample_at(now, seconds_ago=600 + 660, value=80.0),
                _sample_at(now, seconds_ago=420, value=80.0),
                _sample_at(now, seconds_ago=120, value=80.0),
            ],
        )
    )
    cases.append(
        _case(
            "test_one_minute_series_with_anchor_still_breaches",
            _condition(critical_threshold=None),
            [_sample(now, minutes_ago=i, value=80.0) for i in range(12, -1, -1)],
        )
    )
    cases.append(
        _case(
            "test_sample_at_window_start_supersedes_the_anchor",
            _condition(critical_threshold=None, breach_minutes=10),
            [
                _sample(now, minutes_ago=15, value=50.0),
                _sample(now, minutes_ago=10, value=80.0),
                _sample(now, minutes_ago=5, value=80.0),
            ],
        )
    )
    for age in [30, 90, 150, 210, 270]:
        cases.append(
            _case(
                f"test_breach_shorter_than_sampling_interval_uses_the_anchor_alone[{age}]",
                _condition(breach_minutes=1),
                _five_minute_series(now, offset_seconds=age, count=4, value=80.0),
            )
        )
    below = _five_minute_series(now, offset_seconds=150, count=3, value=80.0)
    below[-1] = _sample_at(now, seconds_ago=150, value=50.0)
    cases.append(
        _case(
            "test_breach_shorter_than_sampling_interval_ok_when_anchor_is_below",
            _condition(critical_threshold=None, breach_minutes=1),
            _by_time(below),
        )
    )
    cases.append(
        _case(
            "test_stale_sample_within_allowance_is_evaluated",
            _condition(stale_after_seconds=3600),
            [_sample(now, minutes_ago=20, value=50.0)],
        )
    )
    cases.append(
        _case(
            "test_stale_sample_past_allowance_warns",
            _condition(stale_after_seconds=3600),
            [_sample(now, minutes_ago=61, value=50.0)],
        )
    )
    cases.append(
        _case(
            "test_intermittent_condition_reports_stale_data_as_ok",
            _condition(alert_when_stale=False),
            [_sample(now, minutes_ago=20, value=80.0)],
        )
    )
    cases.append(
        _case(
            "test_intermittent_condition_with_no_samples_is_ok",
            _condition(alert_when_stale=False),
            [],
        )
    )
    cases.append(
        _case(
            "test_intermittent_condition_still_alerts_on_fresh_breach",
            _condition(alert_when_stale=False),
            _by_time([_sample(now, minutes_ago=i, value=80.0) for i in range(6)]),
        )
    )
    cases.append(
        _case(
            "test_default_condition_staleness_is_unchanged",
            _condition(),
            [_sample(now, minutes_ago=11, value=50.0)],
        )
    )
    return cases


# --- branches and corner cases -------------------------------------------------


def _fresh(
    value: float, *, minutes: int = 5, **fields: Any
) -> list[influx.MetricSample]:
    """One sample per minute over ``minutes`` + 1 minutes, newest at ``NOW``."""
    return [
        _sample_at(NOW, seconds_ago=60 * i, value=value, **fields)
        for i in range(minutes, -1, -1)
    ]


def _operator_cases() -> list[dict[str, Any]]:
    cases = []
    for operator in (
        ComparisonOperator.GT,
        ComparisonOperator.GTE,
        ComparisonOperator.LT,
        ComparisonOperator.LTE,
    ):
        for value in (59.5, 60.0, 60.5, 75.0, float("nan")):
            cases.append(
                _case(
                    f"operator_{operator}_value_{value!r}",
                    _condition(operator=operator),
                    _fresh(value),
                )
            )
    cases.append(
        _case(
            "unknown_operator_raises_key_error_when_compared",
            _condition(operator="between"),
            _fresh(80.0),
        )
    )
    cases.append(
        _case(
            "unknown_operator_with_quote_raises_key_error",
            _condition(operator="it's"),
            _fresh(80.0),
        )
    )
    cases.append(
        _case(
            "unknown_operator_without_thresholds_is_never_compared",
            _condition(
                operator="between", warning_threshold=None, critical_threshold=None
            ),
            _fresh(80.0),
        )
    )
    cases.append(
        _case(
            "unknown_operator_on_stale_series_is_never_compared",
            _condition(operator="between"),
            [_sample(NOW, minutes_ago=20, value=80.0)],
        )
    )
    cases.append(
        _case(
            "unknown_operator_with_no_samples",
            _condition(operator="between"),
            [],
        )
    )
    return cases


def _threshold_cases() -> list[dict[str, Any]]:
    cases = [
        _case(
            "no_thresholds_is_ok",
            _condition(warning_threshold=None, critical_threshold=None),
            _fresh(80.0),
        ),
        _case(
            "critical_only_breached",
            _condition(warning_threshold=None),
            _fresh(80.0),
        ),
        _case(
            "critical_only_not_breached",
            _condition(warning_threshold=None),
            _fresh(70.0),
        ),
        _case(
            "critical_checked_before_warning",
            _condition(warning_threshold=90.0, critical_threshold=70.0),
            _fresh(95.0),
        ),
    ]
    for label, threshold in (
        ("repr_long", 0.1 + 0.2),
        ("repr_exponent", 1e16),
        ("repr_small", 1e-05),
        ("negative_zero", -0.0),
        ("infinity", float("inf")),
        ("negative_infinity", float("-inf")),
        ("integral", 75.0),
    ):
        cases.append(
            _case(
                f"threshold_message_{label}",
                _condition(
                    operator=ComparisonOperator.LTE,
                    warning_threshold=None,
                    critical_threshold=threshold,
                ),
                _fresh(float("-inf")),
            )
        )
    cases.append(
        _case(
            "nan_latest_value_is_reported",
            _condition(),
            [*_fresh(80.0)[:-1], _sample_at(NOW, seconds_ago=0, value=float("nan"))],
        )
    )
    cases.append(
        _case(
            "large_breach_minutes_in_message",
            _condition(breach_minutes=1234),
            _fresh(80.0, minutes=3),
        )
    )
    return cases


def _staleness_cases() -> list[dict[str, Any]]:
    def one(seconds_ago: float, value: float = 50.0) -> list[influx.MetricSample]:
        return [_sample_at(NOW, seconds_ago=seconds_ago, value=value)]

    return [
        _case("stale_exactly_at_limit_is_fresh", _condition(), one(600)),
        _case(
            "stale_fraction_below_next_second_is_fresh", _condition(), one(600.999999)
        ),
        _case("stale_one_second_over_limit", _condition(), one(601)),
        _case(
            "stale_allowance_zero_falls_back_to_global",
            _condition(stale_after_seconds=0),
            one(601),
        ),
        _case(
            "stale_allowance_huge",
            _condition(stale_after_seconds=2**62),
            one(10**9),
        ),
        _case(
            "stale_global_huge_integer",
            _condition(),
            one(10**9),
            stale_warning_seconds=10**30,
        ),
        _case(
            "stale_global_negative_flags_a_fresh_sample",
            _condition(),
            one(1),
            stale_warning_seconds=-5,
        ),
        _case(
            "future_sample_truncates_towards_zero",
            _condition(),
            one(-0.5),
            stale_warning_seconds=-1,
        ),
        _case(
            "future_sample_negative_age_message",
            _condition(),
            one(-90),
            stale_warning_seconds=-100,
        ),
        _case(
            "stale_intermittent_message_and_value",
            _condition(alert_when_stale=False),
            one(3600, 12.5),
        ),
        _case(
            "stale_age_rounds_like_total_seconds",
            _condition(),
            [
                influx.MetricSample(
                    ts=datetime(1, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
                    value=1.0,
                    host="h",
                    metric="ha.sensor.office_humidity",
                    service=None,
                )
            ],
        ),
        _case(
            "now_with_microseconds_is_kept",
            _condition(),
            [_sample_at(NOW, seconds_ago=30, value=50.0)],
            now=NOW.replace(microsecond=123456),
        ),
    ]


def _anchor_cases() -> list[dict[str, Any]]:
    cond = _condition(critical_threshold=None, breach_minutes=10)

    def at(seconds_ago: float, value: float = 80.0) -> influx.MetricSample:
        return _sample_at(NOW, seconds_ago=seconds_ago, value=value)

    return [
        _case(
            "anchor_exactly_at_carry_limit_is_used",
            cond,
            [at(600 + 600), at(420), at(120)],
        ),
        _case(
            "anchor_one_microsecond_past_carry_limit_is_dropped",
            cond,
            [at(600 + 600.000001), at(420), at(120)],
        ),
        _case(
            "anchor_carry_uses_the_global_setting_not_the_allowance",
            _condition(
                critical_threshold=None, breach_minutes=10, stale_after_seconds=3600
            ),
            [at(600 + 660), at(420), at(120)],
        ),
        _case(
            "anchor_carry_with_a_larger_global_setting",
            cond,
            [at(600 + 660), at(420), at(120)],
            stale_warning_seconds=900,
        ),
        _case(
            "anchor_carry_negative_global_setting_drops_every_anchor",
            _condition(
                critical_threshold=None, breach_minutes=10, stale_after_seconds=3600
            ),
            [at(601), at(420), at(120)],
            stale_warning_seconds=-1,
        ),
        _case(
            "anchor_alone_decides_without_window_samples",
            _condition(critical_threshold=None, breach_minutes=1),
            [at(300), at(150)],
        ),
        _case(
            "anchor_alone_too_old_is_not_breached",
            _condition(
                critical_threshold=None, breach_minutes=1, stale_after_seconds=3600
            ),
            [at(60 + 601)],
        ),
        _case(
            "grace_exactly_one_minute_after_window_start",
            cond,
            [at(540), at(120)],
        ),
        _case(
            "grace_one_microsecond_late",
            cond,
            [at(539.999999), at(120)],
        ),
        _case(
            "sample_at_window_start_without_anchor",
            cond,
            [at(600), at(120)],
        ),
        _case(
            "breach_minutes_zero_uses_samples_at_now",
            _condition(critical_threshold=None, breach_minutes=0),
            [at(60, 50.0), at(0)],
        ),
        _case(
            "breach_minutes_zero_anchor_only",
            _condition(critical_threshold=None, breach_minutes=0),
            [at(60)],
        ),
        _case(
            "duplicate_timestamps_keep_input_order",
            _condition(critical_threshold=None),
            [at(0, 50.0), *_fresh(80.0)[:-1], at(0, 81.0)],
        ),
        _case(
            "unsorted_series_is_sorted_by_time",
            _condition(),
            [at(0, 80.0), at(240), at(300), at(60), at(120), at(180)],
        ),
    ]


def _overflow_cases() -> list[dict[str, Any]]:
    fresh = _fresh(80.0, minutes=1)
    return [
        _case(
            "breach_minutes_beyond_timedelta_days",
            _condition(breach_minutes=1_440_000_000_000),
            fresh,
        ),
        _case(
            "breach_minutes_beyond_c_int_days",
            _condition(breach_minutes=1440 * 2**31),
            fresh,
        ),
        _case(
            "breach_minutes_largest_timedelta_leaves_the_date_range",
            _condition(breach_minutes=1_439_999_999_999),
            fresh,
        ),
        _case(
            "window_start_before_year_one",
            _condition(breach_minutes=1_200_000_000),
            fresh,
        ),
        _case(
            "window_start_far_in_the_past_is_fine",
            _condition(breach_minutes=1_000_000_000),
            fresh,
        ),
        _case(
            "grace_start_after_year_9999",
            _condition(breach_minutes=0),
            [
                influx.MetricSample(
                    ts=datetime(9999, 12, 31, 23, 59, 30, tzinfo=UTC),
                    value=80.0,
                    host="h",
                    metric="ha.sensor.office_humidity",
                    service=None,
                )
            ],
            now=datetime(9999, 12, 31, 23, 59, 30, tzinfo=UTC),
        ),
        _case(
            "overflow_not_reached_without_thresholds",
            _condition(
                breach_minutes=1_440_000_000_000,
                warning_threshold=None,
                critical_threshold=None,
            ),
            fresh,
        ),
    ]


def _series_cases() -> list[dict[str, Any]]:
    def s(seconds_ago: float, value: float, **fields: Any) -> influx.MetricSample:
        return _sample_at(NOW, seconds_ago=seconds_ago, value=value, **fields)

    def series(value: float, **fields: Any) -> list[influx.MetricSample]:
        return [s(60 * i, value, **fields) for i in range(5, -1, -1)]

    cases = [
        _case(
            "empty_dimension_and_none_are_one_series",
            _condition(),
            [
                *[s(60 * i, 80.0, service="") for i in range(5, 2, -1)],
                *[s(60 * i, 80.0, service=None) for i in range(2, -1, -1)],
            ],
        ),
        _case(
            "tag_does_not_override_a_dimension",
            _condition(),
            [
                *[
                    s(60 * i, 80.0, tags={"host": "other", "metric": "x"})
                    for i in range(5, 2, -1)
                ],
                *[s(60 * i, 80.0) for i in range(2, -1, -1)],
            ],
        ),
        _case(
            "tag_fills_a_missing_dimension",
            _condition(),
            [
                *[
                    s(60 * i, 80.0, service=None, tags={"service": "homeassistant"})
                    for i in range(5, 2, -1)
                ],
                *[s(60 * i, 80.0) for i in range(2, -1, -1)],
            ],
        ),
        _case(
            "empty_tag_value_splits_series_with_equal_labels",
            _condition(),
            [*series(80.0, tags={"foo": ""}), *series(90.0)],
        ),
        _case(
            "empty_tag_value_equal_labels_healthy_takes_first",
            _condition(),
            [*series(40.0, tags={"foo": ""}), *series(30.0)],
        ),
        _case(
            "ties_by_latest_sample_then_label",
            _condition(),
            [
                *series(80.0, collector_host="zeta"),
                *series(80.0, collector_host="alpha"),
                *[s(60 * i, 80.0, collector_host="mid") for i in range(6, 0, -1)],
            ],
        ),
        _case(
            "breached_tie_on_latest_takes_smallest_label",
            _condition(),
            [
                *series(80.0, collector_host="zeta"),
                *series(90.0, collector_host="alpha"),
            ],
        ),
        _case(
            "healthy_tie_on_latest_takes_greatest_label",
            _condition(),
            [
                *series(10.0, collector_host="alpha"),
                *series(20.0, collector_host="zeta"),
            ],
        ),
        _case(
            "three_series_worst_status_wins",
            _condition(),
            [
                *series(40.0, collector_host="a"),
                *series(65.0, collector_host="b"),
                *series(80.0, collector_host="c"),
            ],
        ),
        _case(
            "warning_series_beats_stale_warning_by_age",
            _condition(),
            [
                *series(65.0, collector_host="breach"),
                s(1200, 10.0, collector_host="stale"),
            ],
        ),
        _case(
            "intermittent_all_stale_reports_newest",
            _condition(alert_when_stale=False),
            [
                s(1200, 10.0, collector_host="a"),
                s(900, 20.0, collector_host="b"),
            ],
        ),
        _case(
            "intermittent_stale_next_to_fresh_ok",
            _condition(alert_when_stale=False),
            [s(1200, 10.0, collector_host="a"), *series(20.0, collector_host="b")],
        ),
        _case(
            "labels_name_only_the_varying_dimensions",
            _condition(),
            [
                *series(
                    80.0,
                    collector_host="x",
                    subject_type="host",
                    subject_id="m",
                    tags={"mountpoint": "/", "fs": "apfs"},
                ),
                *series(
                    40.0,
                    collector_host="y",
                    subject_type="host",
                    subject_id="m",
                    tags={"mountpoint": "/", "fs": "zfs"},
                ),
            ],
        ),
        _case(
            "labels_include_a_varying_metric",
            _condition(),
            [*series(80.0, metric="a.metric"), *series(40.0, metric="b.metric")],
        ),
        _case(
            "labels_use_every_dimension_and_tag",
            _condition(),
            [
                *series(
                    80.0,
                    service="svc",
                    subject_type="host",
                    subject_id="sid",
                    source_system="sys",
                    source_instance="inst",
                    source_entity_id="ent",
                    collector_service="cs",
                    collector_host="ch",
                    tags={"zz": "1", "aa": "2"},
                ),
                *series(40.0, host="other"),
            ],
        ),
        _case(
            "labels_sort_by_code_point",
            _condition(),
            [
                *series(80.0, tags={"name": "été"}),
                *series(80.0, tags={"name": "Zulu"}),
                *series(80.0, tags={"name": "alpha"}),
            ],
        ),
        _case(
            "query_order_of_series_does_not_matter_for_the_message",
            _condition(),
            [*series(80.0, tags={"b": "2"}), *series(80.0, tags={"a": "1"})],
        ),
    ]
    return cases


def _query_error_cases() -> list[dict[str, Any]]:
    return [
        _case(
            "query_error_propagates",
            _condition(),
            [],
            query_error=RuntimeError("Influx query failed: boom"),
        )
    ]


def _total_seconds() -> list[dict[str, Any]]:
    micros = [0, 1, -1, 500_000, -500_000, 999_999, -999_999, 1_000_000, 600_999_999]
    for base in (2**53 - 1, 2**53, 2**53 + 1, 2**60, 10**17, 3 * 10**17):
        for delta in (-1_000_001, -1, 0, 1, 999_999):
            micros.extend([base + delta, -(base + delta)])
    for seconds in (63_851_284_799, 10_292_961_599, 9_007_199_254):
        for remainder in (
            0,
            1,
            2,
            3,
            499_999,
            500_000,
            500_001,
            999_996,
            999_997,
            999_998,
            999_999,
        ):
            micros.append(seconds * 1_000_000 + remainder)
    result = []
    for value in dict.fromkeys(micros):
        seconds = timedelta(microseconds=value).total_seconds()
        result.append({"micros": value, "repr": repr(seconds), "int": int(seconds)})
    return result


def build_conditions() -> dict[str, Any]:
    return {
        "cases": [
            *_python_test_cases(),
            *_operator_cases(),
            *_threshold_cases(),
            *_staleness_cases(),
            *_anchor_cases(),
            *_overflow_cases(),
            *_series_cases(),
            *_query_error_cases(),
        ],
        "total_seconds": _total_seconds(),
    }


GENERATORS = {"conditions.json": build_conditions}
