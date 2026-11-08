from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from django.core.exceptions import ValidationError

from graphyard import influx
from graphyard.models import (
    ComparisonOperator,
    ConditionDefinition,
    PipelineHeartbeat,
    StatusLevel,
)
from graphyard.services import (
    ConditionEvaluation,
    evaluate_condition,
    evaluate_conditions_once,
)


def _condition(
    *,
    warning_threshold: float | None = 60.0,
    critical_threshold: float | None = 75.0,
    breach_minutes: int = 5,
) -> ConditionDefinition:
    return ConditionDefinition(
        name="test-condition",
        enabled=True,
        metric_name="ha.sensor.office_humidity",
        host_filter="",
        service_filter="",
        tags_filter={},
        operator=ComparisonOperator.GT,
        warning_threshold=warning_threshold,
        critical_threshold=critical_threshold,
        window_minutes=30,
        breach_minutes=breach_minutes,
    )


def _sample(now: datetime, *, minutes_ago: int, value: float) -> influx.MetricSample:
    return influx.MetricSample(
        ts=now - timedelta(minutes=minutes_ago),
        value=value,
        host="homeassistant",
        metric="ha.sensor.office_humidity",
        service="homeassistant",
        tags={},
    )


def test_evaluate_condition_no_samples_returns_warning(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition()
    monkeypatch.setattr(
        "graphyard.services.influx.query_condition_window", lambda *a, **k: []
    )

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.WARNING
    assert result.last_value is None
    assert "No samples available" in result.message


def test_evaluate_condition_stale_data_returns_warning(monkeypatch, settings):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 60
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition()
    monkeypatch.setattr(
        "graphyard.services.influx.query_condition_window",
        lambda *a, **k: [_sample(now, minutes_ago=2, value=80.0)],
    )

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.WARNING
    assert "stale" in result.message.lower()
    assert result.last_value == 80.0


def test_evaluate_condition_critical_threshold_path(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=75.0, breach_minutes=5
    )
    samples = [_sample(now, minutes_ago=i, value=80.0) for i in [5, 4, 3, 2, 1, 0]]
    monkeypatch.setattr(
        "graphyard.services.influx.query_condition_window",
        lambda *a, **k: sorted(samples, key=lambda item: item.ts),
    )

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.CRITICAL
    assert result.last_value == 80.0


def test_evaluate_condition_warning_threshold_path(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=75.0, breach_minutes=5
    )
    samples = [_sample(now, minutes_ago=i, value=65.0) for i in [5, 4, 3, 2, 1, 0]]
    monkeypatch.setattr(
        "graphyard.services.influx.query_condition_window",
        lambda *a, **k: sorted(samples, key=lambda item: item.ts),
    )

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.WARNING
    assert result.last_value == 65.0


def test_evaluate_condition_grace_window_prevents_false_breach(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=None, breach_minutes=5
    )
    samples = [_sample(now, minutes_ago=i, value=80.0) for i in [3, 2, 1, 0]]
    monkeypatch.setattr(
        "graphyard.services.influx.query_condition_window",
        lambda *a, **k: sorted(samples, key=lambda item: item.ts),
    )

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.OK
    assert result.last_value == 80.0


def test_evaluate_conditions_once_updates_condition_and_heartbeat(db, monkeypatch):
    condition = ConditionDefinition.objects.create(
        name="saved-condition",
        enabled=True,
        metric_name="ha.sensor.office_humidity",
        host_filter="",
        service_filter="",
        tags_filter={},
        operator=ComparisonOperator.GT,
        warning_threshold=60.0,
        critical_threshold=75.0,
        window_minutes=30,
        breach_minutes=5,
    )
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(
        "graphyard.services.evaluate_condition",
        lambda *a, **k: ConditionEvaluation(
            status=StatusLevel.CRITICAL,
            message="test critical",
            last_value=88.0,
            evaluated_at=now,
        ),
    )

    monkeypatch.setattr("graphyard.services.timezone.now", lambda: now)
    run = evaluate_conditions_once(condition_id=condition.id)

    assert run.total == 1
    assert run.failed == 0

    condition.refresh_from_db()
    assert condition.status == StatusLevel.CRITICAL
    assert condition.message == "test critical"
    assert condition.last_value == 88.0
    assert condition.last_evaluated == now

    heartbeat = PipelineHeartbeat.objects.get(name="condition_evaluator")
    assert heartbeat.status == StatusLevel.OK
    assert heartbeat.last_success is not None


def _series_sample(
    now: datetime,
    *,
    minutes_ago: float,
    value: float,
    mountpoint: str,
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


def _disk_condition() -> ConditionDefinition:
    return ConditionDefinition(
        name="disk",
        enabled=True,
        metric_name="host.filesystem_used_ratio",
        host_filter="macmini",
        service_filter="",
        tags_filter={},
        operator=ComparisonOperator.GTE,
        warning_threshold=0.80,
        critical_threshold=0.90,
        window_minutes=30,
        breach_minutes=5,
    )


def _flux_order(*series: list[influx.MetricSample]) -> list[influx.MetricSample]:
    """Mimic v2/Flux: each table sorted by time, tables concatenated."""
    return [
        sample for table in series for sample in sorted(table, key=lambda item: item.ts)
    ]


def _global_order(*series: list[influx.MetricSample]) -> list[influx.MetricSample]:
    """Mimic v3/SQL: one result sorted globally by time."""
    return sorted(
        [sample for table in series for sample in table], key=lambda item: item.ts
    )


def _full_disk_and_healthy(now: datetime):
    full = [
        _series_sample(now, minutes_ago=i, value=0.95, mountpoint="/data")
        for i in [6, 5, 4, 3, 2, 1, 0]
    ]
    healthy = [
        _series_sample(now, minutes_ago=i, value=0.40, mountpoint="/")
        for i in [6, 5, 4, 3, 2, 1, 0]
    ]
    return full, healthy


def test_one_breaching_series_next_to_healthy_one_is_critical(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    full, healthy = _full_disk_and_healthy(now)
    for ordered in (
        _flux_order(full, healthy),
        _flux_order(healthy, full),
        _global_order(full, healthy),
    ):
        monkeypatch.setattr(
            "graphyard.services.influx.query_condition_window",
            lambda *a, _ordered=ordered, **k: list(_ordered),
        )

        result = evaluate_condition(_disk_condition(), now=now)

        assert result.status == StatusLevel.CRITICAL
        assert result.last_value == 0.95
        assert "mountpoint=/data" in result.message
        assert "1 of 2 series" in result.message


def test_stale_series_next_to_fresh_one_is_warning(monkeypatch, settings):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 120
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    fresh = [
        _series_sample(now, minutes_ago=i, value=0.40, mountpoint="/")
        for i in [3, 2, 1, 0]
    ]
    stale = [
        _series_sample(now, minutes_ago=i, value=0.50, mountpoint="/backup")
        for i in [20, 15, 10]
    ]
    # Previously, Flux table order decided the result: with the fresh series
    # last, the stale one was hidden. Every order must now warn.
    for ordered in (
        _flux_order(stale, fresh),
        _flux_order(fresh, stale),
        _global_order(stale, fresh),
    ):
        monkeypatch.setattr(
            "graphyard.services.influx.query_condition_window",
            lambda *a, _ordered=ordered, **k: list(_ordered),
        )

        result = evaluate_condition(_disk_condition(), now=now)

        assert result.status == StatusLevel.WARNING
        assert "stale" in result.message.lower()
        assert "mountpoint=/backup" in result.message
        assert result.last_value == 0.50


def test_healthy_multi_series_reports_newest_value(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    first = [
        _series_sample(now, minutes_ago=i, value=0.30, mountpoint="/")
        for i in [3, 2, 1]
    ]
    second = [
        _series_sample(now, minutes_ago=i, value=0.20, mountpoint="/data")
        for i in [3, 2, 1, 0]
    ]
    monkeypatch.setattr(
        "graphyard.services.influx.query_condition_window",
        lambda *a, **k: _flux_order(second, first),
    )

    result = evaluate_condition(_disk_condition(), now=now)

    assert result.status == StatusLevel.OK
    assert result.last_value == 0.20
    assert result.message == "Condition is within thresholds (2 series)"


def test_series_split_by_collector_dimension(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)

    def _collector_sample(minutes_ago: int, value: float, collector: str):
        return influx.MetricSample(
            ts=now - timedelta(minutes=minutes_ago),
            value=value,
            host="homeassistant",
            metric="ha.sensor.office_humidity",
            service="homeassistant",
            collector_host=collector,
            tags={},
        )

    breaching = [_collector_sample(i, 80.0, "macmini") for i in [5, 4, 3, 2, 1, 0]]
    healthy = [_collector_sample(i, 40.0, "pi") for i in [5, 4, 3, 2, 1, 0]]
    monkeypatch.setattr(
        "graphyard.services.influx.query_condition_window",
        lambda *a, **k: _global_order(breaching, healthy),
    )

    result = evaluate_condition(_condition(), now=now)

    assert result.status == StatusLevel.CRITICAL
    assert "collector_host=macmini" in result.message
    assert result.last_value == 80.0


# --- sparse series: carry the last sample before the window forward ----------


def _sample_at(
    now: datetime, *, seconds_ago: float, value: float
) -> influx.MetricSample:
    return influx.MetricSample(
        ts=now - timedelta(seconds=seconds_ago),
        value=value,
        host="homeassistant",
        metric="ha.sensor.office_humidity",
        service="homeassistant",
        tags={},
    )


def _five_minute_series(
    now: datetime, *, offset_seconds: float, count: int, value: float
) -> list[influx.MetricSample]:
    """Samples every 300 s, newest ``offset_seconds`` ago, oldest first."""
    return [
        _sample_at(now, seconds_ago=offset_seconds + 300 * i, value=value)
        for i in reversed(range(count))
    ]


def _patch_window(monkeypatch, samples: list[influx.MetricSample]) -> None:
    monkeypatch.setattr(
        "graphyard.services.influx.query_condition_window",
        lambda *a, **k: sorted(samples, key=lambda item: item.ts),
    )


@pytest.mark.parametrize(
    "offset_seconds", [0, 30, 60, 90, 120, 150, 180, 210, 240, 270]
)
def test_five_minute_series_breaches_for_every_phase_offset(
    monkeypatch, offset_seconds
):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=None, breach_minutes=10
    )
    _patch_window(
        monkeypatch,
        _five_minute_series(now, offset_seconds=offset_seconds, count=6, value=80.0),
    )

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.WARNING
    assert "for 10m" in result.message


def test_five_minute_series_critical_for_every_evaluation(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=75.0, breach_minutes=10
    )
    # Scout repro phases: -12/-7/-2, -10/-5/0, -13.5/-8.5/-3.5 minutes.
    for offset in (120, 0, 210):
        _patch_window(
            monkeypatch,
            _five_minute_series(now, offset_seconds=offset, count=3, value=80.0),
        )
        assert evaluate_condition(condition, now=now).status == StatusLevel.CRITICAL


def test_five_minute_series_with_one_in_window_sample_below_is_ok(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=None, breach_minutes=10
    )
    samples = _five_minute_series(now, offset_seconds=120, count=4, value=80.0)
    samples[-2] = _sample_at(now, seconds_ago=420, value=50.0)
    _patch_window(monkeypatch, samples)

    assert evaluate_condition(condition, now=now).status == StatusLevel.OK


def test_anchor_below_threshold_means_breach_not_held_yet(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=None, breach_minutes=10
    )
    # Anchor at -12 min is below; in-window samples at -7 and -2 are above.
    samples = [
        _sample_at(now, seconds_ago=720, value=50.0),
        _sample_at(now, seconds_ago=420, value=80.0),
        _sample_at(now, seconds_ago=120, value=80.0),
    ]
    _patch_window(monkeypatch, samples)

    assert evaluate_condition(condition, now=now).status == StatusLevel.OK


def test_series_younger_than_breach_window_is_not_breached(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=None, breach_minutes=10
    )
    # First sample ever at -7 min: no anchor, starts too late for the grace.
    samples = [
        _sample_at(now, seconds_ago=420, value=80.0),
        _sample_at(now, seconds_ago=120, value=80.0),
    ]
    _patch_window(monkeypatch, samples)

    assert evaluate_condition(condition, now=now).status == StatusLevel.OK


def test_anchor_older_than_staleness_limit_is_not_carried_forward(
    monkeypatch, settings
):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 600
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=None, breach_minutes=10
    )
    # Anchor 11 minutes before the window start (data gap), then fresh data
    # that starts too late for the one-minute grace.
    samples = [
        _sample_at(now, seconds_ago=600 + 660, value=80.0),
        _sample_at(now, seconds_ago=420, value=80.0),
        _sample_at(now, seconds_ago=120, value=80.0),
    ]
    _patch_window(monkeypatch, samples)

    assert evaluate_condition(condition, now=now).status == StatusLevel.OK


def test_one_minute_series_with_anchor_still_breaches(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=None, breach_minutes=5
    )
    samples = [_sample(now, minutes_ago=i, value=80.0) for i in range(12, -1, -1)]
    _patch_window(monkeypatch, samples)

    assert evaluate_condition(condition, now=now).status == StatusLevel.WARNING


def test_query_condition_window_looks_back_far_enough_for_the_anchor(
    monkeypatch, settings
):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 600
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    captured: dict[str, datetime] = {}

    def fake_query_range(metric, start, stop, **kwargs):
        captured["start"] = start
        return []

    monkeypatch.setattr("graphyard.influx.query_range", fake_query_range)

    condition = _condition(breach_minutes=10)
    condition.window_minutes = 10
    influx.query_condition_window(condition, now=now)
    assert captured["start"] == now - timedelta(minutes=20)

    condition.window_minutes = 30
    influx.query_condition_window(condition, now=now)
    assert captured["start"] == now - timedelta(minutes=30)


def test_sample_at_window_start_supersedes_the_anchor(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=None, breach_minutes=10
    )
    samples = [
        _sample(now, minutes_ago=15, value=50.0),
        _sample(now, minutes_ago=10, value=80.0),
        _sample(now, minutes_ago=5, value=80.0),
    ]
    _patch_window(monkeypatch, samples)

    assert evaluate_condition(condition, now=now).status == StatusLevel.WARNING


@pytest.mark.parametrize("age_seconds", [30, 90, 150, 210, 270])
def test_breach_shorter_than_sampling_interval_uses_the_anchor_alone(
    monkeypatch, age_seconds
):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=75.0, breach_minutes=1
    )
    _patch_window(
        monkeypatch,
        _five_minute_series(now, offset_seconds=age_seconds, count=4, value=80.0),
    )

    assert evaluate_condition(condition, now=now).status == StatusLevel.CRITICAL


def test_breach_shorter_than_sampling_interval_ok_when_anchor_is_below(
    monkeypatch,
):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=None, breach_minutes=1
    )
    samples = _five_minute_series(now, offset_seconds=150, count=3, value=80.0)
    samples[-1] = _sample_at(now, seconds_ago=150, value=50.0)
    _patch_window(monkeypatch, samples)

    assert evaluate_condition(condition, now=now).status == StatusLevel.OK


# --- per-condition staleness allowance ---------------------------------------


def test_stale_sample_within_allowance_is_evaluated(monkeypatch, settings):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 600
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition()
    condition.stale_after_seconds = 3600
    _patch_window(monkeypatch, [_sample(now, minutes_ago=20, value=50.0)])

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.OK
    assert result.message == "Condition is within thresholds"


def test_stale_sample_past_allowance_warns(monkeypatch, settings):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 600
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition()
    condition.stale_after_seconds = 3600
    _patch_window(monkeypatch, [_sample(now, minutes_ago=61, value=50.0)])

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.WARNING
    assert "stale (3660s old)" in result.message


def test_intermittent_condition_reports_stale_data_as_ok(monkeypatch, settings):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 600
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition()
    condition.alert_when_stale = False
    _patch_window(monkeypatch, [_sample(now, minutes_ago=20, value=80.0)])

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.OK
    assert "not alerting (intermittent)" in result.message
    assert result.last_value == 80.0


def test_intermittent_condition_with_no_samples_is_ok(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition()
    condition.alert_when_stale = False
    _patch_window(monkeypatch, [])

    result = evaluate_condition(condition, now=now)

    assert result.status == StatusLevel.OK
    assert "No samples available" in result.message
    assert "not alerting (intermittent)" in result.message


def test_intermittent_condition_still_alerts_on_fresh_breach(monkeypatch):
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition(
        warning_threshold=60.0, critical_threshold=75.0, breach_minutes=5
    )
    condition.alert_when_stale = False
    _patch_window(
        monkeypatch, [_sample(now, minutes_ago=i, value=80.0) for i in range(6)]
    )

    assert evaluate_condition(condition, now=now).status == StatusLevel.CRITICAL


def test_default_condition_staleness_is_unchanged(monkeypatch, settings):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 600
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    condition = _condition()
    assert condition.stale_after_seconds is None
    assert condition.alert_when_stale is True
    assert condition.stale_limit_seconds() == 600
    _patch_window(monkeypatch, [_sample(now, minutes_ago=11, value=50.0)])

    assert evaluate_condition(condition, now=now).status == StatusLevel.WARNING


def test_query_looks_back_over_a_long_staleness_allowance(monkeypatch, settings):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 600
    now = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    captured: dict[str, datetime] = {}

    def fake_query_range(metric, start, stop, **kwargs):
        captured["start"] = start
        return []

    monkeypatch.setattr("graphyard.influx.query_range", fake_query_range)

    condition = _condition(breach_minutes=10)
    condition.stale_after_seconds = 43200
    influx.query_condition_window(condition, now=now)
    assert captured["start"] == now - timedelta(minutes=720)


def test_stale_allowance_must_be_positive():
    condition = _condition()
    condition.stale_after_seconds = 0
    with pytest.raises(ValidationError):
        condition.clean()


@pytest.mark.parametrize("field", ["warning_threshold", "critical_threshold"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_condition_clean_rejects_nonfinite_threshold(field, value):
    condition = _condition()
    setattr(condition, field, value)
    with pytest.raises(ValidationError) as error:
        condition.clean()
    assert field in error.value.message_dict


@pytest.mark.django_db
@pytest.mark.parametrize("field", ["warning_threshold", "critical_threshold"])
@pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
def test_condition_admin_form_rejects_nonfinite_threshold(field, value):
    from django.contrib.admin.sites import AdminSite

    from graphyard.admin import ConditionDefinitionAdmin

    model_admin = ConditionDefinitionAdmin(ConditionDefinition, AdminSite())
    form_class = model_admin.get_form(None)
    form = form_class(
        data={
            "name": "fixture",
            "metric_name": "fixture.metric",
            "operator": "gt",
            "warning_threshold": "60",
            "critical_threshold": "75",
            "window_minutes": "30",
            "breach_minutes": "5",
            "status": "ok",
            field: value,
        }
    )
    assert not form.is_valid()
    assert field in form.errors
    assert not ConditionDefinition.objects.exists()


def test_condition_clean_preserves_optional_unbounded_finite_thresholds():
    _condition(warning_threshold=None, critical_threshold=-1000).clean()
    _condition(warning_threshold=1000, critical_threshold=None).clean()


@pytest.mark.parametrize(
    "ages,carry,expected",
    [
        ([600, 0], 60, StatusLevel.OK),
        (list(range(600, -1, -60)), 60, StatusLevel.CRITICAL),
        ([600, 300, 0], 300, StatusLevel.CRITICAL),
        ([601, 540, 0], 60, StatusLevel.OK),
        ([600, 540, 480, 420, 360, 300, 240, 180, 120, 61], 60, StatusLevel.OK),
    ],
)
@pytest.mark.parametrize("intermittent", [False, True])
def test_breach_requires_coverage_throughout_window(
    monkeypatch, settings, ages, carry, expected, intermittent
):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = carry
    now = datetime(2026, 3, 4, 12, tzinfo=UTC)
    condition = _condition(breach_minutes=10)
    condition.stale_after_seconds = 3600
    condition.alert_when_stale = not intermittent
    samples = [
        influx.MetricSample(
            ts=now - timedelta(seconds=age),
            value=80.0,
            host="synthetic",
            service=None,
            metric=condition.metric_name,
            tags={},
        )
        for age in ages
    ]
    monkeypatch.setattr(influx, "query_condition_window", lambda *a, **k: samples)
    assert evaluate_condition(condition, now=now).status == expected


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("late_failure", [False, True])
def test_old_evaluation_cannot_replace_new_result(monkeypatch, late_failure):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, local
    from django.db import connections
    from graphyard import services

    condition = _condition(breach_minutes=1)
    condition.save()
    older = datetime(2026, 3, 4, 12, tzinfo=UTC)
    newer = older + timedelta(microseconds=1)
    state = local()
    entered, published = Event(), Event()
    monkeypatch.setattr(services.timezone, "now", lambda: state.attempt)
    monkeypatch.setattr(services, "record_heartbeat", lambda *a, **k: None)

    def query(*args, **kwargs):
        if kwargs["now"] == older:
            entered.set()
            assert published.wait(10)
            if late_failure:
                raise RuntimeError("synthetic late failure")
            return [_sample(older, minutes_ago=0, value=99.0)]
        return [_sample(newer, minutes_ago=0, value=42.0)]

    def run(attempt):
        state.attempt = attempt
        try:
            return evaluate_conditions_once(condition_id=condition.pk)
        finally:
            connections.close_all()

    monkeypatch.setattr(influx, "query_condition_window", query)
    with ThreadPoolExecutor(max_workers=2) as pool:
        old = pool.submit(run, older)
        assert entered.wait(10)
        try:
            assert pool.submit(run, newer).result(timeout=10).failed == 0
        finally:
            published.set()
        assert old.result(timeout=10).failed == int(late_failure)
    condition.refresh_from_db()
    assert condition.status == StatusLevel.OK
    assert condition.last_value == 42.0
    assert condition.last_evaluated == newer
    assert condition.message == "Condition is within thresholds"


@pytest.mark.parametrize("anchor_age", [1190, 1201, None])
def test_unusable_anchor_preserves_initial_grace(monkeypatch, settings, anchor_age):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 600
    now = datetime(2026, 3, 4, 12, tzinfo=UTC)
    ages = [*range(540, -1, -60)]
    if anchor_age is not None:
        ages.insert(0, anchor_age)
    samples = [
        influx.MetricSample(
            ts=now - timedelta(seconds=age),
            value=80.0,
            host="synthetic",
            service=None,
            metric="synthetic.metric",
            tags={},
        )
        for age in ages
    ]
    monkeypatch.setattr(influx, "query_condition_window", lambda *a, **k: samples)
    assert (
        evaluate_condition(_condition(breach_minutes=10), now=now).status
        == StatusLevel.CRITICAL
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
    "edit",
    [
        {"warning_threshold": 1.0},
        {"tags_filter": {"synthetic": "new"}},
        {"enabled": False},
        {"metric_name": "changed"},
        {"alert_when_stale": False},
    ],
)
@pytest.mark.parametrize("failure", [False, True])
def test_definition_edit_fences_pending_publication(monkeypatch, edit, failure):
    condition = _condition(breach_minutes=1)
    condition.status = StatusLevel.WARNING
    condition.message = "retained evidence"
    condition.last_value = 17
    condition.save()

    def query(*args, **kwargs):
        ConditionDefinition.objects.filter(pk=condition.pk).update(**edit)
        if failure:
            raise ValueError("synthetic query failure")
        return [_sample(kwargs["now"], minutes_ago=0, value=99)]

    monkeypatch.setattr(influx, "query_condition_window", query)
    evaluate_conditions_once(condition_id=condition.pk)
    condition.refresh_from_db()
    assert condition.status == StatusLevel.WARNING
    assert condition.message == "retained evidence"
    assert condition.last_value == 17
    assert condition.last_evaluated is None


@pytest.mark.django_db
def test_noncanonical_stored_json_still_publishes(monkeypatch):
    from django.db import connection

    condition = _condition(breach_minutes=1)
    condition.save()
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE condition_definition SET tags_filter = %s WHERE id = %s",
            ['{"synthetic":"x","number":1e3}', condition.pk],
        )
    monkeypatch.setattr(influx, "query_condition_window", lambda *a, **k: [])
    evaluate_conditions_once(condition_id=condition.pk)
    condition.refresh_from_db()
    assert condition.last_evaluated is not None
    assert condition.message == "No samples available in condition window"
