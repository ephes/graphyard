from __future__ import annotations

from datetime import UTC, datetime, timedelta

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
