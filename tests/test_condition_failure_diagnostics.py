import logging

import pytest

from graphyard import services
from graphyard.models import ConditionDefinition, PipelineHeartbeat, StatusLevel


@pytest.mark.django_db
@pytest.mark.parametrize("stage", ["evaluation", "publication", "failure_publication"])
def test_condition_failure_reports_stage_safely_and_continues(
    monkeypatch, caplog, stage
):
    first = ConditionDefinition.objects.create(
        name="a synthetic", metric_name="test.value"
    )
    second = ConditionDefinition.objects.create(
        name="b synthetic", metric_name="test.value"
    )
    evaluated = []
    real_publish = services._publish_condition_result

    def evaluate(condition, now):
        evaluated.append(condition.pk)
        if condition.pk == first.pk and stage == "evaluation":
            raise RuntimeError("invented-query-credential-marker")
        return services.ConditionEvaluation("ok", "ok", 1.0, now)

    def publish(condition_id, *args, **kwargs):
        if condition_id == first.pk and stage in ("publication", "failure_publication"):
            if stage == "failure_publication" or kwargs["status"] == "ok":
                raise ValueError("invented-query-credential-marker")
        return real_publish(condition_id, *args, **kwargs)

    monkeypatch.setattr(services, "evaluate_condition", evaluate)
    monkeypatch.setattr(services, "_publish_condition_result", publish)
    result = services.evaluate_conditions_once()
    assert result.total == 2 and result.failed == 1
    assert evaluated == [first.pk, second.pk]
    events = [
        r for r in caplog.records if r.getMessage().startswith("Condition failure ")
    ]
    assert len(events) == 1
    assert events[0].condition_id == first.pk
    assert events[0].stage == ("evaluation" if stage == "evaluation" else "publication")
    assert events[0].category == (
        "RuntimeError" if stage == "evaluation" else "ValueError"
    )
    assert (
        f"failure_published={str(stage != 'failure_publication').lower()}"
        in events[0].getMessage()
    )
    assert not events[0].exc_info
    assert "invented-query-credential-marker" not in caplog.text


@pytest.mark.django_db
@pytest.mark.parametrize("change", ["deleted", "edited", "superseded", "unchanged"])
def test_failure_published_reports_actual_write(monkeypatch, caplog, change):
    ConditionDefinition.objects.create(name="synthetic", metric_name="test.value")

    def evaluate(item, now):
        rows = ConditionDefinition.objects.filter(pk=item.pk)
        if change == "deleted":
            rows.delete()
        elif change == "edited":
            rows.update(metric_name="test.changed")
        elif change == "superseded":
            rows.update(last_evaluated=now)
        raise ValueError("synthetic failure")

    monkeypatch.setattr(services, "evaluate_condition", evaluate)
    assert services.evaluate_conditions_once().failed == 1
    assert f"failure_published={str(change == 'unchanged').lower()}" in caplog.text


@pytest.mark.django_db
def test_condition_scan_publishes_results_and_heartbeat_when_log_handler_fails(
    monkeypatch,
):
    first = ConditionDefinition.objects.create(name="first", metric_name="test.value")
    second = ConditionDefinition.objects.create(name="second", metric_name="test.value")
    logged = []

    class FailingHandler(logging.Handler):
        def emit(self, record):
            logged.append(record.getMessage())
            raise OSError("synthetic log failure")

    def evaluate(condition, now):
        if condition.pk == first.pk:
            raise RuntimeError("synthetic evaluation failure")
        return services.ConditionEvaluation(StatusLevel.OK, "ok", 1.0, now)

    monkeypatch.setattr(services, "evaluate_condition", evaluate)
    handler = FailingHandler(level=logging.ERROR)
    services.logger.addHandler(handler)
    try:
        result = services.evaluate_conditions_once()
    finally:
        services.logger.removeHandler(handler)

    assert result == services.ConditionEvaluationRun(total=2, failed=1)
    assert len(logged) == 1 and f"condition_id={first.pk}" in logged[0]
    first.refresh_from_db()
    second.refresh_from_db()
    assert first.status == StatusLevel.CRITICAL
    assert first.message == "evaluation error: RuntimeError"
    assert second.status == StatusLevel.OK
    assert second.last_value == 1.0
    heartbeat = PipelineHeartbeat.objects.get(name="condition_evaluator")
    assert heartbeat.status == StatusLevel.WARNING
    assert heartbeat.last_error == "1 condition(s) failed"
    assert heartbeat.details == {"evaluated": 2, "failed": 1}
