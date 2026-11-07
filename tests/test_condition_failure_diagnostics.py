import pytest

from graphyard import services
from graphyard.models import ConditionDefinition


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
