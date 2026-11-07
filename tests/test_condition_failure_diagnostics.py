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
    assert not events[0].exc_info
    assert "invented-query-credential-marker" not in caplog.text
