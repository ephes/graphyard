from __future__ import annotations

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard.services import ConditionEvaluationRun, MetricCollectionSpecRun


def test_start_agent_run_once_calls_both_loops(monkeypatch):
    calls = {"metrics": 0, "conditions": 0}

    def fake_run_metric_collection_specs_once(**kwargs):
        del kwargs
        calls["metrics"] += 1
        return MetricCollectionSpecRun(
            total=2, failed=0, warning=0, ingested=2, skipped=0
        )

    def fake_evaluate_conditions_once(**kwargs):
        del kwargs
        calls["conditions"] += 1
        return ConditionEvaluationRun(total=3, failed=0)

    monkeypatch.setattr(
        "graphyard.management.commands.start_agent.run_metric_collection_specs_once",
        fake_run_metric_collection_specs_once,
    )
    monkeypatch.setattr(
        "graphyard.management.commands.start_agent.evaluate_conditions_once",
        fake_evaluate_conditions_once,
    )

    call_command("start_agent", "--run-once")

    assert calls["metrics"] == 1
    assert calls["conditions"] == 1


def test_start_agent_requires_at_least_one_loop_enabled():
    with pytest.raises(CommandError):
        call_command(
            "start_agent", "--disable-metrics", "--disable-conditions", "--run-once"
        )


@pytest.mark.parametrize("metric_result", ["ok", "failed", "exception"])
@pytest.mark.parametrize("condition_result", ["ok", "failed", "exception"])
def test_one_shot_cli_reports_all_enabled_failures(
    monkeypatch, metric_result, condition_result
):
    from io import StringIO
    from unittest.mock import Mock
    from graphyard.management.commands.start_agent import Command

    metrics = Mock(
        return_value=MetricCollectionSpecRun(
            total=1,
            failed=int(metric_result == "failed"),
            warning=0,
            ingested=0,
            skipped=0,
        )
    )
    conditions = Mock(
        return_value=ConditionEvaluationRun(
            total=1, failed=int(condition_result == "failed")
        )
    )
    if metric_result == "exception":
        metrics.side_effect = RuntimeError("synthetic metric failure")
    if condition_result == "exception":
        conditions.side_effect = RuntimeError("synthetic condition failure")
    monkeypatch.setattr(
        "graphyard.management.commands.start_agent.run_metric_collection_specs_once",
        metrics,
    )
    monkeypatch.setattr(
        "graphyard.management.commands.start_agent.evaluate_conditions_once", conditions
    )
    command = Command(stdout=StringIO(), stderr=StringIO(), no_color=True)
    monkeypatch.setattr(command, "check", lambda **kwargs: None)
    monkeypatch.setattr("django.db.connections.close_all", lambda: None)
    if metric_result == condition_result == "ok":
        command.run_from_argv(["manage.py", "start_agent", "--run-once"])
    else:
        with pytest.raises(SystemExit) as err:
            command.run_from_argv(["manage.py", "start_agent", "--run-once"])
        assert err.value.code == 1
        assert (
            "CommandError: One or more enabled agent loops failed."
            in command.stderr.getvalue()
        )
    metrics.assert_called_once_with(due_only=False)
    conditions.assert_called_once_with()


def test_continuous_agent_keeps_scheduling_after_failure(monkeypatch):
    from unittest.mock import Mock

    metrics = Mock(
        side_effect=[
            RuntimeError("synthetic failure"),
            MetricCollectionSpecRun(
                total=1, failed=0, warning=0, ingested=0, skipped=0
            ),
        ]
    )
    conditions = Mock(return_value=ConditionEvaluationRun(total=1, failed=1))
    monkeypatch.setattr(
        "graphyard.management.commands.start_agent.run_metric_collection_specs_once",
        metrics,
    )
    monkeypatch.setattr(
        "graphyard.management.commands.start_agent.evaluate_conditions_once", conditions
    )
    clock = [0.0]
    monkeypatch.setattr(
        "graphyard.management.commands.start_agent.time.monotonic", lambda: clock[0]
    )

    def sleep(seconds):
        clock[0] += seconds
        if clock[0] >= 2:
            raise KeyboardInterrupt

    monkeypatch.setattr("graphyard.management.commands.start_agent.time.sleep", sleep)
    call_command("start_agent", metrics_interval=1, condition_interval=1)
    assert metrics.call_count == conditions.call_count == 2


@pytest.mark.parametrize("disabled", ["--disable-metrics", "--disable-conditions"])
def test_one_shot_disabled_loop_is_never_run(monkeypatch, disabled):
    from unittest.mock import Mock

    metrics = Mock(
        return_value=MetricCollectionSpecRun(
            total=1, failed=0, warning=0, ingested=1, skipped=0
        )
    )
    conditions = Mock(return_value=ConditionEvaluationRun(total=1, failed=0))
    if disabled == "--disable-metrics":
        metrics.side_effect = RuntimeError("disabled metrics must not run")
    else:
        conditions.side_effect = RuntimeError("disabled conditions must not run")
    monkeypatch.setattr(
        "graphyard.management.commands.start_agent.run_metric_collection_specs_once",
        metrics,
    )
    monkeypatch.setattr(
        "graphyard.management.commands.start_agent.evaluate_conditions_once", conditions
    )
    call_command("start_agent", "--run-once", disabled)
    assert metrics.call_count == int(disabled != "--disable-metrics")
    assert conditions.call_count == int(disabled != "--disable-conditions")
