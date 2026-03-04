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
