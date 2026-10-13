from io import StringIO
from unittest.mock import Mock

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard.management.commands.evaluate_conditions import Command
from graphyard.services import ConditionEvaluationRun


@pytest.mark.parametrize("failed", [0, 1])
def test_evaluate_conditions_command_handle_reports_summary(monkeypatch, failed):
    evaluate = Mock(return_value=ConditionEvaluationRun(total=3, failed=failed))
    monkeypatch.setattr(
        "graphyard.management.commands.evaluate_conditions.evaluate_conditions_once",
        evaluate,
    )
    stdout = StringIO()
    command = Command(stdout=stdout, no_color=True)
    if failed:
        with pytest.raises(CommandError, match="1 condition evaluation.*failed"):
            call_command(command, condition_id=7)
        assert "completed with failures (1/3)" in stdout.getvalue()
    else:
        call_command(command, condition_id=7)
        assert "completed (3)" in stdout.getvalue()
    evaluate.assert_called_once_with(condition_id=7)


@pytest.mark.parametrize("failed,exit_code", [(0, 0), (1, 1)])
def test_evaluate_conditions_command_cli_exit_status(monkeypatch, failed, exit_code):
    monkeypatch.setattr(
        "graphyard.management.commands.evaluate_conditions.evaluate_conditions_once",
        lambda **kwargs: ConditionEvaluationRun(total=3, failed=failed),
    )
    monkeypatch.setattr(Command, "check", lambda *args, **kwargs: None)
    monkeypatch.setattr("django.db.connections.close_all", lambda: None)
    stdout, stderr = StringIO(), StringIO()
    command = Command(stdout=stdout, stderr=stderr, no_color=True)
    if exit_code:
        with pytest.raises(SystemExit) as error:
            command.run_from_argv(["manage.py", "evaluate_conditions", "--no-color"])
        assert error.value.code == exit_code
        assert "CommandError:" in stderr.getvalue()
    else:
        command.run_from_argv(["manage.py", "evaluate_conditions", "--no-color"])
        assert not stderr.getvalue()
    assert "completed" in stdout.getvalue()
