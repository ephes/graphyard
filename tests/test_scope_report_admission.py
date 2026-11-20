from datetime import UTC, datetime
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError


@pytest.mark.django_db
@pytest.mark.parametrize(
    "command", ["report_ingest_token_hosts", "report_ingest_token_bindings"]
)
@pytest.mark.parametrize("days", [0, -1, 36501, 10**100, 999999999])
def test_invalid_lookback_has_no_queries(command, days, django_assert_num_queries):
    with django_assert_num_queries(0), pytest.raises(CommandError, match="--days"):
        call_command(command, days=days, stdout=StringIO())


@pytest.mark.django_db
@pytest.mark.parametrize(
    "command", ["report_ingest_token_hosts", "report_ingest_token_bindings"]
)
def test_datetime_underflow_is_command_error(
    command, monkeypatch, django_assert_num_queries
):
    monkeypatch.setattr(
        "django.utils.timezone.now",
        lambda: datetime(1, 1, 1, tzinfo=UTC),
    )
    stdout = StringIO()
    with (
        django_assert_num_queries(0),
        pytest.raises(
            CommandError, match="^--days exceeds the supported datetime range$"
        ),
    ):
        call_command(command, days=1, stdout=stdout)
    assert stdout.getvalue() == ""
