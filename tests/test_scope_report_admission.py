from datetime import UTC, datetime
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError


@pytest.mark.django_db
@pytest.mark.parametrize("days", [0, -1, 36501, 10**100, 999999999])
def test_invalid_lookback_has_no_queries(days, django_assert_num_queries):
    with django_assert_num_queries(0), pytest.raises(CommandError, match="--days"):
        call_command("report_ingest_token_hosts", days=days, stdout=StringIO())


@pytest.mark.django_db
def test_datetime_underflow_is_command_error(monkeypatch, django_assert_num_queries):
    monkeypatch.setattr(
        "graphyard.management.commands.report_ingest_token_hosts.timezone.now",
        lambda: datetime(1, 1, 1, tzinfo=UTC),
    )
    with django_assert_num_queries(0), pytest.raises(CommandError, match="--days"):
        call_command("report_ingest_token_hosts", days=1, stdout=StringIO())
