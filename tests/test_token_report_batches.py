"""Report query and projection budgets over synthetic tokens."""

from io import StringIO

import pytest
from django.core.management import call_command
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from graphyard.models import IngestToken, IngestTokenHostUsage


def seed(count, enabled=True, identifiers=1):
    tokens = IngestToken.objects.bulk_create(
        [
            IngestToken(
                name=f"synthetic-{enabled}-{i:04}",
                token_hash="never-fetch-this",
                enabled=enabled,
            )
            for i in range(count)
        ]
    )
    now = timezone.now()
    IngestTokenHostUsage.objects.bulk_create(
        [
            IngestTokenHostUsage(
                token=token,
                host_id=f"host_{i}" if identifiers == 1 else f"host_{i}_{j}",
                first_seen_at=now,
                last_seen_at=now,
            )
            for i, token in enumerate(tokens)
            for j in range(identifiers)
        ]
    )
    return tokens


def report(command, **options):
    output = StringIO()
    with CaptureQueriesContext(connection) as queries:
        call_command(command, stdout=output, **options)
    return output.getvalue(), [q["sql"] for q in queries]


@pytest.mark.django_db
@pytest.mark.parametrize("count", [2, 100, 205])
def test_binding_report_batch_budget_and_independent_oracle(count, monkeypatch):
    tokens = seed(count)

    def no_usage_models(*args, **kwargs):
        pytest.fail("report hydrated a usage model")

    monkeypatch.setattr(IngestTokenHostUsage, "from_db", no_usage_models)
    output, queries = report("report_ingest_token_bindings")
    assert sum(sql.startswith("SELECT") for sql in queries) <= 1 + (count + 99) // 100
    assert all('"token_hash"' not in sql for sql in queries)
    assert "never-fetch-this" not in output
    lines = output.splitlines()[1:]
    for i, token in enumerate(tokens):
        assert lines[i * 3] == f"name={token.name} enabled=True binding=unbound"
        assert lines[i * 3 + 1].startswith(f"  host_{i} last_seen=")
        assert lines[i * 3 + 2] == f"  proposed binding=host_{i}"


@pytest.mark.django_db
@pytest.mark.parametrize("include_disabled", [False, True])
def test_host_report_selected_usage_projection_and_batch_budget(
    include_disabled, monkeypatch
):
    enabled = seed(2)
    disabled = seed(205, enabled=False, identifiers=70)
    IngestToken.objects.filter(pk=enabled[0].pk).update(host_scope=["other"])

    def no_models(*args, **kwargs):
        pytest.fail("host report hydrated a model")

    monkeypatch.setattr(IngestToken, "from_db", no_models)
    monkeypatch.setattr(IngestTokenHostUsage, "from_db", no_models)
    output, queries = report(
        "report_ingest_token_hosts", include_disabled=include_disabled
    )
    selected = enabled + disabled if include_disabled else enabled
    assert (
        sum(sql.startswith("SELECT") for sql in queries)
        == 1 + (len(selected) + 99) // 100
    )
    assert all('"token_hash"' not in sql for sql in queries)
    usage_queries = [sql for sql in queries if 'FROM "ingest_token_host_usage"' in sql]
    assert len(usage_queries) == (len(selected) + 99) // 100
    # SQL must select usage for the admitted batch, never a global history scan.
    for sql in usage_queries:
        assert '"token_id" IN (' in sql
        ids = sql.split('"token_id" IN (', 1)[1].split(")", 1)[0].split(",")
        assert len(ids) <= 100
        assert set(map(int, ids)) <= {token.pk for token in selected}
    assert output.count("token id=") == len(selected)
    assert "outside current scope" in output
    assert "suggested --host-scope host_0" in output
    assert (disabled[0].name in output) == include_disabled


@pytest.mark.django_db
def test_spool_limit_refuses_before_any_token_presentation(monkeypatch):
    from graphyard import token_reports
    from django.core.management.base import CommandError

    seed(2)
    monkeypatch.setattr(token_reports, "MAX_REPORT_BYTES", 1)
    output = StringIO()
    with pytest.raises(CommandError, match="64 MiB"):
        call_command("report_ingest_token_hosts", stdout=output)
    assert "token id=" not in output.getvalue()


@pytest.mark.django_db
def test_usage_projection_limit_refuses_restored_oversize_history():
    from django.core.management.base import CommandError

    seed(1, identifiers=501)
    output = StringIO()
    with pytest.raises(CommandError, match="500 identifiers"):
        call_command("report_ingest_token_bindings", stdout=output)
    assert "name=synthetic" not in output.getvalue()
