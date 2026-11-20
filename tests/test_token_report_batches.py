"""Report query and projection budgets over synthetic tokens."""

from concurrent.futures import ThreadPoolExecutor
from io import StringIO

import pytest
from django.core.management import call_command
from django.db import connection, connections
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from graphyard.models import IngestToken, IngestTokenHostUsage
from graphyard.token_reports import token_usage_batches


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


def reads(queries):
    """Row reads: plain SELECTs, or PostgreSQL server-side cursors over a SELECT.

    Inside the report's snapshot transaction PostgreSQL ``.iterator()`` runs
    ``DECLARE ... CURSOR ... FOR SELECT``; ``SET TRANSACTION`` is not a read.
    """
    return [
        sql
        for sql in queries
        if sql.startswith("SELECT")
        or (sql.startswith("DECLARE ") and " FOR SELECT " in sql)
    ]


@pytest.mark.django_db(databases=["default", "secondary"], transaction=True)
def test_spool_keeps_token_and_usage_reads_on_the_selected_snapshot():
    alias = "secondary"
    reader = connections[alias]
    now = timezone.now()
    for database, host in [("default", "other_database"), (alias, "original")]:
        token = IngestToken.objects.using(database).create(
            pk=123, name=host, token_hash="synthetic"
        )
        IngestTokenHostUsage.objects.using(database).create(
            token=token, host_id=host, first_seen_at=now, last_seen_at=now
        )

    def change_usage():
        try:
            return (
                IngestTokenHostUsage.objects.using(alias)
                .filter(token_id=123)
                .update(host_id="committed_later")
            )
        finally:
            connections[alias].close()

    def before_usage_read(execute, sql, params, many, context):
        if 'FROM "ingest_token_host_usage"' in sql:
            assert reader.in_atomic_block
            # PostgreSQL lets the writer commit between the two reads.
            # SQLite's BEGIN IMMEDIATE reserves the writer instead.
            if reader.vendor == "postgresql":
                with ThreadPoolExecutor(max_workers=1) as executor:
                    assert executor.submit(change_usage).result(timeout=10) == 1
        return execute(sql, params, many, context)

    tokens = IngestToken.objects.using(alias).values("id", "name")
    with reader.execute_wrapper(before_usage_read):
        [(token, usages)] = token_usage_batches(tokens, now)
    assert token == {"id": 123, "name": "original"}
    assert [usage["host_id"] for usage in usages] == ["original"]
    assert not reader.in_atomic_block
    if reader.vendor == "postgresql":
        assert IngestTokenHostUsage.objects.using(alias).get().host_id == "committed_later"


@pytest.mark.django_db
@pytest.mark.parametrize("count", [2, 100, 205])
def test_binding_report_batch_budget_and_independent_oracle(count, monkeypatch):
    tokens = seed(count)

    def no_usage_models(*args, **kwargs):
        pytest.fail("report hydrated a usage model")

    monkeypatch.setattr(IngestTokenHostUsage, "from_db", no_usage_models)
    output, queries = report("report_ingest_token_bindings")
    assert len(reads(queries)) <= 1 + (count + 99) // 100
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
    assert len(reads(queries)) == 1 + (len(selected) + 99) // 100
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
