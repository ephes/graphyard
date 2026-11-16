"""Reports must read policy and usage from one committed generation."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("mode", ["DEFERRED", "IMMEDIATE"])
@pytest.mark.parametrize(
    "command", ["report_ingest_token_hosts", "report_ingest_token_bindings"]
)
def test_audit_generation(tmp_path, mode, command):
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, __file__, str(tmp_path / "synthetic.sqlite3"), mode, command],
        env={
            **os.environ,
            "PYTHONPATH": os.pathsep.join(
                [str(root / "src"), str(root / "src/django")]
            ),
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def probe(path, mode, command):
    os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.test"
    import django
    from django.conf import settings

    settings.DATABASES["default"]["NAME"] = path
    settings.DATABASES["default"]["OPTIONS"]["transaction_mode"] = mode
    settings.DATABASES["default"]["OPTIONS"]["timeout"] = 0.05
    settings.GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS = 50
    django.setup()
    from concurrent.futures import ThreadPoolExecutor
    from io import StringIO
    from django.core.management import call_command
    from django.db import OperationalError, connection, connections, transaction
    from django.utils import timezone
    from graphyard.models import IngestToken, IngestTokenHostUsage

    with connection.schema_editor() as editor:
        editor.create_model(IngestToken)
        editor.create_model(IngestTokenHostUsage)
    with connection.cursor() as cursor:
        cursor.execute("PRAGMA journal_mode=WAL")
    token = IngestToken.objects.create(
        name="synthetic-old",
        token_hash="never-fetch",
        host_scope=["old"],
        bound_subject_id="old",
    )
    now = timezone.now()
    IngestTokenHostUsage.objects.create(
        token=token, host_id="old", first_seen_at=now, last_seen_at=now
    )
    fired = committed = False

    def writer():
        nonlocal committed
        try:
            with transaction.atomic():
                IngestToken.objects.filter(pk=token.pk).update(
                    name="synthetic-new",
                    enabled=False,
                    host_scope=["new"],
                    bound_subject_id="new",
                    token_hash="rotated-never-fetch",
                )
                IngestTokenHostUsage.objects.filter(token=token).update(host_id="new")
            committed = True
        except OperationalError as err:
            assert mode == "IMMEDIATE" and "locked" in str(err), err
        finally:
            connections.close_all()

    def pause(execute, sql, params, many, context):
        nonlocal fired
        assert '"token_hash"' not in sql, sql
        if (
            not fired
            and sql.lstrip().upper().startswith("SELECT")
            and 'FROM "ingest_token_host_usage"' in sql
        ):
            fired = True
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(writer).result(timeout=10)
        return execute(sql, params, many, context)

    class Output(StringIO):
        def write(self, value):
            assert not connection.in_atomic_block, "terminal output holds a snapshot"
            return super().write(value)

    output = Output()
    with connection.execute_wrapper(pause):
        call_command(command, stdout=output)
    text = output.getvalue()
    assert fired
    assert "synthetic-old" in text and "synthetic-new" not in text, text
    assert "  old " in text and "  new " not in text, text
    if command.endswith("_hosts"):
        assert (
            "suggested --host-scope old" in text and "outside current scope" not in text
        ), text
    else:
        assert "binding=old" in text and "proposed binding=old" in text, text
    if not committed:
        writer()
    assert committed


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "command", ["report_ingest_token_hosts", "report_ingest_token_bindings"]
)
def test_postgres_audit_snapshot_across_batches(command):
    from concurrent.futures import ThreadPoolExecutor
    from io import StringIO
    from django.core.management import call_command
    from django.db import connection, connections, transaction
    from django.utils import timezone
    from graphyard.models import IngestToken, IngestTokenHostUsage

    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL concurrency oracle; SQLite has independent probes")
    tokens = IngestToken.objects.bulk_create(
        [
            IngestToken(
                name=f"synthetic-{i:03}",
                token_hash="never-fetch",
                host_scope=["old"],
                bound_subject_id="old",
            )
            for i in range(101)
        ]
    )
    now = timezone.now()
    IngestTokenHostUsage.objects.bulk_create(
        [
            IngestTokenHostUsage(
                token=token, host_id="old", first_seen_at=now, last_seen_at=now
            )
            for token in tokens
        ]
    )
    fired = False

    def writer():
        try:
            with transaction.atomic():
                IngestToken.objects.update(
                    enabled=False,
                    host_scope=["new"],
                    bound_subject_id="new",
                    token_hash="rotated-never-fetch",
                )
                IngestTokenHostUsage.objects.update(host_id="new")
        finally:
            connections.close_all()

    def pause(execute, sql, params, many, context):
        nonlocal fired
        assert '"token_hash"' not in sql
        if not fired and 'FROM "ingest_token_host_usage"' in sql:
            fired = True
            with connection.cursor() as cursor:
                cursor.execute("SHOW transaction_isolation")
                assert cursor.fetchone()[0] == "repeatable read"
                cursor.execute("SHOW transaction_read_only")
                assert cursor.fetchone()[0] == "on"
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(writer).result(timeout=10)
        return execute(sql, params, many, context)

    class Output(StringIO):
        def write(self, value):
            assert not connection.in_atomic_block
            return super().write(value)

    output = Output()
    with connection.execute_wrapper(pause):
        call_command(command, stdout=output)
    assert fired and not connection.in_atomic_block
    text = output.getvalue()
    assert "  new " not in text and text.count("  old ") == 101, text
    if command.endswith("_hosts"):
        assert text.count("suggested --host-scope old") == 101, text
        assert "outside current scope" not in text
    else:
        assert text.count("enabled=True binding=old") == 101, text
        assert text.count("proposed binding=old") == 101, text


if __name__ == "__main__":
    probe(*sys.argv[1:])
