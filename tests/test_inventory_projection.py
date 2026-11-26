"""Synthetic fleet cost and cross-connection snapshot checks."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("count", [1, 12])
def test_summary_projects_counts_without_decoding_reports(count, monkeypatch):
    from datetime import timedelta
    from uuid import uuid4
    from django.db import connection
    from django.db.models import JSONField
    from django.test.utils import CaptureQueriesContext
    from django.utils import timezone
    from graphyard import inventory
    from graphyard.models import (
        HostRegistry,
        InventoryCategory,
        InventoryHost,
        InventorySnapshot,
    )

    now = timezone.now()
    expected = []
    for i in range(count):
        name = f"synthetic-{i:02}"
        host = InventoryHost.objects.create(
            host=HostRegistry.objects.create(host_id=name)
        )
        categories = {
            c: {"status": "ok", "items": [{"name": "synthetic"}]}
            for c in inventory.CATEGORIES
        }
        categories["packages"]["items"] = [
            {"name": f"synthetic-{index}"} for index in range(2048)
        ]
        InventorySnapshot.objects.bulk_create(
            InventorySnapshot(
                host=host,
                snapshot_id=uuid4(),
                observed_at=now - timedelta(days=2),
                digest="a" * 64,
                report={"categories": categories, "gaps": []},
            )
            for _ in range(8)
        )
        good = InventorySnapshot.objects.create(
            host=host,
            snapshot_id=uuid4(),
            observed_at=now - timedelta(hours=1),
            digest="a" * 64,
            report={"categories": categories, "gaps": []},
        )
        bad_categories = {
            **categories,
            "applications": {
                "status": "error",
                "items": [],
                "partial_items": [{"id": "synthetic"}],
                "error": "probe_failed",
            },
        }
        latest = InventorySnapshot.objects.create(
            host=host,
            snapshot_id=uuid4(),
            observed_at=now,
            digest="b" * 64,
            report={"categories": bad_categories, "gaps": ["synthetic_gap"]},
        )
        for c in inventory.CATEGORIES:
            InventoryCategory.objects.create(
                host=host,
                name=c,
                latest_attempt=latest,
                latest_success=good if c == "applications" else latest,
            )
        expected.append(
            {
                "host": name,
                "observed_at": now.isoformat(),
                "received_at": latest.received_at.isoformat(),
                "stale": False,
                "alert": True,
                "delivery_alert": False,
                "coverage_alert": True,
                "warning_after_seconds": host.warning_after_seconds,
                "alert_when_stale": True,
                "categories": {
                    c: {
                        "status": "error" if c == "applications" else "ok",
                        "observed_at": now.isoformat(),
                        "last_success_at": (
                            good.observed_at if c == "applications" else now
                        ).isoformat(),
                        "count": 2048 if c == "packages" else 1,
                    }
                    for c in sorted(inventory.CATEGORIES)
                },
                "gaps": ["synthetic_gap"],
            }
        )
    decoded = []
    original = JSONField.from_db_value

    def decode(self, value, expression, connection):
        result = original(self, value, expression, connection)
        if isinstance(result, dict) and "categories" in result:
            decoded.append(result)
        return result

    monkeypatch.setattr(JSONField, "from_db_value", decode)
    with CaptureQueriesContext(connection) as queries:
        actual = inventory.summaries(now)
    assert actual == expected
    assert not decoded
    selects = [q for q in queries if q["sql"].lstrip().upper().startswith("SELECT")]
    assert len(selects) <= 4


@pytest.mark.parametrize("projection", ["host", "fleet"])
@pytest.mark.parametrize("mode", ["IMMEDIATE", "DEFERRED"])
def test_projection_survives_concurrent_ingest_and_pruning(tmp_path, projection, mode):
    _run_sqlite_probe(tmp_path, projection, mode)


def test_sqlite_status_reads_host_and_release_health_in_one_snapshot(tmp_path):
    _run_sqlite_probe(tmp_path, "status", "DEFERRED")


@pytest.mark.parametrize("view", ["detail", "applications"])
@pytest.mark.parametrize("mode", ["IMMEDIATE", "DEFERRED"])
def test_host_pages_read_warning_policy_with_their_evidence(tmp_path, view, mode):
    _run_sqlite_probe(tmp_path, f"page-{view}", mode)


def _run_sqlite_probe(tmp_path, projection, mode):
    root = Path(__file__).resolve().parents[1]
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(root / "src"), str(root / "src/django")]),
    }
    result = subprocess.run(
        [
            sys.executable,
            __file__,
            str(tmp_path / "synthetic.sqlite3"),
            projection,
            mode,
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _probe(db_path, projection, mode):
    # A private file DB gives portable WAL snapshot isolation with real commits.
    os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.test"
    import django
    from django.conf import settings

    settings.DATABASES["default"]["NAME"] = db_path
    settings.ALLOWED_HOSTS = ["testserver"]
    settings.GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS = 50
    settings.DATABASES["default"]["OPTIONS"]["timeout"] = 0.05
    settings.DATABASES["default"]["OPTIONS"]["transaction_mode"] = mode
    django.setup()
    from concurrent.futures import ThreadPoolExecutor
    from datetime import timedelta
    from uuid import uuid4
    import hashlib
    from django.core.management import call_command
    from django.db import connection, connections
    from django.test import Client
    from django.utils import timezone
    from graphyard import inventory
    from graphyard.models import (
        HostRegistry,
        InventoryCredential,
        InventoryHost,
        InventorySnapshot,
    )

    call_command("migrate", verbosity=0)
    with connection.cursor() as cursor:
        cursor.execute("PRAGMA journal_mode=WAL")
        assert cursor.fetchone()[0] == "wal"
    if projection == "status":
        _assert_status_snapshot()
        connection.close()
        return
    if projection.startswith("page-"):
        _assert_page_snapshot(projection.removeprefix("page-"), mode)
        connection.close()
        return
    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="synthetic")
    )
    secret = "synthetic-inventory-secret-for-tests-only"
    credential = InventoryCredential.objects.create(
        host=host, digest=hashlib.sha256(secret.encode()).hexdigest()
    )
    auth = f"Bearer {credential.pk}.{secret}"
    now = timezone.now()

    def body(i):
        return {
            "schema_version": 1,
            "snapshot_id": str(uuid4()),
            "host": "synthetic",
            "observed_at": (now + timedelta(seconds=i)).isoformat(),
            "collector": "synthetic/1",
            "categories": {
                c: {"status": "ok", "items": [{"name": str(i)}]}
                for c in inventory.CATEGORIES
            },
            "gaps": [],
        }

    first = body(0)

    def send(report):
        response = Client().post(
            "/v1/inventory",
            report,
            content_type="application/json",
            HTTP_AUTHORIZATION=auth,
        )
        assert response.status_code == 200, response.content

    send(first)
    original_id = InventorySnapshot.objects.get().pk
    # Fill the retention window without changing the active generation.
    for i in range(31):
        InventorySnapshot.objects.create(
            host=host,
            snapshot_id=uuid4(),
            observed_at=now - timedelta(days=1),
            digest="a" * 64,
            report=first,
        )
    fired = False
    writer_codes = []
    next_report = body(1)

    def writer():
        try:
            response = Client().post(
                "/v1/inventory",
                next_report,
                content_type="application/json",
                HTTP_AUTHORIZATION=auth,
            )
            assert response.status_code in (200, 503), response.content
            writer_codes.append(response.status_code)
        finally:
            connections.close_all()

    def pause(execute, sql, params, many, context):
        nonlocal fired
        result = execute(sql, params, many, context)
        if (
            not fired
            and 'FROM "graphyard_inventorycategory"' in sql
            and sql.lstrip().upper().startswith("SELECT")
        ):
            fired = True
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(writer).result(timeout=15)
        return result

    with connection.execute_wrapper(pause):
        if projection == "host":
            state = inventory.host_state(host)
            result = inventory.host_summary(host, now, state)
        else:
            result = inventory.summaries(now)[0]
    assert fired
    assert result["observed_at"] == first["observed_at"], result
    assert not result["alert"], result
    assert all(
        c["count"] == 1 and c["observed_at"] == first["observed_at"]
        for c in result["categories"].values()
    ), result
    assert writer_codes == ([200] if mode == "DEFERRED" else [503])
    if writer_codes == [503]:
        send(next_report)
    assert not InventorySnapshot.objects.filter(pk=original_id).exists(), (
        "writer must prune the report read by the old generation"
    )
    assert not connection.in_atomic_block
    connection.close()


@pytest.mark.django_db(transaction=True)
def test_postgres_projection_reads_one_repeatable_read_snapshot():
    from django.db import connection

    from graphyard.inventory import _read_snapshot

    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL isolation oracle; SQLite uses its configured mode")
    with _read_snapshot():
        with connection.cursor() as cursor:
            cursor.execute("SHOW transaction_isolation")
            isolation = cursor.fetchone()[0]
            cursor.execute("SHOW transaction_read_only")
            read_only = cursor.fetchone()[0]
    assert (isolation, read_only) == ("repeatable read", "on")
    assert not connection.in_atomic_block


@pytest.mark.django_db(transaction=True)
def test_postgres_status_reads_host_and_release_health_in_one_snapshot():
    from django.db import connection

    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL snapshot oracle")
    _assert_status_snapshot()


def _assert_status_snapshot():
    from concurrent.futures import ThreadPoolExecutor
    import json
    from django.conf import settings
    from django.contrib.auth.models import AnonymousUser
    from django.db import connection, connections, transaction
    from django.test import RequestFactory, override_settings
    from django.utils import timezone
    from graphyard import inventory
    from graphyard.models import HostRegistry, InventoryHost, InventoryRelease

    host = HostRegistry.objects.create(host_id="synthetic")
    InventoryHost.objects.create(host=host)
    InventoryRelease.objects.create(
        source_id="synthetic", definition={}, checked_at=timezone.now(), version="1.0"
    )
    fired = False

    def writer():
        try:
            with transaction.atomic():
                HostRegistry.objects.filter(pk=host.pk).update(enabled=False)
                InventoryRelease.objects.filter(pk="synthetic").update(error="failed")
        finally:
            connections.close_all()

    def pause(execute, sql, params, many, context):
        nonlocal fired
        if (
            not fired
            and 'FROM "graphyard_inventoryrelease"' in sql
            and sql.lstrip().upper().startswith("SELECT")
        ):
            fired = True
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(writer).result(timeout=15)
        return execute(sql, params, many, context)

    def fetch():
        request = RequestFactory().get(
            "/v1/inventory/status",
            HTTP_AUTHORIZATION=f"Bearer {settings.GRAPHYARD_INVENTORY_MONITOR_TOKEN}",
        )
        request.user = AnonymousUser()
        response = inventory.status(request)
        assert response.status_code == 200
        assert not connection.in_atomic_block
        return json.loads(response.content)

    with override_settings(GRAPHYARD_INVENTORY_MONITOR_TOKEN="synthetic-monitor"):
        with connection.execute_wrapper(pause):
            before = fetch()
        after = fetch()
    assert fired
    assert before["summary"]["attention"] == 1
    assert before["summary"]["release_attention"] == 0
    assert before["releases"]["sources"][0]["status"] == "ok"
    assert after["summary"]["attention"] == 0
    assert after["summary"]["release_attention"] == 1
    assert after["releases"]["sources"][0]["status"] == "failed"


def _assert_page_snapshot(view, mode):
    """A host page pairs its warning policy with the report generation it shows."""
    from concurrent.futures import ThreadPoolExecutor
    from datetime import timedelta
    from uuid import uuid4
    import hashlib
    from django.contrib.auth import get_user_model
    from django.db import OperationalError, connection, connections
    from django.test import Client
    from django.test.utils import setup_test_environment
    from django.urls import reverse
    from django.utils import timezone
    from graphyard import inventory
    from graphyard.models import HostRegistry, InventoryCredential, InventoryHost

    setup_test_environment()  # Record template contexts outside pytest.
    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="synthetic"),
        warning_after_seconds=600,
    )
    secret = "synthetic-inventory-secret-for-tests-only"
    credential = InventoryCredential.objects.create(
        host=host, digest=hashlib.sha256(secret.encode()).hexdigest()
    )
    auth = f"Bearer {credential.pk}.{secret}"
    now = timezone.now()

    def body(i):
        return {
            "schema_version": 1,
            "snapshot_id": str(uuid4()),
            "host": "synthetic",
            "observed_at": (now + timedelta(seconds=i)).isoformat(),
            "collector": "synthetic/1",
            "categories": {
                c: {"status": "ok", "items": [{"name": str(i)}]}
                for c in inventory.CATEGORIES
            },
            "gaps": [],
        }

    def send(report):
        return (
            Client()
            .post(
                "/v1/inventory",
                report,
                content_type="application/json",
                HTTP_AUTHORIZATION=auth,
            )
            .status_code
        )

    first, second = body(0), body(1)
    assert send(first) == 200
    reader = get_user_model().objects.create_user(username="reader")
    fired = False
    writer_codes = []

    def writer():
        try:
            # Reconfigure the warning policy, then admit a newer generation.
            InventoryHost.objects.filter(pk=host.pk).update(warning_after_seconds=1200)
            writer_codes.append(send(second))
        except OperationalError:
            writer_codes.append("busy")
        finally:
            connections.close_all()

    def pause(execute, sql, params, many, context):
        nonlocal fired
        result = execute(sql, params, many, context)
        if (
            not fired
            and 'FROM "graphyard_inventoryhost"' in sql
            and sql.lstrip().upper().startswith("SELECT")
        ):
            fired = True
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(writer).result(timeout=15)
        return result

    client = Client()
    client.force_login(reader)
    route = "inventory_detail" if view == "detail" else "inventory_applications"
    with connection.execute_wrapper(pause):
        response = client.get(reverse(f"graphyard:{route}", args=["synthetic"]))
    assert fired
    assert response.status_code == 200
    assert not connection.in_atomic_block
    shown = response.context["host"]
    pair = (shown["warning_after_seconds"], shown["observed_at"])
    old = (600, first["observed_at"])
    new = (1200, second["observed_at"])
    if mode == "IMMEDIATE":
        # The read snapshot holds the write lock; the writer waits out its timeout.
        assert writer_codes == ["busy"], writer_codes
        assert pair == old, pair
    else:
        assert writer_codes == [200], writer_codes
        assert pair in (old, new), pair


if __name__ == "__main__":
    _probe(*sys.argv[1:])
