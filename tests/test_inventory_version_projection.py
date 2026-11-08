"""Synthetic version-page query costs and real two-connection generation races."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
import hashlib
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

import pytest


def _report(now, version):
    from graphyard.inventory import CATEGORIES

    categories = {name: {"status": "ok", "items": []} for name in CATEGORIES}
    categories["applications"]["items"] = [
        {"id": "synthetic", "status": "ok", "items": {"installed_version": version}}
    ]
    return {
        "schema_version": 1,
        "snapshot_id": str(uuid4()),
        "host": "synthetic",
        "observed_at": now.isoformat(),
        "collector": "synthetic/1",
        "categories": categories,
        "gaps": [],
    }


def _release(now):
    from graphyard.models import InventoryRelease

    return InventoryRelease.objects.create(
        source_id="github:synthetic/synthetic",
        definition={
            "kind": "github",
            "project": "synthetic/synthetic",
            "targets": [{"kind": "application", "name": "synthetic"}],
        },
        version="1.0",
        checked_at=now,
    )


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("count", [1, 50, 200])
@pytest.mark.parametrize("selected", [False, True])
def test_version_page_batches_evidence_and_preserves_counts(
    count, selected, rf, monkeypatch
):
    from django.db import connection
    from django.db.models import JSONField
    from django.test.utils import CaptureQueriesContext
    from django.utils import timezone
    from graphyard import inventory, inventory_releases
    from graphyard.models import (
        HostRegistry,
        InventoryHost,
        InventorySnapshot,
        InventoryCategory,
    )

    now = timezone.now() - timedelta(minutes=1)
    _release(now)
    hosts = []
    expected = []
    for i in range(count):
        host = InventoryHost.objects.create(
            host=HostRegistry.objects.create(host_id=f"synthetic-{i:03}")
        )
        hosts.append(host)
        report = _report(now, "0.9" if i % 2 else "1.0")
        snapshot = InventorySnapshot.objects.create(
            host=host,
            snapshot_id=report["snapshot_id"],
            observed_at=now,
            digest="a" * 64,
            report=report,
        )
        for name in ("applications", "packages"):
            InventoryCategory.objects.create(
                host=host, name=name, latest_attempt=snapshot, latest_success=snapshot
            )
        if not selected or i == 0:
            rows, _ = inventory_releases.rows_for_host(
                host, inventory_releases.registry(), timezone.now()
            )
            expected.extend({**row, "host": host.host.host_id} for row in rows)
    expected.sort(
        key=lambda row: ({"update": 0, "equal": 3}[row["status"]], row["host"])
    )
    decoded = []
    original = JSONField.from_db_value

    def decode(self, value, expression, connection):
        result = original(self, value, expression, connection)
        if self.model is InventorySnapshot:
            decoded.append(result)
        return result

    monkeypatch.setattr(JSONField, "from_db_value", decode)
    monkeypatch.setattr(inventory, "render", lambda request, template, context: context)
    params = {"host": hosts[0].host.host_id} if selected else {}
    request = rf.get("/inventory/versions/", params)
    request.user = SimpleNamespace(is_authenticated=True)
    with CaptureQueriesContext(connection) as queries:
        context = inventory.version_list(request)
    assert list(context["page"]) == expected[:50]
    assert context["page"].paginator.count == len(expected)
    assert context["counts"] == {
        key: sum(row["status"] == key for row in expected)
        for key in ("update", "equal", "ahead", "unknown")
    }
    assert len(context["hosts"]) == count
    assert len(decoded) == (1 if selected else count)
    selects = [q for q in queries if q["sql"].lstrip().upper().startswith("SELECT")]
    assert len(selects) <= (7 if selected else 6)
    # Counts precede status/search filtering, and sorting precedes pagination.
    filtered = rf.get(
        "/inventory/versions/", {**params, "status": "update", "q": "synthetic"}
    )
    filtered.user = request.user
    context = inventory.version_list(filtered)
    assert context["counts"]["equal"] == sum(
        row["status"] == "equal" for row in expected
    )
    assert (
        list(context["page"])
        == [row for row in expected if row["status"] == "update"][:50]
    )


@pytest.mark.parametrize("mode", ["IMMEDIATE", "DEFERRED"])
def test_version_page_sqlite_generation_race(tmp_path, mode):
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, __file__, str(tmp_path / "synthetic.sqlite3"), mode],
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


@pytest.mark.django_db(transaction=True)
def test_version_page_postgres_generation_race():
    from django.db import connection

    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL contract; SQLite races run in private WAL databases")
    _race()


def _race():
    from django.db import connection, connections, transaction, OperationalError
    from django.test import Client, RequestFactory
    from django.utils import timezone
    from graphyard import inventory
    from graphyard.models import (
        HostRegistry,
        InventoryHost,
        InventorySnapshot,
        InventoryCredential,
    )

    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="synthetic")
    )
    test_value = "synthetic-inventory-secret-for-tests-only"
    credential = InventoryCredential.objects.create(
        host=host, digest=hashlib.sha256(test_value.encode()).hexdigest()
    )
    auth = f"Bearer {credential.pk}.{test_value}"
    now = timezone.now() - timedelta(minutes=1)
    release = _release(now)

    def send(report):
        response = Client().post(
            "/v1/inventory",
            report,
            content_type="application/json",
            HTTP_AUTHORIZATION=auth,
        )
        assert response.status_code == 200, response.content

    first = _report(now, "1.0")
    send(first)
    original_id = InventorySnapshot.objects.get().pk
    for _ in range(31):
        InventorySnapshot.objects.create(
            host=host,
            snapshot_id=uuid4(),
            observed_at=now - timedelta(days=1),
            digest="a" * 64,
            report=first,
        )
    next_report = _report(now + timedelta(seconds=1), "2.0")
    fired = False
    writer_codes = []

    def writer():
        try:
            with transaction.atomic():
                type(release).objects.filter(pk=release.pk).update(version="2.0")
            send(next_report)
            writer_codes.append(200)
        except OperationalError:
            assert connection.vendor == "sqlite"
            writer_codes.append(503)
        finally:
            connections.close_all()

    def pause(execute, sql, params, many, context):
        nonlocal fired
        result = execute(sql, params, many, context)
        if (
            not fired
            and 'FROM "graphyard_inventoryrelease"' in sql
            and sql.lstrip().upper().startswith("SELECT")
        ):
            fired = True
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(writer).result(timeout=15)
        return result

    def render(request, template, context):
        assert not connection.in_atomic_block, "render must run outside the snapshot"
        # No lazy evidence queries may escape into rendering.
        with connection.execute_wrapper(
            lambda *args: pytest.fail("lazy SQL during rendering")
        ):
            list(context["hosts"])
            list(context["page"])
        return context

    request = RequestFactory().get("/inventory/versions/")
    request.user = SimpleNamespace(is_authenticated=True)
    with patch.object(inventory, "render", render), connection.execute_wrapper(pause):
        result = inventory.version_list(request)
    assert fired, "the concurrent writer must execute between registry and host reads"
    (row,) = result["page"]
    assert (row["installed"], row["available"], row["status"]) == (
        "1.0",
        "1.0",
        "equal",
    ), row
    assert row["snapshot"].pk == original_id
    immediate = (
        connection.vendor == "sqlite"
        and connection.settings_dict["OPTIONS"]["transaction_mode"] == "IMMEDIATE"
    )
    assert writer_codes == ([503] if immediate else [200])
    if immediate:
        writer()
        assert writer_codes == [503, 200]
    assert not InventorySnapshot.objects.filter(pk=original_id).exists(), (
        "writer must prune original evidence"
    )


if __name__ == "__main__":
    os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.test"
    from django.conf import settings
    import django

    settings.DATABASES["default"]["NAME"] = sys.argv[1]
    settings.DATABASES["default"]["OPTIONS"].update(
        timeout=0.05, transaction_mode=sys.argv[2]
    )
    settings.GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS = 50
    settings.ALLOWED_HOSTS = ["testserver"]
    django.setup()
    from django.core.management import call_command
    from django.db import connection

    call_command("migrate", verbosity=0)
    with connection.cursor() as cursor:
        cursor.execute("PRAGMA journal_mode=WAL")
        assert cursor.fetchone()[0] == "wal"
    _race()
    connection.close()
