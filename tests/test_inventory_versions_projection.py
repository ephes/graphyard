"""Version page: one read generation and batched, selected-host evidence."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


def _report(version, packages_status="ok"):
    return {
        "categories": {
            "applications": {
                "status": "ok",
                "items": [
                    {
                        "id": "synthetic-app",
                        "status": "ok",
                        "items": {"installed_version": version},
                    }
                ],
            },
            "packages": {
                "status": packages_status,
                "items": [{"kind": "brew", "name": "synthetic", "version": version}],
            },
            "units": {"status": "ok", "items": []},
            "containers": {"status": "ok", "items": []},
        },
        "gaps": [],
    }


def _host(name, version, observed_at):
    from uuid import uuid4

    from graphyard.models import (
        HostRegistry,
        InventoryCategory,
        InventoryHost,
        InventorySnapshot,
    )

    host = InventoryHost.objects.create(host=HostRegistry.objects.create(host_id=name))
    snapshot = InventorySnapshot.objects.create(
        host=host,
        snapshot_id=uuid4(),
        observed_at=observed_at,
        digest="a" * 64,
        report=_report(version),
    )
    for name in ("applications", "packages", "units", "containers"):
        InventoryCategory.objects.create(
            host=host, name=name, latest_attempt=snapshot, latest_success=snapshot
        )
    return host, snapshot


def _count_decodes(monkeypatch):
    from django.db.models import JSONField

    decoded = []
    original = JSONField.from_db_value

    def decode(self, value, expression, connection):
        result = original(self, value, expression, connection)
        if isinstance(result, dict) and "categories" in result:
            decoded.append(result)
        return result

    monkeypatch.setattr(JSONField, "from_db_value", decode)
    return decoded


@pytest.mark.django_db
def test_version_page_queries_are_constant_and_decode_each_report_once(
    client, django_user_model, monkeypatch
):
    from django.db import connection
    from django.test.utils import CaptureQueriesContext
    from django.urls import reverse
    from django.utils import timezone

    now = timezone.now()
    client.force_login(django_user_model.objects.create_user(username="reader"))
    url = reverse("graphyard:inventory_versions")
    decoded = _count_decodes(monkeypatch)

    def measure():
        decoded.clear()
        with CaptureQueriesContext(connection) as queries:
            response = client.get(url)
        assert response.status_code == 200
        return response, len(queries)

    _host("synthetic-00", "1.0", now)
    response, single = measure()
    assert len(decoded) == 1
    assert len(response.context["page"]) == 2
    for i in range(1, 12):
        _host(f"synthetic-{i:02}", "1.0", now)
    response, fleet = measure()
    assert fleet == single
    assert len(decoded) == 12
    assert {row["host"] for row in response.context["page"]} == {
        f"synthetic-{i:02}" for i in range(12)
    }


@pytest.mark.django_db
def test_version_page_decodes_only_application_and_package_evidence(
    client, django_user_model, monkeypatch
):
    from datetime import timedelta
    from uuid import uuid4

    from django.urls import reverse
    from django.utils import timezone

    from graphyard.models import InventoryCategory, InventorySnapshot

    now = timezone.now()
    referenced = set()
    expected_rows = set()
    for basis in ("partial", "historical"):
        host, success = _host(basis, "1.0", now - timedelta(minutes=3))

        def store_report(minutes_ago, categories):
            return InventorySnapshot.objects.create(
                host=host,
                snapshot_id=uuid4(),
                observed_at=now - timedelta(minutes=minutes_ago),
                digest="b" * 64,
                report={"categories": categories, "gaps": []},
            )

        failed_applications = {"status": "error", "items": []}
        if basis == "partial":
            failed_applications["partial_items"] = _report("2.0")["categories"][
                "applications"
            ]["items"]
        attempt = store_report(2, {"applications": failed_applications})
        packages = store_report(
            2, {"packages": _report("3.0")["categories"]["packages"]}
        )
        units = store_report(
            1, {"units": {"status": "ok", "items": [{"name": basis * 10_000}]}}
        )
        latest = store_report(0, {"containers": {"status": "ok", "items": []}})
        pointers = InventoryCategory.objects.filter(host=host)
        pointers.filter(name="applications").update(latest_attempt=attempt)
        pointers.filter(name="packages").update(
            latest_attempt=packages, latest_success=packages
        )
        pointers.filter(name="units").update(latest_attempt=units, latest_success=units)

        referenced.update((attempt.pk, packages.pk, success.pk))
        chosen = attempt if basis == "partial" else success
        expected_rows.update(
            (
                (
                    basis,
                    "application",
                    basis,
                    "2.0" if basis == "partial" else "1.0",
                    chosen.pk,
                ),
                (basis, "brew (upstream only)", "successful", "3.0", packages.pk),
            )
        )

    client.force_login(django_user_model.objects.create_user(username="reader"))
    decoded = _count_decodes(monkeypatch)
    response = client.get(reverse("graphyard:inventory_versions"))
    assert response.status_code == 200
    assert {
        (row["host"], row["kind"], row["basis"], row["installed"], row["snapshot"].pk)
        for row in response.context["page"]
    } == expected_rows
    assert len(decoded) == len(referenced)
    assert all(
        set(report["categories"]) != {"units"}
        and set(report["categories"]) != {"containers"}
        for report in decoded
    )

    response = client.get(reverse("graphyard:inventory_detail", args=["historical"]))
    assert response.status_code == 200
    assert response.context["latest"] == latest
    assert {
        category.name: category.latest_attempt
        for category in response.context["categories"]
    } == {
        "applications": attempt,
        "packages": packages,
        "units": units,
        "containers": success,
    }


@pytest.mark.django_db
def test_selected_host_reads_only_its_own_evidence(
    client, django_user_model, monkeypatch
):
    from django.urls import reverse
    from django.utils import timezone

    now = timezone.now()
    snapshots = {}
    for i in range(12):
        name = f"synthetic-{i:02}"
        snapshots[name] = _host(name, f"1.{i}", now)[1]
    client.force_login(django_user_model.objects.create_user(username="reader"))
    decoded = _count_decodes(monkeypatch)
    response = client.get(
        reverse("graphyard:inventory_versions"), {"host": "synthetic-03"}
    )
    assert response.status_code == 200
    assert decoded == [snapshots["synthetic-03"].report]
    rows = list(response.context["page"])
    assert {row["host"] for row in rows} == {"synthetic-03"}
    assert {row["installed"] for row in rows} == {"1.3"}
    # The host filter still offers every enabled host.
    assert len(response.context["hosts"]) == 12

    decoded.clear()
    response = client.get(
        reverse("graphyard:inventory_versions"), {"host": "not-registered"}
    )
    assert response.status_code == 200
    assert decoded == [] and len(response.context["page"]) == 0


@pytest.mark.django_db
def test_python_version_comparison_inherits_application_failure(
    client, django_user_model
):
    from django.urls import reverse
    from django.utils import timezone

    from graphyard.models import InventoryRelease

    now = timezone.now()
    host, snapshot = _host("synthetic", "1.0", now)
    snapshot.report["categories"]["applications"]["items"] = [
        {
            "id": f"{status}-app",
            "status": status,
            "items": {
                "installed_version": "1.0",
                "python": {
                    "status": "ok",
                    "items": [{"name": "synthetic-package", "version": "1.0"}],
                },
            },
        }
        for status in ("error", "ok")
    ]
    snapshot.save(update_fields=["report"])
    InventoryRelease.objects.create(
        source_id="pypi:synthetic-package",
        definition={
            "kind": "pypi",
            "project": "synthetic-package",
            "targets": [
                {"kind": "application", "name": "error-app"},
                {"kind": "application", "name": "ok-app"},
                {"kind": "python", "name": "synthetic-package"},
            ],
        },
        version="2.0",
        checked_at=now,
    )

    client.force_login(django_user_model.objects.create_user(username="reader"))
    response = client.get(reverse("graphyard:inventory_versions"))
    assert response.status_code == 200
    rows = {
        (row["application"], row["kind"]): row
        for row in response.context["page"]
        if row["kind"] in ("application", "python")
    }
    assert len(rows) == 4
    for kind in ("application", "python"):
        failed = rows["error-app", kind]
        current = rows["ok-app", kind]
        assert failed["status"] == "unknown"
        assert failed["reason"] == "Installed-version observation missing or stale"
        assert current["status"] == "update"
        assert failed["installed"] == current["installed"] == "1.0"
        assert failed["available"] == current["available"] == "2.0"
        assert failed["snapshot"] == current["snapshot"] == snapshot
    source_url = reverse(
        "graphyard:inventory_download", args=[host.host.host_id, snapshot.snapshot_id]
    )
    assert f'href="{source_url}"' in response.content.decode()


@pytest.mark.django_db(transaction=True)
def test_postgres_version_page_reads_one_repeatable_read_snapshot(
    client, django_user_model
):
    from django.db import connection
    from django.urls import reverse
    from django.utils import timezone

    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL isolation oracle; SQLite uses its configured mode")
    _host("synthetic", "1.0", timezone.now())
    client.force_login(django_user_model.objects.create_user(username="reader"))
    seen = []

    def inspect(execute, sql, params, many, context):
        if 'FROM "graphyard_inventorycategory"' in sql and not seen:
            with context["connection"].cursor() as cursor:
                cursor.execute("SHOW transaction_isolation")
                seen.append(cursor.fetchone()[0])
                cursor.execute("SHOW transaction_read_only")
                seen.append(cursor.fetchone()[0])
        return execute(sql, params, many, context)

    with connection.execute_wrapper(inspect):
        response = client.get(reverse("graphyard:inventory_versions"))
    assert response.status_code == 200
    assert seen == ["repeatable read", "on"]


@pytest.mark.parametrize("mode", ["IMMEDIATE", "DEFERRED"])
def test_version_page_survives_concurrent_report_and_pruning(tmp_path, mode):
    root = Path(__file__).resolve().parents[1]
    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join([str(root / "src"), str(root / "src/django")]),
    }
    result = subprocess.run(
        [sys.executable, __file__, str(tmp_path / "synthetic.sqlite3"), mode],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _probe(db_path, mode):
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
    from django.test.utils import setup_test_environment

    setup_test_environment()  # Record response contexts outside pytest.
    from concurrent.futures import ThreadPoolExecutor
    from uuid import uuid4
    from django.contrib.auth import get_user_model
    from django.core.management import call_command
    from django.db import OperationalError, connection, connections, transaction
    from django.test import Client
    from django.utils import timezone
    from graphyard.models import InventoryCategory, InventorySnapshot

    call_command("migrate", verbosity=0)
    with connection.cursor() as cursor:
        cursor.execute("PRAGMA journal_mode=WAL")
        assert cursor.fetchone()[0] == "wal"
    now = timezone.now()
    hosts = [_host(name, "1.0", now) for name in ("synthetic-a", "synthetic-b")]
    old_ids = [snapshot.pk for _, snapshot in hosts]
    user = get_user_model().objects.create_user(username="reader")
    client = Client()
    client.force_login(user)
    fired = False
    writer_outcomes = []

    def publish_and_prune():
        # A newer generation for both hosts, committed atomically, that also
        # prunes the reports the reader started with.
        with transaction.atomic():
            for host, old in hosts:
                new = InventorySnapshot.objects.create(
                    host=host,
                    snapshot_id=uuid4(),
                    observed_at=now,
                    digest="b" * 64,
                    report=_report("2.0"),
                )
                InventoryCategory.objects.filter(host=host).update(
                    latest_attempt=new, latest_success=new
                )
                old.delete()

    def writer():
        try:
            publish_and_prune()
            writer_outcomes.append("committed")
        except OperationalError:
            writer_outcomes.append("locked")
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
        response = client.get("/inventory/versions/")
    assert fired
    assert response.status_code == 200, response.status_code
    rows = list(response.context["page"])
    assert {row["host"] for row in rows} == {"synthetic-a", "synthetic-b"}, rows
    assert {row["installed"] for row in rows} == {"1.0"}, rows
    assert {row["snapshot"].pk for row in rows} == set(old_ids), rows
    assert writer_outcomes == (["committed"] if mode == "DEFERRED" else ["locked"])
    if writer_outcomes == ["locked"]:
        publish_and_prune()
    assert not InventorySnapshot.objects.filter(pk__in=old_ids).exists()
    response = client.get("/inventory/versions/")
    assert {row["installed"] for row in response.context["page"]} == {"2.0"}
    assert not connection.in_atomic_block
    connection.close()


if __name__ == "__main__":
    _probe(*sys.argv[1:])
