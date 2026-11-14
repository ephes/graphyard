"""Health cache isolation and real independent-connection snapshot evidence."""

import os
from pathlib import Path
import subprocess
import sys

import pytest
from django.db import DatabaseError


@pytest.mark.django_db
@pytest.mark.parametrize(
    "state",
    [
        "anonymous",
        "authenticated",
        "expired",
        "logout",
        "db-error",
        "influx-error",
        "anonymous-influx-error",
    ],
)
def test_health_cache_policy_and_private_projection(
    client, django_user_model, monkeypatch, state
):
    from graphyard.models import PipelineHeartbeat
    from django.utils import timezone

    marker = "synthetic-private-controller"
    PipelineHeartbeat.objects.create(
        name="condition_evaluator",
        last_success=timezone.now(),
        last_error=marker,
        details={"internal": marker},
    )
    monkeypatch.setattr(
        "graphyard.views.influx_health",
        lambda: {"status": "pass", "message": marker, "name": marker},
    )
    if state in {"authenticated", "logout", "db-error", "influx-error"}:
        client.force_login(django_user_model.objects.create_user(username="synthetic"))
        prior = client.get("/v1/health")
        assert marker in prior.content.decode()
        if state == "logout":
            client.logout()
    elif state == "expired":
        client.cookies["sessionid"] = "synthetic-expired"
    if state == "db-error":

        def fail(*args, **kwargs):
            raise DatabaseError(marker)

        monkeypatch.setattr("graphyard.views.PipelineHeartbeat.objects.filter", fail)
    if state in {"influx-error", "anonymous-influx-error"}:

        def fail():
            raise RuntimeError(marker)

        monkeypatch.setattr("graphyard.views.influx_health", fail)
    response = client.get("/v1/health")
    assert response.status_code == (503 if state == "db-error" else 200)
    assert response["Cache-Control"] == "private, no-store"
    assert "cookie" in {h.strip().lower() for h in response["Vary"].split(",")}
    detailed = state in {"authenticated", "influx-error"}
    assert (marker in response.content.decode()) == detailed
    if not detailed:
        assert set(response.json()["components"]["influxdb"]) == {"status"}


@pytest.mark.django_db
def test_health_resolves_session_login_outside_the_read_only_snapshot(
    client, django_user_model, monkeypatch
):
    """Session login may write (flush, key rotation); PostgreSQL's snapshot is read-only."""
    from contextlib import contextmanager

    from django.contrib import auth

    import graphyard.views as views

    inside = {"snapshot": False, "resolved_inside": False, "calls": 0}
    real_snapshot, real_get_user = views.read_snapshot, auth.get_user

    @contextmanager
    def snapshot():
        with real_snapshot():
            inside["snapshot"] = True
            try:
                yield
            finally:
                inside["snapshot"] = False

    def get_user(request):
        inside["calls"] += 1
        inside["resolved_inside"] |= inside["snapshot"]
        return real_get_user(request)

    monkeypatch.setattr(views, "read_snapshot", snapshot)
    monkeypatch.setattr(auth, "get_user", get_user)
    client.force_login(django_user_model.objects.create_user(username="synthetic"))
    response = client.get("/v1/health")
    assert response.status_code == 200
    # The authenticated caller still gets the detailed projection.
    assert "details" in response.json()["components"]["pipelines"]["metric_ingest"]
    assert inside["calls"] >= 1
    assert not inside["resolved_inside"]


@pytest.mark.django_db
def test_health_session_lookup_failure_is_database_evidence(client, monkeypatch):
    """A session lookup that fails is a database failure, not an anonymous caller."""
    from contextlib import contextmanager

    import graphyard.views as views

    marker = "synthetic-session-lookup-marker"
    entered = []

    @contextmanager
    def snapshot():
        entered.append(True)
        yield

    def fail(request):
        raise DatabaseError(marker)

    monkeypatch.setattr(views, "read_snapshot", snapshot)
    monkeypatch.setattr("django.contrib.auth.get_user", fail)
    monkeypatch.setattr(
        "graphyard.views.influx_health",
        lambda: {"status": "pass", "message": marker, "name": marker},
    )
    response = client.get("/v1/health")
    assert response.status_code == 503
    assert response.json()["components"]["database"]["status"] == "critical"
    assert set(response.json()["components"]["influxdb"]) == {"status"}
    assert marker not in response.content.decode()
    assert entered == []


@pytest.mark.django_db
def test_health_method_error_is_not_cacheable(client):
    response = client.post("/v1/health")
    assert response.status_code == 405
    assert response["Cache-Control"] == "private, no-store"
    assert "Cookie" in response["Vary"]


def test_health_vary_preserves_existing_fields():
    from django.http import HttpResponse
    from django.test import RequestFactory
    from graphyard.middleware import InventoryCachePolicyMiddleware

    response = HttpResponse(headers={"Vary": "Accept-Language, Cookie"})
    result = InventoryCachePolicyMiddleware(lambda request: response)(
        RequestFactory().get("/v1/health")
    )
    assert result["Vary"] == "Accept-Language, Cookie"


@pytest.mark.parametrize("mode", ["DEFERRED", "IMMEDIATE"])
def test_health_reads_one_committed_generation(tmp_path, mode):
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


def _probe(db_path, mode):
    os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.test"
    import django
    from django.conf import settings

    settings.DATABASES["default"]["NAME"] = db_path
    settings.ALLOWED_HOSTS = ["testserver"]
    settings.DATABASES["default"]["OPTIONS"]["transaction_mode"] = mode
    settings.DATABASES["default"]["OPTIONS"]["timeout"] = 0.05
    settings.GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS = 50
    django.setup()
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import patch
    from django.core.management import call_command
    from django.db import OperationalError, connection, connections, transaction
    from django.test import Client
    from django.utils import timezone
    from graphyard.models import ConditionDefinition, PipelineHeartbeat

    call_command("migrate", verbosity=0)
    with connection.cursor() as cursor:
        cursor.execute("PRAGMA journal_mode=WAL")
        assert cursor.fetchone()[0] == "wal"
    now = timezone.now()
    ConditionDefinition.objects.create(name="synthetic", status="ok")
    names = ("metric_ingest", "metric_collectors", "condition_evaluator")
    for name in names:
        PipelineHeartbeat.objects.create(name=name, status="ok", last_success=now)
    fired = False
    committed = False
    condition_queries = []

    def writer():
        nonlocal committed
        try:
            with transaction.atomic():
                ConditionDefinition.objects.update(status="critical")
                PipelineHeartbeat.objects.update(
                    status="critical", last_error="synthetic"
                )
            committed = True
        except OperationalError as err:
            # BEGIN IMMEDIATE briefly reserves the writer; its retry commits
            # after the reader releases the snapshot.
            assert mode == "IMMEDIATE" and "locked" in str(err), err
        finally:
            connections.close_all()

    def pause(execute, sql, params, many, context):
        nonlocal fired
        result = execute(sql, params, many, context)
        if (
            sql.lstrip().upper().startswith("SELECT")
            and '"condition_definition"' in sql
        ):
            condition_queries.append(sql)
        if not fired and '"pipeline_heartbeat"' in sql:
            fired = True
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(writer).result(timeout=10)
        return result

    def remote_health():
        assert not connection.in_atomic_block, "remote I/O holds the DB snapshot"
        return {"status": "pass"}

    with (
        patch("graphyard.views.influx_health", remote_health),
        connection.execute_wrapper(pause),
    ):
        response = Client().get("/v1/health")
    assert fired and response.status_code == 200, response.content
    body = response.json()
    assert body["conditions"] == {"total": 1, "ok": 1, "warning": 0, "critical": 0}, (
        body
    )
    assert all(p["status"] == "ok" for p in body["components"]["pipelines"].values()), (
        body
    )
    assert len(condition_queries) == 1, condition_queries
    if mode == "DEFERRED":
        assert committed
    if not committed:
        writer()
    assert committed and ConditionDefinition.objects.get().status == "critical"
    connection.close()


if __name__ == "__main__":
    _probe(*sys.argv[1:])
