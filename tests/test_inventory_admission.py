"""Real independent-connection inventory admission with synthetic reports."""

import copy
import hashlib
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from threading import Barrier, Event

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection, connections
from django.test import Client, RequestFactory
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from uuid import uuid4

from graphyard import inventory
from graphyard.models import (
    HostRegistry,
    InventoryCategory,
    InventoryCredential,
    InventoryHost,
    InventorySnapshot,
)

pytestmark = pytest.mark.django_db(transaction=True)


@pytest.fixture
def admission():
    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="synthetic")
    )
    material = "synthetic-inventory-secret-for-tests-only"
    credential = InventoryCredential.objects.create(
        host=host, digest=hashlib.sha256(material.encode()).hexdigest()
    )
    body = {
        "schema_version": 1,
        "snapshot_id": str(uuid4()),
        "host": "synthetic",
        "observed_at": timezone.now().isoformat(),
        "collector": "synthetic/1",
        "categories": {
            name: {"status": "ok", "items": []} for name in inventory.CATEGORIES
        },
        "gaps": [],
    }
    return host, f"Bearer {credential.pk}.{material}", body


def post(auth, body):
    return Client().post(
        "/v1/inventory", body, content_type="application/json", HTTP_AUTHORIZATION=auth
    )


def test_ingest_locks_registry_before_inventory_host(admission):
    _, auth, body = admission
    with CaptureQueriesContext(connection) as queries:
        assert post(auth, body).status_code == 200
    sql = [q["sql"].lower() for q in queries]
    admitted_reads = [
        q
        for q in sql
        if q.startswith("select")
        and ('from "host_registry"' in q or 'from "graphyard_inventoryhost"' in q)
    ]
    # The first admitted read is the same registry row issuance locks, even on
    # SQLite where BEGIN IMMEDIATE supplies the actual serialization.
    assert 'from "host_registry"' in admitted_reads[0]


@pytest.mark.parametrize("remove_registry", [False, True])
def test_host_retired_after_authentication(admission, monkeypatch, remove_registry):
    host, auth, body = admission
    registry_id = host.host_id
    authenticated, resume = Event(), Event()
    authenticate = inventory.authenticate

    def pause(request):
        credential = authenticate(request)
        assert credential is not None
        authenticated.set()
        assert resume.wait(timeout=10)
        return credential

    def deliver():
        try:
            return post(auth, body)
        finally:
            connections.close_all()

    monkeypatch.setattr(inventory, "authenticate", pause)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(deliver)
        try:
            assert authenticated.wait(timeout=10)
            host.delete()
            if remove_registry:
                HostRegistry.objects.get(pk=registry_id).delete()
        finally:
            resume.set()
        response = future.result(timeout=15)

    assert response.status_code == 401
    assert response.json() == {"error": "credential revoked"}
    assert not InventorySnapshot.objects.exists()
    assert not InventoryCategory.objects.exists()


@pytest.mark.parametrize(
    ("length", "status"),
    [
        ("longer", 400),
        ("shorter", 400),
        ("0", 400),
        ("", 413),
        ("+1", 413),
        ("１２", 413),
        ("²", 413),
        pytest.param("9" * 5_000, 413, id="oversized"),
        ("padded", 200),
        (None, 200),
    ],
)
def test_inventory_declared_length(admission, monkeypatch, length, status):
    _, auth, body = admission
    request = RequestFactory().post(
        "/v1/inventory", body, content_type="application/json", HTTP_AUTHORIZATION=auth
    )
    size = int(request.META["CONTENT_LENGTH"])
    # Simulate a bounded adapter that delivers bytes independently of the header.
    if length is None:
        del request.META["CONTENT_LENGTH"]
    else:
        request.META["CONTENT_LENGTH"] = {
            "longer": str(size + 2),
            "shorter": str(size - 2),
            "padded": "0" * 5_000 + str(size),
        }.get(length, length)

    if status != 200:

        def unexpected(_body):
            pytest.fail("Invalid representation reached JSON decoding or hashing")

        monkeypatch.setattr(inventory, "decode_report", unexpected)
        monkeypatch.setattr(inventory, "canonical", unexpected)

    response = inventory.ingest(request)

    assert response.status_code == status
    assert InventorySnapshot.objects.count() == (1 if status == 200 else 0)
    if status != 200:
        assert not InventoryCategory.objects.exists()


@pytest.mark.parametrize("conflicting", [False, True])
def test_concurrent_snapshot_identity_is_immutable(admission, monkeypatch, conflicting):
    host, auth, body = admission
    other = copy.deepcopy(body)
    if conflicting:
        other["categories"]["packages"]["items"] = [
            {"name": "synthetic", "version": "2"}
        ]
    barrier = Barrier(2)
    authenticate = inventory.authenticate

    def synchronized(request):
        result = authenticate(request)
        barrier.wait(timeout=10)
        return result

    def deliver(data):
        try:
            response = post(auth, data)
            return response.status_code, response.json()
        finally:
            connections.close_all()

    with monkeypatch.context() as patch:
        patch.setattr(inventory, "authenticate", synchronized)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(deliver, data) for data in (body, other)]
            results = [f.result(timeout=15) for f in futures]
    # SQLite shared-cache contention is retryable; no partial snapshot may escape.
    for i, (code, _) in enumerate(results):
        if code == 503:
            response = post(auth, (body, other)[i])
            results[i] = response.status_code, response.json()
    assert sorted(code for code, _ in results) == (
        [200, 409] if conflicting else [200, 200]
    )
    snapshot = InventorySnapshot.objects.get(host=host)
    winner = next(
        i
        for i, (code, result) in enumerate(results)
        if code == 200 and not result["duplicate"]
    )
    assert snapshot.report == (body, other)[winner]
    before = snapshot.received_at
    assert post(auth, snapshot.report).json()["duplicate"] is True
    snapshot.refresh_from_db()
    assert snapshot.received_at == before
    categories = list(InventoryCategory.objects.filter(host=host))
    assert len(categories) == len(inventory.CATEGORIES)
    assert all(
        c.latest_attempt_id == c.latest_success_id == snapshot.pk for c in categories
    )
    if not conflicting:
        assert sorted(result["duplicate"] for _, result in results) == [False, True]


@pytest.mark.parametrize("first", ["ingest", "rotation"])
def test_rotation_and_ingest_share_admission_lock(admission, first):
    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL row-lock oracle; SQLite races are covered separately")
    _, auth, body = admission
    held = Event()
    release = Event()

    def pause(execute, sql, params, many, context):
        result = execute(sql, params, many, context)
        target = (
            'FROM "graphyard_inventorysnapshot"'
            if first == "ingest"
            else 'FROM "host_registry"'
        )
        if target in sql and (first == "ingest" or "FOR UPDATE" in sql):
            held.set()
            assert release.wait(timeout=10)
        return result

    def rotate():
        call_command(
            "create_inventory_credential",
            host="synthetic",
            rotate=True,
            stdout=StringIO(),
        )

    def owner():
        try:
            with connection.execute_wrapper(pause):
                if first == "ingest":
                    assert post(auth, body).status_code == 200
                else:
                    rotate()
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(owner)
        try:
            assert held.wait(timeout=10)
            with connection.cursor() as cursor:
                cursor.execute("SET lock_timeout = '150ms'")
            if first == "ingest":
                with pytest.raises(CommandError, match="conflicted"):
                    rotate()
                assert not InventorySnapshot.objects.exists()
            else:
                assert post(auth, body).status_code == 503
                assert not InventorySnapshot.objects.exists()
        finally:
            release.set()
            with connection.cursor() as cursor:
                cursor.execute("SET lock_timeout = 0")
        future.result(timeout=15)
    if first == "ingest":
        rotate()
    assert post(auth, body).status_code == 401
    assert InventorySnapshot.objects.count() == (1 if first == "ingest" else 0)


def test_contended_admission_can_retry_without_partial_state(admission):
    host, auth, body = admission
    held, release = Event(), Event()

    def hold():
        from django.db import transaction

        try:
            with transaction.atomic():
                HostRegistry.objects.select_for_update().get(pk=host.host_id)
                held.set()
                assert release.wait(timeout=10)
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(hold)
        try:
            assert held.wait(timeout=10)
            if connection.vendor == "postgresql":
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = '150ms'")
            assert post(auth, body).status_code == 503
            assert not InventorySnapshot.objects.exists()
            assert not InventoryCategory.objects.exists()
        finally:
            release.set()
            if connection.vendor == "postgresql":
                with connection.cursor() as cursor:
                    cursor.execute("SET lock_timeout = 0")
        future.result(timeout=15)
    assert post(auth, body).status_code == 200
    assert InventorySnapshot.objects.count() == 1
    assert InventoryCategory.objects.count() == len(inventory.CATEGORIES)
