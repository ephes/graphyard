"""Synthetic credential lifecycle and migration checks on the selected DB backend."""

from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from threading import Barrier

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import IntegrityError, connections, transaction
from django.db.migrations.executor import MigrationExecutor

from graphyard.models import HostRegistry, InventoryCredential, InventoryHost


@pytest.mark.django_db
def test_only_one_enabled_credential_per_host():
    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="synthetic")
    )
    InventoryCredential.objects.create(host=host, digest="a" * 64)
    with pytest.raises(IntegrityError), transaction.atomic():
        InventoryCredential.objects.create(host=host, digest="b" * 64)
    InventoryCredential.objects.create(host=host, digest="c" * 64, enabled=False)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("rotate", [False, True])
def test_concurrent_issuance_preserves_one_enabled_credential(rotate):
    registered = HostRegistry.objects.create(host_id="synthetic")
    if rotate:
        host = InventoryHost.objects.create(host=registered)
        InventoryCredential.objects.create(host=host, digest="a" * 64)
    barrier = Barrier(2)

    def issue():
        output = StringIO()
        try:
            barrier.wait(timeout=10)
            call_command(
                "create_inventory_credential",
                host="synthetic",
                rotate=rotate,
                stdout=output,
            )
            return True
        except CommandError:
            assert output.getvalue() == ""
            return False
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: issue(), range(2)))
    assert any(results)
    assert (
        InventoryCredential.objects.filter(host__host=registered, enabled=True).count()
        == 1
    )
    if not rotate:
        assert sum(results) == 1


@pytest.mark.django_db(transaction=True)
def test_migration_refuses_duplicates_without_changing_credentials():
    connection = connections["default"]
    before = [("graphyard", "0012_ingest_token_subject_binding")]
    after = [("graphyard", "0013_inventory_single_enabled_credential")]
    executor = MigrationExecutor(connection)
    executor.migrate(before)
    apps = executor.loader.project_state(before).apps
    registry = apps.get_model("graphyard", "HostRegistry").objects.create(
        host_id="synthetic"
    )
    host = apps.get_model("graphyard", "InventoryHost").objects.create(host=registry)
    credentials = apps.get_model("graphyard", "InventoryCredential")
    for digest in ("a" * 64, "b" * 64):
        credentials.objects.create(host=host, digest=digest)
    original = list(credentials.objects.values().order_by("id"))
    try:
        with pytest.raises(
            RuntimeError, match="multiple enabled inventory credentials"
        ) as error:
            MigrationExecutor(connection).migrate(after)
        assert str(host.pk) in str(error.value)
        assert "a" * 64 not in str(error.value)
        assert list(credentials.objects.values().order_by("id")) == original
    finally:
        credentials.objects.all().delete()
        MigrationExecutor(connection).migrate(after)
