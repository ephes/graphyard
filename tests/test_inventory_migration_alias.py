"""Migration source-alias oracle using independent synthetic databases."""

import pytest
from django.db import IntegrityError, connections, transaction
from django.db.migrations.executor import MigrationExecutor

BEFORE = [("graphyard", "0012_ingest_token_subject_binding")]
AFTER = [("graphyard", "0013_inventory_single_enabled_credential")]


@pytest.mark.django_db(transaction=True, databases=["default", "secondary"])
@pytest.mark.parametrize("secondary_duplicates", [False, True])
def test_credential_migration_uses_source_alias_and_preserves_history(
    secondary_duplicates,
):
    models = {}
    originals = {}
    for alias in ("default", "secondary"):
        executor = MigrationExecutor(connections[alias])
        executor.migrate(BEFORE)
        apps = executor.loader.project_state(BEFORE).apps
        registry = (
            apps.get_model("graphyard", "HostRegistry")
            .objects.using(alias)
            .create(host_id="synthetic")
        )
        host = (
            apps.get_model("graphyard", "InventoryHost")
            .objects.using(alias)
            .create(host=registry)
        )
        credentials = apps.get_model("graphyard", "InventoryCredential")
        models[alias] = credentials
        credentials.objects.using(alias).create(host=host, digest="a" * 64)
        credentials.objects.using(alias).create(
            host=host,
            digest="b" * 64,
            enabled=alias == "default" or secondary_duplicates,
        )
        originals[alias] = list(
            credentials.objects.using(alias).order_by("id").values()
        )
    try:
        secondary = connections["secondary"]
        credentials = models["secondary"]
        if secondary_duplicates:
            with pytest.raises(
                RuntimeError, match="multiple enabled inventory credentials"
            ) as error:
                MigrationExecutor(secondary).migrate(AFTER)
            assert "a" * 64 not in str(error.value)
            assert "b" * 64 not in str(error.value)
            assert (
                list(credentials.objects.using("secondary").order_by("id").values())
                == originals["secondary"]
            )
            assert (
                "graphyard",
                "0013_inventory_single_enabled_credential",
            ) not in MigrationExecutor(secondary).loader.applied_migrations
            # Operator explicitly retires the chosen duplicate; migration never does.
            credentials.objects.using("secondary").filter(digest="b" * 64).update(
                enabled=False
            )
        expected = list(credentials.objects.using("secondary").order_by("id").values())
        MigrationExecutor(secondary).migrate(AFTER)
        assert (
            list(credentials.objects.using("secondary").order_by("id").values())
            == expected
        )
        with pytest.raises(IntegrityError), transaction.atomic(using="secondary"):
            credentials.objects.using("secondary").filter(digest="b" * 64).update(
                enabled=True
            )
        assert (
            list(credentials.objects.using("secondary").order_by("id").values())
            == expected
        )
        assert (
            list(models["default"].objects.using("default").order_by("id").values())
            == originals["default"]
        )
    finally:
        for alias, credentials in models.items():
            credentials.objects.using(alias).all().delete()
            MigrationExecutor(connections[alias]).migrate(AFTER)
