"""Synthetic routed credentials must retire and replace on one database."""

from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import IntegrityError, router
from django.db.models.query import QuerySet

from graphyard.models import (
    HostRegistry,
    InventoryHost,
    InventoryCredential,
    IngestToken,
)


class SecondaryRouter:
    def db_for_read(self, model, **hints):
        return "secondary" if model._meta.app_label == "graphyard" else None

    db_for_write = db_for_read


@pytest.mark.django_db(transaction=True, databases=["default", "secondary"])
@pytest.mark.parametrize("kind", ["ingest", "inventory"])
def test_routed_rotation_failure_rolls_back_retirement(monkeypatch, kind):
    if kind == "ingest":
        IngestToken.objects.using("secondary").create(
            name="synthetic", token_hash="synthetic"
        )
        model = IngestToken
        command, args = "create_ingest_token", {"name": "synthetic"}
        save = model.save

        def fail_new(instance, *args, **kwargs):
            if instance.pk is None:
                raise IntegrityError("synthetic creation failure")
            return save(instance, *args, **kwargs)

        monkeypatch.setattr(model, "save", fail_new)
    else:
        registered = HostRegistry.objects.using("secondary").create(host_id="synthetic")
        host = InventoryHost.objects.using("secondary").create(host=registered)
        model = InventoryCredential
        model.objects.using("secondary").create(host=host, digest="a" * 64)
        command, args = "create_inventory_credential", {"host": "synthetic"}
        create = QuerySet.create

        def fail_new(rows, **values):
            if rows.model is model:
                raise IntegrityError("synthetic creation failure")
            return create(rows, **values)

        monkeypatch.setattr(QuerySet, "create", fail_new)
    before = list(model.objects.using("secondary").values())
    monkeypatch.setattr(router, "routers", [SecondaryRouter()])
    output = StringIO()
    with pytest.raises(CommandError):
        call_command(command, rotate=True, stdout=output, **args)
    assert list(model.objects.using("secondary").values()) == before
    assert model.objects.using("default").count() == 0
    assert output.getvalue() == ""


@pytest.mark.django_db(transaction=True, databases=["default", "secondary"])
@pytest.mark.parametrize("kind", ["ingest", "inventory"])
def test_explicit_database_issuance(monkeypatch, kind):
    # Explicit --database follows ordinary Django precedence over a router.
    if kind == "inventory":
        HostRegistry.objects.using("secondary").create(host_id="synthetic")
    output = StringIO()
    call_command(
        "create_ingest_token" if kind == "ingest" else "create_inventory_credential",
        database="secondary",
        stdout=output,
        **({"name": "synthetic"} if kind == "ingest" else {"host": "synthetic"}),
    )
    model = IngestToken if kind == "ingest" else InventoryCredential
    assert model.objects.using("secondary").filter(enabled=True).count() == 1
    assert model.objects.using("default").count() == 0
    assert output.getvalue()


@pytest.mark.django_db(databases=["default", "secondary"])
def test_split_inventory_router_refused_before_queries(
    monkeypatch, django_assert_num_queries
):
    class SplitRouter(SecondaryRouter):
        def db_for_write(self, model, **hints):
            return "default" if model is InventoryHost else "secondary"

    monkeypatch.setattr(router, "routers", [SplitRouter()])
    with django_assert_num_queries(0), pytest.raises(CommandError, match="database"):
        call_command("create_inventory_credential", host="synthetic")
