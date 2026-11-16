"""Portable inventory freshness admission using synthetic hosts only."""

import io
import math
from datetime import timedelta
from types import SimpleNamespace

import pytest
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError
from django.forms import modelform_factory
from django.utils import timezone

from graphyard.inventory import host_summary
from graphyard.models import HostRegistry, InventoryCredential, InventoryHost


@pytest.mark.django_db
@pytest.mark.parametrize("seconds", [59, 2**31, 10**100])
def test_invalid_staleness_refuses_before_rotation_or_any_query(
    seconds, django_assert_num_queries
):
    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="synthetic")
    )
    InventoryCredential.objects.create(host=host, digest="a" * 64)
    before = list(InventoryCredential.objects.values())
    output = io.StringIO()
    with (
        django_assert_num_queries(0),
        pytest.raises(CommandError, match="between 60 and 2147483647"),
    ):
        call_command(
            "create_inventory_credential",
            host="synthetic",
            rotate=True,
            warning_after_seconds=seconds,
            stdout=output,
        )
    assert output.getvalue() == ""
    assert list(InventoryCredential.objects.values()) == before
    host.refresh_from_db()
    assert host.warning_after_seconds == 172800


@pytest.mark.django_db
@pytest.mark.parametrize("seconds", [60, 2**31 - 1])
def test_boundary_staleness_round_trips(seconds):
    HostRegistry.objects.create(host_id="synthetic")
    call_command(
        "create_inventory_credential",
        host="synthetic",
        warning_after_seconds=seconds,
        stdout=io.StringIO(),
    )
    host = InventoryHost.objects.get()
    assert host.warning_after_seconds == seconds
    host.full_clean()
    now = timezone.now()
    observed = now - timedelta(seconds=seconds)
    snapshot = SimpleNamespace(
        observed_at=observed, received_at=now, report={"gaps": []}
    )
    summary = host_summary(host, now, state=(snapshot, []))
    assert math.isfinite(summary["warning_after_seconds"])
    assert summary["stale"] is False
    assert (
        host_summary(host, now + timedelta(seconds=1), state=(snapshot, []))["stale"]
        is True
    )


@pytest.mark.parametrize("seconds", [59, 2**31, 10**100])
def test_model_and_admin_share_staleness_range(seconds):
    field = InventoryHost._meta.get_field("warning_after_seconds")
    with pytest.raises(ValidationError):
        field.clean(seconds, InventoryHost())
    form_type = modelform_factory(InventoryHost, fields=["warning_after_seconds"])
    form = form_type(data={"warning_after_seconds": seconds})
    assert not form.is_valid()
