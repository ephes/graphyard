"""Synthetic admin writes must retain collector-owned state."""

import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from django.db import connection, connections, transaction
from django.utils import timezone

from graphyard.models import HostRegistry, IngestToken, ServiceRegistry, SubjectRegistry
from test_admin_definition_ownership import edit_form


def post_data(client, row):
    url = f"/admin/graphyard/{row._meta.model_name}/{row.pk}/change/"
    form = client.get(url).context["adminform"].form
    data = {
        name: json.dumps(value)
        if isinstance(value, dict)
        else ""
        if value is None
        else value
        for name, value in ((name, form.initial.get(name, "")) for name in form.fields)
    }
    return url, data


def independent_update(model, pk, changes):
    def update():
        try:
            model.objects.filter(pk=pk).update(**changes)
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(update).result(timeout=5)


@pytest.mark.django_db(transaction=True)
def test_token_admin_preserves_current_usage_and_rejects_stale_revocation(admin_client):
    row = IngestToken.objects.create(name="synthetic", token_hash="synthetic-old")
    url, data = post_data(admin_client, row)
    now = timezone.now()
    independent_update(IngestToken, row.pk, {"last_used_at": now})
    response = admin_client.post(url, {**data, "name": "edited"})
    assert response.status_code == 302
    row.refresh_from_db()
    assert row.last_used_at == now
    url, data = post_data(admin_client, row)
    independent_update(
        IngestToken,
        row.pk,
        {
            "token_hash": "synthetic-rotated",
            "enabled": False,
            "revoked_at": now,
        },
    )
    response = admin_client.post(url, {**data, "enabled": True})
    assert response.status_code == 200
    assert "Reload" in str(response.context["adminform"].form.non_field_errors())
    row.refresh_from_db()
    assert not row.enabled
    assert row.token_hash == "synthetic-rotated"
    assert row.revoked_at == now


@pytest.mark.django_db
def test_token_form_narrow_save_preserves_telemetry():
    row = IngestToken.objects.create(name="synthetic", token_hash="synthetic-old")
    form = edit_form(row, name="edited")
    now = timezone.now()
    IngestToken.objects.filter(pk=row.pk).update(last_used_at=now)
    assert form.is_valid(), form.errors
    form.save()
    row.refresh_from_db()
    assert row.last_used_at == now


@pytest.mark.django_db(transaction=True)
def test_token_admin_sqlite_writer_conflict_is_controlled(admin_client):
    if connection.vendor != "sqlite":
        pytest.skip("SQLite writer admission contract")
    row = IngestToken.objects.create(name="synthetic", token_hash="synthetic")
    url, data = post_data(admin_client, row)
    locked, release = Event(), Event()

    def hold_writer():
        try:
            with transaction.atomic():
                IngestToken.objects.filter(pk=row.pk).update(name="held")
                locked.set()
                assert release.wait(5)
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=1) as pool:
        worker = pool.submit(hold_writer)
        assert locked.wait(5)
        try:
            response = admin_client.post(url, data)
            assert response.status_code == 409
            assert b"Reload" in response.content
            assert b"locked" not in response.content
        finally:
            release.set()
            worker.result(timeout=5)


def registry_row(kind):
    host = HostRegistry.objects.create(host_id="edge")
    if kind == "host":
        return host
    if kind == "service":
        return ServiceRegistry.objects.create(service_id="mail", host=host)
    return SubjectRegistry.objects.create(subject_type="host", subject_id="edge")


@pytest.mark.django_db
@pytest.mark.parametrize("kind", ["host", "service", "subject"])
def test_registry_narrow_save_retains_observations(kind):
    row = registry_row(kind)
    form = edit_form(row, display_name="Operator label")
    expected = {"last_seen_at": timezone.now()}
    if kind == "subject":
        expected["source_system"] = "synthetic-collector"
    type(row).objects.filter(pk=row.pk).update(**expected)
    assert form.is_valid(), form.errors
    form.save()
    row.refresh_from_db()
    assert row.display_name == "Operator label"
    assert all(getattr(row, key) == value for key, value in expected.items())


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("kind", ["host", "service", "subject"])
def test_registry_post_retains_ingest_and_refuses_changed_identity(
    admin_client, monkeypatch, kind
):
    from django.test import Client

    row = registry_row(kind)
    url, data = post_data(admin_client, row)
    credential = IngestToken(name="synthetic")
    credential.set_token("synthetic-credential")
    credential.save()
    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))

    def ingest():
        try:
            return (
                Client()
                .post(
                    "/v1/metrics",
                    data=[
                        {
                            "ts": timezone.now().isoformat(),
                            "host": "edge",
                            "service": "mail",
                            "metric": "m",
                            "value": 1,
                        }
                    ],
                    content_type="application/json",
                    HTTP_AUTHORIZATION="Bearer synthetic-credential",
                )
                .status_code
            )
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(ingest).result(timeout=5) == 202
    expected = type(row).objects.get(pk=row.pk).last_seen_at
    assert expected is not None
    response = admin_client.post(url, {**data, "display_name": "Operator label"})
    assert response.status_code == 302
    row.refresh_from_db()
    assert row.last_seen_at == expected
    url, data = post_data(admin_client, row)
    if kind == "service":
        other = HostRegistry.objects.create(host_id="other")
        changes = {"host_id": other.pk}
    elif kind == "host":
        changes = {"host_id": "renamed"}
    else:
        changes = {"subject_id": "renamed"}
    independent_update(type(row), row.pk, changes)
    response = admin_client.post(url, {**data, "display_name": "Stale label"})
    assert response.status_code == 200
    assert "Reload" in str(response.context["adminform"].form.non_field_errors())
    row.refresh_from_db()
    assert row.display_name == "Operator label"


@pytest.mark.django_db(transaction=True)
def test_token_admin_refuses_actual_rotation_on_independent_connection(admin_client):
    from io import StringIO
    from django.core.management import call_command

    row = IngestToken.objects.create(name="synthetic", token_hash="synthetic")
    url, data = post_data(admin_client, row)

    def rotate():
        try:
            call_command(
                "create_ingest_token", name="synthetic", rotate=True, stdout=StringIO()
            )
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(rotate).result(timeout=5)
    response = admin_client.post(url, {**data, "name": "stale", "enabled": True})
    assert response.status_code == 200
    assert "Reload" in str(response.context["adminform"].form.non_field_errors())
    row.refresh_from_db()
    assert not row.enabled
    assert row.name == f"synthetic (retired #{row.pk})"
    assert IngestToken.objects.get(name="synthetic").pk != row.pk


@pytest.mark.django_db
@pytest.mark.parametrize("kind", ["token", "host", "service", "subject"])
@pytest.mark.parametrize("deleted", [False, True])
def test_save_rechecks_identity_and_security_after_validation(kind, deleted):
    from django.core.exceptions import ValidationError

    row = (
        IngestToken.objects.create(name="synthetic", token_hash="synthetic")
        if kind == "token"
        else registry_row(kind)
    )
    form = edit_form(
        row, **({"name": "edited"} if kind == "token" else {"display_name": "edited"})
    )
    assert form.is_valid(), form.errors
    rows = type(row).objects.filter(pk=row.pk)
    if deleted:
        rows.delete()
    elif kind == "token":
        rows.update(bound_subject_id="other")
    elif kind == "service":
        rows.update(host=None)
    elif kind == "host":
        rows.update(host_id="other")
    else:
        rows.update(subject_id="other")
    before = list(rows.values())
    with pytest.raises(ValidationError, match="Reload"):
        form.save()
    assert list(rows.values()) == before
