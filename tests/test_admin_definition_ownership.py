"""Admin edits own definitions; runners own observations and scheduling."""

import json

import pytest
from django.contrib import admin
from django.forms.models import model_to_dict
from django.test import RequestFactory
from django.utils import timezone

from graphyard.models import ConditionDefinition, MetricCollectionSpec
from graphyard import services


def edit_form(row, **changes):
    request = RequestFactory().get("/admin/")
    request.user = type("Superuser", (), {"has_perm": lambda *args: True})()
    form_class = admin.site._registry[type(row)].get_form(request, row)
    initial = form_class(instance=row)
    data = model_to_dict(row)
    data.update(initial.initial)
    data.update(changes)
    for key, value in list(data.items()):
        if isinstance(value, dict):
            data[key] = json.dumps(value)
    return form_class(data=data, instance=row)


@pytest.mark.django_db
@pytest.mark.parametrize("kind", ["spec", "condition"])
def test_definition_edit_preserves_concurrent_runtime_publication(kind):
    if kind == "spec":
        row = MetricCollectionSpec.objects.create(name="synthetic", config={})
        form = edit_form(row, interval_seconds=120)
        definition = {
            "name": row.name,
            "spec_type": row.spec_type,
            "interval_seconds": 60,
            "config_text": "{}",
        }
        assert services._finish_due_collection_spec(
            row.pk,
            0,
            definition=definition,
            next_run_time=900,
            last_status="critical",
            last_error="synthetic failure",
            last_run_at=timezone.now(),
        )
        fields = ("next_run_time", "last_status", "last_error", "last_run_at")
    else:
        row = ConditionDefinition.objects.create(
            name="synthetic", metric_name="cpu", warning_threshold=50
        )
        form = edit_form(row, warning_threshold=60)
        assert services._publish_condition_result(
            row.pk,
            timezone.now(),
            definition={},
            status="critical",
            message="synthetic breach",
            last_value=99,
        )
        fields = ("status", "message", "last_value", "last_evaluated")
    expected = type(row).objects.values(*fields).get(pk=row.pk)
    assert form.is_valid(), form.errors
    form.save()
    assert type(row).objects.values(*fields).get(pk=row.pk) == expected


@pytest.mark.django_db
@pytest.mark.parametrize("kind", ["spec", "condition"])
def test_stale_definition_and_secret_rotation_conflict(kind):
    if kind == "spec":
        row = MetricCollectionSpec.objects.create(
            name="synthetic", config={"token": "old"}
        )
        changes = {"config": {"token": "rotated"}}
    else:
        row = ConditionDefinition.objects.create(
            name="synthetic", metric_name="cpu", warning_threshold=50
        )
        changes = {"warning_threshold": 70}
    stale = edit_form(row, name="stale overwrite")
    current = edit_form(type(row).objects.get(pk=row.pk), **changes)
    assert current.is_valid(), current.errors
    current.save()
    assert not stale.is_valid()
    assert "reload" in str(stale.errors).lower()
    row.refresh_from_db()
    assert row.name == "synthetic"
    if kind == "spec":
        assert row.config["token"] == "rotated"


@pytest.mark.django_db
def test_runner_fields_not_editable():
    spec = MetricCollectionSpec.objects.create(name="synthetic")
    condition = ConditionDefinition.objects.create(
        name="synthetic", metric_name="cpu", warning_threshold=1
    )
    assert "next_run_time" not in edit_form(spec).fields
    assert "status" not in edit_form(condition).fields


@pytest.mark.django_db
@pytest.mark.parametrize("kind", ["spec", "condition"])
def test_admin_post_preserves_runtime_and_returns_stale_form_conflict(
    admin_client, kind
):
    if kind == "spec":
        row = MetricCollectionSpec.objects.create(
            name="synthetic", config={"token": "synthetic-secret"}
        )
        expected = {
            "next_run_time": 900,
            "last_status": "critical",
            "last_error": "synthetic failure",
        }
    else:
        row = ConditionDefinition.objects.create(
            name="synthetic", metric_name="cpu", warning_threshold=50
        )
        expected = {
            "status": "critical",
            "message": "synthetic breach",
            "last_value": 99,
            "last_evaluated": timezone.now(),
        }
    url = f"/admin/graphyard/{row._meta.model_name}/{row.pk}/change/"
    response = admin_client.get(url)
    assert response.status_code == 200
    assert b"synthetic-secret" not in response.content
    form = response.context["adminform"].form
    data = {name: form.initial.get(name, "") for name in form.fields}
    data = {
        name: json.dumps(value)
        if isinstance(value, dict)
        else ""
        if value is None
        else value
        for name, value in data.items()
    }
    data["name"] = "edited"
    type(row).objects.filter(pk=row.pk).update(**expected)
    response = admin_client.post(url, data)
    assert response.status_code == 302
    row.refresh_from_db()
    assert {name: getattr(row, name) for name in expected} == expected
    if kind == "spec":
        assert row.config["token"] == "synthetic-secret"
    # A fresh POST constructs its instance from the database, while the signed
    # hidden revision still describes the previous GET.
    response = admin_client.post(url, {**data, "name": "stale overwrite"})
    assert response.status_code == 200
    assert "Reload" in str(response.context["adminform"].form.non_field_errors())
    row.refresh_from_db()
    assert row.name == "edited"


@pytest.mark.django_db
@pytest.mark.parametrize("kind", ["spec", "condition"])
def test_invalid_bound_definition_cannot_be_saved(kind):
    if kind == "spec":
        row = MetricCollectionSpec.objects.create(
            name="synthetic", spec_type="http_page_probe", config={}
        )
        form = edit_form(row, config={"verify_tls": "false"})
    else:
        row = ConditionDefinition.objects.create(
            name="synthetic", metric_name="cpu", warning_threshold=50
        )
        form = edit_form(row, warning_threshold="NaN")
    before = type(row).objects.values().get(pk=row.pk)
    assert not form.is_valid()
    with pytest.raises(ValueError, match="didn't validate"):
        form.save()
    assert type(row).objects.values().get(pk=row.pk) == before
