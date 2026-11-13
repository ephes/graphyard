"""Only synthetic sentinels: errors must identify fields without leaking values."""

import io
import json
import traceback

import pytest
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard.models import MetricCollectionSpec

SECRET = "SYNTHETIC-PRIVATE-SECRET-79"
SPEC = {
    "name": "synthetic",
    "spec_type": "http_page_probe",
    "config": {"url": "https://example.test/"},
}


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", ""),
        ("spec_type", SECRET),
        ("interval_seconds", SECRET * 1000),
        ("enabled", SECRET),
        ("config", [{"password": SECRET}]),
    ],
    ids=["name", "spec_type", "interval_seconds", "enabled", "config"],
)
def test_shape_diagnostics_reveal_only_index_and_field(tmp_path, field, value):
    spec = {**SPEC, "name": SECRET, field: value}
    path = tmp_path / "synthetic.json"
    path.write_text(json.dumps([spec]))
    stdout, stderr = io.StringIO(), io.StringIO()
    with pytest.raises(CommandError) as error:
        call_command(
            "apply_metric_collection_specs",
            file=str(path),
            prune=True,
            stdout=stdout,
            stderr=stderr,
        )
    rendered = "".join(traceback.format_exception(error.value))
    assert SECRET not in rendered + stdout.getvalue() + stderr.getvalue()
    assert "index 0" in str(error.value)
    assert field in str(error.value)
    assert len(str(error.value)) < 150


@pytest.mark.django_db
@pytest.mark.parametrize("mode", ["create", "update", "unchanged"])
def test_model_config_errors_are_private_and_apply_rolls_back(tmp_path, mode):
    bad = {
        **SPEC,
        "name": SECRET,
        "config": {"request_timeout_seconds": {"password": SECRET}},
    }
    existing = MetricCollectionSpec.objects.create(
        name=SECRET,
        spec_type="http_page_probe",
        config=bad["config"] if mode == "unchanged" else SPEC["config"],
    )
    if mode == "create":
        existing.name = "kept"
        existing.save()
    before = list(MetricCollectionSpec.objects.values())
    path = tmp_path / "synthetic.json"
    # First spec would be created; validation failure must roll it back too.
    path.write_text(json.dumps([{**SPEC, "name": "new"}, bad]))
    stdout, stderr = io.StringIO(), io.StringIO()
    with pytest.raises(CommandError) as error:
        call_command(
            "apply_metric_collection_specs",
            file=str(path),
            prune=True,
            stdout=stdout,
            stderr=stderr,
        )
    assert SECRET not in "".join(traceback.format_exception(error.value))
    assert "index 1" in str(error.value)
    assert "config.request_timeout_seconds" in str(error.value)
    assert error.value.__cause__ is error.value.__context__ is None
    assert stdout.getvalue() == stderr.getvalue() == ""
    assert list(MetricCollectionSpec.objects.values()) == before


@pytest.mark.django_db
def test_unknown_model_validation_fields_and_messages_are_never_echoed(
    tmp_path, monkeypatch
):
    def reject(*args, **kwargs):
        raise ValidationError({SECRET: SECRET, "config": SECRET})

    monkeypatch.setattr(MetricCollectionSpec, "full_clean", reject)
    path = tmp_path / "synthetic.json"
    path.write_text(json.dumps([SPEC]))
    with pytest.raises(CommandError) as error:
        call_command("apply_metric_collection_specs", file=str(path))
    assert "config" in str(error.value)
    assert SECRET not in "".join(traceback.format_exception(error.value))


def test_duplicate_names_do_not_disclose_the_name(tmp_path):
    path = tmp_path / "synthetic.json"
    spec = {**SPEC, "name": SECRET}
    path.write_text(json.dumps([spec, spec]))
    with pytest.raises(CommandError, match="index 1.*duplicated name") as error:
        call_command("apply_metric_collection_specs", file=str(path))
    assert SECRET not in str(error.value)


@pytest.mark.django_db
@pytest.mark.parametrize(
    "field,value", [("name", SECRET * 20), ("interval_seconds", 10**100)]
)
def test_model_field_limits_are_reported_without_the_value(tmp_path, field, value):
    path = tmp_path / "synthetic.json"
    path.write_text(json.dumps([{**SPEC, field: value}]))
    with pytest.raises(CommandError, match=f"index 0.*{field}") as error:
        call_command("apply_metric_collection_specs", file=str(path))
    assert SECRET not in "".join(traceback.format_exception(error.value))
    assert len(str(error.value)) < 150
