from __future__ import annotations

import io
import json

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard.models import MetricCollectionSpec, MetricCollectionSpecType


def _write_specs_file(tmp_path, payload: object) -> str:
    path = tmp_path / "metric-collection-specs.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


@pytest.mark.django_db
def test_apply_metric_collection_specs_creates_specs_from_root_object(tmp_path):
    spec_file = _write_specs_file(
        tmp_path,
        {
            "metric_collection_specs": [
                {
                    "name": "Home Assistant Environment Scan",
                    "enabled": True,
                    "spec_type": MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
                    "interval_seconds": 60,
                    "config": {
                        "base_url": "http://macmini.local:10020",
                        "access_token": "token",
                        "host_id": "homeassistant",
                        "service_id": "homeassistant",
                    },
                }
            ]
        },
    )

    stdout = io.StringIO()
    call_command("apply_metric_collection_specs", "--file", spec_file, stdout=stdout)

    spec = MetricCollectionSpec.objects.get(name="Home Assistant Environment Scan")
    assert spec.enabled is True
    assert spec.spec_type == MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN
    assert spec.interval_seconds == 60
    assert spec.config["base_url"] == "http://macmini.local:10020"
    assert "created=1" in stdout.getvalue()
    assert "updated=0" in stdout.getvalue()


@pytest.mark.django_db
def test_apply_metric_collection_specs_accepts_unifi_device_traffic_spec(tmp_path):
    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": "UniFi USW Uplink Traffic",
                "enabled": True,
                "spec_type": MetricCollectionSpecType.UNIFI_DEVICE_TRAFFIC,
                "interval_seconds": 60,
                "config": {
                    "base_url": "https://127.0.0.1:8443",
                    "username": "homeassistant",
                    "password": "secret",
                    "site_id": "default",
                    "device_name": "USW Pro XG 8 PoE",
                    "subject_id": "usw_pro_xg_8_poe",
                },
            }
        ],
    )

    call_command("apply_metric_collection_specs", "--file", spec_file)

    spec = MetricCollectionSpec.objects.get(name="UniFi USW Uplink Traffic")
    assert spec.spec_type == MetricCollectionSpecType.UNIFI_DEVICE_TRAFFIC
    assert spec.config["device_name"] == "USW Pro XG 8 PoE"


@pytest.mark.django_db
def test_apply_metric_collection_specs_accepts_http_page_probe_spec(tmp_path):
    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": "Wersdoerfer Blog Page Probe",
                "enabled": True,
                "spec_type": MetricCollectionSpecType.HTTP_PAGE_PROBE,
                "interval_seconds": 300,
                "config": {
                    "url": "https://wersdoerfer.de/blogs/ephes_blog/",
                    "subject_id": "wersdoerfer_blog",
                    "collector_host": "macmini",
                },
            }
        ],
    )

    call_command("apply_metric_collection_specs", "--file", spec_file)

    spec = MetricCollectionSpec.objects.get(name="Wersdoerfer Blog Page Probe")
    assert spec.spec_type == MetricCollectionSpecType.HTTP_PAGE_PROBE
    assert spec.interval_seconds == 300
    assert spec.config["subject_id"] == "wersdoerfer_blog"


@pytest.mark.django_db
def test_apply_metric_collection_specs_is_idempotent_for_matching_state(tmp_path):
    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": "Home Assistant Environment Scan",
                "enabled": True,
                "spec_type": MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
                "interval_seconds": 60,
                "config": {
                    "base_url": "http://macmini.local:10020",
                    "access_token": "token",
                },
            }
        ],
    )

    call_command("apply_metric_collection_specs", "--file", spec_file)

    stdout = io.StringIO()
    call_command("apply_metric_collection_specs", "--file", spec_file, stdout=stdout)

    spec = MetricCollectionSpec.objects.get(name="Home Assistant Environment Scan")
    assert spec.interval_seconds == 60
    assert "created=0" in stdout.getvalue()
    assert "updated=0" in stdout.getvalue()
    assert "unchanged=1" in stdout.getvalue()


@pytest.mark.django_db
def test_apply_metric_collection_specs_updates_existing_spec_and_resets_next_run_time(
    tmp_path,
):
    spec = MetricCollectionSpec.objects.create(
        name="Home Assistant Environment Scan",
        enabled=False,
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_SENSOR,
        interval_seconds=300,
        next_run_time=123456789,
        config={
            "base_url": "http://old.local:10020",
            "access_token": "old-token",
            "entity_id": "sensor.office_temperature",
        },
    )

    spec_file = _write_specs_file(
        tmp_path,
        {
            "metric_collection_specs": [
                {
                    "name": "Home Assistant Environment Scan",
                    "enabled": True,
                    "spec_type": MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
                    "interval_seconds": 60,
                    "config": {
                        "base_url": "http://macmini.local:10020",
                        "access_token": "new-token",
                        "host_id": "homeassistant",
                    },
                }
            ]
        },
    )

    stdout = io.StringIO()
    call_command("apply_metric_collection_specs", "--file", spec_file, stdout=stdout)

    spec.refresh_from_db()
    assert spec.enabled is True
    assert spec.spec_type == MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN
    assert spec.interval_seconds == 60
    assert spec.next_run_time == 0
    assert spec.config == {
        "base_url": "http://macmini.local:10020",
        "access_token": "new-token",
        "host_id": "homeassistant",
    }
    assert "created=0" in stdout.getvalue()
    assert "updated=1" in stdout.getvalue()


def test_apply_metric_collection_specs_rejects_invalid_spec_file_shape(tmp_path):
    spec_file = _write_specs_file(
        tmp_path, {"metric_collection_specs": {"bad": "shape"}}
    )

    with pytest.raises(CommandError, match="metric_collection_specs"):
        call_command("apply_metric_collection_specs", "--file", spec_file)


def test_apply_metric_collection_specs_rejects_missing_file(tmp_path):
    missing_file = tmp_path / "missing.json"

    with pytest.raises(CommandError, match="Spec file not found"):
        call_command("apply_metric_collection_specs", "--file", str(missing_file))


def test_apply_metric_collection_specs_rejects_invalid_spec_type(tmp_path):
    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": "Broken Spec",
                "enabled": True,
                "spec_type": "nope",
                "interval_seconds": 60,
                "config": {},
            }
        ],
    )

    with pytest.raises(CommandError, match="invalid spec_type"):
        call_command("apply_metric_collection_specs", "--file", spec_file)


@pytest.mark.parametrize("interval_seconds", [0, -1, True])
def test_apply_metric_collection_specs_rejects_invalid_interval_seconds(
    tmp_path, interval_seconds
):
    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": "Broken Interval Spec",
                "enabled": True,
                "spec_type": MetricCollectionSpecType.HTTP_JSON_METRIC,
                "interval_seconds": interval_seconds,
                "config": {},
            }
        ],
    )

    with pytest.raises(CommandError, match="invalid interval_seconds"):
        call_command("apply_metric_collection_specs", "--file", spec_file)


def test_apply_metric_collection_specs_rejects_non_dict_config(tmp_path):
    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": "Broken Config Spec",
                "enabled": True,
                "spec_type": MetricCollectionSpecType.HTTP_JSON_METRIC,
                "interval_seconds": 60,
                "config": ["not", "a", "dict"],
            }
        ],
    )

    with pytest.raises(CommandError, match="invalid config"):
        call_command("apply_metric_collection_specs", "--file", spec_file)


def test_apply_metric_collection_specs_rejects_duplicate_names(tmp_path):
    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": "Duplicate Spec",
                "enabled": True,
                "spec_type": MetricCollectionSpecType.HTTP_JSON_METRIC,
                "interval_seconds": 60,
                "config": {},
            },
            {
                "name": " Duplicate Spec ",
                "enabled": True,
                "spec_type": MetricCollectionSpecType.HTTP_JSON_METRIC,
                "interval_seconds": 60,
                "config": {},
            },
        ],
    )

    with pytest.raises(CommandError, match="duplicated"):
        call_command("apply_metric_collection_specs", "--file", spec_file)


@pytest.mark.django_db
def test_apply_metric_collection_specs_does_not_delete_omitted_specs_without_prune(
    tmp_path,
):
    kept_spec = MetricCollectionSpec.objects.create(
        name="Home Assistant Environment Scan",
        enabled=True,
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
        interval_seconds=60,
        config={"base_url": "http://macmini.local:10020", "access_token": "token"},
    )
    omitted_spec = MetricCollectionSpec.objects.create(
        name="Ad Hoc HTTP Metric",
        enabled=True,
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        interval_seconds=60,
        config={"url": "https://example.invalid/metrics"},
    )

    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": kept_spec.name,
                "enabled": True,
                "spec_type": MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
                "interval_seconds": 60,
                "config": {
                    "base_url": "http://macmini.local:10020",
                    "access_token": "token",
                },
            }
        ],
    )

    stdout = io.StringIO()
    call_command("apply_metric_collection_specs", "--file", spec_file, stdout=stdout)

    assert MetricCollectionSpec.objects.filter(name=kept_spec.name).exists()
    assert MetricCollectionSpec.objects.filter(name=omitted_spec.name).exists()
    assert "deleted=0" in stdout.getvalue()


@pytest.mark.django_db
def test_apply_metric_collection_specs_prune_deletes_omitted_specs(tmp_path):
    kept_spec = MetricCollectionSpec.objects.create(
        name="Home Assistant Environment Scan",
        enabled=True,
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
        interval_seconds=60,
        config={"base_url": "http://macmini.local:10020", "access_token": "token"},
    )
    omitted_spec = MetricCollectionSpec.objects.create(
        name="Ad Hoc HTTP Metric",
        enabled=True,
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        interval_seconds=60,
        config={"url": "https://example.invalid/metrics"},
    )

    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": kept_spec.name,
                "enabled": True,
                "spec_type": MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
                "interval_seconds": 60,
                "config": {
                    "base_url": "http://macmini.local:10020",
                    "access_token": "token",
                },
            }
        ],
    )

    stdout = io.StringIO()
    call_command(
        "apply_metric_collection_specs",
        "--file",
        spec_file,
        "--prune",
        stdout=stdout,
    )

    assert MetricCollectionSpec.objects.filter(name=kept_spec.name).exists()
    assert not MetricCollectionSpec.objects.filter(name=omitted_spec.name).exists()
    assert "deleted Ad Hoc HTTP Metric" in stdout.getvalue()
    assert "deleted=1" in stdout.getvalue()


@pytest.mark.django_db
def test_apply_metric_collection_specs_prune_keeps_present_specs(tmp_path):
    kept_spec = MetricCollectionSpec.objects.create(
        name="Home Assistant Environment Scan",
        enabled=False,
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
        interval_seconds=300,
        config={"base_url": "http://old.local:10020", "access_token": "old-token"},
    )

    spec_file = _write_specs_file(
        tmp_path,
        [
            {
                "name": kept_spec.name,
                "enabled": True,
                "spec_type": MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN,
                "interval_seconds": 60,
                "config": {
                    "base_url": "http://macmini.local:10020",
                    "access_token": "new-token",
                },
            }
        ],
    )

    stdout = io.StringIO()
    call_command(
        "apply_metric_collection_specs",
        "--file",
        spec_file,
        "--prune",
        stdout=stdout,
    )

    kept_spec.refresh_from_db()
    assert kept_spec.enabled is True
    assert kept_spec.interval_seconds == 60
    assert kept_spec.config == {
        "base_url": "http://macmini.local:10020",
        "access_token": "new-token",
    }
    assert "updated Home Assistant Environment Scan" in stdout.getvalue()
    assert "deleted=0" in stdout.getvalue()


def test_apply_metric_collection_specs_prune_rejects_empty_desired_set(tmp_path):
    spec_file = _write_specs_file(tmp_path, [])

    with pytest.raises(CommandError, match="empty desired spec set"):
        call_command("apply_metric_collection_specs", "--file", spec_file, "--prune")
