from __future__ import annotations

import hashlib
from datetime import UTC, datetime

import pytest
from django.contrib.auth.hashers import make_password
from django.db import OperationalError
from django.urls import reverse

from graphyard.models import (
    ConditionDefinition,
    IngestToken,
    LEGACY_FAST_TOKEN_HASH_PREFIXES,
    PREFERRED_FAST_TOKEN_HASH_PREFIX,
    StatusLevel,
)


@pytest.mark.django_db
def test_metrics_endpoint_requires_bearer_token(client):
    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "metric": "cpu.usage_percent",
            "value": 21.2,
            "tags": {"core": "all"},
        }
    ]
    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
    )

    assert response.status_code == 401


@pytest.mark.django_db
def test_metrics_endpoint_logs_auth_rejection_count(client, caplog):
    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "metric": "cpu.usage_percent",
            "value": 21.2,
        }
    ]

    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
    )

    assert response.status_code == 401
    assert any(
        "metrics_ingest_rejected category=auth rejected_requests=1"
        in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.django_db
def test_metrics_endpoint_accepts_valid_token(client, monkeypatch):
    ingest_token = IngestToken(name="macmini")
    ingest_token.set_token("secret-token")
    ingest_token.save()

    captured = {"count": 0}

    def fake_write_points(points):
        captured["count"] = len(points)
        return len(points)

    monkeypatch.setattr("graphyard.views.write_points", fake_write_points)

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "service": "mastodon",
            "metric": "disk.used_percent",
            "value": 79.1,
            "tags": {"mount": "/"},
        }
    ]
    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )

    assert response.status_code == 202
    assert response.json()["ingested"] == 1
    assert captured["count"] == 1

    ingest_token.refresh_from_db()
    assert ingest_token.token_hash.startswith(f"{PREFERRED_FAST_TOKEN_HASH_PREFIX}$")
    assert ingest_token.last_used_at is not None


@pytest.mark.django_db
def test_metrics_endpoint_accepts_vector_batched_list_payload(client, monkeypatch):
    ingest_token = IngestToken(name="macmini")
    ingest_token.set_token("secret-token")
    ingest_token.save()

    captured = {"count": 0, "metrics": []}

    def fake_write_points(points):
        captured["count"] = len(points)
        captured["metrics"] = [item.metric for item in points]
        return len(points)

    monkeypatch.setattr("graphyard.views.write_points", fake_write_points)

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "service": "host_metrics",
            "metric": "host.cpu_seconds_total",
            "value": 123.0,
            "tags": {"collector": "vector.host_metrics", "metric_kind": "counter"},
        },
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "service": "host_metrics",
            "metric": "host.memory_available_bytes",
            "value": 2048.0,
            "tags": {"collector": "vector.host_metrics", "metric_kind": "gauge"},
        },
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "service": "host_metrics",
            "metric": "host.filesystem_used_ratio",
            "value": 0.42,
            "tags": {
                "collector": "vector.host_metrics",
                "metric_kind": "gauge",
                "mountpoint": "/",
            },
        },
    ]

    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )

    assert response.status_code == 202
    assert response.json()["ingested"] == 3
    assert captured["count"] == 3
    assert captured["metrics"] == [
        "host.cpu_seconds_total",
        "host.memory_available_bytes",
        "host.filesystem_used_ratio",
    ]


@pytest.mark.django_db
def test_metrics_endpoint_upgrades_legacy_token_hash(client, monkeypatch):
    ingest_token = IngestToken.objects.create(
        name="macmini",
        token_hash=make_password("legacy-secret"),
    )
    assert not ingest_token.token_hash.startswith("sha256$")

    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "metric": "disk.used_percent",
            "value": 79.1,
        }
    ]
    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer legacy-secret",
    )

    assert response.status_code == 202

    ingest_token.refresh_from_db()
    assert ingest_token.token_hash.startswith(f"{PREFERRED_FAST_TOKEN_HASH_PREFIX}$")
    assert ingest_token.last_used_at is not None


@pytest.mark.django_db
def test_metrics_endpoint_upgrades_legacy_fast_hash_prefix(client, monkeypatch):
    legacy_prefix = LEGACY_FAST_TOKEN_HASH_PREFIXES[0]
    digest = hashlib.sha256("legacy-fast-secret".encode("utf-8")).hexdigest()
    ingest_token = IngestToken.objects.create(
        name="macmini",
        token_hash=f"{legacy_prefix}${digest}",
    )
    assert ingest_token.token_hash.startswith(f"{legacy_prefix}$")

    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "metric": "disk.used_percent",
            "value": 79.1,
        }
    ]
    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer legacy-fast-secret",
    )

    assert response.status_code == 202

    ingest_token.refresh_from_db()
    assert ingest_token.token_hash.startswith(f"{PREFERRED_FAST_TOKEN_HASH_PREFIX}$")


@pytest.mark.django_db
def test_metrics_endpoint_throttles_last_used_updates(client, monkeypatch):
    ingest_token = IngestToken(name="macmini")
    ingest_token.set_token("secret-token")
    ingest_token.save()

    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "metric": "disk.used_percent",
            "value": 79.1,
        }
    ]

    first_response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )
    assert first_response.status_code == 202

    ingest_token.refresh_from_db()
    first_last_used_at = ingest_token.last_used_at
    assert first_last_used_at is not None

    second_response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )
    assert second_response.status_code == 202

    ingest_token.refresh_from_db()
    assert ingest_token.last_used_at == first_last_used_at


@pytest.mark.django_db
def test_metrics_endpoint_rejects_unknown_subject_type(client, monkeypatch):
    ingest_token = IngestToken(name="collector")
    ingest_token.set_token("secret-token")
    ingest_token.save()
    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "metric": "ha.sensor.office_temperature",
            "value": 22.1,
            "subject_type": "unknown_thing",
            "subject_id": "office_temperature",
            "source_system": "homeassistant",
            "source_instance": "ha-main",
            "collector_service": "graphyard-agent",
            "collector_host": "macmini",
        }
    ]

    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )

    assert response.status_code == 400
    assert "subject_type" in response.json()["error"]


@pytest.mark.django_db
def test_metrics_endpoint_logs_parse_rejection_count(client, monkeypatch, caplog):
    ingest_token = IngestToken(name="collector")
    ingest_token.set_token("secret-token")
    ingest_token.save()
    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "metric": "host.cpu_seconds_total",
            "value": "not-a-number",
        }
    ]

    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )

    assert response.status_code == 400
    assert any(
        "metrics_ingest_rejected category=parse parse_rejected=1 normalization_rejected=0"
        in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.django_db
def test_metrics_endpoint_logs_normalization_rejection_count(
    client, monkeypatch, caplog
):
    ingest_token = IngestToken(name="collector")
    ingest_token.set_token("secret-token")
    ingest_token.save()
    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "metric": "ha.sensor.office_temperature",
            "value": 22.1,
            "subject_type": "unknown_thing",
            "subject_id": "office_temperature",
            "source_system": "homeassistant",
            "source_instance": "ha-main",
            "collector_service": "graphyard-agent",
            "collector_host": "macmini",
        }
    ]

    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )

    assert response.status_code == 400
    assert any(
        "metrics_ingest_rejected category=normalization parse_rejected=0 normalization_rejected=1"
        in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.django_db
def test_metrics_endpoint_normalizes_subject_id(client, monkeypatch):
    ingest_token = IngestToken(name="collector")
    ingest_token.set_token("secret-token")
    ingest_token.save()

    captured: dict[str, object] = {}

    def fake_write_points(points):
        captured["subject_id"] = points[0].subject_id
        captured["source_instance"] = points[0].source_instance
        return len(points)

    monkeypatch.setattr("graphyard.views.write_points", fake_write_points)

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "metric": "ha.sensor.office_temperature",
            "value": 22.1,
            "subject_type": "environment_sensor",
            "subject_id": "Office Temperature",
            "source_system": "homeassistant",
            "collector_service": "graphyard-agent",
            "collector_host": "macmini",
        }
    ]

    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )

    assert response.status_code == 202
    assert captured["subject_id"] == "office_temperature"
    assert captured["source_instance"] == "default"


@pytest.mark.django_db
def test_record_heartbeat_safe_swallows_only_lock_errors(monkeypatch):
    def fake_record_heartbeat(*args, **kwargs):  # noqa: ANN002, ANN003
        del args, kwargs
        raise OperationalError("database is locked")

    monkeypatch.setattr("graphyard.views.record_heartbeat", fake_record_heartbeat)

    from graphyard.views import _record_heartbeat_safe

    _record_heartbeat_safe("metric_ingest", status=StatusLevel.OK)


@pytest.mark.django_db
def test_record_heartbeat_safe_reraises_non_lock_errors(monkeypatch):
    def fake_record_heartbeat(*args, **kwargs):  # noqa: ANN002, ANN003
        del args, kwargs
        raise OperationalError("no such table: pipeline_heartbeat")

    monkeypatch.setattr("graphyard.views.record_heartbeat", fake_record_heartbeat)

    from graphyard.views import _record_heartbeat_safe

    with pytest.raises(OperationalError):
        _record_heartbeat_safe("metric_ingest", status=StatusLevel.OK)


@pytest.mark.django_db
def test_metrics_endpoint_swallows_registry_lock_errors(client, monkeypatch):
    ingest_token = IngestToken(name="collector")
    ingest_token.set_token("secret-token")
    ingest_token.save()

    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))
    monkeypatch.setattr("graphyard.views._record_heartbeat_safe", lambda *a, **k: None)

    def fake_touch_registry(points):  # noqa: ANN001
        del points
        raise OperationalError("database is locked")

    monkeypatch.setattr(
        "graphyard.views.touch_registry_from_points", fake_touch_registry
    )

    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "metric": "disk.used_percent",
            "value": 79.1,
        }
    ]

    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )

    assert response.status_code == 202
    assert response.json()["ingested"] == 1


@pytest.mark.django_db
def test_metrics_endpoint_reraises_non_lock_registry_errors(client, monkeypatch):
    ingest_token = IngestToken(name="collector")
    ingest_token.set_token("secret-token")
    ingest_token.save()

    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))
    monkeypatch.setattr("graphyard.views._record_heartbeat_safe", lambda *a, **k: None)

    def fake_touch_registry(points):  # noqa: ANN001
        del points
        raise OperationalError("no such table: subject_registry")

    monkeypatch.setattr(
        "graphyard.views.touch_registry_from_points", fake_touch_registry
    )

    client.raise_request_exception = False
    payload = [
        {
            "ts": "2026-03-04T12:00:00Z",
            "host": "macmini",
            "metric": "disk.used_percent",
            "value": 79.1,
        }
    ]

    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer secret-token",
    )

    assert response.status_code == 500


@pytest.mark.django_db
def test_conditions_endpoints(client):
    condition = ConditionDefinition.objects.create(
        name="Humidity too high",
        metric_name="ha.sensor.living_room_humidity",
        operator="gt",
        warning_threshold=65,
        critical_threshold=75,
        window_minutes=30,
        breach_minutes=10,
        status=StatusLevel.WARNING,
        last_evaluated=datetime(2026, 3, 4, 10, 0, tzinfo=UTC),
        message="humidity > 65 for 10m",
        enabled=True,
    )

    list_response = client.get(reverse("graphyard:conditions_list"))
    assert list_response.status_code == 200
    payload = list_response.json()
    assert payload["conditions"][0]["status"] == "warning"

    detail_response = client.get(
        reverse("graphyard:condition_detail", kwargs={"condition_id": condition.id})
    )
    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["id"] == condition.id
    assert detail["config"]["metric_name"] == condition.metric_name
    assert "subject_type_filter" in detail["config"]
    assert "subject_id_filter" in detail["config"]


@pytest.mark.django_db
def test_health_endpoint_shape(client, monkeypatch):
    monkeypatch.setattr(
        "graphyard.views.influx_health",
        lambda: {"status": "pass", "message": "ready", "name": "influxdb"},
    )

    response = client.get(reverse("graphyard:health"))
    assert response.status_code == 200
    payload = response.json()

    assert payload["status"] in {"ok", "warning", "critical"}
    assert "components" in payload
    assert "database" in payload["components"]
    assert "influxdb" in payload["components"]
    assert "pipelines" in payload["components"]
