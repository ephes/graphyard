from __future__ import annotations

from datetime import UTC, datetime

import pytest
from django.urls import reverse

from graphyard.models import ConditionDefinition, IngestToken, StatusLevel


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
    assert ingest_token.last_used_at is not None


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
