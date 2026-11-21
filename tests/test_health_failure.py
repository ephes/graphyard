"""Synthetic failures through the production health endpoint."""

from unittest.mock import patch

import pytest
from django.db import DatabaseError
from django.urls import reverse

from graphyard.models import ConditionDefinition, PipelineHeartbeat


@pytest.mark.django_db
@pytest.mark.parametrize("stage", ["probe", "heartbeat", "count", "session"])
def test_database_failure_returns_bounded_unavailable_health(
    client, monkeypatch, stage
):
    monkeypatch.setattr("graphyard.views.influx_health", lambda: {"status": "pass"})
    target = {
        "probe": "graphyard.views.connection.cursor",
        "heartbeat": "graphyard.views.PipelineHeartbeat.objects.filter",
        "count": "graphyard.views.ConditionDefinition.objects.filter",
        "session": "django.contrib.auth.get_user",
    }[stage]
    # Force lazy session-user resolution without a real account/database session.
    if stage == "session":
        monkeypatch.setattr(
            "django.contrib.sessions.backends.db.SessionStore.load",
            lambda self: {"_auth_user_id": "1", "_auth_user_backend": "synthetic"},
        )
        client.cookies["sessionid"] = "synthetic"
    with (
        patch.object(
            PipelineHeartbeat.objects, "filter", wraps=PipelineHeartbeat.objects.filter
        ) as heartbeats,
        patch.object(
            ConditionDefinition.objects,
            "filter",
            wraps=ConditionDefinition.objects.filter,
        ) as counts,
        patch(
            target, side_effect=DatabaseError("invented-url-password-body-marker")
        ) as failed,
    ):
        response = client.get(reverse("graphyard:health"))
        assert failed.called
        assert response.status_code == 503
        payload = response.json()
        assert payload["status"] == "critical"
        assert payload["components"]["database"]["status"] == "critical"
        assert payload["conditions"] == dict.fromkeys(
            ("total", "ok", "warning", "critical")
        )
        for component in payload["components"]["pipelines"].values():
            assert component["status"] == "warning"
            assert component["last_success"] is None
            assert component["age_seconds"] is None
        assert "invented-url-password-body-marker" not in response.content.decode()
        if stage == "probe":
            heartbeats.assert_not_called()
            counts.assert_not_called()
        elif stage == "heartbeat":
            assert failed.call_count == 1
            counts.assert_not_called()
        elif stage == "count":
            assert failed.call_count == 1


@pytest.mark.django_db
@pytest.mark.parametrize(
    "remote_status,expected_status",
    [("pass", "ok"), ("fail", "warning"), (None, "critical")],
)
def test_influx_teardown_failure_preserves_probe_outcome(
    client, django_user_model, monkeypatch, caplog, remote_status, expected_status
):
    from unittest.mock import MagicMock

    from influxdb_client import HealthCheck

    from graphyard import influx

    influx_client = MagicMock()
    influx_client.__enter__.return_value = influx_client
    influx_client.__exit__.side_effect = lambda *_: influx_client.close()
    influx_client.close.side_effect = OSError("synthetic private cleanup error")
    if remote_status is None:
        influx_client.health.side_effect = RuntimeError("synthetic transport error")
    else:
        influx_client.health.return_value = HealthCheck(
            status=remote_status, message="synthetic probe reply", name="synthetic"
        )
    monkeypatch.setattr(influx, "_build_client", lambda: influx_client)
    client.force_login(django_user_model.objects.create_user(username="synthetic"))

    response = client.get(reverse("graphyard:health"))

    influx_client.health.assert_called_once_with()
    influx_client.close.assert_called_once_with()
    assert response.status_code == 200
    component = response.json()["components"]["influxdb"]
    assert component["status"] == expected_status
    if remote_status is None:
        assert component["message"] == "synthetic transport error"
    else:
        assert component["message"] == "synthetic probe reply"
        assert component["name"] == "synthetic"
    assert [
        record.getMessage()
        for record in caplog.records
        if record.name == "graphyard.influx"
    ] == ["InfluxDB health client cleanup failed (OSError)"]
