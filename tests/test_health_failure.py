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
