from unittest.mock import Mock

import pytest
from django.db import DatabaseError
from django.urls import reverse
from django.utils import timezone

from graphyard.models import IngestToken, PipelineHeartbeat


@pytest.mark.django_db
@pytest.mark.parametrize(
    "stage",
    ["touch_registry_from_points", "record_token_host_usage", "record_heartbeat"],
)
def test_postwrite_database_failure_still_accepts(client, monkeypatch, caplog, stage):
    token = IngestToken(name="synthetic")
    token.set_token("invented-credential")
    token.save()
    write = Mock(return_value=1)
    monkeypatch.setattr("graphyard.views.write_points", write)
    failed = Mock(side_effect=DatabaseError("untrusted private diagnostic"))
    monkeypatch.setattr(f"graphyard.views.{stage}", failed)
    client.raise_request_exception = False
    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=[
            {
                "ts": timezone.now().isoformat(),
                "host": "synthetic",
                "metric": "load",
                "value": 1,
            }
        ],
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer invented-credential",
    )
    assert failed.called
    write.assert_called_once()
    assert response.status_code == 202
    assert response.json() == {"status": "accepted", "ingested": 1}
    assert "metrics_ingest_bookkeeping_degraded" in caplog.text
    assert "untrusted private diagnostic" not in caplog.text
    if stage != "record_heartbeat":
        heartbeat = PipelineHeartbeat.objects.get(name="metric_ingest")
        assert heartbeat.status == "warning"
        assert heartbeat.details["bookkeeping_degraded"] is True


@pytest.mark.django_db
@pytest.mark.parametrize(
    "stage",
    ["touch_registry_from_points", "record_token_host_usage", "record_heartbeat"],
)
def test_postwrite_unexpected_failure_is_not_swallowed(client, monkeypatch, stage):
    token = IngestToken(name="synthetic")
    token.set_token("invented-credential")
    token.save()
    monkeypatch.setattr("graphyard.views.write_points", Mock(return_value=1))
    failed = Mock(side_effect=TypeError("synthetic programming defect"))
    monkeypatch.setattr(f"graphyard.views.{stage}", failed)
    with pytest.raises(TypeError, match="synthetic programming defect"):
        client.post(
            reverse("graphyard:metrics_ingest"),
            data=[
                {
                    "ts": timezone.now().isoformat(),
                    "host": "synthetic",
                    "metric": "load",
                    "value": 1,
                }
            ],
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer invented-credential",
        )
    assert failed.called
