from datetime import timedelta

import pytest
from django.db.models.query import QuerySet
from django.utils import timezone

from graphyard import influx, services
from graphyard.models import (
    IngestToken,
    IngestTokenHostUsage,
    SubjectRegistry,
    HostRegistry,
    ServiceRegistry,
)


@pytest.mark.django_db
def test_usage_update_rechecks_watermark_in_update(monkeypatch):
    now = timezone.now()
    token = IngestToken.objects.create(name="synthetic", token_hash="synthetic")
    usage = IngestTokenHostUsage.objects.create(
        token=token,
        host_id="edge",
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now - timedelta(days=1),
    )
    real_update = QuerySet.update
    fired = False

    def update(rows, **values):
        nonlocal fired
        if rows.model is IngestTokenHostUsage and not fired:
            fired = True
            real_update(
                IngestTokenHostUsage.objects.filter(pk=usage.pk),
                last_seen_at=now + timedelta(hours=2),
            )
        return real_update(rows, **values)

    monkeypatch.setattr(QuerySet, "update", update)
    services.record_token_host_usage(token, {"edge"})
    usage.refresh_from_db()
    assert usage.last_seen_at == now + timedelta(hours=2)


@pytest.mark.django_db
@pytest.mark.parametrize("offset", [0, 1])
def test_delayed_registry_provenance_cannot_replace_newer_observation(
    monkeypatch, offset
):
    now = timezone.now()
    HostRegistry.objects.create(host_id="a")
    b = HostRegistry.objects.create(host_id="b")
    subject = SubjectRegistry.objects.create(
        subject_type="host",
        subject_id="a",
        source_system="new",
        last_seen_at=now + timedelta(seconds=offset),
    )
    service = ServiceRegistry.objects.create(
        service_id="synthetic", host=b, last_seen_at=subject.last_seen_at
    )
    point = influx.MetricPoint(
        ts=now,
        metric="cpu",
        value=1,
        subject_type="host",
        subject_id="a",
        source_system="old",
        collector_host="a",
        collector_service="synthetic",
        host="a",
        service="synthetic",
    )
    monkeypatch.setattr(services.timezone, "now", lambda: now)
    services.touch_registry_from_points([point])
    subject.refresh_from_db()
    service.refresh_from_db()
    assert subject.source_system == "new"
    assert service.host_id == b.pk
    assert subject.last_seen_at == now + timedelta(seconds=offset)
