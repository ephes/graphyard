"""Separate-connection ordering oracles for the opt-in PostgreSQL lane."""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Event, local

import pytest
from django.db import connection, connections, transaction
from django.utils import timezone

from graphyard import services, influx
from graphyard.admin import MetricCollectionSpecAdminForm, ConditionDefinitionAdminForm
from graphyard.models import (
    MetricCollectionSpec,
    ConditionDefinition,
    SubjectRegistry,
    HostRegistry,
    ServiceRegistry,
)

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(
        connection.vendor != "postgresql",
        reason="PostgreSQL separate-connection row-lock oracle",
    ),
]


@pytest.mark.parametrize("kind", ["spec", "condition"])
def test_admin_definition_save_retains_other_connection_result(kind):
    if kind == "spec":
        row = MetricCollectionSpec.objects.create(name="synthetic")
        form_type = MetricCollectionSpecAdminForm
        result = {
            "last_status": "critical",
            "last_error": "synthetic",
            "next_run_time": 900,
        }
        changes = {"interval_seconds": 120}
    else:
        row = ConditionDefinition.objects.create(
            name="synthetic", metric_name="cpu", warning_threshold=50
        )
        form_type = ConditionDefinitionAdminForm
        result = {
            "status": "critical",
            "message": "synthetic",
            "last_value": 99,
            "last_evaluated": timezone.now(),
        }
        changes = {"warning_threshold": 60}
    initial = form_type(instance=row).initial
    data = {
        key: json.dumps(value) if isinstance(value, dict) else value
        for key, value in {**initial, **changes}.items()
    }

    def publish():
        try:
            if kind == "spec":
                assert services._finish_due_collection_spec(
                    row.pk, 0, definition={}, **result
                )
            else:
                assert services._publish_condition_result(
                    row.pk,
                    result["last_evaluated"],
                    definition={},
                    **{k: v for k, v in result.items() if k != "last_evaluated"},
                )
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(publish).result(timeout=10)
    with transaction.atomic():
        form = form_type(data=data, instance=row)
        assert form.is_valid(), form.errors
        form.save()
    row.refresh_from_db()
    assert {key: getattr(row, key) for key in result} == result


def test_delayed_registry_request_keeps_newer_connection_provenance(monkeypatch):
    now = timezone.now()
    old_host = HostRegistry.objects.create(host_id="old")
    HostRegistry.objects.create(host_id="new")
    service = ServiceRegistry.objects.create(
        service_id="synthetic", host=old_host, last_seen_at=now - timedelta(days=1)
    )
    subject = SubjectRegistry.objects.create(
        subject_type="environment_sensor",
        subject_id="synthetic",
        source_system="initial",
        last_seen_at=now - timedelta(days=1),
    )
    state = local()
    ready, resume = Event(), Event()
    load = services._load_registry_rows

    def delayed_load(batch):
        if getattr(state, "delayed", False):
            state.delayed = False
            ready.set()
            assert resume.wait(timeout=10)
        return load(batch)

    monkeypatch.setattr(services, "_load_registry_rows", delayed_load)
    monkeypatch.setattr(
        services.timezone, "now", lambda: getattr(state, "observed", now)
    )

    def touch(old):
        try:
            state.observed = now if old else now + timedelta(seconds=1)
            state.delayed = old
            point = influx.MetricPoint(
                ts=now,
                metric="cpu",
                value=1,
                subject_type="environment_sensor",
                subject_id="synthetic",
                source_system="old" if old else "new",
                collector_host="old" if old else "new",
                collector_service="synthetic",
                service="synthetic",
            )
            services.touch_registry_from_points([point])
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=2) as pool:
        delayed = pool.submit(touch, True)
        try:
            assert ready.wait(timeout=10)
            pool.submit(touch, False).result(timeout=10)
        finally:
            resume.set()
        delayed.result(timeout=10)
    subject.refresh_from_db()
    service.refresh_from_db()
    assert subject.source_system == "new"
    assert service.host.host_id == "new"
    assert subject.last_seen_at == service.last_seen_at == now + timedelta(seconds=1)


def test_two_admin_definition_edits_serialize_and_one_conflicts():
    from threading import Barrier

    row = ConditionDefinition.objects.create(
        name="synthetic", metric_name="cpu", warning_threshold=50
    )
    initial = {
        key: json.dumps(value) if isinstance(value, dict) else value
        for key, value in ConditionDefinitionAdminForm(instance=row).initial.items()
    }
    barrier = Barrier(2)

    def edit(threshold):
        try:
            current = ConditionDefinition.objects.get(pk=row.pk)
            barrier.wait(timeout=10)
            with transaction.atomic():
                form = ConditionDefinitionAdminForm(
                    data={**initial, "warning_threshold": threshold}, instance=current
                )
                valid = form.is_valid()
                if valid:
                    form.save()
                else:
                    assert "Reload" in str(form.non_field_errors())
                return valid
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sum(pool.map(edit, [60, 70])) == 1


def test_usage_update_preserves_newer_connection_observation(monkeypatch):
    from django.db.models.query import QuerySet
    from graphyard.models import IngestToken, IngestTokenHostUsage

    now = timezone.now()
    token = IngestToken.objects.create(name="synthetic", token_hash="synthetic")
    usage = IngestTokenHostUsage.objects.create(
        token=token,
        host_id="edge",
        first_seen_at=now - timedelta(days=1),
        last_seen_at=now - timedelta(days=1),
    )
    state = local()
    ready, resume = Event(), Event()
    update = QuerySet.update

    def delayed_update(rows, **values):
        if rows.model is IngestTokenHostUsage and getattr(state, "delayed", False):
            state.delayed = False
            ready.set()
            assert resume.wait(timeout=10)
        return update(rows, **values)

    monkeypatch.setattr(QuerySet, "update", delayed_update)
    monkeypatch.setattr(
        services.timezone, "now", lambda: getattr(state, "observed", now)
    )

    def record(old):
        try:
            state.observed = now if old else now + timedelta(hours=2)
            state.delayed = old
            services.record_token_host_usage(token, {"edge"})
        finally:
            connections.close_all()

    with ThreadPoolExecutor(max_workers=2) as pool:
        delayed = pool.submit(record, True)
        try:
            assert ready.wait(timeout=10)
            pool.submit(record, False).result(timeout=10)
        finally:
            resume.set()
        delayed.result(timeout=10)
    usage.refresh_from_db()
    assert usage.last_seen_at == now + timedelta(hours=2)
