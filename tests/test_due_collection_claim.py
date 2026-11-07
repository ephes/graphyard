"""Due runs claim before I/O; all collectors and clocks are synthetic."""

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import UTC, datetime, timedelta
from threading import Barrier, Event, Lock

import pytest
from django.db import connections

from graphyard import services
from graphyard.models import MetricCollectionSpec, MetricCollectionSpecType, StatusLevel


def spec():
    return MetricCollectionSpec.objects.create(
        name="synthetic",
        spec_type=MetricCollectionSpecType.HTTP_JSON_METRIC,
        interval_seconds=600,
        next_run_time=0,
        config={},
    )


@pytest.mark.django_db(transaction=True)
def test_two_due_runners_collect_once(monkeypatch):
    row = spec()
    now = datetime(2026, 3, 4, 12, tzinfo=UTC)
    monkeypatch.setattr(services.timezone, "now", lambda: now)
    from unittest.mock import Mock

    heartbeat = Mock()
    monkeypatch.setattr(services, "record_heartbeat", heartbeat)
    snapshot_barrier, update_lock = Barrier(2), Lock()
    original_claim = services._claim_due_collection_spec

    def claim(*args):
        snapshot_barrier.wait(timeout=10)
        # Both snapshots are loaded. Serialize only the test DB statements so
        # shared in-memory SQLite contention cannot obscure the CAS result.
        with update_lock:
            return original_claim(*args)

    monkeypatch.setattr(services, "_claim_due_collection_spec", claim)
    entered, finish = Event(), Event()
    calls = []

    def collect(row):
        calls.append(row.pk)
        if len(calls) == 1:
            entered.set()
            assert finish.wait(10)
        return StatusLevel.OK, 1, 0, ""

    def run():
        try:
            return services.run_metric_collection_specs_once(due_only=True)
        finally:
            connections.close_all()

    monkeypatch.setattr(services, "_run_single_metric_collection_spec", collect)
    with ThreadPoolExecutor(max_workers=2) as pool:
        attempts = [pool.submit(run), pool.submit(run)]
        assert entered.wait(10)
        try:
            done, pending = wait(attempts, timeout=10, return_when=FIRST_COMPLETED)
            assert len(done) == len(pending) == 1
            assert next(iter(done)).result().total == 0
            assert calls == [row.pk]
            heartbeat.assert_not_called()
        finally:
            finish.set()
        assert sum(attempt.result(timeout=10).total for attempt in attempts) == 1
    heartbeat.assert_called_once()


@pytest.mark.django_db
def test_crashed_claim_expires_and_old_completion_cannot_overwrite(monkeypatch):
    row = spec()
    now = datetime(2026, 3, 4, 12, tzinfo=UTC)
    current = [now]
    monkeypatch.setattr(services.timezone, "now", lambda: current[0])
    monkeypatch.setattr(services, "record_heartbeat", lambda *a, **k: None)
    calls = []

    def collect(item):
        calls.append(item.pk)
        if len(calls) == 1:
            item.refresh_from_db()
            assert item.next_run_time == int(now.timestamp()) + 300
            current[0] = now + timedelta(seconds=300)
            assert services.run_metric_collection_specs_once(due_only=True).total == 1
            return StatusLevel.CRITICAL, 0, 0, "old failure"
        return StatusLevel.OK, 1, 0, ""

    monkeypatch.setattr(services, "_run_single_metric_collection_spec", collect)
    services.run_metric_collection_specs_once(due_only=True)
    row.refresh_from_db()
    assert len(calls) == 2
    assert row.last_status == StatusLevel.OK
    assert row.last_error == ""
    assert row.next_run_time == int(current[0].timestamp()) + 600
    assert services.run_metric_collection_specs_once(due_only=True).total == 0
    # An explicit non-due manual run retains its previous semantics.
    assert services.run_metric_collection_specs_once().total == 1


@pytest.mark.django_db
def test_claim_survives_process_exit_and_retries_at_deadline(monkeypatch):
    row = spec()
    now = datetime(2026, 3, 4, 12, tzinfo=UTC)
    current = [now]
    monkeypatch.setattr(services.timezone, "now", lambda: current[0])

    def crash(item):
        raise SystemExit("synthetic crash")

    monkeypatch.setattr(services, "_run_single_metric_collection_spec", crash)
    with pytest.raises(SystemExit):
        services.run_metric_collection_specs_once(due_only=True)
    row.refresh_from_db()
    assert row.next_run_time == int(now.timestamp()) + 300
    current[0] += timedelta(seconds=299)
    assert services.run_metric_collection_specs_once(due_only=True).total == 0
    current[0] += timedelta(seconds=1)
    monkeypatch.setattr(
        services,
        "_run_single_metric_collection_spec",
        lambda _: (StatusLevel.CRITICAL, 0, 0, "synthetic failure"),
    )
    assert services.run_metric_collection_specs_once(due_only=True).failed == 1
    row.refresh_from_db()
    assert row.next_run_time == int(current[0].timestamp()) + 600


@pytest.mark.django_db
@pytest.mark.parametrize("due_only", [False, True])
@pytest.mark.parametrize(
    "edit",
    [
        {"config": {"synthetic": "changed"}},
        {"spec_type": MetricCollectionSpecType.HTTP_PAGE_PROBE},
        {"interval_seconds": 900},
        {"enabled": False},
        {"name": "renamed"},
        {"next_run_time": 9999999999},
        None,
    ],
)
def test_collection_definition_edit_rejects_completion(monkeypatch, due_only, edit):
    row = spec()

    def collect(item):
        if edit is None:
            MetricCollectionSpec.objects.filter(pk=row.pk).delete()
        else:
            MetricCollectionSpec.objects.filter(pk=row.pk).update(**edit)
        return StatusLevel.CRITICAL, 2, 3, "synthetic old failure"

    monkeypatch.setattr(services, "_run_single_metric_collection_spec", collect)
    result = services.run_metric_collection_specs_once(due_only=due_only)
    assert result.total == result.failed == result.ingested == result.skipped == 0
    from graphyard.models import PipelineHeartbeat

    assert not PipelineHeartbeat.objects.exists()
    if edit is not None:
        row.refresh_from_db()
        assert row.last_run_at is None
        assert row.last_error == ""
        for field, value in edit.items():
            assert getattr(row, field) == value


@pytest.mark.django_db
@pytest.mark.parametrize("old_failed", [True, False])
def test_superseded_collection_preserves_newer_heartbeat(monkeypatch, old_failed):
    from graphyard.models import PipelineHeartbeat

    spec()
    now = datetime(2026, 3, 4, 12, tzinfo=UTC)
    current = [now]
    monkeypatch.setattr(services.timezone, "now", lambda: current[0])
    calls = []

    def collect(item):
        calls.append(item.pk)
        if len(calls) == 1:
            current[0] += timedelta(seconds=300)
            fresh = services.run_metric_collection_specs_once(due_only=True)
            assert fresh.total == 1 and fresh.failed == int(not old_failed)
            return (StatusLevel.CRITICAL if old_failed else StatusLevel.OK), 8, 9, "old"
        return (StatusLevel.OK if old_failed else StatusLevel.CRITICAL), 1, 0, "new"

    monkeypatch.setattr(services, "_run_single_metric_collection_spec", collect)
    result = services.run_metric_collection_specs_once(due_only=True)
    assert result.total == result.failed == result.ingested == result.skipped == 0
    heartbeat = PipelineHeartbeat.objects.get(name="metric_collectors")
    assert heartbeat.status == (StatusLevel.OK if old_failed else StatusLevel.CRITICAL)
    assert heartbeat.details["total"] == 1
    assert heartbeat.details["ingested"] == 1


@pytest.mark.django_db
@pytest.mark.parametrize("due_only", [True, False])
def test_unchanged_truthy_enabled_collection_settles(monkeypatch, due_only):
    from django.db import connection

    row = spec()
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE metric_collection_spec SET enabled = 2 WHERE id = %s", [row.pk]
        )
    now = datetime(2026, 3, 4, 12, tzinfo=UTC)
    monkeypatch.setattr(services.timezone, "now", lambda: now)
    monkeypatch.setattr(
        services,
        "_run_single_metric_collection_spec",
        lambda _: (StatusLevel.OK, 1, 0, ""),
    )
    result = services.run_metric_collection_specs_once(due_only=due_only)
    assert result.total == result.ingested == 1
    row.refresh_from_db()
    assert row.last_status == StatusLevel.OK and row.last_run_at == now
    assert row.next_run_time == int(now.timestamp()) + 600
