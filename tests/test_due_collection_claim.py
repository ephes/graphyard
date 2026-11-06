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
