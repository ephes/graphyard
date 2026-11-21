import json
import sqlite3
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from django.db import DatabaseError, OperationalError, connection, transaction
from django.utils import timezone

from graphyard import services
from graphyard.models import PipelineHeartbeat, StatusLevel
from graphyard.views import _heartbeat_component


@pytest.mark.parametrize("stored", ["ok", "warning", "critical"])
@pytest.mark.parametrize(
    "age,aged", [(99, "ok"), (100, "warning"), (299, "warning"), (300, "critical")]
)
def test_projection_never_reduces_severity(settings, stored, age, aged):
    settings.HEARTBEAT_WARNING_SECONDS = 100
    settings.HEARTBEAT_CRITICAL_SECONDS = 300
    now = timezone.now()
    heartbeat = PipelineHeartbeat(
        name="synthetic",
        status=stored,
        last_success=now - timedelta(seconds=age),
        last_error="fixed failure",
        details={"count": 1},
    )
    result = _heartbeat_component(heartbeat, now)
    ranks = ["ok", "warning", "critical"]
    assert result["status"] == max((stored, aged), key=ranks.index)
    assert result["message"] == "fixed failure"
    assert result["details"] == {"count": 1}


@pytest.fixture(autouse=True)
def clear_cache():
    services._heartbeat_write_cache.clear()
    yield
    services._heartbeat_write_cache.clear()


@pytest.mark.django_db(transaction=True)
def test_throttle_tracks_only_current_committed_state():
    real_write = PipelineHeartbeat.objects.update_or_create
    with (
        patch.object(
            PipelineHeartbeat.objects, "update_or_create", wraps=real_write
        ) as write,
        patch.object(services.time, "monotonic", return_value=10),
    ):

        def publish(status, interval=60):
            return services.record_heartbeat(
                "synthetic",
                status=status,
                success=status == StatusLevel.OK,
                min_update_interval_seconds=interval,
            )

        publish("ok")
        publish("ok")
        assert write.call_count == 1
        publish("critical", 0)
        publish("ok")
        assert write.call_count == 3
        assert PipelineHeartbeat.objects.get(name="synthetic").status == "ok"


@pytest.mark.django_db(transaction=True)
def test_failed_publication_is_retried():
    real_write = PipelineHeartbeat.objects.update_or_create
    with patch.object(
        PipelineHeartbeat.objects,
        "update_or_create",
        side_effect=DatabaseError("synthetic failure"),
    ):
        with pytest.raises(DatabaseError):
            services.record_heartbeat(
                "synthetic", status="ok", min_update_interval_seconds=60
            )
    with patch.object(
        PipelineHeartbeat.objects, "update_or_create", wraps=real_write
    ) as write:
        services.record_heartbeat(
            "synthetic", status="ok", min_update_interval_seconds=60
        )
        assert write.call_count == 1


@pytest.mark.django_db(transaction=True)
def test_rolled_back_publication_is_retried():
    with pytest.raises(ValueError), transaction.atomic():
        services.record_heartbeat(
            "synthetic", status="ok", min_update_interval_seconds=60
        )
        raise ValueError("rollback")
    services.record_heartbeat("synthetic", status="ok", min_update_interval_seconds=60)
    assert PipelineHeartbeat.objects.filter(name="synthetic").exists()


@pytest.mark.django_db(transaction=True)
def test_transaction_does_not_throttle_a_recovery_to_cached_state():
    services.record_heartbeat("synthetic", status="ok", min_update_interval_seconds=60)
    real_write = PipelineHeartbeat.objects.update_or_create
    with patch.object(
        PipelineHeartbeat.objects, "update_or_create", wraps=real_write
    ) as write:
        with transaction.atomic():
            services.record_heartbeat("synthetic", status="critical")
            services.record_heartbeat(
                "synthetic", status="ok", min_update_interval_seconds=60
            )
        assert write.call_count == 2
    assert PipelineHeartbeat.objects.get(name="synthetic").status == "ok"


@pytest.mark.django_db(transaction=True)
def test_outer_commit_invalidates_throttle():
    with transaction.atomic():
        services.record_heartbeat(
            "synthetic", status="ok", min_update_interval_seconds=60
        )
    assert "synthetic" not in services._heartbeat_write_cache
    with patch.object(
        services, "_write_heartbeat", return_value=PipelineHeartbeat()
    ) as write:
        services.record_heartbeat(
            "synthetic", status="ok", min_update_interval_seconds=60
        )
        write.assert_called_once()


def test_overlapping_publications_do_not_cache_uncertain_order(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    entered, release = Event(), Event()

    def write(name, **kwargs):
        if kwargs["status"] == "ok":
            entered.set()
            assert release.wait(5)
        return PipelineHeartbeat(name=name, status=kwargs["status"])

    monkeypatch.setattr(services, "_write_heartbeat", write)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(services.record_heartbeat, "synthetic", status="ok")
        assert entered.wait(5)
        try:
            pool.submit(
                services.record_heartbeat, "synthetic", status="critical"
            ).result(timeout=5)
        finally:
            release.set()
        first.result(timeout=5)
    assert "synthetic" not in services._heartbeat_write_cache
    assert not services._heartbeat_inflight


def test_commit_order_may_differ_from_completion_order(monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, Lock, local

    first_entered, second_committed, finish_second = Event(), Event(), Event()
    thread = local()
    lock = Lock()
    stored = []

    class GateLock:
        def __enter__(self):
            if getattr(thread, "hold_completion", False):
                assert finish_second.wait(5)
            lock.acquire()

        def __exit__(self, *args):
            lock.release()

    def write(name, **kwargs):
        if kwargs["status"] == "ok":
            first_entered.set()
            assert second_committed.wait(5)
            stored.append("ok")
        else:
            stored.append("critical")
            thread.hold_completion = True
            second_committed.set()
        return PipelineHeartbeat(name=name, status=kwargs["status"])

    monkeypatch.setattr(services, "_heartbeat_write_lock", GateLock())
    monkeypatch.setattr(services, "_write_heartbeat", write)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(services.record_heartbeat, "synthetic", status="ok")
        assert first_entered.wait(5)
        second = pool.submit(services.record_heartbeat, "synthetic", status="critical")
        try:
            first.result(timeout=5)
        finally:
            finish_second.set()
        second.result(timeout=5)
    assert stored == ["critical", "ok"]
    assert "synthetic" not in services._heartbeat_write_cache


@pytest.mark.parametrize("stored", ["ok", "warning", "critical"])
@pytest.mark.parametrize("offset", [None, 61, 86400])
def test_invalid_success_cannot_establish_health(stored, offset):
    now = timezone.now()
    heartbeat = PipelineHeartbeat(
        name="synthetic",
        status=stored,
        last_success=None if offset is None else now + timedelta(seconds=offset),
    )
    result = _heartbeat_component(heartbeat, now)
    assert result["status"] == ("critical" if stored == "critical" else "warning")
    assert result["age_seconds"] is None
    assert result["message"] == (
        "No successful run yet"
        if offset is None
        else "Success timestamp is in the future"
    )


@pytest.mark.parametrize("offset", [0, 1, 60])
def test_small_future_skew_is_tolerated_without_negative_age(offset):
    now = timezone.now()
    heartbeat = PipelineHeartbeat(
        name="synthetic",
        status="ok",
        last_success=now + timedelta(seconds=offset),
    )
    result = _heartbeat_component(heartbeat, now)
    assert result["status"] == "ok"
    assert result["age_seconds"] == 0


@pytest.mark.parametrize("offset", [None, 61])
def test_invalid_success_preserves_recorded_failure(offset):
    now = timezone.now()
    heartbeat = PipelineHeartbeat(
        name="synthetic",
        status="critical",
        last_error="recorded failure",
        last_success=None if offset is None else now + timedelta(seconds=offset),
    )
    result = _heartbeat_component(heartbeat, now)
    assert result["status"] == "critical"
    assert "recorded failure" in result["message"]
    if offset is not None:
        assert "Success timestamp is in the future" in result["message"]


_RETRY_CODES = (
    Path(__file__).resolve().parents[1] / "agent-rs/fixtures/heartbeat/retry_codes.json"
)


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize(
    "case",
    [
        *json.loads(_RETRY_CODES.read_text(encoding="utf-8")),
        {"code": None, "message": "database is locked", "retryable": True},
        {"code": None, "message": "not: database is locked", "retryable": False},
    ],
)
def test_heartbeat_retry_uses_sqlite_result_code(case):
    if connection.vendor != "sqlite":
        pytest.skip("heartbeat busy retries are SQLite-only")

    driver_error = sqlite3.OperationalError(case["message"])
    if case["code"] is not None:
        driver_error.sqlite_errorcode = case["code"]
    error = OperationalError(case["message"])
    error.__cause__ = driver_error
    heartbeat = PipelineHeartbeat(name="synthetic")

    with (
        patch.object(
            PipelineHeartbeat.objects,
            "update_or_create",
            side_effect=[error, (heartbeat, False)],
        ) as write,
        patch.object(services.time, "sleep") as sleep,
    ):
        if case["retryable"]:
            assert services.record_heartbeat("synthetic", status="ok") is heartbeat
            assert write.call_count == 2
            sleep.assert_called_once_with(0.05)
        else:
            with pytest.raises(OperationalError) as raised:
                services.record_heartbeat("synthetic", status="ok")
            assert raised.value is error
            assert write.call_count == 1
            sleep.assert_not_called()


@pytest.mark.django_db(transaction=True, databases=["default", "secondary"])
@pytest.mark.parametrize("alias", ["default", "secondary"])
def test_heartbeat_leaves_busy_snapshot_retry_to_caller(alias):
    driver_error = sqlite3.OperationalError("stale snapshot")
    driver_error.sqlite_errorcode = sqlite3.SQLITE_BUSY_SNAPSHOT
    error = OperationalError("stale snapshot")
    error.__cause__ = driver_error

    with (
        patch.object(services.router, "db_for_write", return_value=alias),
        patch.object(
            PipelineHeartbeat.objects, "update_or_create", side_effect=error
        ) as write,
        patch.object(services.time, "sleep") as sleep,
    ):
        with pytest.raises(OperationalError) as raised, transaction.atomic(using=alias):
            PipelineHeartbeat.objects.using(alias).create(name="caller-write")
            if alias == "default":
                services._write_heartbeat(
                    "synthetic", status="ok", last_error="", details=None, success=True
                )
            else:
                services.record_heartbeat("synthetic", status="ok")

    assert raised.value is error
    write.assert_called_once()
    sleep.assert_not_called()
    assert (
        not PipelineHeartbeat.objects.using(alias).filter(name="caller-write").exists()
    )
