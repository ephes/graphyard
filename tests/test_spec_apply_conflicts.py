"""Synthetic spec-apply conflicts on real independent database connections."""

import json
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from threading import Barrier, Event

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection, connections

from graphyard.models import MetricCollectionSpec

pytestmark = pytest.mark.django_db(transaction=True)


def desired(name="target", interval=60):
    return {
        "name": name,
        "spec_type": "http_page_probe",
        "config": {"url": "https://example.test/"},
        "interval_seconds": interval,
    }


def file(tmp_path, specs, name="synthetic.json"):
    path = tmp_path / name
    path.write_text(json.dumps(specs))
    return str(path)


def apply(path, prune=False):
    output = StringIO()
    try:
        call_command(
            "apply_metric_collection_specs", file=path, prune=prune, stdout=output
        )
        return "ok", output.getvalue()
    except CommandError as error:
        assert output.getvalue() == ""
        return "conflict", str(error)
    finally:
        connections.close_all()


@pytest.mark.parametrize("create", [False, True])
def test_simultaneous_apply_conflicts_are_retryable(tmp_path, monkeypatch, create):
    if not create:
        MetricCollectionSpec.objects.create(**desired(interval=300))
    paths = [
        file(tmp_path, [desired(interval=i)], name=f"synthetic-{i}.json")
        for i in (60, 120)
    ]
    barrier = Barrier(2)
    original = MetricCollectionSpec.full_clean
    if connection.vendor == "postgresql":

        def clean(self, *args, **kwargs):
            original(self, *args, **kwargs)
            barrier.wait(timeout=10)

        monkeypatch.setattr(MetricCollectionSpec, "full_clean", clean)

    def run(path):
        if connection.vendor != "postgresql":
            barrier.wait(timeout=10)
        return apply(path)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, path) for path in paths]
        results = [future.result(timeout=15) for future in futures]
    monkeypatch.setattr(MetricCollectionSpec, "full_clean", original)
    assert any(result[0] == "ok" for result in results)
    if connection.vendor == "postgresql":
        assert sorted(result[0] for result in results) == ["conflict", "ok"]
    assert MetricCollectionSpec.objects.count() == 1
    for path, result in zip(paths, results):
        if result[0] == "conflict":
            assert "retry" in result[1].lower()
            assert apply(path)[0] == "ok"
    assert MetricCollectionSpec.objects.count() == 1


@pytest.mark.parametrize("edit_target", [True, False])
@pytest.mark.parametrize("desired_interval", [60, 300])
def test_admin_edit_wins_over_stale_apply_and_rolls_back_prune(
    tmp_path, monkeypatch, edit_target, desired_interval
):
    if connection.vendor != "postgresql":
        pytest.skip("PostgreSQL concurrent write oracle; SQLite serializes writers")
    target = MetricCollectionSpec.objects.create(**desired(interval=300))
    stale = MetricCollectionSpec.objects.create(**desired("keep-on-conflict"))
    path = file(
        tmp_path,
        [desired("created-before-conflict"), desired(interval=desired_interval)],
    )
    held, release = Event(), Event()
    original = MetricCollectionSpec.full_clean

    def clean(self, *args, **kwargs):
        original(self, *args, **kwargs)
        if self.name == "target":
            held.set()
            assert release.wait(timeout=10)

    monkeypatch.setattr(MetricCollectionSpec, "full_clean", clean)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(apply, path, True)
        try:
            assert held.wait(timeout=10)
            # The same public save path used by Django admin.
            edited = target if edit_target else stale
            edited.interval_seconds = 180
            edited.save()
        finally:
            release.set()
        result = future.result(timeout=15)
    assert result[0] == "conflict"
    assert "retry" in result[1].lower()
    target.refresh_from_db()
    assert target.interval_seconds == (180 if edit_target else 300)
    edited.refresh_from_db()
    assert edited.interval_seconds == 180
    assert MetricCollectionSpec.objects.filter(pk=stale.pk).exists()
    assert not MetricCollectionSpec.objects.filter(
        name="created-before-conflict"
    ).exists()


@pytest.mark.parametrize(
    "failure",
    [
        "integrity",
        "contention",
        "readonly",
        "connection",
        "serialization",
        "deadlock",
        "lock-timeout",
    ],
)
def test_storage_conflict_rolls_back_apply_before_output(
    tmp_path, monkeypatch, failure
):
    from django.db import IntegrityError, OperationalError

    MetricCollectionSpec.objects.create(**desired("keep-on-conflict"))
    before = list(MetricCollectionSpec.objects.values())
    path = file(tmp_path, [desired("first-created"), desired("fail-second")])
    original = MetricCollectionSpec.save
    triggered = []

    def save(self, *args, **kwargs):
        if self.name == "fail-second":
            triggered.append(True)
            import sqlite3

            if failure in {"contention", "readonly"}:
                cause = sqlite3.OperationalError("private synthetic database detail")
                cause.sqlite_errorcode = (
                    sqlite3.SQLITE_BUSY
                    if failure == "contention"
                    else sqlite3.SQLITE_READONLY
                )
                raise OperationalError("private synthetic database detail") from cause
            if failure in {"serialization", "deadlock", "lock-timeout"}:
                cause = Exception("private synthetic database detail")
                cause.sqlstate = {
                    "serialization": "40001",
                    "deadlock": "40P01",
                    "lock-timeout": "55P03",
                }[failure]
                raise OperationalError("private synthetic database detail") from cause
            error_type = IntegrityError if failure == "integrity" else OperationalError
            raise error_type("private synthetic database detail")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(MetricCollectionSpec, "save", save)
    output = StringIO()
    retryable = failure in {"contention", "serialization", "deadlock", "lock-timeout"}
    expected = "retry" if retryable else "storage failure"
    with pytest.raises(CommandError, match=expected) as error:
        call_command(
            "apply_metric_collection_specs", file=path, prune=True, stdout=output
        )
    assert triggered == [True]
    assert "private synthetic" not in str(error.value)
    if not retryable:
        assert "retry" not in str(error.value).lower()
    assert output.getvalue() == ""
    assert list(MetricCollectionSpec.objects.values()) == before
