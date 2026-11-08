"""Current-token authorization at the external write boundary, synthetic only."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from django.db import DatabaseError, connections
from django.test import Client
from django.utils import timezone

from graphyard import views
from graphyard.models import IngestToken

pytestmark = pytest.mark.django_db(transaction=True)


def token():
    row = IngestToken(name="synthetic", last_used_at=timezone.now())
    row.set_token("synthetic-secret")
    row.save()
    return row


def post():
    return Client().post(
        "/v1/metrics",
        [{"ts": timezone.now().isoformat(), "host": "edge", "metric": "m", "value": 1}],
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer synthetic-secret",
    )


@pytest.mark.parametrize("change", ["revoke", "scope", "binding", "hash", "delete"])
def test_token_changed_during_parsing_is_rechecked(monkeypatch, change):
    row = token()
    parse = views._parse_metrics_payload
    writes = []

    def parse_then_change(payload):
        points = parse(payload)
        if change == "revoke":
            row.revoke()
        elif change == "scope":
            row.host_scope = ["other"]
            row.save(update_fields=["host_scope"])
        elif change == "binding":
            row.bound_subject_id = "other"
            row.save(update_fields=["bound_subject_id"])
        elif change == "hash":
            row.set_token("replacement-secret")
            row.save(update_fields=["token_hash"])
        else:
            row.delete()
        return points

    monkeypatch.setattr(views, "_parse_metrics_payload", parse_then_change)
    monkeypatch.setattr(
        views, "write_points", lambda points: writes.append(points) or 1
    )
    response = post()
    assert response.status_code == (403 if change in ("scope", "binding") else 401)
    assert writes == []


def test_admitted_write_precedes_completed_revocation(monkeypatch):
    row = token()
    writing, finish, attempting, revoked = Event(), Event(), Event(), Event()
    events = []

    def write(points):
        writing.set()
        assert finish.wait(10)
        events.append("sink finished")
        return len(points)

    def ingest():
        try:
            return post().status_code
        finally:
            connections.close_all()

    def revoke():
        try:
            attempting.set()
            try:
                row.revoke()
            except DatabaseError:
                # SQLite shared-cache refuses contention rather than waiting.
                # A failed revoke is not a completed authorization boundary.
                return False
            events.append("revoked")
            revoked.set()
            return True
        finally:
            connections.close_all()

    monkeypatch.setattr(views, "write_points", write)
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(ingest)
        try:
            assert writing.wait(10)
            revocation = pool.submit(revoke)
            assert attempting.wait(10)
            assert not revoked.wait(0.2)
        finally:
            finish.set()
        assert pending.result(15) == 202
        completed = revocation.result(15)
    if not completed:
        row.revoke()
        events.append("revoked")
    assert events == ["sink finished", "revoked"]
    assert post().status_code == 401


def test_admission_database_error_does_not_call_sink_or_heartbeat(monkeypatch):
    from django.db.models.query import QuerySet
    from django.db import OperationalError

    token()
    update = QuerySet.update
    calls = []

    def fail_admission(self, **kwargs):
        if self.model is IngestToken and "enabled" in kwargs:
            raise OperationalError("synthetic private lock detail")
        return update(self, **kwargs)

    monkeypatch.setattr(QuerySet, "update", fail_admission)
    monkeypatch.setattr(views, "write_points", lambda points: calls.append("sink"))
    monkeypatch.setattr(
        views, "_record_heartbeat_safe", lambda *a, **k: calls.append("heartbeat")
    )
    response = post()
    assert response.status_code == 503
    assert "private" not in response.content.decode()
    assert calls == []
