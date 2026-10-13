from datetime import UTC, datetime, timedelta

import pytest

from graphyard.models import IngestToken

NOW = datetime(2026, 10, 13, 12, tzinfo=UTC)


@pytest.fixture
def ingest(db, client, monkeypatch):
    token = IngestToken(name="limits")
    token.set_token("limits-secret")
    token.save()
    written = []
    monkeypatch.setattr("graphyard.views._ingest_now", lambda: NOW)
    monkeypatch.setattr(
        "graphyard.views.write_points",
        lambda points: written.extend(points) or len(points),
    )

    def post(changes):
        point = {"ts": NOW.isoformat(), "host": "edge", "metric": "m", "value": 1}
        return client.post(
            "/v1/metrics",
            [{**point}, {**point, **changes}],
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer limits-secret",
        )

    return post, written


@pytest.mark.parametrize(
    "seconds,expected", [(-604800, 202), (300, 202), (-604800.001, 400), (300.001, 400)]
)
def test_timestamp_edges(ingest, seconds, expected):
    post, written = ingest
    response = post({"ts": (NOW + timedelta(seconds=seconds)).isoformat()})
    assert response.status_code == expected
    if expected == 400:
        assert "index 1" in response.json()["error"]
        assert not written


def test_configured_backfill(ingest, settings):
    settings.GRAPHYARD_INGEST_MAX_AGE_SECONDS = 1209600
    settings.GRAPHYARD_INGEST_MAX_FUTURE_SECONDS = 600
    post, _ = ingest
    assert post({"ts": (NOW - timedelta(days=14)).isoformat()}).status_code == 202
    assert post({"ts": (NOW + timedelta(seconds=600)).isoformat()}).status_code == 202


@pytest.mark.parametrize(
    "field",
    ["metric", "host", "service", "subject_id", "collector_host", "source_instance"],
)
@pytest.mark.parametrize("length,expected", [(256, 202), (257, 400)])
def test_identity_lengths(ingest, field, length, expected):
    post, written = ingest
    changes = {field: "a" * length}
    if field == "subject_id":
        changes["host"] = changes[field]
    response = post(changes)
    assert response.status_code == expected
    if expected == 400:
        assert "index 1" in response.json()["error"]
        assert not written


@pytest.mark.parametrize(
    "tags,expected",
    [
        ({str(i): "v" for i in range(32)}, 202),
        ({str(i): "v" for i in range(33)}, 400),
        ({"k" * 256: "v" * 256}, 202),
        ({"k" * 257: "v"}, 400),
        ({"k": "v" * 257}, 400),
    ],
)
def test_tag_limits(ingest, tags, expected):
    post, written = ingest
    response = post({"tags": tags})
    assert response.status_code == expected
    if expected == 400:
        assert "index 1" in response.json()["error"]
        assert not written


@pytest.mark.parametrize(
    "timestamp", ["0001-01-01T00:00:00+01:00", "9999-12-31T23:59:59-01:00"]
)
def test_extreme_timezone_timestamp_is_indexed_error(ingest, timestamp):
    post, written = ingest
    response = post({"ts": timestamp})
    assert response.status_code == 400
    assert "index 1" in response.json()["error"]
    assert not written
