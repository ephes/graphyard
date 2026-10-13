from datetime import UTC, datetime, timedelta

import pytest

from graphyard.models import (
    HostRegistry,
    IngestToken,
    ServiceRegistry,
    SubjectRegistry,
)

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
    ["metric", "collector_host", "source_instance"],
)
@pytest.mark.parametrize("length,expected", [(256, 202), (257, 400)])
def test_identity_lengths(ingest, field, length, expected):
    post, written = ingest
    changes = {field: "a" * length}
    response = post(changes)
    assert response.status_code == expected
    if expected == 400:
        assert "index 1" in response.json()["error"]
        assert not written


@pytest.mark.parametrize(
    "field,model,model_field",
    [
        ("host", HostRegistry, "host_id"),
        ("service", ServiceRegistry, "service_id"),
        ("subject_id", SubjectRegistry, "subject_id"),
        ("source_system", SubjectRegistry, "source_system"),
    ],
)
@pytest.mark.parametrize("extra", [0, 1, 128])
def test_registry_identity_lengths(ingest, field, model, model_field, extra):
    post, written = ingest
    limit = model._meta.get_field(model_field).max_length
    changes = {"source_system": "legacy"}
    changes[field] = "a" * (limit + extra)
    if field == "subject_id":
        changes.update(subject_type="service", host=None)
    response = post(changes)
    assert response.status_code == (400 if extra else 202)
    if extra:
        error = response.json()["error"]
        assert "index 1" in error
        assert field in error
        assert str(limit) in error
        assert not written
        assert not HostRegistry.objects.exists()
        assert not ServiceRegistry.objects.exists()
        assert not SubjectRegistry.objects.exists()
    else:
        assert len(written) == 2
        assert model.objects.filter(**{model_field: changes[field]}).exists()


@pytest.mark.parametrize("subject_type", ["host", "service"])
def test_normalized_subject_identity_length(ingest, subject_type):
    post, written = ingest
    # Lowercasing can expand Unicode: İ becomes i + a combining dot, then i_.
    response = post(
        {
            "host": None,
            "subject_type": subject_type,
            "subject_id": "İ" * 65,
            "collector_host": "edge",
        }
    )
    assert response.status_code == 400
    assert "subject_id" in response.json()["error"]
    assert "128" in response.json()["error"]
    assert not written
    assert not SubjectRegistry.objects.exists()


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


def test_service_fallback_source_system_length(ingest):
    post, written = ingest
    response = post({"service": "a" * 65})
    assert response.status_code == 400
    assert "source_system" in response.json()["error"]
    assert "64" in response.json()["error"]
    assert not written
    assert not SubjectRegistry.objects.exists()
