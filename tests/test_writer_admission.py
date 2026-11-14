"""Synthetic identities exercise API and direct writer admission."""

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from graphyard import influx


def point(**changes):
    return replace(
        influx.MetricPoint(
            ts=datetime(2026, 11, 14, tzinfo=UTC),
            metric="load",
            value=1,
            subject_type="host",
            subject_id="edge",
            source_system="test",
            collector_service="agent",
            collector_host="edge",
        ),
        **changes,
    )


@pytest.mark.parametrize("control", ["\n", "\r", "\t", "\0", "\x7f", "\x85"])
@pytest.mark.parametrize(
    "field", ["metric", "source_entity_id", "service", "subject_id", "collector_host"]
)
def test_direct_writer_refuses_control_identity(field, control):
    with pytest.raises(ValueError, match="control characters"):
        influx.normalize_metric_point(point(**{field: "a" + control + "b"}))
    influx.normalize_metric_point(
        point(
            **{
                field: r"a\nb"
                if field in ("metric", "source_entity_id", "service")
                else "ab"
            }
        )
    )


@pytest.mark.parametrize("tags", [{"a\nb": "c"}, {"a": "b\nc"}])
def test_custom_tag_controls_refused(tags):
    with pytest.raises(ValueError, match="control characters"):
        influx.normalize_metric_point(point(tags=tags))


@pytest.mark.parametrize("field", ["metric", "host", "service"])
def test_api_refuses_controls_before_effects(client, db, monkeypatch, field):
    from graphyard.models import IngestToken, SubjectRegistry

    token = IngestToken(name="synthetic")
    token.set_token("synthetic-token")
    token.save()
    monkeypatch.setattr(
        "graphyard.views._ingest_now", lambda: datetime(2026, 11, 14, tzinfo=UTC)
    )

    def no_write(_):
        pytest.fail("sink called")

    monkeypatch.setattr("graphyard.views.write_points", no_write)
    payload = {
        "ts": "2026-11-14T00:00:00Z",
        "host": "edge",
        "metric": "load",
        "value": 1,
    }
    payload[field] = "edge\n"
    response = client.post(
        "/v1/metrics",
        data=[payload],
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer synthetic-token",
    )
    assert response.status_code == 400
    assert not SubjectRegistry.objects.exists()


@pytest.mark.parametrize(
    "field,limit",
    [
        ("subject_id", 128),
        ("host", 128),
        ("service", 128),
        ("source_system", 64),
        ("metric", 256),
        ("source_instance", 256),
        ("collector_host", 256),
        ("collector_service", 256),
        ("source_entity_id", 256),
    ],
)
def test_direct_dimensions_match_api_limits(field, limit):
    with pytest.raises(ValueError, match="overlong"):
        influx.normalize_metric_point(point(**{field: "x" * (limit + 1)}))


@pytest.mark.parametrize(
    "tags", [{str(n): "v" for n in range(33)}, {"k": "v" * 257}, {"k" * 257: "v"}]
)
def test_direct_tags_match_api_limits(tags):
    with pytest.raises(ValueError, match="tags|overlong"):
        influx.normalize_metric_point(point(tags=tags))
