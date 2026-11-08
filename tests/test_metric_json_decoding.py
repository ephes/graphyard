"""Raw synthetic wire payloads exercise decoding before ingest side effects."""

from datetime import UTC, datetime
import json
import sys

import pytest

from graphyard.models import (
    HostRegistry,
    IngestToken,
    IngestTokenHostUsage,
    PipelineHeartbeat,
    ServiceRegistry,
    SubjectRegistry,
)

POINT = '{"ts":"2026-11-08T00:00:00Z","host":"edge","metric":"m","value":1}'
DUPLICATES = [
    '{"metrics":[],"metrics":[' + POINT + "]}",
    "[" + POINT.replace('"value":1', '"value":99,"value":1') + "]",
    "[" + POINT.replace('"host":"edge"', '"host":"foreign","host":"edge"') + "]",
    "["
    + POINT.replace('"value":1', '"value":1,"tags":{"private":1,"private":2}')
    + "]",
]


@pytest.mark.django_db
@pytest.mark.parametrize("ndjson", [False, True])
@pytest.mark.parametrize(
    "bad",
    ["[" + POINT[:-1] + ',"unused":' + "9" * 5000 + "}]", *DUPLICATES],
    ids=["integer", "envelope", "value", "host", "nested"],
)
def test_decoder_rejections_are_bounded_and_have_no_ingest_side_effects(
    client, monkeypatch, caplog, ndjson, bad
):
    # Fix the interpreter policy for this test rather than depend on machine env.
    previous = sys.get_int_max_str_digits()
    sys.set_int_max_str_digits(4300)
    token = IngestToken(name="synthetic", bound_subject_id="edge")
    token.set_token("synthetic-secret")
    token.save()
    written = []
    monkeypatch.setattr(
        "graphyard.views._ingest_now", lambda: datetime(2026, 11, 8, tzinfo=UTC)
    )
    monkeypatch.setattr("graphyard.views.write_points", lambda p: written.append(p))
    # Put the bad record first too: json.loads may fail before detecting extra data.
    body = (bad.removeprefix("[").removesuffix("]") + "\n" + POINT) if ndjson else bad
    try:
        response = client.post(
            "/v1/metrics",
            body,
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer synthetic-secret",
        )
    finally:
        sys.set_int_max_str_digits(previous)
    assert response.status_code == 400
    error = response.json()["error"]
    assert len(error) < 100
    assert "private" not in error and "foreign" not in error
    assert (
        "integer" in error
        if '"unused"' in bad
        else "Duplicate JSON object key at member" in error
    )
    assert not written
    token.refresh_from_db()
    assert token.last_used_at is None
    for model in (HostRegistry, ServiceRegistry, SubjectRegistry, IngestTokenHostUsage):
        assert not model.objects.exists()
    heartbeat = PipelineHeartbeat.objects.get(name="metric_ingest")
    assert heartbeat.details == {
        "category": "parse",
        "parse_rejected": 1,
        "normalization_rejected": 0,
        "rejected_count": 1,
        "total_metrics": 2 if ndjson else 0,
    }
    assert heartbeat.last_error == error
    assert any(
        "category=parse parse_rejected=1" in r.getMessage() for r in caplog.records
    )


@pytest.mark.django_db
@pytest.mark.parametrize("ndjson", [False, True])
def test_repeated_array_records_are_still_accepted(client, monkeypatch, ndjson):
    from django.utils import timezone

    token = IngestToken(name="synthetic")
    token.set_token("synthetic-secret")
    token.save()
    point = json.loads(POINT)
    point["ts"] = timezone.now().isoformat()
    body = "\n".join([json.dumps(point)] * 2) if ndjson else json.dumps([point] * 2)
    written = []
    monkeypatch.setattr(
        "graphyard.views.write_points", lambda p: written.extend(p) or len(p)
    )
    response = client.post(
        "/v1/metrics",
        body,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer synthetic-secret",
    )
    assert response.status_code == 202
    assert len(written) == 2
