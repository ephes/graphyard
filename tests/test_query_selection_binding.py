"""Complete synthetic replies must match the query before any evaluation."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from graphyard import influx, services
from graphyard.models import ConditionDefinition

NOW = datetime(2026, 11, 1, tzinfo=UTC)


def install(monkeypatch, settings, mode, changes):
    settings.INFLUX_API_MODE = mode
    good = dict(
        time=(NOW - timedelta(seconds=1)).isoformat(),
        value=99,
        metric="cpu",
        host="edge",
        service="web",
        subject_type="host",
        subject_id="edge",
        mountpoint="/",
    )
    bad = {**good, **changes}
    if bad.pop("drop_metric", False):
        bad.pop("metric")
    rows = [good, bad]

    async def sql(*args):
        return rows

    monkeypatch.setattr(influx, "_read_v3_sql_rows", sql)
    records = [
        SimpleNamespace(
            values=row,
            get_value=lambda r=row: r["value"],
            get_time=lambda r=row: datetime.fromisoformat(r["time"]),
        )
        for row in rows
    ]
    monkeypatch.setattr(
        influx, "_query_flux_tables", lambda query: [SimpleNamespace(records=records)]
    )


@pytest.mark.parametrize("mode", ["v2", "v3", "auto"])
@pytest.mark.parametrize(
    "changes",
    [
        dict(metric="other"),
        dict(drop_metric=True),
        dict(metric=None),
        dict(host="other"),
        dict(service="other"),
        dict(subject_type="sensor"),
        dict(subject_id="other"),
        dict(mountpoint="/other"),
        dict(time=NOW.isoformat()),
        dict(time=(NOW + timedelta(seconds=1)).isoformat()),
        dict(time=(NOW - timedelta(hours=2)).isoformat()),
    ],
)
def test_entire_reply_rejected_on_selection_mismatch(
    monkeypatch, settings, mode, changes
):
    install(monkeypatch, settings, mode, changes)
    with pytest.raises(influx.InfluxConfigurationError, match="selection"):
        influx.query_range(
            "cpu",
            NOW - timedelta(hours=1),
            NOW,
            host="edge",
            service="web",
            subject_type="host",
            subject_id="edge",
            tags={"mountpoint": "/"},
        )


@pytest.mark.parametrize("mode", ["v2", "v3"])
def test_start_inclusive_and_unfiltered_sparse_dimensions(monkeypatch, settings, mode):
    install(
        monkeypatch,
        settings,
        mode,
        {
            "time": (NOW - timedelta(hours=1)).isoformat(),
            "host": "other",
            "mountpoint": "/other",
        },
    )
    samples = influx.query_range("cpu", NOW - timedelta(hours=1), NOW)
    assert len(samples) == 2


@pytest.mark.django_db
@pytest.mark.parametrize("mode", ["v2", "v3"])
def test_selection_error_publishes_failure_without_partial_value(
    monkeypatch, settings, mode
):
    install(monkeypatch, settings, mode, {"metric": "other"})
    row = ConditionDefinition.objects.create(
        name="synthetic", metric_name="cpu", warning_threshold=50, last_value=7
    )
    monkeypatch.setattr(services.timezone, "now", lambda: NOW)
    assert services.evaluate_conditions_once(condition_id=row.pk).failed == 1
    row.refresh_from_db()
    assert row.last_value == 7
    assert row.status == "critical"
    assert row.message == "evaluation error: InfluxConfigurationError"
