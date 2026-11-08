"""Corrupt query evidence must not disappear from condition evaluation."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from influxdb_client.client.flux_table import FluxRecord
import pytest

from graphyard import influx
from graphyard.models import ConditionDefinition, StatusLevel
from graphyard.services import evaluate_conditions_once

NOW = datetime(2026, 11, 8, tzinfo=UTC)


@pytest.mark.django_db
@pytest.mark.parametrize("backend", ["v2", "v3", "auto"])
@pytest.mark.parametrize(
    "fault",
    [
        "missing-value",
        "missing-time",
        "bad-time",
        "overflow-time",
        "null-value",
        "null-time",
    ],
)
def test_corrupt_row_beside_healthy_evidence_fails_evaluation(
    monkeypatch, settings, backend, fault
):
    settings.INFLUX_API_MODE = backend
    settings.INFLUX_URL = "http://influx.invalid"
    good = {"time": NOW.isoformat(), "value": 10, "host": "synthetic", "metric": "m"}
    bad = {**good, "value": 99}
    if fault.startswith("missing-"):
        bad.pop(fault.removeprefix("missing-"))
    elif fault.startswith("null-"):
        bad[fault.removeprefix("null-")] = None
    else:
        bad["time"] = (
            "9999-12-31T23:59:59-01:00"
            if fault == "overflow-time"
            else "private-invalid-time"
        )
    if backend == "v3":

        async def rows(*args):
            return [good, bad]

        monkeypatch.setattr(influx, "_read_v3_sql_rows", rows)
    else:
        records = []
        for row in (good, bad):
            values = {"metric": "m", "host": "synthetic"}
            if "value" in row:
                values["_value"] = row["value"]
            if "time" in row:
                values["_time"] = (
                    NOW
                    if row is good
                    or fault not in ("bad-time", "overflow-time", "null-time")
                    else row["time"]
                )
            records.append(FluxRecord(0, values))
        monkeypatch.setattr(
            influx, "_query_flux_tables", lambda _: [SimpleNamespace(records=records)]
        )
    condition = ConditionDefinition.objects.create(
        name="synthetic",
        metric_name="m",
        warning_threshold=60,
        critical_threshold=75,
        alert_when_stale=False,
        status=StatusLevel.OK,
        last_value=17,
        last_evaluated=NOW - timedelta(hours=1),
    )
    monkeypatch.setattr("graphyard.services.timezone.now", lambda: NOW)
    result = evaluate_conditions_once(condition_id=condition.pk)
    condition.refresh_from_db()
    assert result.failed == 1
    assert condition.status == StatusLevel.CRITICAL
    assert condition.message == "evaluation error: InfluxConfigurationError"
    assert condition.last_value == 17


@pytest.mark.parametrize("row", [None, [], "private", 42])
def test_sql_nonobject_row_refuses_entire_response(monkeypatch, settings, row):
    settings.INFLUX_API_MODE = "v3"
    settings.INFLUX_URL = "http://influx.invalid"

    async def rows(*args):
        return [{"time": NOW.isoformat(), "value": 10}, row]

    monkeypatch.setattr(influx, "_read_v3_sql_rows", rows)
    with pytest.raises(influx.InfluxConfigurationError, match="Invalid.*row"):
        influx.query_range("m", NOW, NOW)


@pytest.mark.parametrize("backend", ["v2", "v3"])
def test_genuinely_empty_response_remains_empty(monkeypatch, settings, backend):
    settings.INFLUX_API_MODE = backend
    settings.INFLUX_URL = "http://influx.invalid"

    async def rows(*args):
        return []

    monkeypatch.setattr(influx, "_read_v3_sql_rows", rows)
    monkeypatch.setattr(influx, "_query_flux_tables", lambda _: [])
    assert influx.query_range("m", NOW, NOW) == []
