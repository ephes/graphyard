from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import httpx
import pytest

from graphyard import influx
from graphyard.models import ConditionDefinition, StatusLevel
from graphyard.services import evaluate_conditions_once


NOW = datetime(2026, 10, 13, 12, tzinfo=UTC)


@pytest.fixture(autouse=True)
def influx_url(settings):
    settings.INFLUX_URL = "http://influx.local"


def install_rows(monkeypatch, settings, backend, values):
    settings.INFLUX_API_MODE = backend
    rows = [
        {
            "time": (NOW - timedelta(minutes=len(values) - index - 1)).isoformat(),
            "value": value,
            "metric": "host.disk_usage",
            "host": "h1",
            "mountpoint": "/" if index else "/data",
        }
        for index, value in enumerate(values)
    ]
    if backend == "v3":
        monkeypatch.setattr(
            influx.httpx,
            "post",
            lambda *a, **k: httpx.Response(
                200, json=rows, request=httpx.Request("POST", "http://influx/query")
            ),
        )
        return

    class Record:
        def __init__(self, row):
            self.values = {**row, "_value": row["value"]}

        def get_value(self):
            return self.values["_value"]

        def get_time(self):
            return datetime.fromisoformat(self.values["time"])

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def query_api(self):
            return SimpleNamespace(
                query=lambda **kwargs: [
                    SimpleNamespace(records=[Record(r) for r in rows])
                ]
            )

    monkeypatch.setattr(influx, "_build_client", Client)


@pytest.mark.parametrize("backend", ["v2", "v3", "auto"])
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_nonfinite_query_evidence_is_an_evaluation_error(
    db, monkeypatch, settings, backend, value
):
    # auto uses Flux here: a decoding error must not trigger SQL fallback.
    install_rows(monkeypatch, settings, backend, [20.0, 25.0, value])
    condition = ConditionDefinition.objects.create(
        name="query-safety",
        metric_name="host.disk_usage",
        warning_threshold=60,
        critical_threshold=75,
        alert_when_stale=False,
        status=StatusLevel.OK,
        last_value=20.0,
        last_evaluated=NOW - timedelta(hours=1),
    )
    run = evaluate_conditions_once(condition_id=condition.id)
    condition.refresh_from_db()

    assert run.failed == 1
    assert condition.status == StatusLevel.CRITICAL
    assert "evaluation error" in condition.message
    assert "finite" in condition.message
    assert condition.last_evaluated == NOW - timedelta(hours=1)
    assert condition.last_value == 20.0


@pytest.mark.parametrize("backend", ["v2", "v3"])
def test_finite_query_keeps_multiple_series(monkeypatch, settings, backend):
    install_rows(monkeypatch, settings, backend, [20.0, 25.0])
    samples = influx.query_range("host.disk_usage", NOW - timedelta(minutes=5), NOW)
    assert [s.value for s in samples] == [20.0, 25.0]
    assert [s.tags["mountpoint"] for s in samples] == ["/data", "/"]
