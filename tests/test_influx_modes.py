from __future__ import annotations

from datetime import UTC, datetime, timedelta

from influxdb_client.rest import ApiException
import pytest

from graphyard import influx


def test_query_range_auto_falls_back_to_v3_on_v2_404(monkeypatch, settings):
    settings.INFLUX_API_MODE = "auto"

    start = datetime.now(UTC) - timedelta(minutes=5)
    stop = datetime.now(UTC)
    expected = [
        influx.MetricSample(
            ts=datetime.now(UTC),
            value=42.0,
            host="h1",
            metric="m1",
            service="s1",
            tags={},
        )
    ]

    def _fail_v2(*args, **kwargs):
        del args, kwargs
        raise ApiException(status=404, reason="Not found")

    monkeypatch.setattr(influx, "_query_range_v2_flux", _fail_v2)
    monkeypatch.setattr(influx, "_query_range_v3_sql", lambda *a, **k: expected)

    result = influx.query_range("m1", start, stop)

    assert result == expected


def test_query_range_auto_does_not_hide_non_404_errors(monkeypatch, settings):
    settings.INFLUX_API_MODE = "auto"

    start = datetime.now(UTC) - timedelta(minutes=5)
    stop = datetime.now(UTC)

    def _fail_v2(*args, **kwargs):
        del args, kwargs
        raise ApiException(status=500, reason="server error")

    monkeypatch.setattr(influx, "_query_range_v2_flux", _fail_v2)

    with pytest.raises(ApiException):
        influx.query_range("m1", start, stop)


def test_query_range_v2_flux_uses_safe_bracket_access_for_tag_keys(
    monkeypatch, settings
):
    settings.INFLUX_API_MODE = "v2"
    settings.INFLUX_BUCKET = "graphyard"
    settings.INFLUX_ORG = "graphyard"
    settings.INFLUX_MEASUREMENT = "graphyard_metrics"

    captured: dict[str, str] = {}

    class _FakeQueryApi:
        def query(self, query: str, org: str):
            captured["query"] = query
            captured["org"] = org
            return []

    class _FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            del exc_type, exc, tb

        def query_api(self) -> _FakeQueryApi:
            return _FakeQueryApi()

    monkeypatch.setattr(influx, "_build_client", lambda: _FakeClient())

    start = datetime.now(UTC) - timedelta(minutes=5)
    stop = datetime.now(UTC)
    influx.query_range("m1", start, stop, tags={'bad"tag': 'v"1'})

    query_text = captured["query"]
    assert 'r["bad\\"tag"] == "v\\"1"' in query_text
    assert "r.bad" not in query_text
