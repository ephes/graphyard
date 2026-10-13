from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from graphyard import influx
from graphyard.views import _parse_metrics_payload, MetricsPayloadValidationError


@pytest.mark.parametrize(
    "value,escaped", [("${x}", r"\${x}"), (r"\${x}", r"\\\${x}"), ("$5", "$5")]
)
def test_flux_interpolation_literal_in_query(monkeypatch, settings, value, escaped):
    settings.INFLUX_BUCKET = value
    settings.INFLUX_MEASUREMENT = value
    captured = []

    class Client:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def query_api(self):
            return SimpleNamespace(
                query=lambda **kw: captured.append(kw["query"]) or []
            )

    monkeypatch.setattr(influx, "_build_client", Client)
    now = datetime.now(UTC)
    influx._query_range_v2_flux(
        value, now, now, host=value, service=value, tags={value: value}
    )
    assert f'from(bucket: "{escaped}")' in captured[0]
    assert f'r["{escaped}"] == "{escaped}"' in captured[0]
    assert f'r["metric"] == "{escaped}"' in captured[0]
    assert f'r._measurement == "{escaped}"' in captured[0]
    assert f'r["host"] == "{escaped}"' in captured[0]
    assert f'r["service"] == "{escaped}"' in captured[0]


def test_metric_log_escapes_newline(caplog):
    with pytest.raises(MetricsPayloadValidationError):
        _parse_metrics_payload(
            [
                {
                    "ts": datetime.now(UTC).isoformat(),
                    "host": "edge",
                    "subject_type": "invalid",
                    "metric": "first\nforged",
                    "value": 1,
                }
            ]
        )
    records = [r for r in caplog.records if "Rejected metric payload" in r.getMessage()]
    assert len(records) == 1
    assert "\n" not in records[0].getMessage()
    assert r"first\nforged" in records[0].getMessage()


def test_ingest_normalization_record_caps_error_details(caplog):
    with pytest.raises(MetricsPayloadValidationError):
        _parse_metrics_payload(
            [{"ts": "x" * 1024, "host": "edge", "metric": "m", "value": 1}]
        )
    records = [r for r in caplog.records if "Rejected metric payload" in r.getMessage()]
    assert len(records) == 1
    assert len(records[0].args[-1]) == 512
