"""Collector exceptions may contain URL credentials or arbitrary remote bodies."""

import logging

import httpx
import pytest
from django.urls import reverse

from graphyard import services
from graphyard.models import MetricCollectionSpec, PipelineHeartbeat


SENTINEL = "invented-credential-body-marker"
CONFIGS = {
    "home_assistant_sensor": {
        "base_url": "https://example.test",
        "access_token": "synthetic",
        "entity_id": "sensor.test",
    },
    "home_assistant_env_scan": {
        "base_url": "https://example.test",
        "access_token": "synthetic",
    },
    "http_json_metric": {
        "url": "https://example.test/value",
        "metric_path": "value",
        "metric_name": "test.value",
    },
    "http_page_probe": {"url": "https://example.test/page", "subject_id": "test"},
    "unifi_device_traffic": {
        "base_url": "https://example.test",
        "username": "synthetic",
        "password": "synthetic",
        "device_name": "test",
        "subject_id": "test",
    },
}


@pytest.mark.django_db
@pytest.mark.parametrize("spec_type", CONFIGS)
@pytest.mark.parametrize("failure", ["transport", "unexpected", "http"])
def test_collector_failure_is_safe_before_persistence(
    client, django_user_model, monkeypatch, caplog, spec_type, failure
):
    client.force_login(django_user_model.objects.create_user(username="synthetic"))

    def respond(request):
        if failure == "transport":
            raise httpx.ConnectError(
                f"https://user:{SENTINEL}@example.test/?key={SENTINEL}", request=request
            )
        if failure == "unexpected":
            raise RuntimeError(SENTINEL)
        return httpx.Response(503, text=SENTINEL)

    real_client = httpx.Client
    monkeypatch.setattr(
        services.httpx,
        "Client",
        lambda **kwargs: real_client(transport=httpx.MockTransport(respond), **kwargs),
    )
    monkeypatch.setattr(services.influx, "write_points", lambda points: len(points))
    monkeypatch.setattr(services, "touch_registry_from_points", lambda points: None)
    config = dict(CONFIGS[spec_type])
    key = "url" if "url" in config else "base_url"
    config[key] = (
        config[key].replace("https://", f"https://user:{SENTINEL}@")
        # UniFi refuses base URL queries before transport; keep this test on
        # its transport/error path with a private path segment instead.
        + (
            f"/{SENTINEL}"
            if spec_type == "unifi_device_traffic"
            else f"?key={SENTINEL}"
        )
    )
    spec = MetricCollectionSpec.objects.create(
        name="synthetic", spec_type=spec_type, config=config
    )
    with caplog.at_level(logging.INFO):
        run = services.run_metric_collection_specs_once()
    assert run.failed + run.warning == 1
    spec.refresh_from_db()
    assert spec.last_error
    heartbeat = PipelineHeartbeat.objects.get(name="metric_collectors")
    assert SENTINEL not in spec.last_error + heartbeat.last_error + caplog.text
    if failure in ("transport", "unexpected"):
        assert (
            "ConnectError" if failure == "transport" else "RuntimeError"
        ) in spec.last_error
    if failure == "http" and spec_type == "http_json_metric":
        assert "503" in spec.last_error
    monkeypatch.setattr("graphyard.views.influx_health", lambda: {"status": "pass"})
    assert SENTINEL not in client.get(reverse("graphyard:health")).content.decode()


@pytest.mark.django_db
def test_escaping_collector_exception_does_not_log_traceback(monkeypatch, caplog):
    spec = MetricCollectionSpec.objects.create(
        name="synthetic", spec_type="http_json_metric", config={}
    )

    def fail(spec):
        raise RuntimeError(SENTINEL)

    monkeypatch.setitem(services._SPEC_EXECUTORS, spec.spec_type, fail)
    services.run_metric_collection_specs_once()
    spec.refresh_from_db()
    assert SENTINEL not in spec.last_error + caplog.text
    assert "RuntimeError" in spec.last_error
    assert all(r.exc_info is None for r in caplog.records)
