"""Probe transport secrets must never become metric/registry provenance."""

import json

import httpx
import pytest

from graphyard import influx, services
from graphyard.models import (
    MetricCollectionSpec,
    SubjectRegistry,
    ServiceRegistry,
    HostRegistry,
)


@pytest.mark.django_db
@pytest.mark.parametrize("identity", [None, "catalog_variant_a", "catalog_variant_b"])
def test_probe_identity_keeps_auth_private_and_transport_unchanged(
    monkeypatch, identity
):
    query = "?signature=synthetic-secret" if identity else ""
    url = f"https://synthetic-user:synthetic-password@probe.test/catalog{query}#private-fragment"
    config = {"url": url, "subject_id": "synthetic_probe"}
    if identity:
        config["source_entity_id"] = identity
    spec = MetricCollectionSpec(
        name="synthetic probe", spec_type="http_page_probe", config=config
    )
    requests = []
    real_client = httpx.Client

    def respond(request):
        requests.append(request)
        return httpx.Response(200, content=b"ok")

    monkeypatch.setattr(
        services.httpx,
        "Client",
        lambda **kwargs: real_client(**kwargs, transport=httpx.MockTransport(respond)),
    )
    points = []

    def write(batch):
        points.extend(batch)
        return len(batch)

    monkeypatch.setattr(influx, "write_points", write)
    outcome = services._execute_http_page_probe_spec(spec)
    assert outcome[:3] == ("ok", 5, 0)
    assert (
        requests[0].headers["authorization"]
        == "Basic c3ludGhldGljLXVzZXI6c3ludGhldGljLXBhc3N3b3Jk"
    )
    assert requests[0].url.query == (b"signature=synthetic-secret" if identity else b"")
    expected = identity or "https://probe.test/catalog"
    assert {p.source_entity_id for p in points} == {expected}
    wire = "\n".join(
        influx.Point("metrics")
        .tag("source_entity_id", p.source_entity_id)
        .field("value", p.value)
        .to_line_protocol()
        for p in points
    )
    registry = json.dumps(
        [
            list(model.objects.values())
            for model in (SubjectRegistry, ServiceRegistry, HostRegistry)
        ],
        default=str,
    )
    for secret in (
        "synthetic-user",
        "synthetic-password",
        "synthetic-secret",
        "private-fragment",
    ):
        assert secret not in repr(points) + wire + registry


def test_query_selected_endpoints_require_explicit_identity_before_transport(
    monkeypatch,
):
    def no_client(**kwargs):
        pytest.fail("query endpoint without source identity reached transport")

    monkeypatch.setattr(services.httpx, "Client", no_client)
    spec = MetricCollectionSpec(
        name="synthetic",
        spec_type="http_page_probe",
        config={
            "url": "https://probe.test/catalog?variant=a",
            "subject_id": "synthetic",
        },
    )
    assert services._execute_http_page_probe_spec(spec) == (
        "critical",
        0,
        0,
        "page probes with query parameters require config.source_entity_id",
    )
