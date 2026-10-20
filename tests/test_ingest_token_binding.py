import io
from datetime import timedelta

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.db.migrations.executor import MigrationExecutor
from django.urls import reverse
from django.utils import timezone

from graphyard import services
from graphyard.admin import IngestTokenAdminForm
from graphyard.models import (
    HostRegistry,
    IngestToken,
    IngestTokenHostUsage,
    ServiceRegistry,
    SubjectRegistry,
    PipelineHeartbeat,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def writes(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "graphyard.views.write_points",
        lambda points: calls.append(points) or len(points),
    )
    return calls


def token(binding="edge", **kwargs):
    row = IngestToken(name="producer", bound_subject_id=binding, **kwargs)
    row.set_token("binding-test-credential")
    row.save()
    return row


def point(host="edge", **kwargs):
    return {
        "ts": timezone.now().isoformat(),
        "host": host,
        "metric": "m",
        "value": 1,
        **kwargs,
    }


def post(client, payload, ndjson=False):
    return client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/x-ndjson" if ndjson else "application/json",
        HTTP_AUTHORIZATION="Bearer binding-test-credential",
    )


@pytest.mark.parametrize("enforce", [False, True])
def test_bound_token_accepts_canonical_and_legacy_points(
    client, writes, settings, enforce
):
    settings.GRAPHYARD_REQUIRE_BOUND_INGEST_TOKENS = enforce
    token("edge_lan")
    response = post(
        client,
        [
            point(" Edge.LAN ", service="mail"),
            point(
                None,
                subject_type="host",
                subject_id="edge_lan",
                collector_host="edge-lan",
            ),
        ],
    )
    assert response.status_code == 202
    assert len(writes[0]) == 2
    assert list(HostRegistry.objects.values_list("host_id", flat=True)) == ["edge_lan"]
    assert ServiceRegistry.objects.get().host.host_id == "edge_lan"


@pytest.mark.parametrize(
    "extra",
    [
        {"host": "victim"},
        {
            "host": None,
            "subject_type": "host",
            "subject_id": "victim",
            "collector_host": "edge",
        },
        {
            "host": "edge",
            "subject_type": "host",
            "subject_id": "victim",
            "collector_host": "edge",
        },
        {"collector_host": "victim"},
        {
            "host": None,
            "subject_type": "service",
            "subject_id": "victim",
            "collector_host": "edge",
        },
        {
            "host": "victim",
            "subject_type": "service",
            "subject_id": "edge",
            "collector_host": "edge",
        },
    ],
)
def test_binding_validates_whole_batch_before_any_write(client, writes, extra):
    row = token(host_scope=["edge", "victim"])
    response = post(client, [point(service="new"), point(**extra)])
    assert response.status_code == 403
    assert "binding" in response.json()["error"]
    assert writes == []
    assert not HostRegistry.objects.exists()
    assert not SubjectRegistry.objects.exists()
    assert not ServiceRegistry.objects.exists()
    assert not IngestTokenHostUsage.objects.filter(token=row).exists()


def test_binding_rejects_ndjson_batch(client, writes):
    import json

    token()
    response = post(
        client,
        "\n".join(json.dumps(point(host)) for host in ["edge", "victim"]),
        ndjson=True,
    )
    assert response.status_code == 403
    assert writes == []


def test_bound_token_cannot_take_foreign_service(client, writes):
    token()
    victim = HostRegistry.objects.create(host_id="victim")
    service = ServiceRegistry.objects.create(service_id="mail", host=victim)
    response = post(client, [point(service="new"), point(service="mail")])
    assert response.status_code == 403
    assert "another host" in response.json()["error"]
    assert writes == []
    service.refresh_from_db()
    assert service.host == victim
    assert service.last_seen_at is None
    assert not ServiceRegistry.objects.filter(service_id="new").exists()
    assert not SubjectRegistry.objects.exists()


def test_registry_guard_does_not_reparent_service_changed_after_validation(
    client, writes, monkeypatch
):
    token()
    own = HostRegistry.objects.create(host_id="edge")
    victim = HostRegistry.objects.create(host_id="victim")
    service = ServiceRegistry.objects.create(service_id="mail", host=own)

    def write(points):
        ServiceRegistry.objects.filter(pk=service.pk).update(host=victim)
        return len(points)

    monkeypatch.setattr("graphyard.views.write_points", write)
    assert post(client, [point(service="mail")]).status_code == 202
    service.refresh_from_db()
    assert service.host == victim
    assert service.last_seen_at is None


@pytest.mark.parametrize("binding,scope", [(None, []), (None, ["edge"]), ("", [])])
def test_required_binding_rejects_legacy_and_empty_bindings(
    client, writes, settings, binding, scope
):
    settings.GRAPHYARD_REQUIRE_BOUND_INGEST_TOKENS = True
    token(binding, host_scope=scope)
    assert post(client, [point()]).status_code == 403
    assert writes == []
    assert not HostRegistry.objects.exists()


def test_legacy_token_keeps_cross_host_and_reparent_behavior(client, writes, settings):
    assert settings.GRAPHYARD_REQUIRE_BOUND_INGEST_TOKENS is False
    token(None)
    victim = HostRegistry.objects.create(host_id="victim")
    service = ServiceRegistry.objects.create(service_id="mail", host=victim)
    assert post(client, [point(service="mail"), point("another")]).status_code == 202
    service.refresh_from_db()
    assert service.host.host_id == "edge"


def test_binding_does_not_widen_existing_host_scope(client, writes):
    token("edge", host_scope=["other"])
    assert post(client, [point()]).status_code == 403
    assert writes == []


def run(command, **kwargs):
    output = io.StringIO()
    call_command(command, stdout=output, **kwargs)
    return output.getvalue()


def test_report_lists_all_names_recent_identifiers_and_only_single_proposals():
    row = token(None)
    multi = IngestToken.objects.create(name="multi", token_hash="never-print-hash")
    none = IngestToken.objects.create(
        name="disabled", enabled=False, token_hash="secret-hash"
    )
    services.record_token_host_usage(row, {"edge"})
    services.record_token_host_usage(multi, {"edge", "victim"})
    services.record_token_host_usage(none, {"ancient"})
    IngestTokenHostUsage.objects.filter(token=none).update(
        last_seen_at=timezone.now() - timedelta(days=40)
    )
    before = (
        list(IngestToken.objects.values()),
        list(IngestTokenHostUsage.objects.values()),
    )
    output = run("report_ingest_token_bindings", days=7)
    assert "ingest_token_host_usage.last_seen_at" in output
    assert "subject_id AND collector_host" in output
    assert "name=producer" in output
    assert "proposed binding=edge" in output
    assert output.count("proposed binding=") == 1
    assert "no proposal: multiple" in output
    assert "name=disabled" in output
    assert "ancient" not in output
    assert "no proposal: no recorded writes" in output
    assert "token id=" not in output
    for row in IngestToken.objects.all():
        assert row.token_hash not in output
    assert "binding-test-credential" not in output
    assert before == (
        list(IngestToken.objects.values()),
        list(IngestTokenHostUsage.objects.values()),
    )
    assert "ancient" in run("report_ingest_token_bindings", days=60)


@pytest.mark.parametrize("days", [0, -1, 36501])
def test_report_invalid_days(days):
    with pytest.raises(CommandError):
        run("report_ingest_token_bindings", days=days)


def test_bind_by_name_normalizes_and_admin_can_clear(admin_client):
    row = token(None)
    output = run("bind_ingest_token", name=row.name, subject="Edge.LAN")
    row.refresh_from_db()
    assert row.bound_subject_id == "edge_lan"
    assert row.token_hash not in output
    assert "binding-test-credential" not in output
    response = admin_client.get(reverse("admin:graphyard_ingesttoken_changelist"))
    assert "edge_lan" in response.content.decode()
    data = {
        "name": row.name,
        "token_hash": row.token_hash,
        "enabled": "on",
        "host_scope": "",
        "bound_subject_id": "",
    }
    form = IngestTokenAdminForm(data=data, instance=row)
    assert form.is_valid(), form.errors
    form.save()
    row.refresh_from_db()
    assert row.bound_subject_id is None
    form = IngestTokenAdminForm(
        data={**data, "bound_subject_id": "edge*"}, instance=row
    )
    assert not form.is_valid()


@pytest.mark.parametrize("subject", ["*", "edge*", "", "/etc/hosts", "x" * 129])
def test_bind_rejects_invalid_subject_without_changes(subject):
    row = token(None)
    with pytest.raises(CommandError):
        run("bind_ingest_token", name=row.name, subject=subject)
    row.refresh_from_db()
    assert row.bound_subject_id is None


def test_bind_unknown_name():
    with pytest.raises(CommandError):
        run("bind_ingest_token", name="missing", subject="edge")


@pytest.mark.parametrize("revoked", [False, True])
def test_replacement_keeps_binding(revoked):
    row = token()
    if revoked:
        row.revoke()
    run("create_ingest_token", name=row.name, rotate=not revoked, unrestricted=True)
    assert IngestToken.objects.get(name="producer").bound_subject_id == "edge"


@pytest.mark.django_db(transaction=True)
def test_binding_migration_reversible_keeps_legacy_tokens():
    before = [("graphyard", "0011_ingest_token_host_scope")]
    after = [("graphyard", "0012_ingest_token_subject_binding")]
    try:
        executor = MigrationExecutor(connection)
        executor.migrate(before)
        old = executor.loader.project_state(before).apps.get_model(
            "graphyard", "IngestToken"
        )
        old.objects.create(name="legacy", token_hash="stored-hash", host_scope=["edge"])
        executor = MigrationExecutor(connection)
        executor.migrate(after)
        row = IngestToken.objects.get(name="legacy")
        assert row.bound_subject_id is None
        assert row.enabled and row.host_scope == ["edge"]
        assert row.token_hash == "stored-hash"
        row.bound_subject_id = "edge"
        row.save()
        MigrationExecutor(connection).migrate(before)
        assert old.objects.get(name="legacy").token_hash == "stored-hash"
    finally:
        executor = MigrationExecutor(connection)
        executor.migrate(executor.loader.graph.leaf_nodes())
        IngestToken.objects.all().delete()


@pytest.mark.parametrize("payload", ["unbound", "mismatch", "foreign"])
def test_binding_rejection_has_fixed_heartbeat_and_log(
    client, writes, settings, caplog, payload
):
    settings.GRAPHYARD_REQUIRE_BOUND_INGEST_TOKENS = True
    row = token(None if payload == "unbound" else "edge")
    if payload == "foreign":
        victim = HostRegistry.objects.create(host_id="victim")
        ServiceRegistry.objects.create(service_id="mail", host=victim)
        batch = [point(), point(service="mail")]
    else:
        batch = [point("victim" if payload == "mismatch" else "edge")]
    response = post(client, batch)
    assert response.status_code == 403
    if payload == "foreign":
        assert "index 1" in response.json()["error"]
    assert writes == []
    assert any(
        "category=binding" in record.getMessage()
        and f"token_id={row.pk}" in record.getMessage()
        for record in caplog.records
    )
    heartbeat = PipelineHeartbeat.objects.get(name="metric_ingest")
    assert heartbeat.status == "warning"
    assert heartbeat.details["category"] == "binding"
    assert "victim" not in str(heartbeat.details) + heartbeat.last_error
    assert "producer" not in str(heartbeat.details) + heartbeat.last_error
    assert "mail" not in str(heartbeat.details) + heartbeat.last_error


def test_blank_legacy_host_is_absent_for_bound_canonical_point(client, writes):
    token()
    assert (
        post(
            client,
            [point(" ", subject_type="host", subject_id="edge", collector_host="edge")],
        ).status_code
        == 202
    )


def test_empty_string_binding_keeps_legacy_behavior_when_requirement_off(
    client, writes
):
    token("")
    assert post(client, [point("other")]).status_code == 202


def test_report_retired_token_uses_stored_name_and_preserves_usage_history():
    row = token()
    services.record_token_host_usage(row, {"edge"})
    run("create_ingest_token", name=row.name, rotate=True)
    row.refresh_from_db()
    output = run("report_ingest_token_bindings")
    assert f"name={row.name} enabled=False" in output
    retired = output.split(f"name={row.name}")[1]
    assert "proposed binding=edge" in retired
    assert "name=producer enabled=True" in output
    assert "no proposal: no recorded writes" in output
    assert "token id=" not in output
    assert row.token_hash not in output
