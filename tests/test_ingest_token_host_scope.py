from __future__ import annotations

import io
from datetime import timedelta

import pytest
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.db.models import JSONField, Value
from django.db.migrations.executor import MigrationExecutor
from django.urls import reverse
from django.utils import timezone

from graphyard import services
from graphyard.admin import IngestTokenAdminForm
from graphyard.models import (
    HostRegistry,
    IngestToken,
    IngestTokenHostUsage,
    PipelineHeartbeat,
    SubjectRegistry,
)
from graphyard.token_scope import (
    clean_host_scope,
    host_in_scope,
    parse_host_scope_text,
)

SECRET = "secret-token"


def make_token(name: str = "edge", host_scope: object = None) -> IngestToken:
    token = IngestToken(name=name)
    if host_scope is not None:
        token.host_scope = host_scope
    token.set_token(SECRET)
    token.save()
    return token


def store_raw_scope(token: IngestToken, raw: object) -> None:
    # Bypasses validation; ``None`` is stored as JSON null, which SQLite accepts.
    IngestToken.objects.filter(pk=token.pk).update(
        host_scope=Value(raw, output_field=JSONField())
    )


def point(host: str = "edge", **extra: object) -> dict[str, object]:
    return {
        "ts": "2026-03-04T12:00:00Z",
        "host": host,
        "metric": "disk.used_percent",
        "value": 12.5,
        **extra,
    }


@pytest.fixture
def written(monkeypatch):
    calls: list[list[object]] = []

    def fake_write_points(points):
        calls.append(points)
        return len(points)

    monkeypatch.setattr("graphyard.views.write_points", fake_write_points)
    return calls


def post(client, payload):
    return client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION=f"Bearer {SECRET}",
    )


# --- scope syntax ---------------------------------------------------------


@pytest.mark.parametrize(
    ("scope", "identifier", "allowed"),
    [
        ([], "anything", True),
        (None, "anything", False),
        (["edge"], "edge", True),
        (["edge"], "EDGE", True),
        (["edge"], "edge2", False),
        (["edge"], "other", False),
        (["mac-mini.lan"], "mac_mini_lan", True),
        (["mac_mini_lan"], "mac-mini.lan", True),
        (["web-*"], "web-1", True),
        (["web-*"], "web_1", True),
        (["web-*"], "web", False),
        (["web-*"], "webserver", False),
        (["web*"], "webserver", True),
        (["web*"], "web", True),
        (["web*"], "db1", False),
        (["edge", "db*"], "db1", True),
        (["edge"], "", False),
        # Damaged stored values allow nothing instead of everything.
        ("edge", "edge", False),
        ({"edge": True}, "edge", False),
        (["*"], "edge", False),
        (["edge", 5], "edge", False),
        (["ed*ge"], "edge", False),
        ([".*"], "edge", False),
    ],
)
def test_host_in_scope(scope, identifier, allowed):
    assert host_in_scope(scope, identifier) is allowed


@pytest.mark.parametrize(
    "entry",
    ["*", "", "a*b", "a**", "*a", "a b", "a/b", "a|b", "(a)", "a$", "-a", 5, "x" * 129],
)
def test_clean_host_scope_rejects_anything_but_names_with_trailing_wildcard(entry):
    with pytest.raises(ValidationError):
        clean_host_scope([entry])


def test_clean_host_scope_normalizes_and_deduplicates():
    assert clean_host_scope([" Edge ", "edge", "web-*"]) == ["edge", "web-*"]
    assert parse_host_scope_text("edge, web-*\n db1 ") == ["edge", "web-*", "db1"]
    assert parse_host_scope_text("  ") == []


def test_clean_host_scope_limits_entry_count():
    with pytest.raises(ValidationError):
        clean_host_scope([f"host{idx}" for idx in range(65)])


@pytest.mark.django_db
def test_model_validation_rejects_unclean_scope():
    token = make_token(host_scope=["edge"])
    token.full_clean()
    for bad in (["Edge"], ["*"], "edge", ["edge", "edge"]):
        token.host_scope = bad
        with pytest.raises(ValidationError):
            token.full_clean()


# --- ingest enforcement ---------------------------------------------------


@pytest.mark.django_db
def test_unscoped_token_still_writes_any_host(client, written):
    make_token()

    response = post(client, [point("edge"), point("other-host")])

    assert response.status_code == 202
    assert response.json()["ingested"] == 2
    assert len(written) == 1


@pytest.mark.django_db
def test_scoped_token_writes_hosts_in_scope(client, written):
    make_token(host_scope=["edge", "web-*"])

    response = post(client, [point("edge"), point("web-1"), point("Web_2")])

    assert response.status_code == 202
    assert response.json()["ingested"] == 3


@pytest.mark.django_db
def test_scoped_token_gets_403_for_other_host_and_writes_nothing(
    client, written, caplog
):
    token = make_token(host_scope=["edge"])

    response = post(client, [point("edge"), point("victim")])

    assert response.status_code == 403
    assert response.json() == {
        "error": "Metric at index 1 is outside the host scope of this token"
    }
    assert written == []
    assert not HostRegistry.objects.exists()
    assert not SubjectRegistry.objects.exists()
    assert not IngestTokenHostUsage.objects.exists()
    assert any(
        f"metrics_ingest_rejected category=scope rejected_requests=1 rejected_count=1 total_metrics=2 token_id={token.pk} index=1"
        in record.getMessage()
        for record in caplog.records
    )
    heartbeat = PipelineHeartbeat.objects.get(name="metric_ingest")
    assert heartbeat.details["category"] == "scope"
    # The heartbeat is public: it must not name the hosts involved.
    assert "victim" not in heartbeat.last_error
    assert "victim" not in str(heartbeat.details)


@pytest.mark.django_db
@pytest.mark.parametrize(
    "item",
    [
        # canonical subject fields instead of the legacy host field
        {"subject_type": "host", "subject_id": "victim", "collector_host": "edge"},
        # spelling variants that normalize to the other host
        {"subject_type": "host", "subject_id": " VICTIM ", "collector_host": "edge"},
        # another subject type naming the other host
        {"subject_type": "service", "subject_id": "victim", "collector_host": "edge"},
        # own subject, but claiming to be collected by the other host
        {"subject_type": "host", "subject_id": "edge", "collector_host": "victim"},
        {"host": "edge", "collector_host": "victim.lan"},
    ],
)
def test_scoped_token_cannot_reach_other_host_through_any_identifier(
    client, written, item
):
    make_token(host_scope=["edge"])
    payload = {
        "ts": "2026-03-04T12:00:00Z",
        "metric": "disk.used_percent",
        "value": 1,
        "source_system": "vector",
        "collector_service": "vector",
        **item,
    }

    response = post(client, {"metrics": [payload]})

    assert response.status_code == 403
    assert written == []


@pytest.mark.django_db
def test_scope_is_checked_for_ndjson_bodies(client, written):
    make_token(host_scope=["edge"])
    body = (
        '{"ts":"2026-03-04T12:00:00Z","host":"edge","metric":"m","value":1}\n'
        '{"ts":"2026-03-04T12:00:00Z","host":"victim","metric":"m","value":1}\n'
    )

    response = client.post(
        reverse("graphyard:metrics_ingest"),
        data=body,
        content_type="application/x-ndjson",
        HTTP_AUTHORIZATION=f"Bearer {SECRET}",
    )

    assert response.status_code == 403
    assert written == []


@pytest.mark.django_db
@pytest.mark.parametrize(
    "damaged",
    ["edge", None, {}, ["*"], 0, "", ["edge", "edge"], ["Edge"]]
    + [["edge"] + [f"h{idx}" for idx in range(64)]],
)
def test_damaged_scope_fails_closed(client, written, damaged):
    token = make_token(host_scope=["edge"])
    store_raw_scope(token, damaged)

    response = post(client, [point("edge")])

    assert response.status_code == 403
    assert written == []


@pytest.mark.django_db
def test_invalid_payload_is_still_400_for_scoped_token(client, written):
    make_token(host_scope=["edge"])

    response = post(client, [{"host": "victim", "metric": "m", "value": 1}])

    assert response.status_code == 400
    assert written == []


# --- usage recording and report -------------------------------------------


@pytest.mark.django_db
def test_ingest_records_which_hosts_a_token_wrote(client, written):
    token = make_token()

    assert post(client, [point("edge"), point("Other-Host")]).status_code == 202
    assert post(client, [point("edge")]).status_code == 202

    assert sorted(
        IngestTokenHostUsage.objects.filter(token=token).values_list(
            "host_id", flat=True
        )
    ) == ["edge", "other_host"]


@pytest.mark.django_db
def test_usage_last_seen_is_throttled_and_bounded(monkeypatch):
    token = make_token()
    services.record_token_host_usage(token, {"edge"})
    usage = IngestTokenHostUsage.objects.get()
    first_seen = usage.last_seen_at

    services.record_token_host_usage(token, {"edge"})
    usage.refresh_from_db()
    assert usage.last_seen_at == first_seen

    IngestTokenHostUsage.objects.update(last_seen_at=first_seen - timedelta(hours=2))
    services.record_token_host_usage(token, {"edge"})
    usage.refresh_from_db()
    assert usage.last_seen_at >= first_seen

    monkeypatch.setattr(services, "MAX_TOKEN_HOST_USAGE_ROWS", 3)
    services.record_token_host_usage(token, {f"h{idx}" for idx in range(10)})
    services.record_token_host_usage(token, {"late", "x" * 200})
    assert IngestTokenHostUsage.objects.filter(token=token).count() == 3


def run_report(**options: object) -> str:
    stdout = io.StringIO()
    call_command("report_ingest_token_hosts", stdout=stdout, **options)
    return stdout.getvalue()


@pytest.mark.django_db
def test_report_lists_recent_hosts_per_token_and_changes_nothing():
    now = timezone.now()
    edge = make_token("edge", host_scope=["edge"])
    central = make_token("central")
    revoked = make_token("old")
    revoked.revoke()
    rows = [
        (edge, "edge", 1),
        (edge, "victim", 2),
        (edge, "ancient", 40),
        (central, "switch_1", 3),
        (revoked, "gone", 1),
    ]
    for token, host_id, age_days in rows:
        seen = now - timedelta(days=age_days)
        IngestTokenHostUsage.objects.create(
            token=token, host_id=host_id, first_seen_at=seen, last_seen_at=seen
        )
    before = list(IngestTokenHostUsage.objects.values_list("pk", "last_seen_at"))
    tokens_before = list(IngestToken.objects.values())

    output = run_report(days=7)

    assert f"token id={edge.pk} name=edge enabled host_scope=edge" in output
    assert (
        f"token id={central.pk} name=central enabled host_scope=unrestricted" in output
    )
    edge_block = output.split("name=edge ")[1].split("token id=")[0]
    assert "  edge  last_seen=" in edge_block
    assert (
        "outside current scope"
        in [line for line in edge_block.splitlines() if line.startswith("  victim")][0]
    )
    assert "ancient" not in output
    assert "suggested --host-scope edge,victim" in edge_block
    assert "switch_1" in output
    assert "gone" not in output
    assert "gone" in run_report(days=7, include_disabled=True)
    assert "ancient" in run_report(days=60)
    assert (
        list(IngestTokenHostUsage.objects.values_list("pk", "last_seen_at")) == before
    )
    assert list(IngestToken.objects.values()) == tokens_before


@pytest.mark.django_db
def test_report_rejects_non_positive_days():
    with pytest.raises(CommandError):
        run_report(days=0)


# --- create_ingest_token ---------------------------------------------------


def create(name: str, **options: object) -> str:
    stdout = io.StringIO()
    call_command("create_ingest_token", name=name, stdout=stdout, **options)
    return stdout.getvalue()


@pytest.mark.django_db
def test_create_ingest_token_sets_scope():
    output = create("edge", host_scope=["Edge,web-*", "db1"])

    assert IngestToken.objects.get(name="edge").host_scope == ["edge", "web-*", "db1"]
    assert "host_scope=edge,web-*,db1" in output


@pytest.mark.django_db
def test_create_ingest_token_without_scope_is_unrestricted():
    output = create("edge")

    assert IngestToken.objects.get(name="edge").host_scope == []
    assert "host_scope=unrestricted" in output


@pytest.mark.django_db
@pytest.mark.parametrize("scope", [["*"], ["a b*c"], [""], ["edge,/etc"]])
def test_create_ingest_token_rejects_invalid_scope(scope):
    with pytest.raises(CommandError):
        create("edge", host_scope=scope)
    assert not IngestToken.objects.exists()


@pytest.mark.django_db
def test_rotation_keeps_scope_unless_told_otherwise():
    create("edge", host_scope=["edge"])

    create("edge", rotate=True)
    assert IngestToken.objects.get(name="edge").host_scope == ["edge"]

    create("edge", rotate=True, host_scope=["edge,edge2"])
    assert IngestToken.objects.get(name="edge").host_scope == ["edge", "edge2"]

    create("edge", rotate=True, unrestricted=True)
    assert IngestToken.objects.get(name="edge").host_scope == []

    with pytest.raises(CommandError):
        create("edge", rotate=True, unrestricted=True, host_scope=["edge"])


@pytest.mark.django_db
def test_recreating_a_revoked_token_keeps_its_scope():
    create("edge", host_scope=["edge"])
    IngestToken.objects.get(name="edge").revoke()

    create("edge")

    assert IngestToken.objects.get(name="edge").host_scope == ["edge"]


@pytest.mark.django_db
@pytest.mark.parametrize(
    "damaged",
    [None, "edge", ["*"], ["edge", "edge"], [f"h{idx}" for idx in range(65)]],
)
def test_rotation_refuses_to_inherit_a_damaged_scope(damaged):
    create("edge", host_scope=["edge"])
    store_raw_scope(IngestToken.objects.get(name="edge"), damaged)

    with pytest.raises(CommandError):
        create("edge", rotate=True)
    assert IngestToken.objects.filter(enabled=True).count() == 1

    create("edge", rotate=True, host_scope=["edge"])
    assert IngestToken.objects.get(name="edge").host_scope == ["edge"]


@pytest.mark.django_db
def test_report_gives_no_suggestion_that_exceeds_the_scope_limit():
    token = make_token()
    now = timezone.now()
    IngestTokenHostUsage.objects.bulk_create(
        IngestTokenHostUsage(
            token=token, host_id=f"h{idx}", first_seen_at=now, last_seen_at=now
        )
        for idx in range(65)
    )

    output = run_report(days=7)

    assert "suggested --host-scope" not in output
    assert "at most 64 entries" in output


@pytest.mark.django_db
def test_usage_cap_holds_when_rows_appear_between_check_and_insert(monkeypatch):
    # A concurrent request filled the table after this one looked at it.
    token = make_token()
    monkeypatch.setattr(services, "MAX_TOKEN_HOST_USAGE_ROWS", 5)
    now = timezone.now()
    IngestTokenHostUsage.objects.bulk_create(
        IngestTokenHostUsage(
            token=token, host_id=f"a{idx}", first_seen_at=now, last_seen_at=now
        )
        for idx in range(5)
    )

    services.record_token_host_usage(token, {f"b{idx}" for idx in range(5)})

    assert sorted(IngestTokenHostUsage.objects.values_list("host_id", flat=True)) == [
        f"a{idx}" for idx in range(5)
    ]


# --- admin ------------------------------------------------------------------


@pytest.mark.django_db
def test_admin_form_edits_scope_as_text():
    token = make_token(host_scope=["edge", "web-*"])

    form = IngestTokenAdminForm(instance=token)
    assert form.initial["host_scope"] == "edge\nweb-*"

    data = {
        "name": "edge",
        "token_hash": token.token_hash,
        "enabled": "on",
        "host_scope": "Edge, db*\nweb-1",
    }
    form = IngestTokenAdminForm(data=data, instance=token)
    assert form.is_valid(), form.errors
    form.save()
    token.refresh_from_db()
    assert token.host_scope == ["edge", "db*", "web-1"]

    form = IngestTokenAdminForm(data={**data, "host_scope": ""}, instance=token)
    assert form.is_valid(), form.errors
    form.save()
    token.refresh_from_db()
    assert token.host_scope == []


@pytest.mark.django_db
@pytest.mark.parametrize("text", ["*", "edge, a*b", "we b/*", "^edge$"])
def test_admin_form_rejects_invalid_scope(text):
    token = make_token()

    form = IngestTokenAdminForm(
        data={
            "name": "edge",
            "token_hash": token.token_hash,
            "enabled": "on",
            "host_scope": text,
        },
        instance=token,
    )

    assert not form.is_valid()
    assert "host_scope" in form.errors


@pytest.mark.django_db
def test_admin_changelist_and_change_page_render(admin_client):
    token = make_token(host_scope=["edge"])
    IngestTokenHostUsage.objects.create(
        token=token,
        host_id="edge",
        first_seen_at=timezone.now(),
        last_seen_at=timezone.now(),
    )

    for url in (
        reverse("admin:graphyard_ingesttoken_changelist"),
        reverse("admin:graphyard_ingesttoken_change", args=[token.pk]),
        reverse("admin:graphyard_ingesttokenhostusage_changelist"),
    ):
        assert admin_client.get(url).status_code == 200


# --- migration ---------------------------------------------------------------


@pytest.mark.django_db(transaction=True)
def test_host_scope_migration_is_reversible_and_keeps_tokens():
    before = [("graphyard", "0010_condition_stale_allowance")]
    after = [("graphyard", "0011_ingest_token_host_scope")]
    try:
        executor = MigrationExecutor(connection)
        executor.migrate(before)
        old_token = executor.loader.project_state(before).apps.get_model(
            "graphyard", "IngestToken"
        )
        old_token.objects.create(name="legacy", token_hash="graphyard-sha256$abc")
        assert "ingest_token_host_usage" not in connection.introspection.table_names()

        executor = MigrationExecutor(connection)
        executor.migrate(after)
        token = IngestToken.objects.get(name="legacy")
        assert token.host_scope == []
        assert token.enabled is True

        executor = MigrationExecutor(connection)
        executor.migrate(before)
        assert "ingest_token_host_usage" not in connection.introspection.table_names()
        assert old_token.objects.filter(name="legacy").exists()
    finally:
        executor = MigrationExecutor(connection)
        executor.migrate(executor.loader.graph.leaf_nodes())
        IngestToken.objects.all().delete()
