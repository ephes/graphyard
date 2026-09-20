import copy
import hashlib
import io
import json
from datetime import timedelta
from uuid import uuid4

import pytest
from django.core.management import call_command
from django.db import OperationalError
from django.urls import reverse
from django.utils import timezone

from graphyard.inventory import CATEGORIES, MAX_BYTES
from graphyard.models import (
    HostRegistry,
    InventoryCategory,
    InventoryCredential,
    InventoryHost,
    InventorySnapshot,
)

pytestmark = pytest.mark.django_db


@pytest.fixture
def enrolled():
    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="studio")
    )
    secret = "synthetic-inventory-secret-for-tests-only"
    credential = InventoryCredential.objects.create(
        host=host, digest=hashlib.sha256(secret.encode()).hexdigest()
    )
    return host, credential, f"Bearer {credential.pk}.{secret}"


def report(**overrides):
    data = {
        "schema_version": 1,
        "snapshot_id": str(uuid4()),
        "host": "studio",
        "observed_at": timezone.now().isoformat(),
        "collector": "software-estate/1",
        "categories": {name: {"status": "ok", "items": []} for name in CATEGORIES},
        "gaps": [],
    }
    data["categories"]["packages"]["items"] = [{"name": "Größe", "version": "1.2.3"}]
    data.update(overrides)
    return data


def send(client, enrolled, data):
    return client.post(
        reverse("graphyard:inventory_ingest"),
        data=data,
        content_type="application/json",
        HTTP_AUTHORIZATION=enrolled[2],
    )


def test_host_bound_credential_and_no_reader_access(client, enrolled):
    assert (
        client.post(
            reverse("graphyard:inventory_ingest"),
            data=report(),
            content_type="application/json",
        ).status_code
        == 401
    )
    assert send(client, enrolled, report(host="atlas")).status_code == 403
    assert (
        client.get(
            reverse("graphyard:inventory_status"), HTTP_AUTHORIZATION=enrolled[2]
        ).status_code
        == 401
    )
    assert InventorySnapshot.objects.count() == 0


def test_repeated_delivery_commits_one_snapshot_and_preserves_time(client, enrolled):
    data = report()
    assert send(client, enrolled, [data]).json()["duplicate"] is False
    before = InventorySnapshot.objects.get().received_at
    assert send(client, enrolled, data).json()["duplicate"] is True
    snapshot = InventorySnapshot.objects.get()
    assert snapshot.received_at == before
    assert snapshot.report == data
    assert InventoryCategory.objects.count() == 4
    changed = copy.deepcopy(data)
    changed["categories"]["packages"]["items"] = []
    assert send(client, enrolled, changed).status_code == 409
    assert InventorySnapshot.objects.count() == 1


def test_error_does_not_erase_last_success_and_old_report_cannot_replace_new(
    client, enrolled
):
    old_time = timezone.now() - timedelta(hours=2)
    good = report(observed_at=old_time.isoformat())
    assert send(client, enrolled, good).status_code == 200
    bad = report()
    bad["categories"]["packages"] = {
        "status": "error",
        "items": [],
        "error": "access denied",
    }
    assert send(client, enrolled, bad).status_code == 200
    category = InventoryCategory.objects.get(name="packages")
    assert str(category.latest_success.snapshot_id) == good["snapshot_id"]
    assert str(category.latest_attempt.snapshot_id) == bad["snapshot_id"]
    older = report(observed_at=(old_time - timedelta(hours=1)).isoformat())
    assert send(client, enrolled, older).status_code == 200
    category.refresh_from_db()
    assert str(category.latest_success.snapshot_id) == good["snapshot_id"]
    assert str(category.latest_attempt.snapshot_id) == bad["snapshot_id"]


def test_partial_applications_remain_failed_but_visible_and_do_not_erase_success(
    client,
    enrolled,
    django_user_model,
):
    good = report(observed_at=(timezone.now() - timedelta(minutes=1)).isoformat())
    good["categories"]["applications"]["items"] = [{"id": "previous-success"}]
    assert send(client, enrolled, good).status_code == 200
    partial = report()
    partial["categories"]["applications"] = {
        "status": "error",
        "items": [],
        "error": "application_probe_failed",
        "partial_items": [
            {
                "id": "visible-app",
                "status": "ok",
                "items": {"installed_version": "1.2.3"},
            },
            {
                "id": "<script>unsafe</script>",
                "status": "error",
                "items": [],
                "error": "PermissionError",
            },
        ],
    }
    assert send(client, enrolled, partial).status_code == 200
    category = InventoryCategory.objects.get(name="applications")
    assert str(category.latest_success.snapshot_id) == good["snapshot_id"]
    assert str(category.latest_attempt.snapshot_id) == partial["snapshot_id"]
    client.force_login(django_user_model.objects.create_user(username="partial-reader"))
    status = client.get(reverse("graphyard:inventory_status")).json()["hosts"]["studio"]
    assert status["alert"] is True
    assert status["categories"]["applications"]["status"] == "error"
    page = client.get(reverse("graphyard:inventory_detail", args=["studio"]))
    assert page.status_code == 200
    html = page.content.decode()
    assert "Incomplete application inventory" in html
    assert "visible-app" in html and "1.2.3" in html and "previous-success" in html
    assert "<script>unsafe</script>" not in html
    assert "&lt;script&gt;unsafe&lt;/script&gt;" in html
    response = client.get(
        reverse("graphyard:inventory_download", args=["studio", partial["snapshot_id"]])
    )
    assert response.json() == partial


@pytest.mark.parametrize(
    "category,status,items",
    [
        ("packages", "error", []),
        ("applications", "ok", []),
        ("applications", "unsupported", []),
        ("applications", "error", {}),
        ("applications", "error", ["not-an-object"]),
    ],
)
def test_invalid_partial_application_evidence_rejected(
    client, enrolled, category, status, items
):
    data = report()
    data["categories"][category] = {
        "status": status,
        "items": [],
        "partial_items": items,
    }
    assert send(client, enrolled, data).status_code == 400
    assert not InventorySnapshot.objects.exists()


@pytest.mark.parametrize("case", ["count", "depth"])
def test_partial_evidence_shares_request_limits(client, enrolled, case):
    data = report()
    value = {"leaf": "bounded"}
    for _ in range(20):
        value = {"nested": value}
    partial = [{}] * 100001 if case == "count" else [value]
    data["categories"]["applications"] = {
        "status": "error",
        "items": [],
        "partial_items": partial,
    }
    assert send(client, enrolled, data).status_code == 400
    assert not InventorySnapshot.objects.exists()


def test_partial_preview_is_bounded_and_full_download_retained(
    client, enrolled, django_user_model
):
    data = report()
    items = [
        {"id": f"partial-app-{i:03d}", "status": "error", "error": "x" * 10000}
        for i in range(60)
    ]
    data["categories"]["applications"] = {
        "status": "error",
        "items": [],
        "partial_items": items,
    }
    assert send(client, enrolled, data).status_code == 200
    client.force_login(django_user_model.objects.create_user(username="bounded-reader"))
    page = client.get(reverse("graphyard:inventory_detail", args=["studio"]))
    assert b"partial-app-049" in page.content and b"partial-app-050" not in page.content
    assert b"60 partial entries" in page.content
    assert len(page.content) < 50000
    assert (
        client.get(
            reverse(
                "graphyard:inventory_download", args=["studio", data["snapshot_id"]]
            )
        ).json()
        == data
    )


@pytest.mark.parametrize("shape", ["dict", "string", "list"])
def test_partial_coverage_preview_is_bounded_for_untrusted_shapes(
    client, enrolled, django_user_model, shape
):
    data = report()
    coverage = (
        {f"gap-{i}": "x" for i in range(5000)}
        if shape == "dict"
        else ("x" * 100000 if shape == "string" else ["<" * 1000] * 100)
    )
    data["categories"]["applications"] = {
        "status": "error",
        "items": [],
        "partial_items": [{"id": "app", "items": {"coverage": coverage}}] * 50,
    }
    # All shapes fit the unchanged 8 MiB request limit.
    assert send(client, enrolled, data).status_code == 200
    client.force_login(django_user_model.objects.create_user(username="shape-reader"))
    html = client.get(reverse("graphyard:inventory_detail", args=["studio"])).content
    assert html.count(b"<li>") <= 501
    assert len(html) < 500000
    assert b"gap-4999" not in html
    assert b"app" in html
    if shape == "list":
        assert b"&lt;" * 160 in html
        assert b"<<<" not in html


def test_complete_empty_category_means_removed(client, enrolled):
    assert (
        send(
            client,
            enrolled,
            report(observed_at=(timezone.now() - timedelta(minutes=1)).isoformat()),
        ).status_code
        == 200
    )
    empty = report()
    empty["categories"]["packages"]["items"] = []
    assert send(client, enrolled, empty).status_code == 200
    assert (
        InventoryCategory.objects.get(name="packages").latest_success.report[
            "categories"
        ]["packages"]["items"]
        == []
    )


@pytest.mark.parametrize(
    "change",
    [
        {"schema_version": True},
        {"schema_version": 2},
        {"host": []},
        {"observed_at": "2026-01-01"},
        {"observed_at": (timezone.now() + timedelta(days=1)).isoformat()},
        {"snapshot_id": "not-a-uuid"},
        {"categories": {}},
        {"gaps": "bad"},
    ],
)
def test_invalid_reports_are_rejected(client, enrolled, change):
    assert send(client, enrolled, report(**change)).status_code == 400
    assert InventorySnapshot.objects.count() == 0


def test_malformed_failed_and_deep_reports(client, enrolled):
    bad = report()
    bad["categories"]["packages"]["status"] = "error"
    assert send(client, enrolled, bad).status_code == 400
    nested = {}
    for _ in range(20):
        nested = {"child": nested}
    bad = report()
    bad["categories"]["packages"]["items"] = [nested]
    assert send(client, enrolled, bad).status_code == 400
    bad = report()
    bad["categories"]["packages"]["items"] = [{"value": float("nan")}]
    assert send(client, enrolled, bad).status_code == 400


def test_large_report_and_limit(client, enrolled):
    data = report()
    data["categories"]["packages"]["items"] = [
        {"name": f"pkg-{i}", "metadata": "x" * 150} for i in range(25000)
    ]
    assert len(json.dumps(data)) > 4 * 1024 * 1024
    assert send(client, enrolled, data).status_code == 200
    assert InventorySnapshot.objects.get().report == data
    oversized = client.post(
        reverse("graphyard:inventory_ingest"),
        data=b"x" * (MAX_BYTES + 1),
        content_type="application/json",
        HTTP_AUTHORIZATION=enrolled[2],
    )
    assert oversized.status_code == 413


def test_revocation(client, enrolled):
    enrolled[1].enabled = False
    enrolled[1].save()
    assert send(client, enrolled, report()).status_code == 401


def test_transaction_failure_returns_retryable_error_without_partial_state(
    client, enrolled, monkeypatch
):
    def fail(*args, **kwargs):
        raise OperationalError("test storage failure")

    monkeypatch.setattr(InventoryCategory.objects, "get_or_create", fail)
    assert send(client, enrolled, report()).status_code == 503
    assert InventorySnapshot.objects.count() == 0


def test_status_includes_missing_host_and_laptop_policy(client, enrolled, settings):
    settings.GRAPHYARD_INVENTORY_MONITOR_TOKEN = "synthetic-monitor"
    host = enrolled[0]
    host.alert_when_stale = False
    host.save()
    data = report(observed_at=(timezone.now() - timedelta(days=7)).isoformat())
    assert send(client, enrolled, data).status_code == 200
    InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="missing-server")
    )
    response = client.get(
        reverse("graphyard:inventory_status"),
        HTTP_AUTHORIZATION="Bearer synthetic-monitor",
    )
    assert response.status_code == 200
    state = response.json()
    assert state["summary"] == {"total": 2, "attention": 1}
    assert state["hosts"]["studio"]["stale"] is True
    assert state["hosts"]["studio"]["alert"] is False
    assert state["hosts"]["missing-server"]["alert"] is True


def test_reader_pages_and_download_are_private_and_escape_payload(
    client, enrolled, django_user_model
):
    data = report()
    data["gaps"] = ["<script>alert(1)</script>"]
    assert send(client, enrolled, data).status_code == 200
    routes = [
        reverse("graphyard:inventory_index"),
        reverse("graphyard:inventory_detail", args=["studio"]),
        reverse("graphyard:inventory_download", args=["studio", data["snapshot_id"]]),
    ]
    for route in routes:
        assert client.get(route).status_code == 302
    user = django_user_model.objects.create_user(username="reader", password="test")
    client.force_login(user)
    for route in routes:
        assert client.get(route).status_code == 200
    assert b"<script>" not in client.get(routes[1]).content
    assert client.get(routes[2]).json() == data


def test_enrollment_rotation_is_explicit(client, enrolled):
    from django.core.management.base import CommandError

    with pytest.raises(CommandError):
        call_command("create_inventory_credential", host="studio")
    output = io.StringIO()
    call_command(
        "create_inventory_credential",
        host="studio",
        rotate=True,
        intermittent=True,
        stdout=output,
    )
    assert send(client, enrolled, report()).status_code == 401
    enrolled[0].refresh_from_db()
    assert enrolled[0].alert_when_stale is False
    assert (
        client.post(
            reverse("graphyard:inventory_ingest"),
            data=report(),
            content_type="application/json",
            HTTP_AUTHORIZATION="Bearer " + output.getvalue().strip(),
        ).status_code
        == 200
    )


def test_retention_keeps_last_success_referenced_by_failed_category(client, enrolled):
    old = timezone.now() - timedelta(days=10)
    good = report(observed_at=old.isoformat())
    assert send(client, enrolled, good).status_code == 200
    for day in range(35):
        data = report(observed_at=(old + timedelta(minutes=day + 1)).isoformat())
        data["categories"]["packages"] = {"status": "error", "items": []}
        assert send(client, enrolled, data).status_code == 200
    assert InventorySnapshot.objects.count() == 33
    assert (
        str(InventoryCategory.objects.get(name="packages").latest_success.snapshot_id)
        == good["snapshot_id"]
    )


def test_rotation_preserves_existing_host_freshness_policy(enrolled):
    host = enrolled[0]
    host.alert_when_stale = False
    host.warning_after_seconds = 604800
    host.save()
    call_command(
        "create_inventory_credential", host="studio", rotate=True, stdout=io.StringIO()
    )
    host.refresh_from_db()
    assert host.alert_when_stale is False
    assert host.warning_after_seconds == 604800


def test_credential_lookup_database_failure_is_retryable(client, enrolled, monkeypatch):
    calls = []

    def fail(*args, **kwargs):
        calls.append(True)
        raise OperationalError("credential storage unavailable")

    monkeypatch.setattr(InventoryCredential.objects, "select_related", fail)
    assert send(client, enrolled, report()).status_code == 503
    assert calls == [True]
    assert InventorySnapshot.objects.count() == 0


def test_summary_decodes_each_distinct_report_once(
    client, enrolled, django_user_model, monkeypatch
):
    from django.db.models import JSONField

    data = report()
    data["categories"]["packages"]["items"] = [{"payload": "x" * 1000000}]
    assert send(client, enrolled, data).status_code == 200
    client.force_login(django_user_model.objects.create_user(username="memory-reader"))
    decoded = []
    original = JSONField.from_db_value

    def count_reports(self, value, expression, connection):
        result = original(self, value, expression, connection)
        if isinstance(result, dict) and "snapshot_id" in result:
            decoded.append(result["snapshot_id"])
        return result

    monkeypatch.setattr(JSONField, "from_db_value", count_reports)
    for route in ("inventory_status", "inventory_index", "inventory_detail"):
        decoded.clear()
        url = reverse(
            "graphyard:" + route, args=["studio"] if route == "inventory_detail" else []
        )
        assert client.get(url).status_code == 200
        assert decoded == [data["snapshot_id"]]


@pytest.mark.parametrize("previous_success", [False, True])
def test_latest_failed_report_has_download_link(
    client, enrolled, django_user_model, previous_success
):
    if previous_success:
        good = report(observed_at=(timezone.now() - timedelta(minutes=1)).isoformat())
        assert send(client, enrolled, good).status_code == 200
    data = report()
    data["categories"] = {name: {"status": "error", "items": []} for name in CATEGORIES}
    assert send(client, enrolled, data).status_code == 200
    client.force_login(django_user_model.objects.create_user(username="failure-reader"))
    response = client.get(reverse("graphyard:inventory_detail", args=["studio"]))
    url = reverse("graphyard:inventory_download", args=["studio", data["snapshot_id"]])
    assert url.encode() in response.content
    assert client.get(url).json() == data


def test_ingest_compares_metadata_without_decoding_stored_reports(
    client, enrolled, monkeypatch
):
    from django.db.models import JSONField

    good = report(observed_at=(timezone.now() - timedelta(minutes=1)).isoformat())
    good["categories"]["packages"]["items"] = [{"payload": "x" * 1000000}]
    assert send(client, enrolled, good).status_code == 200
    decoded = []
    original = JSONField.from_db_value

    def count(self, value, expression, connection):
        result = original(self, value, expression, connection)
        if isinstance(result, dict) and "snapshot_id" in result:
            decoded.append(result["snapshot_id"])
        return result

    monkeypatch.setattr(JSONField, "from_db_value", count)
    assert send(client, enrolled, report()).status_code == 200
    assert send(client, enrolled, good).json()["duplicate"] is True
    assert decoded == []


def test_wrong_content_type_and_disabled_monitor_are_rejected(
    client, enrolled, settings
):
    settings.GRAPHYARD_INVENTORY_MONITOR_TOKEN = ""
    assert (
        client.post(
            reverse("graphyard:inventory_ingest"),
            data="{}",
            content_type="text/plain",
            HTTP_AUTHORIZATION=enrolled[2],
        ).status_code
        == 415
    )
    assert (
        client.get(
            reverse("graphyard:inventory_status"), HTTP_AUTHORIZATION="Bearer "
        ).status_code
        == 401
    )
    assert InventorySnapshot.objects.count() == 0


def test_invalid_freshness_threshold_does_not_issue_credential(enrolled):
    from django.core.management.base import CommandError

    before = InventoryCredential.objects.count()
    with pytest.raises(CommandError, match="at least 60"):
        call_command(
            "create_inventory_credential",
            host="studio",
            rotate=True,
            warning_after_seconds=59,
            stdout=io.StringIO(),
        )
    assert InventoryCredential.objects.count() == before
    enrolled[1].refresh_from_db()
    assert enrolled[1].enabled
