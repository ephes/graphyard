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


def application_page(client, django_user_model, **query):
    user, _ = django_user_model.objects.get_or_create(username="application-reader")
    client.force_login(user)
    return client.get(
        reverse("graphyard:inventory_applications", args=["studio"]), query
    )


def test_application_view_requires_reader_and_does_not_write_inventory(
    client, enrolled, django_user_model
):
    data = report()
    assert send(client, enrolled, data).status_code == 200
    url = reverse("graphyard:inventory_applications", args=["studio"])
    assert client.get(url, HTTP_AUTHORIZATION=enrolled[2]).status_code == 302
    before = list(InventorySnapshot.objects.values())
    response = application_page(client, django_user_model)
    assert response.status_code == 200
    assert b"empty report does not prove" in response.content
    assert list(InventorySnapshot.objects.values()) == before


def test_application_view_shows_observed_versions_dependencies_and_cached_updates(
    client, enrolled, django_user_model
):
    data = report()
    data["categories"]["applications"]["items"] = [
        {
            "id": "media-service",
            "status": "ok",
            "items": {
                "installed_version": "1.2",
                "running_version": "1.1",
                "presence": "installed",
                "runtime": "active",
                "git": {"status": "ok", "items": {"commit": "a" * 40, "dirty": True}},
                "python": {
                    "status": "ok",
                    "items": [
                        {
                            "name": "my-dependency",
                            "version": "4.0",
                            "requires": ['other>=2; extra == "optional"'],
                        }
                    ],
                },
                "package_candidates": [
                    {
                        "name": "media-package",
                        "installed": "1.2",
                        "candidate": "1.3",
                        "update_available": True,
                    }
                ],
            },
        }
    ]
    assert send(client, enrolled, data).status_code == 200
    response = application_page(client, django_user_model)
    html = response.content.decode()
    for value in [
        "media-service",
        "1.2",
        "1.1",
        "my-dependency",
        "4.0",
        "optional",
        "Update available",
        "cache age is unknown",
        "activation is not evaluated",
        "Modified",
        "a" * 40,
    ]:
        assert value in html
    assert data["snapshot_id"] in html


@pytest.mark.parametrize("partial", [False, True])
def test_application_view_distinguishes_partial_and_historical_sources(
    client, enrolled, django_user_model, partial
):
    old = report(observed_at=(timezone.now() - timedelta(days=30)).isoformat())
    old["categories"]["applications"]["items"] = [
        {
            "id": "old-success",
            "status": "ok",
            "items": {"installed_version": "old-version"},
        }
    ]
    assert send(client, enrolled, old).status_code == 200
    latest = report()
    latest["categories"]["applications"] = {"status": "error", "items": []}
    if partial:
        latest["categories"]["applications"]["partial_items"] = [
            {"id": "new-partial", "status": "error", "error": "PermissionError"}
        ]
    assert send(client, enrolled, latest).status_code == 200
    response = application_page(client, django_user_model)
    html = response.content.decode()
    if partial:
        assert "Incomplete application inventory" in html and "new-partial" in html
        assert "old-version" not in html
        assert response.context["snapshot"].snapshot_id.hex == latest[
            "snapshot_id"
        ].replace("-", "")
    else:
        assert "Historical evidence" in html and "old-version" in html
        assert "older than the host" in html
        assert old["snapshot_id"] in html and latest["snapshot_id"] in html
    assert response.context["host"]["alert"] is True


def test_application_view_paginates_and_searches_macos_bundles(
    client, enrolled, django_user_model
):
    data = report()
    data["categories"]["applications"]["items"] = [
        {
            "id": "macos-application-bundles",
            "status": "ok",
            "items": [
                {
                    "name": f"Bundle {i:03}",
                    "version": f"2.{i}",
                    "build": "100",
                    "path": f"/Applications/Bundle {i:03}.app",
                }
                for i in range(45)
            ],
        }
    ]
    assert send(client, enrolled, data).status_code == 200
    first = application_page(client, django_user_model)
    assert b"Bundle 019" in first.content and b"Bundle 020" not in first.content
    second = application_page(client, django_user_model, page=2)
    assert b"Bundle 020" in second.content and b"Bundle 000" not in second.content
    filtered = application_page(
        client, django_user_model, q="bundle 044", page="invalid"
    )
    assert b"Bundle 044" in filtered.content and b"2.44" in filtered.content
    assert b"1 matching of 45" in filtered.content
    assert b"Update availability: Not assessed" in filtered.content
    assert b"Not verified" in filtered.content


@pytest.mark.parametrize(
    "nested",
    [
        None,
        [],
        "<script>unsafe</script>",
        {
            "coverage": {},
            "python": {"status": "ok", "items": {}},
            "git": {"status": "ok", "items": []},
            "package_candidates": ["invalid", {"update_available": "false"}],
        },
    ],
)
def test_application_view_handles_untrusted_shapes_and_escapes_text(
    client, enrolled, django_user_model, nested
):
    data = report()
    data["categories"]["applications"]["items"] = [
        {"id": "<script>unsafe</script>", "status": "ok", "items": nested}
    ]
    assert send(client, enrolled, data).status_code == 200
    response = application_page(client, django_user_model)
    assert response.status_code == 200
    assert b"<script>unsafe</script>" not in response.content
    assert b"&lt;script&gt;unsafe&lt;/script&gt;" in response.content
    assert b"Update available</td>" not in response.content
    assert b"No newer cached candidate</td>" not in response.content


def test_application_dependency_preview_is_bounded_and_download_complete(
    client, enrolled, django_user_model
):
    data = report()
    packages = [
        {"name": f"dep-{i:03}", "version": "1", "requires": ["<" * 1000] * 10}
        for i in range(30)
    ]
    data["categories"]["applications"]["items"] = [
        {
            "id": "bounded-app",
            "status": "ok",
            "items": {"python": {"status": "ok", "items": packages}},
        }
    ]
    assert send(client, enrolled, data).status_code == 200
    response = application_page(client, django_user_model)
    assert b"dep-009" in response.content and b"dep-010" not in response.content
    assert b"30 installed Python packages" in response.content
    assert b"&lt;" * 160 in response.content and b"<<<" not in response.content
    assert len(response.content) < 50000
    assert (
        client.get(
            reverse(
                "graphyard:inventory_download", args=["studio", data["snapshot_id"]]
            )
        ).json()
        == data
    )


def test_application_view_missing_host_report_is_explicit(
    client, enrolled, django_user_model
):
    response = application_page(client, django_user_model)
    assert response.status_code == 200
    assert b"No usable application evidence" in response.content
    assert b"0 matching of 0" in response.content


def test_application_projection_does_not_treat_malformed_dependencies_as_empty():
    from graphyard.inventory_applications import project

    for invalid in [{}, None, "text", ["not-an-object"]]:
        app = project({"items": {"python": {"status": "ok", "items": invalid}}})
        assert app["python_status"] == "Invalid evidence"
    app = project(
        {
            "items": {
                "python": {
                    "status": "ok",
                    "items": [{"name": "dependency", "requires": {}}],
                }
            }
        }
    )
    assert app["packages"][0]["requires_count"] is None
    app = project(
        {
            "items": {
                "package_candidates": [
                    {"installed": "2", "candidate": "2", "update_available": False}
                ]
            }
        }
    )
    assert app["updates"][0]["verdict"] == "No newer cached candidate"


@pytest.mark.parametrize(
    "items,status,error", [([], "error", "PermissionError"), (["invalid"], "ok", None)]
)
def test_application_view_retains_failed_or_malformed_bundle_scan(
    client, enrolled, django_user_model, items, status, error
):
    data = report()
    data["categories"]["applications"] = {
        "status": "error",
        "items": [],
        "partial_items": [
            {
                "id": "macos-application-bundles",
                "status": status,
                "error": error,
                "items": items,
            }
        ],
    }
    assert send(client, enrolled, data).status_code == 200
    html = application_page(client, django_user_model).content
    assert b"macOS bundle scan" in html
    assert (error or "Malformed bundle evidence").encode() in html
    assert b"1 matching of 1" in html


def test_application_view_labels_malformed_coverage_and_counts_comparisons(
    client, enrolled, django_user_model
):
    data = report()
    data["categories"]["applications"]["items"] = [
        {
            "id": "broken",
            "items": {"coverage": {}, "package_candidates": [None, {"name": "pkg"}]},
        }
    ]
    assert send(client, enrolled, data).status_code == 200
    html = application_page(client, django_user_model).content
    assert (
        b"Malformed coverage evidence" in html
        and b"Malformed package comparison evidence" in html
    )
    assert b"2 package comparisons" in html


def test_application_view_bounds_query_and_coverage(
    client, enrolled, django_user_model
):
    data = report()
    data["categories"]["applications"]["items"] = [
        {"id": "a" * 100, "items": {"coverage": [f"gap-{i:02}" for i in range(25)]}}
    ]
    assert send(client, enrolled, data).status_code == 200
    page = application_page(client, django_user_model, q="a" * 500)
    assert page.context["query"] == "a" * 100
    assert b"Coverage gaps: 25" in page.content
    assert b"gap-09" in page.content and b"gap-10" not in page.content


def test_application_view_missing_failed_attempt_keeps_diagnostics_and_link(
    client, enrolled, django_user_model
):
    data = report()
    data["categories"]["applications"] = {
        "status": "error",
        "items": [],
        "error": "application_probe_failed",
    }
    assert send(client, enrolled, data).status_code == 200
    page = application_page(client, django_user_model)
    assert page.status_code == 200
    assert page.context["basis"] == "missing" and page.context["snapshot"] is None
    assert (
        b"Status: error" in page.content and b"application_probe_failed" in page.content
    )
    assert (
        b"Download latest attempt" in page.content
        and data["snapshot_id"].encode() in page.content
    )
    assert b"Download this evidence report" not in page.content


def test_malformed_bundle_error_cannot_hide_invalid_entries():
    from graphyard.inventory_applications import applications, project

    for invalid in [{"error": "nested"}, 0, ["error"]]:
        rows = list(
            applications(
                [
                    {
                        "id": "macos-application-bundles",
                        "status": "ok",
                        "error": invalid,
                        "items": ["bad-entry"],
                    }
                ]
            )
        )
        assert len(rows) == 1
        result = project(rows[0])
        assert "Malformed bundle error" in result["error"]
        assert "Malformed bundle evidence" in result["error"]


def test_bundle_diagnostic_does_not_label_valid_bundle_malformed():
    from graphyard.inventory_applications import applications, project

    rows = list(
        applications(
            [
                {
                    "id": "macos-application-bundles",
                    "status": "ok",
                    "error": "x" * 1000,
                    "items": ["bad-entry", {"name": "valid-bundle", "version": "1"}],
                }
            ]
        )
    )
    assert "Malformed bundle evidence" in project(rows[0])["error"]
    assert "Malformed" not in project(rows[1])["error"]
    assert project(rows[1])["installed"] == "1"


def test_python_sbom_auth_scope_and_historical_source(
    client, enrolled, django_user_model
):
    good = report(observed_at=(timezone.now() - timedelta(days=2)).isoformat())
    good["categories"]["applications"]["items"] = [
        {
            "id": "app/<&>",
            "status": "ok",
            "items": {
                "python": {
                    "status": "ok",
                    "items": [
                        {"name": "example", "version": "1.2", "requires": []},
                    ],
                }
            },
        }
    ]
    assert send(client, enrolled, good).status_code == 200
    bad = report()
    bad["categories"]["applications"] = {"status": "error", "items": []}
    assert send(client, enrolled, bad).status_code == 200
    url = reverse(
        "graphyard:inventory_python_sbom", args=["studio", good["snapshot_id"]]
    )
    query = {"application": "app/<&>"}
    assert client.get(url, query).status_code == 302
    assert client.get(url, query, HTTP_AUTHORIZATION=enrolled[2]).status_code == 302
    client.force_login(django_user_model.objects.create_user(username="sbom-reader"))
    before = list(
        InventoryCategory.objects.values_list("latest_attempt_id", "latest_success_id")
    )
    response = client.get(url, query)
    assert response.status_code == 200
    assert response["Content-Type"] == "application/vnd.cyclonedx+json"
    assert response["Cache-Control"] == "private, no-store"
    assert "<&>" not in response["Content-Disposition"]
    assert response.json()["metadata"]["component"]["name"] == "app/<&>"
    assert client.get(url).status_code == 422
    assert client.post(url, query).status_code == 405
    wrong_host = reverse(
        "graphyard:inventory_python_sbom", args=["atlas", good["snapshot_id"]]
    )
    assert client.get(wrong_host, query).status_code == 404
    missing = reverse("graphyard:inventory_python_sbom", args=["studio", uuid4()])
    assert client.get(missing, query).status_code == 404
    failed = reverse(
        "graphyard:inventory_python_sbom", args=["studio", bad["snapshot_id"]]
    )
    assert client.get(failed, query).status_code == 422
    html = client.get(
        reverse("graphyard:inventory_applications", args=["studio"])
    ).content.decode()
    assert url in html and failed not in html
    assert "Historical evidence" in html and "application=app/%3C%26%3E" in html
    assert InventorySnapshot.objects.count() == 2
    assert (
        list(
            InventoryCategory.objects.values_list(
                "latest_attempt_id", "latest_success_id"
            )
        )
        == before
    )


def health_evidence(host="studio", timestamp=None):
    return {
        "schema_version": 1,
        "source": "software-live/2",
        "host": host,
        "observed_at_epoch": timestamp or timezone.now().timestamp(),
        "max_age_seconds": 1800,
        "checks": {
            name: {
                "status": "ok",
                "observed": True,
                "expected": name != "postgresql",
                "installed_version": "1.2.3",
                "running_version": None,
                "upstream_version": None,
                "issues": [],
                "issues_truncated": False,
            }
            for name in ["os", "postgresql", "traefik"]
        },
        "apt": {"status": "ok", "indexes_fresh": True, "pending_security_count": 0},
    }


def test_software_health_view_preserves_source_time_scope_and_historical_status(
    client, enrolled, django_user_model
):
    stamp = timezone.now() - timedelta(days=1)
    evidence = health_evidence(timestamp=stamp.timestamp())
    evidence["checks"]["traefik"].update(
        status="warning", issues=["<script>fake</script>"]
    )
    data = report(observed_at=(stamp + timedelta(minutes=20)).isoformat())
    data["categories"]["applications"]["items"] = [
        {
            "id": "software_live",
            "status": "ok",
            "items": {"software_health": {"status": "ok", "items": evidence}},
        }
    ]
    assert send(client, enrolled, data).status_code == 200
    client.force_login(django_user_model.objects.create_user(username="health-reader"))
    html = client.get(
        reverse("graphyard:inventory_applications", args=["studio"])
    ).content.decode()
    from django.template import Context, Template

    rendered_source = Template("{{ stamp }}").render(Context({"stamp": stamp}))
    assert "Monitoring observation: " + rendered_source in html
    assert "Recorded software health" in html
    assert "Historical monitoring observation" in html
    assert "Pending APT security updates at observation: <strong>0</strong>" in html
    assert "not expected on this host" in html
    assert (
        "&lt;script&gt;fake&lt;/script&gt;" in html
        and "<script>fake</script>" not in html
    )
    assert "does not establish currency of all applications or containers" in html
    assert InventorySnapshot.objects.get().report == data


@pytest.mark.parametrize(
    "case",
    [
        "failed_probe",
        "wrong_host",
        "future",
        "stale_at_capture",
        "bool_time",
        "malformed_check",
        "malformed_version",
        "bool_count",
        "unknown_apt",
        "stale_indexes",
    ],
)
def test_health_projection_never_turns_missing_invalid_or_stale_indexes_green(case):
    from graphyard.inventory_applications import health_projection

    now = timezone.now()
    data = health_evidence(timestamp=(now - timedelta(seconds=60)).timestamp())
    observation = {"status": "ok", "items": data}
    if case == "failed_probe":
        observation = {"status": "error", "items": []}
    elif case == "wrong_host":
        data["host"] = "another-host"
    elif case == "future":
        data["observed_at_epoch"] = now.timestamp() + 60
    elif case == "stale_at_capture":
        data["observed_at_epoch"] = now.timestamp() - 1801
    elif case == "bool_time":
        data["observed_at_epoch"] = True
    elif case == "malformed_check":
        data["checks"]["os"]["observed"] = "yes"
    elif case == "malformed_version":
        data["checks"]["os"]["installed_version"] = {}
    elif case == "bool_count":
        data["apt"]["pending_security_count"] = False
    elif case == "unknown_apt":
        data["apt"].update(status="unknown", pending_security_count=None)
    elif case == "stale_indexes":
        data["apt"]["indexes_fresh"] = False
    result = health_projection({"software_health": observation}, "studio", now)
    if case in ["unknown_apt", "stale_indexes"]:
        assert result["available"] is True and result["security_count"] is None
    else:
        assert result == {"available": False}


@pytest.mark.parametrize(
    "field,value",
    [
        ("source", "unknown-source"),
        ("schema_version", 2),
        ("max_age_seconds", 86400),
        ("observed_at_epoch", 10**400),
        ("checks", {}),
        ("checks", {"unexpected": {}}),
    ],
)
def test_health_projection_rejects_contract_mismatch(field, value):
    from graphyard.inventory_applications import health_projection

    now = timezone.now()
    data = health_evidence(timestamp=now.timestamp())
    data[field] = value
    assert health_projection(
        {"software_health": {"status": "ok", "items": data}}, "studio", now
    ) == {"available": False}


@pytest.mark.parametrize("issues", [["x"] * 21, [7]])
def test_health_projection_rejects_invalid_issues(issues):
    from graphyard.inventory_applications import health_projection

    now = timezone.now()
    data = health_evidence(timestamp=now.timestamp())
    data["checks"]["os"]["issues"] = issues
    assert health_projection(
        {"software_health": {"status": "ok", "items": data}}, "studio", now
    ) == {"available": False}


def test_health_projection_absent_context_and_fresh_source(monkeypatch):
    from graphyard import inventory_applications as presentation

    now = timezone.now()
    data = health_evidence(timestamp=now.timestamp())
    evidence = {"software_health": {"status": "ok", "items": data}}
    assert presentation.health_projection({}, "studio", now) is None
    for host, date in [(None, now), (7, now), ("studio", None), ("studio", "invalid")]:
        assert presentation.health_projection(evidence, host, date) == {
            "available": False
        }
    monkeypatch.setattr(presentation.time, "time", lambda: now.timestamp() + 60)
    result = presentation.health_projection(evidence, "studio", now)
    assert result["observed_at"] == now
    assert result["historical"] is False


@pytest.mark.parametrize("basis", ["successful", "partial", "historical"])
def test_related_units_keep_application_count_snapshot_and_failure_labels(
    client, enrolled, django_user_model, basis
):
    data = report()
    app = {
        "id": "web",
        "status": "ok",
        "items": {
            "related_units": {
                "status": "ok",
                "items": [
                    {"name": "worker.service", "state": "failed"},
                    {"name": "scheduler.service", "state": "<script>state</script>"},
                    {"name": "missing.service", "state": "not-observed"},
                ],
            }
        },
    }
    data["categories"]["applications"]["items"] = [app]
    if basis == "partial":
        data["categories"]["applications"] = {
            "status": "error",
            "items": [],
            "partial_items": [app],
        }
    assert send(client, enrolled, data).status_code == 200
    if basis == "historical":
        later = report(observed_at=(timezone.now() + timedelta(seconds=1)).isoformat())
        later["categories"]["applications"] = {"status": "error", "items": []}
        assert send(client, enrolled, later).status_code == 200
    before = list(InventorySnapshot.objects.values())
    response = application_page(client, django_user_model)
    html = response.content.decode()
    assert response.context["total"] == 1
    assert "worker.service" in html and "failed" in html and "not-observed" in html
    assert "&lt;script&gt;state&lt;/script&gt;" in html and "<script>" not in html
    assert data["snapshot_id"] in html
    assert "whole-stack health" in html
    if basis == "historical":
        assert "Historical evidence" in html
    if basis == "partial":
        assert "Incomplete application inventory" in html
    assert list(InventorySnapshot.objects.values()) == before


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        {},
        {"status": "ok", "items": [None]},
        {"status": "ok", "items": [{"name": "bad/name.service", "state": "active"}]},
        {"status": "ok", "items": [{"name": "x.service", "state": "active"}] * 2},
        {
            "status": "ok",
            "items": [{"name": f"w{i}.service", "state": "active"} for i in range(33)],
        },
    ],
)
def test_malformed_related_units_are_visible(value):
    from graphyard.inventory_applications import project

    assert project({"id": "app", "items": {"related_units": value}})[
        "related_units"
    ] == {"valid": False}


def test_failed_related_unit_inventory_never_renders_active_state():
    from graphyard.inventory_applications import project

    for status in ["error", "unsupported"]:
        result = project(
            {
                "id": "app",
                "items": {
                    "related_units": {
                        "status": status,
                        "items": [{"name": "worker.service", "state": "active"}],
                    }
                },
            }
        )
        assert result["related_units"]["rows"][0]["state"] == "unknown"
    assert project({"id": "old", "items": {}})["related_units"] is None


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, "Malformed related-unit evidence"),
        ({"status": "ok", "items": []}, "No related units recorded"),
        ({"status": "unsupported", "items": []}, "No related units recorded"),
        ({"status": "error", "items": []}, "No related units recorded"),
        (
            {
                "status": "error",
                "items": [{"name": "worker.service", "state": "active"}],
            },
            "unknown",
        ),
    ],
)
def test_related_unit_error_and_empty_states_reach_reader_html(
    client, enrolled, django_user_model, value, expected
):
    data = report()
    data["categories"]["applications"]["items"] = [
        {"id": "app", "status": "ok", "items": {"related_units": value}}
    ]
    assert send(client, enrolled, data).status_code == 200
    html = application_page(client, django_user_model).content.decode()
    assert expected in html
    assert "<td>active</td>" not in html
    if isinstance(value, dict):
        assert "Unit inventory status: " + value["status"] in html
        if value["items"]:
            assert "<td>unknown</td>" in html
        else:
            assert "Malformed related-unit evidence" not in html
            assert "does not establish complete coverage" in html
            assert "<th>Unit</th>" not in html


def test_old_report_has_no_fabricated_related_unit_section(
    client, enrolled, django_user_model
):
    data = report()
    data["categories"]["applications"]["items"] = [
        {"id": "old", "status": "ok", "items": {}}
    ]
    assert send(client, enrolled, data).status_code == 200
    html = application_page(client, django_user_model).content.decode()
    assert "Related systemd units" not in html
    assert "Malformed related-unit evidence" not in html
