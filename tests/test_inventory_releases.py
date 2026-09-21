import fcntl
import json
from datetime import timedelta
from unittest.mock import Mock

import httpx
import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.urls import reverse
from django.utils import timezone

from graphyard import inventory_releases as releases
from graphyard.models import (
    InventoryCategory,
    InventoryHost,
    InventoryRelease,
    InventorySnapshot,
    HostRegistry,
)

pytestmark = pytest.mark.django_db


def definition(kind="pypi", project="django", target="python"):
    return {
        "kind": kind,
        "project": project,
        "targets": [
            {"kind": target, "name": project if kind != "github" else "navidrome"}
        ],
    }


def cache(version="6.0", **kwargs):
    return InventoryRelease.objects.create(
        source_id="pypi:django",
        definition=definition(),
        version=version,
        checked_at=timezone.now(),
        **kwargs,
    )


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        [definition(), definition()],
        [{**definition(), "url": "http://localhost"}],
        [definition(project="../secret")],
        [definition(project="django?url=localhost")],
        [definition(kind="file")],
        [definition(target="brew")],
    ],
)
def test_registry_rejects_invalid_or_ambiguous_sources_without_mutation(payload):
    row = cache()
    with pytest.raises(ValueError):
        releases.refresh(payload)
    row.refresh_from_db()
    assert row.enabled


@pytest.mark.parametrize(
    "kind,project,payload,expected",
    [
        (
            "pypi",
            "django",
            {
                "info": {"name": "Django", "version": "6.0", "yanked": False},
                "urls": [{"yanked": False}],
            },
            "6.0",
        ),
        (
            "github",
            "navidrome/navidrome",
            {"tag_name": "v0.60.0", "draft": False, "prerelease": False},
            "v0.60.0",
        ),
        (
            "brew",
            "git",
            {"name": "git", "disabled": False, "versions": {"stable": "2.50.0"}},
            "2.50.0",
        ),
    ],
)
def test_fetch_reads_only_constructed_public_source(kind, project, payload, expected):
    seen = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=payload)

    item = definition(kind, project)
    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        assert releases.fetch(client, item) == expected
    assert str(seen[0].url) == releases.url(item)


@pytest.mark.parametrize(
    "change",
    [
        "yanked",
        "empty_files",
        "identity",
        "prerelease",
        "missing_yanked",
        "oversize",
        "redirect",
        "rate_limit",
    ],
)
def test_fetch_failures_do_not_create_release_success(change, monkeypatch):
    payload = {
        "info": {"name": "Django", "version": "6.0", "yanked": False},
        "urls": [{"yanked": False}],
    }
    if change == "yanked":
        payload["info"]["yanked"] = True
    if change == "empty_files":
        payload["urls"] = []
    if change == "identity":
        payload["info"]["name"] = "another-package"
    if change == "prerelease":
        payload["info"]["version"] = "7.0rc1"
    if change == "missing_yanked":
        del payload["info"]["yanked"]
    if change == "oversize":
        monkeypatch.setattr(releases, "MAX_BODY", 8)
    status = 302 if change == "redirect" else 429 if change == "rate_limit" else 200
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(
                status, json=payload, headers={"Location": "http://localhost"}
            )
        ),
        follow_redirects=False,
    ) as client:
        with pytest.raises((ValueError, httpx.HTTPError)):
            releases.fetch(client, definition())


def test_cache_failure_preserves_success_time_and_retry_is_not_skipped(monkeypatch):
    row = cache()
    original = row.checked_at
    monkeypatch.setattr(
        releases, "fetch", Mock(side_effect=ValueError("private response detail"))
    )
    assert releases.refresh([definition()], force=True) == 1
    row.refresh_from_db()
    assert row.version == "6.0" and row.checked_at == original
    assert row.error == "Release lookup failed"
    assert (
        releases.compare("5.0", row, timezone.now(), timezone.now())["status"]
        == "unknown"
    )
    monkeypatch.setattr(releases, "fetch", Mock(return_value="6.1"))
    assert releases.refresh([definition()]) == 0
    row.refresh_from_db()
    assert row.version == "6.1" and not row.error and row.checked_at >= original
    releases.fetch.reset_mock()
    releases.refresh([definition()])
    releases.fetch.assert_not_called()


def test_registry_removal_disables_prior_source():
    row = cache()
    releases.refresh([])
    row.refresh_from_db()
    assert not row.enabled and releases.registry() == {}


@pytest.mark.parametrize(
    "installed,latest,status",
    [
        ("5.2", "6.0", "update"),
        ("6.0", "6.0.0", "equal"),
        ("7.0", "6.0", "ahead"),
        ("6.0rc1", "6.0", "unknown"),
        ("1:2.0-1ubuntu1", "2.0", "unknown"),
        (None, "6.0", "unknown"),
    ],
)
def test_version_ordering_and_unsupported_channels(installed, latest, status):
    assert (
        releases.compare(installed, cache(latest), timezone.now(), timezone.now())[
            "status"
        ]
        == status
    )


@pytest.mark.parametrize(
    "case",
    [
        "installed_stale",
        "source_stale",
        "future_source",
        "future_installed",
        "historical",
        "unregistered",
    ],
)
def test_stale_future_and_missing_evidence_never_matches(case):
    now = timezone.now()
    row = cache()
    observed = now
    if case == "installed_stale":
        observed -= timedelta(days=9)
    if case == "future_installed":
        observed += timedelta(seconds=10)
    if case == "source_stale":
        row.checked_at = now - timedelta(days=9)
    if case == "future_source":
        row.checked_at = now + timedelta(seconds=10)
    if case == "unregistered":
        row = None
    assert (
        releases.compare(
            "6.0", row, observed, now + timedelta(seconds=1), case == "historical"
        )["status"]
        == "unknown"
    )


def test_view_uses_snapshot_metadata_without_network_and_exposes_unknown(
    client, django_user_model, monkeypatch
):
    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="example", enabled=True)
    )
    now = timezone.now() - timedelta(minutes=1)
    data = {
        "applications": {
            "status": "error",
            "items": [],
            "partial_items": [
                {
                    "id": "my-app<script>",
                    "status": "ok",
                    "items": {
                        "python": {
                            "status": "ok",
                            "items": [
                                {"name": "Django", "version": "5.2"},
                                {"name": "private-name", "version": "1.0"},
                            ],
                        }
                    },
                },
                {
                    "id": "dns",
                    "status": "ok",
                    "items": {
                        "package_candidates": [
                            {
                                "name": "unbound",
                                "installed": "1.0",
                                "candidate": "2.0",
                                "update_available": True,
                            }
                        ]
                    },
                },
            ],
        }
    }
    import uuid

    snapshot = InventorySnapshot.objects.create(
        host=host,
        snapshot_id=uuid.uuid4(),
        observed_at=now,
        digest="a" * 64,
        report={"categories": data},
    )
    InventoryCategory.objects.create(
        host=host, name="applications", latest_attempt=snapshot
    )
    row = cache()
    monkeypatch.setattr(
        httpx.Client,
        "send",
        Mock(side_effect=AssertionError("Request view must not fetch")),
    )
    url = reverse("graphyard:inventory_versions")
    assert client.get(url).status_code == 302
    client.force_login(django_user_model.objects.create_user(username="release-reader"))
    response = client.get(url)
    assert response.status_code == 200
    html = response.content.decode()
    assert "Newer upstream release" in html and "No registered release source" in html
    assert "Partial application observation" in html
    assert "Inventory coverage gaps" in html
    assert "index age unknown" in html and "newer candidate recorded" in html
    assert "&lt;script&gt;" in html and "my-app<script>" not in html
    assert str(snapshot.snapshot_id) in html
    assert response.context["counts"]["update"] == 1
    assert len(client.get(url, {"status": "update"}).context["page"]) == 1
    assert len(client.get(url, {"q": "no-match"}).context["page"]) == 0
    snapshot.refresh_from_db()
    assert snapshot.report["categories"] == data
    row.refresh_from_db()
    assert not row.error


def test_command_check_is_read_only_and_lock_prevents_parallel_refresh(
    tmp_path, settings, monkeypatch
):
    settings.BASE_DIR = tmp_path
    policy = tmp_path / "sources.json"
    policy.write_text(json.dumps([definition()]))
    from graphyard.management.commands import refresh_inventory_releases as command

    refresh = Mock(return_value=0)
    monkeypatch.setattr(command, "refresh", refresh)
    call_command("refresh_inventory_releases", file=policy, check=True)
    refresh.assert_not_called()
    lockfile = tmp_path / ".inventory-releases.lock"
    with lockfile.open("w") as lock:
        lockfile.chmod(0o600)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(CommandError, match="already running"):
            call_command("refresh_inventory_releases", file=policy)
    refresh.assert_not_called()
    call_command("refresh_inventory_releases", file=policy)
    refresh.assert_called_once()


def test_weekly_run_refreshes_despite_prior_request_duration_and_jitter(monkeypatch):
    row = cache()
    row.checked_at = timezone.now() - timedelta(days=7) + timedelta(hours=1, minutes=10)
    row.save()
    fetch = Mock(return_value="6.2")
    monkeypatch.setattr(releases, "fetch", fetch)
    releases.refresh([definition()])
    fetch.assert_called_once()


def test_distinct_sources_cannot_claim_same_target():
    first = definition()
    second = {**definition(project="flask"), "targets": first["targets"]}
    with pytest.raises(ValueError, match="Ambiguous"):
        releases.definitions([first, second])


def test_missing_host_categories_and_malformed_python_are_explicit():
    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="missing", enabled=True)
    )
    rows, notes = releases.rows_for_host(host, {}, timezone.now())
    assert (
        not rows
        and "Application evidence: missing" in notes
        and "Package observation missing" in notes
    )
    import uuid

    snapshot = InventorySnapshot.objects.create(
        host=host,
        snapshot_id=uuid.uuid4(),
        observed_at=timezone.now(),
        digest="b" * 64,
        report={
            "categories": {
                "applications": {
                    "status": "ok",
                    "items": [
                        {
                            "id": "bad-python",
                            "status": "ok",
                            "items": {
                                "python": {
                                    "status": "ok",
                                    "items": [{"name": [], "version": {}}],
                                }
                            },
                        }
                    ],
                }
            }
        },
    )
    InventoryCategory.objects.create(
        host=host, name="applications", latest_attempt=snapshot
    )
    rows, notes = releases.rows_for_host(host, {}, timezone.now())
    assert len(rows) == 1 and rows[0]["status"] == "unknown"
    assert rows[0]["kind"] == "application"
    assert "Package observation missing" in notes


def test_failed_brew_probe_retains_historical_unknown_with_source_identity():
    import uuid

    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="brew-host", enabled=True)
    )
    now = timezone.now()
    old = InventorySnapshot.objects.create(
        host=host,
        snapshot_id=uuid.uuid4(),
        observed_at=now - timedelta(hours=1),
        digest="c" * 64,
        report={
            "categories": {
                "packages": {
                    "status": "ok",
                    "items": [{"kind": "brew", "name": "git", "version": "2.50.0_1"}],
                }
            }
        },
    )
    failed = InventorySnapshot.objects.create(
        host=host,
        snapshot_id=uuid.uuid4(),
        observed_at=now,
        digest="d" * 64,
        report={"categories": {"packages": {"status": "error", "items": []}}},
    )
    InventoryCategory.objects.create(
        host=host, name="packages", latest_attempt=failed, latest_success=old
    )
    InventoryRelease.objects.create(
        source_id="brew:git",
        definition=definition("brew", "git", "brew"),
        version="2.50.0",
        checked_at=now,
    )
    rows, notes = releases.rows_for_host(host, releases.registry(), now)
    assert rows[0]["status"] == "unknown" and rows[0]["basis"] == "historical"
    assert rows[0]["snapshot"] == old and rows[0]["installed"] == "2.50.0"
    assert "Latest package observation: error" in notes


def test_linux_packages_are_not_mistaken_for_missing_brew_and_unsupported_is_explicit():
    import uuid

    host = InventoryHost.objects.create(
        host=HostRegistry.objects.create(host_id="debian", enabled=True)
    )
    snapshot = InventorySnapshot.objects.create(
        host=host,
        snapshot_id=uuid.uuid4(),
        observed_at=timezone.now(),
        digest="e" * 64,
        report={
            "categories": {
                "applications": {"status": "ok", "items": []},
                "packages": {
                    "status": "ok",
                    "items": [{"kind": "deb", "name": "bash", "version": "5.2-1"}],
                },
            }
        },
    )
    for name in ("applications", "packages"):
        InventoryCategory.objects.create(
            host=host, name=name, latest_attempt=snapshot, latest_success=snapshot
        )
    InventoryRelease.objects.create(
        source_id="brew:git",
        definition=definition("brew", "git", "brew"),
        version="2.50.0",
        checked_at=timezone.now(),
    )
    rows, notes = releases.rows_for_host(host, releases.registry(), timezone.now())
    assert rows == [] and notes == []
    snapshot.report["categories"]["packages"] = {"status": "unsupported", "items": []}
    snapshot.save()
    InventoryCategory.objects.filter(host=host, name="packages").update(
        latest_success=None
    )
    rows, notes = releases.rows_for_host(host, {}, timezone.now())
    assert rows == [] and notes == ["Latest package observation: unsupported"]


@pytest.mark.parametrize(
    "kind", ["ok", "failed", "missing", "empty_version", "stale", "future", "invalid"]
)
def test_monitor_status_reports_cached_release_failures_without_fetch(
    client, settings, monkeypatch, kind
):
    settings.GRAPHYARD_INVENTORY_MONITOR_TOKEN = "synthetic-monitor"
    row = cache()
    if kind == "failed":
        row.error = "private diagnostic must not be echoed"
    elif kind == "missing":
        row.checked_at = None
    elif kind == "empty_version":
        row.version = ""
    elif kind == "stale":
        row.checked_at = timezone.now() - timedelta(days=9)
    elif kind == "future":
        row.checked_at = timezone.now() + timedelta(hours=1)
    elif kind == "invalid":
        row.version = "not a version"
    row.save()
    before = InventoryRelease.objects.values().get()

    def no_fetch(*args, **kwargs):
        raise AssertionError("monitoring must not contact release sources")

    monkeypatch.setattr(httpx, "Client", no_fetch)
    response = client.get(
        reverse("graphyard:inventory_status"),
        HTTP_AUTHORIZATION="Bearer synthetic-monitor",
    )
    status = response.json()
    expected = {"empty_version": "missing", "future": "stale"}.get(kind, kind)
    assert status["releases"]["sources"][0]["status"] == expected
    assert status["releases"]["total"] == 1
    assert status["summary"]["release_attention"] == int(kind != "ok")
    assert "private diagnostic" not in response.content.decode()
    assert InventoryRelease.objects.values().get() == before
    row.enabled = False
    row.save()
    status = client.get(
        reverse("graphyard:inventory_status"),
        HTTP_AUTHORIZATION="Bearer synthetic-monitor",
    ).json()
    assert status["releases"]["status"] == "not_configured"
    assert status["releases"]["total"] == 0
