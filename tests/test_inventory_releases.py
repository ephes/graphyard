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
    assert releases.refresh([definition()], force=True).failures == 1
    row.refresh_from_db()
    assert row.version == "6.0" and row.checked_at == original
    assert row.error == "Release lookup failed"
    assert (
        releases.compare("5.0", row, timezone.now(), timezone.now())["status"]
        == "unknown"
    )
    monkeypatch.setattr(releases, "fetch", Mock(return_value="6.1"))
    assert releases.refresh([definition()]).failures == 0
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

    refresh = Mock(return_value=releases.RefreshResult())
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


@pytest.mark.parametrize("clock_step", [-1, 1])
@pytest.mark.parametrize("old_failure", [False, True])
def test_older_refresh_cannot_overwrite_newer_completion(
    monkeypatch, old_failure, clock_step
):
    old = timezone.now()
    newer = old + timedelta(seconds=clock_step)
    times = iter([old, newer, newer, old + timedelta(seconds=2)])
    monkeypatch.setattr(releases.timezone, "now", lambda: next(times))
    nested = False

    def fetch(client, item, **kwargs):
        nonlocal nested
        if nested:
            return "6.2"
        nested = True
        assert releases.refresh([definition()], force=True).failures == 0
        if old_failure:
            raise ValueError("old request failed")
        return "6.1"

    monkeypatch.setattr(releases, "fetch", fetch)
    assert releases.refresh([definition()], force=True).failures == int(old_failure)
    row = InventoryRelease.objects.get(pk="pypi:django")
    assert row.version == "6.2"
    assert row.checked_at == newer
    assert row.attempted_at == newer
    assert row.error == ""


def test_slow_stream_exhausts_budget_preserves_success_and_continues(monkeypatch):
    row = cache()
    checked = row.checked_at
    clock = [0.0]
    monkeypatch.setattr(releases, "monotonic", lambda: clock[0], raising=False)
    closed = []

    class SlowStream(httpx.SyncByteStream):
        def __iter__(self):
            for _ in range(10):
                clock[0] += 19
                yield b" "
            yield b'{"info":{"name":"django","version":"6.1","yanked":false},"urls":[{"yanked":false}]}'

        def close(self):
            closed.append(True)

    def handler(request):
        if "/django/" in str(request.url):
            return httpx.Response(200, stream=SlowStream())
        return httpx.Response(
            200,
            json={
                "info": {"name": "flask", "version": "3.1", "yanked": False},
                "urls": [{"yanked": False}],
            },
        )

    factory = httpx.Client
    monkeypatch.setattr(
        releases.httpx,
        "Client",
        lambda **kw: factory(transport=httpx.MockTransport(handler), **kw),
    )
    assert (
        releases.refresh(
            [definition(), definition(project="flask")], force=True
        ).failures
        == 1
    )
    row.refresh_from_db()
    assert row.version == "6.0" and row.checked_at == checked
    assert row.error == "Release lookup failed"
    assert InventoryRelease.objects.get(pk="pypi:flask").version == "3.1"
    assert closed == [True]
    assert clock[0] == 76


@pytest.mark.parametrize(
    "replacement",
    [
        [],
        [definition(project="flask")],
        [{**definition(), "targets": [{"kind": "python", "name": "new-target"}]}],
    ],
)
def test_delayed_policy_cannot_replace_newer_registry(monkeypatch, replacement):
    """A starts first but reaches its policy transaction after B has applied."""
    now = timezone.now()
    calls = 0

    def paused_start():
        nonlocal calls
        calls += 1
        if calls == 1:
            assert releases.refresh(replacement, force=True).failures == 0
        return now

    monkeypatch.setattr(releases.timezone, "now", paused_start)
    monkeypatch.setattr(releases, "fetch", Mock(return_value="1.0"))
    assert releases.refresh([definition()], force=True).failures == 0
    enabled = list(InventoryRelease.objects.filter(enabled=True))
    assert {row.source_id: row.definition for row in enabled} == releases.definitions(
        replacement
    )


VALID_PYPI = b'{"info":{"name":"django","version":"6.1","yanked":false},"urls":[{"yanked":false}]}'


class RecordingStream(httpx.SyncByteStream):
    def __init__(self, *chunks):
        self.chunks = chunks
        self.read = []

    def __iter__(self):
        for chunk in self.chunks:
            self.read.append(chunk)
            yield chunk


def fetch_with(response_factory):
    seen = []

    def handler(request):
        seen.append(request)
        return response_factory()

    with httpx.Client(
        transport=httpx.MockTransport(handler), follow_redirects=False
    ) as client:
        try:
            return releases.fetch(client, definition()), seen
        except ValueError as exc:
            return exc, seen


@pytest.mark.parametrize("encoding", ["gzip", "deflate", "br", "GZIP "])
def test_compressed_release_response_is_refused_before_body_read(encoding):
    import gzip

    stream = RecordingStream(gzip.compress(b"\0" * (1024 * 1024)))
    result, seen = fetch_with(
        lambda: httpx.Response(
            200, stream=stream, headers={"Content-Encoding": encoding}
        )
    )
    assert isinstance(result, ValueError)
    assert "encoding" in str(result)
    assert stream.read == []
    assert seen[0].headers["Accept-Encoding"] == "identity"


@pytest.mark.parametrize("declared", ["9", "abc", "-1"])
def test_over_declared_release_body_is_refused_before_body_read(monkeypatch, declared):
    monkeypatch.setattr(releases, "MAX_BODY", 8)
    stream = RecordingStream(b"{}")
    result, _ = fetch_with(
        lambda: httpx.Response(200, stream=stream, headers={"Content-Length": declared})
    )
    assert isinstance(result, ValueError)
    assert stream.read == []


def test_release_chunk_beyond_budget_is_refused_before_buffering(monkeypatch):
    monkeypatch.setattr(releases, "MAX_BODY", len(VALID_PYPI) - 1)
    grown = []
    original = bytearray.extend

    class Spy(bytearray):
        def extend(self, data):
            original(self, data)
            grown.append(len(self))

    monkeypatch.setattr(releases, "bytearray", Spy, raising=False)
    stream = RecordingStream(VALID_PYPI[:10], VALID_PYPI[10:])
    result, _ = fetch_with(lambda: httpx.Response(200, stream=stream))
    assert isinstance(result, ValueError) and "too large" in str(result)
    assert grown == [10]
    # The exact budget is still accepted.
    monkeypatch.setattr(releases, "MAX_BODY", len(VALID_PYPI))
    stream = RecordingStream(VALID_PYPI)
    assert fetch_with(lambda: httpx.Response(200, stream=stream))[0] == "6.1"


def test_identity_release_response_is_accepted():
    stream = RecordingStream(VALID_PYPI)
    result, _ = fetch_with(
        lambda: httpx.Response(
            200,
            stream=stream,
            headers={
                "Content-Encoding": "identity",
                "Content-Length": str(len(VALID_PYPI)),
            },
        )
    )
    assert result == "6.1"


def test_malformed_release_source_is_isolated_from_other_sources(monkeypatch):
    row = cache()
    checked = row.checked_at
    nested = b"[" * 200_000 + b"]" * 200_000

    def handler(request):
        if "/django/" in str(request.url):
            return httpx.Response(200, content=nested)
        return httpx.Response(
            200,
            json={
                "info": {"name": "flask", "version": "3.1", "yanked": False},
                "urls": [{"yanked": False}],
            },
        )

    factory = httpx.Client
    monkeypatch.setattr(
        releases.httpx,
        "Client",
        lambda **kw: factory(transport=httpx.MockTransport(handler), **kw),
    )
    assert (
        releases.refresh(
            [definition(), definition(project="flask")], force=True
        ).failures
        == 1
    )
    row.refresh_from_db()
    assert row.version == "6.0" and row.checked_at == checked
    assert row.error == "Release lookup failed"
    assert InventoryRelease.objects.get(pk="pypi:flask").version == "3.1"


@pytest.mark.parametrize("change", ["project", "targets"])
def test_definition_change_invalidates_fresh_success_before_lookup(monkeypatch, change):
    row = cache(error="old error", attempted_at=timezone.now())
    row.error = ""
    if change == "project":
        row.definition = definition(project="old-project")
    row.save()
    replacement = definition()
    if change == "targets":
        replacement["targets"] = [{"kind": "python", "name": "new-target"}]
    observed = []

    def lookup(client, item, **kwargs):
        current = InventoryRelease.objects.get(pk=row.pk)
        observed.append(
            (current.version, current.checked_at, current.attempted_at, current.error)
        )
        assert (
            releases.compare("1.0", current, timezone.now(), timezone.now())[
                "available"
            ]
            == "Unknown"
        )
        raise ValueError("new source unavailable")

    monkeypatch.setattr(releases, "fetch", lookup)
    assert releases.refresh([replacement]).failures == 1
    assert observed == [("", None, None, "")]
    row.refresh_from_db()
    assert row.definition == replacement
    assert row.version == "" and row.checked_at is None


@pytest.mark.parametrize("replacement", ["changed", "removed", "aba"])
@pytest.mark.parametrize("old_failure", [False, True])
def test_inflight_completion_is_bound_to_registry_generation(
    monkeypatch, replacement, old_failure
):
    cache()
    now = timezone.now()
    monkeypatch.setattr(releases.timezone, "now", lambda: now)
    original = definition()
    changed = {**original, "targets": [{"kind": "python", "name": "new-target"}]}
    nested = False
    expected = []

    def lookup(client, item, **kwargs):
        nonlocal nested
        if nested:
            raise ValueError("replacement unavailable")
        nested = True
        releases.refresh([] if replacement == "removed" else [changed], force=True)
        if replacement == "aba":
            releases.refresh([original], force=True)
        expected.append(list(InventoryRelease.objects.values()))
        if old_failure:
            raise ValueError("old failure")
        return "99.0"

    monkeypatch.setattr(releases, "fetch", lookup)
    releases.refresh([original], force=True)
    assert list(InventoryRelease.objects.values()) == expected[0]


def test_superseded_refresh_stops_looking_up_and_reports_incomplete(monkeypatch):
    from django.db import transaction
    from django.db.models import F

    from graphyard.models import InventoryReleaseRegistry

    payload = [definition(project=f"project-{i}") for i in range(3)]
    lookups = []

    def lookup(client, item, **kwargs):
        lookups.append(item["project"])
        if len(lookups) == 1:
            # A newer refresh takes the registry while this lookup is in flight.
            with transaction.atomic():
                InventoryReleaseRegistry.objects.filter(pk=1).update(
                    generation=F("generation") + 1
                )
        return "2.0"

    monkeypatch.setattr(releases, "fetch", lookup)
    result = releases.refresh(payload, force=True)
    assert lookups == ["project-0"]
    assert result.skipped == 3 and not result.complete
    assert not InventoryRelease.objects.filter(version="2.0").exists()


def test_superseded_refresh_skips_only_due_sources_and_counts_failure_once(
    monkeypatch,
):
    from django.db import transaction
    from django.db.models import F

    from graphyard.models import InventoryReleaseRegistry

    payload = [definition(project=f"project-{i}") for i in range(3)]
    # project-1 is a fresh cache hit; project-2 needs a lookup.
    InventoryRelease.objects.create(
        source_id="pypi:project-1",
        definition=payload[1],
        version="1.0",
        checked_at=timezone.now(),
    )
    lookups = []

    def lookup(client, item, **kwargs):
        lookups.append(item["project"])
        with transaction.atomic():
            InventoryReleaseRegistry.objects.filter(pk=1).update(
                generation=F("generation") + 1
            )
        raise ValueError("discarded failure")

    monkeypatch.setattr(releases, "fetch", lookup)
    result = releases.refresh(payload)
    assert lookups == ["project-0"]
    # The discarded failure stays a failure; only project-2 is skipped.
    assert (result.failures, result.skipped) == (1, 1)
    assert not result.complete


def test_superseded_failure_on_the_only_due_source_is_incomplete(monkeypatch):
    from django.db import transaction
    from django.db.models import F

    from graphyard.models import InventoryReleaseRegistry

    def lookup(client, item, **kwargs):
        with transaction.atomic():
            InventoryReleaseRegistry.objects.filter(pk=1).update(
                generation=F("generation") + 1
            )
        raise ValueError("discarded failure")

    monkeypatch.setattr(releases, "fetch", lookup)
    result = releases.refresh([definition()], force=True)
    assert result.superseded and result.skipped == 0
    assert not result.complete


def test_superseded_refresh_of_only_fresh_sources_is_incomplete(monkeypatch):
    from django.db import transaction
    from django.db.models import F

    from graphyard.models import InventoryReleaseRegistry

    cache()  # Fresh: no lookup, nothing to publish.
    real_get = InventoryRelease.objects.get

    def get(*args, **kwargs):
        # A newer refresh replaces the registry while this run walks its sources.
        with transaction.atomic():
            InventoryReleaseRegistry.objects.filter(pk=1).update(
                generation=F("generation") + 1
            )
        return real_get(*args, **kwargs)

    monkeypatch.setattr(InventoryRelease.objects, "get", get)
    monkeypatch.setattr(
        releases, "fetch", lambda *a, **k: pytest.fail("fresh source fetched")
    )
    result = releases.refresh([definition()])
    assert result.superseded and not result.complete


def test_refresh_superseded_after_publishing_everything_is_incomplete(monkeypatch):
    from django.db import transaction
    from django.db.models import F

    from graphyard.models import InventoryReleaseRegistry

    monkeypatch.setattr(releases, "fetch", lambda *a, **k: "6.2")
    # Bump the generation right after the only due result is published.
    published = []
    real_qs_update = type(InventoryRelease.objects.all()).update

    def update(self, **values):
        count = real_qs_update(self, **values)
        if "version" in values and not published:
            published.append(values["version"])
            with transaction.atomic():
                InventoryReleaseRegistry.objects.filter(pk=1).update(
                    generation=F("generation") + 1
                )
        return count

    monkeypatch.setattr(type(InventoryRelease.objects.all()), "update", update)
    outcome = releases.refresh([definition()], force=True)
    assert published == ["6.2"]
    assert InventoryRelease.objects.get(pk="pypi:django").version == "6.2"
    assert outcome.superseded and not outcome.complete


def test_whole_refresh_budget_bounds_200_sources_and_preserves_unattempted(monkeypatch):
    payload = [definition(project=f"project-{i}") for i in range(200)]
    previous = timezone.now() - timedelta(days=7)
    for item in payload:
        InventoryRelease.objects.create(
            source_id="pypi:" + item["project"],
            definition=item,
            version="1.0",
            checked_at=previous,
            attempted_at=previous,
        )
    clock = [0.0]
    attempted = []
    closed = []
    monkeypatch.setattr(releases, "monotonic", lambda: clock[0])

    class Stream(httpx.SyncByteStream):
        def __init__(self, project, allowance):
            self.project = project
            self.allowance = allowance

        def __iter__(self):
            # A source stays inside its own 60-second allowance, then another
            # read consumes the remaining whole-run time on the final source.
            for _ in range(5):
                clock[0] += min(10, self.allowance)
                yield b" "
            clock[0] += 9
            yield json.dumps(
                {
                    "info": {"name": self.project, "version": "2.0", "yanked": False},
                    "urls": [{"yanked": False}],
                }
            ).encode()

        def close(self):
            closed.append(self.project)

    def handler(request):
        project = request.url.path.split("/")[2]
        attempted.append(project)
        return httpx.Response(
            200, stream=Stream(project, request.extensions["timeout"]["read"])
        )

    factory = httpx.Client
    monkeypatch.setattr(
        releases.httpx,
        "Client",
        lambda **kw: factory(transport=httpx.MockTransport(handler), **kw),
    )
    result = releases.refresh(payload, force=True)
    assert len(attempted) == 6
    assert clock[0] == 300
    assert closed == attempted
    assert result.failures == 1
    assert result.skipped == 194 and not result.complete
    for row in InventoryRelease.objects.exclude(
        pk__in=["pypi:" + p for p in attempted]
    ):
        assert row.version == "1.0" and row.checked_at == previous
        assert row.attempted_at == previous and row.error == ""


def test_command_reports_incomplete_refresh(tmp_path, settings, monkeypatch, capsys):
    settings.BASE_DIR = tmp_path
    policy = tmp_path / "sources.json"
    policy.write_text(json.dumps([definition(), definition(project="flask")]))
    clock = [0.0]
    monkeypatch.setattr(releases, "monotonic", lambda: clock[0])

    def lookup(client, item, **kwargs):
        clock[0] = 300
        return "6.2"

    monkeypatch.setattr(releases, "fetch", lookup)
    with pytest.raises(CommandError, match="budget"):
        call_command("refresh_inventory_releases", file=policy, force=True)
    assert "1 skipped sources" in capsys.readouterr().out
    row = InventoryRelease.objects.get(pk="pypi:flask")
    assert row.attempted_at is None and row.checked_at is None


def test_fetch_uses_remaining_refresh_budget(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(releases, "monotonic", lambda: clock[0])
    stream = RecordingStream(VALID_PYPI)
    seen = []

    def handler(request):
        seen.append(request.extensions["timeout"])
        clock[0] = 7.0
        return httpx.Response(200, stream=stream)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(ValueError, match="budget"):
            releases.fetch(client, definition(), deadline=7.0)
    assert all(value <= 7 for value in seen[0].values())


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("replacement", ["changed", "removed", "aba"])
def test_separate_connection_cannot_publish_obsolete_lookup(monkeypatch, replacement):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event, current_thread, main_thread
    from django.db import connections

    # Earlier migration regression tests intentionally leave an older schema.
    call_command("migrate", verbosity=0)
    started = Event()
    resume = Event()
    original = definition()
    changed = {**original, "targets": [{"kind": "python", "name": "new-target"}]}

    def lookup(client, item, **kwargs):
        if current_thread() is not main_thread():
            started.set()
            assert resume.wait(timeout=10)
            return "99.0"
        raise ValueError("replacement unavailable")

    def old_refresh():
        try:
            return releases.refresh([original], force=True)
        finally:
            connections.close_all()

    monkeypatch.setattr(releases, "fetch", lookup)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(old_refresh)
        try:
            assert started.wait(timeout=10)
            releases.refresh([] if replacement == "removed" else [changed], force=True)
            if replacement == "aba":
                releases.refresh([original], force=True)
            expected = list(InventoryRelease.objects.values())
        finally:
            resume.set()
        future.result(timeout=10)
    assert list(InventoryRelease.objects.values()) == expected


@pytest.mark.parametrize("failed", [False, True])
def test_current_refresh_publishes_after_wall_clock_rollback(monkeypatch, failed):
    now = timezone.now()
    row = cache(attempted_at=now)
    old_checked_at = row.checked_at
    rolled_back = now - timedelta(hours=1)
    monkeypatch.setattr(releases.timezone, "now", lambda: rolled_back)
    monkeypatch.setattr(
        releases,
        "fetch",
        Mock(side_effect=ValueError("private detail"))
        if failed
        else Mock(return_value="6.2"),
    )
    result = releases.refresh([definition()], force=True)
    row.refresh_from_db()
    assert result.complete and result.failures == int(failed)
    assert row.attempted_at == rolled_back
    assert row.version == ("6.0" if failed else "6.2")
    assert row.checked_at == (old_checked_at if failed else rolled_back)
    assert row.error == ("Release lookup failed" if failed else "")


def test_current_refresh_reports_unpublishable_result(monkeypatch):
    def lookup(*args, **kwargs):
        InventoryRelease.objects.all().delete()
        return "6.2"

    monkeypatch.setattr(releases, "fetch", lookup)
    result = releases.refresh([definition()], force=True)
    assert not result.complete and result.skipped == 1


def test_current_refresh_counts_unpublishable_failure_once(monkeypatch):
    def lookup(*args, **kwargs):
        InventoryRelease.objects.all().delete()
        raise httpx.ConnectError("synthetic outage")

    monkeypatch.setattr(releases, "fetch", lookup)
    result = releases.refresh([definition()], force=True)
    assert (result.failures, result.skipped) == (1, 0)


@pytest.mark.parametrize(
    "item,body",
    [
        (
            definition("github", "sample/widget", "application"),
            b'{"tag_name":"1.0","tag_name":"2.0","draft":false,"prerelease":false}',
        ),
        (
            definition("github", "sample/widget", "application"),
            b'{"tag_name":"2.0","draft":true,"draft":false,"prerelease":false}',
        ),
        (
            definition("github", "sample/widget", "application"),
            b'{"tag_name":"2.0","draft":false,"prerelease":true,"prerelease":false}',
        ),
        (
            definition(),
            b'{"info":{"name":"django","version":"1.0","version":"2.0","yanked":false},"urls":[{"yanked":false}]}',
        ),
        (
            definition(),
            b'{"info":{"name":"other","name":"django","version":"2.0","yanked":false},"urls":[{"yanked":false}]}',
        ),
        (
            definition(),
            b'{"info":{"name":"django","version":"2.0","yanked":true,"yanked":false},"urls":[{"yanked":false}]}',
        ),
        (
            definition(),
            b'{"info":{"name":"django","version":"2.0","yanked":false},"urls":[{"yanked":true,"yanked":false}]}',
        ),
        (
            definition("brew", "git", "brew"),
            b'{"name":"git","disabled":true,"disabled":false,"versions":{"stable":"2.0"}}',
        ),
        (
            definition("brew", "git", "brew"),
            b'{"name":"git","disabled":false,"versions":{"stable":"1.0","stable":"2.0"}}',
        ),
        (
            definition("brew", "git", "brew"),
            b'{"name":"git","disabled":false,"versions":{"stable":"2.0"},"extra":{"x":1,"\\u0078":1}}',
        ),
    ],
)
def test_duplicate_release_members_preserve_cache_and_isolate_source(
    monkeypatch, item, body
):
    key = next(iter(releases.definitions([item])))
    row = InventoryRelease.objects.create(
        source_id=key, definition=item, version="1.0", checked_at=timezone.now()
    )
    checked = row.checked_at
    sibling = definition(project="flask")

    def handler(request):
        if str(request.url) == releases.url(item):
            return httpx.Response(200, content=body)
        return httpx.Response(
            200,
            json={
                "info": {"name": "flask", "version": "3.1", "yanked": False},
                "urls": [{"yanked": False}, {"yanked": False}],
            },
        )

    factory = httpx.Client
    monkeypatch.setattr(
        releases.httpx,
        "Client",
        lambda **kw: factory(transport=httpx.MockTransport(handler), **kw),
    )
    result = releases.refresh([item, sibling], force=True)
    assert result.complete and result.failures == 1
    row.refresh_from_db()
    assert row.version == "1.0" and row.checked_at == checked
    assert row.error == "Release lookup failed"
    assert InventoryRelease.objects.get(pk="pypi:flask").version == "3.1"
