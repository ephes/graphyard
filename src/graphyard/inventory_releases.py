"""Explicit public source registry and cached release comparisons. No host access."""

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache, partial
from time import monotonic

import httpx
from django.db import transaction
from django.db.models import F
from django.utils import timezone
from packaging.version import Version

from .models import InventoryRelease, InventoryReleaseRegistry

MAX_AGE = timedelta(days=8)
REFRESH_AFTER = timedelta(days=6)
MAX_BODY = 16 * 1024 * 1024
FETCH_BUDGET_SECONDS = 60
REFRESH_BUDGET_SECONDS = 300
NAME = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.@+-]{0,99}")


def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def definitions(data):
    if not isinstance(data, list) or len(data) > 200:
        raise ValueError("Expected at most 200 explicit release sources")
    result = {}
    targets = set()
    for item in data:
        if not isinstance(item, dict) or set(item) != {"kind", "project", "targets"}:
            raise ValueError("Invalid source fields")
        kind, project = item["kind"], item["project"]
        if kind not in ("pypi", "github", "brew") or not isinstance(project, str):
            raise ValueError("Unsupported source")
        parts = project.split("/")
        if len(parts) != (2 if kind == "github" else 1) or any(
            not NAME.fullmatch(p) for p in parts
        ):
            raise ValueError("Invalid public project identifier")
        if kind == "pypi":
            project = normalize(project)
        source_id = kind + ":" + project
        if source_id in result:
            raise ValueError("Duplicate release source")
        if not isinstance(item["targets"], list) or not 1 <= len(item["targets"]) <= 20:
            raise ValueError("Invalid source targets")
        selected = []
        for target in item["targets"]:
            if not isinstance(target, dict) or set(target) != {"kind", "name"}:
                raise ValueError("Invalid target fields")
            target_kind, name = target["kind"], target["name"]
            if (
                target_kind not in ("python", "application", "brew")
                or not isinstance(name, str)
                or not NAME.fullmatch(name)
            ):
                raise ValueError("Invalid target")
            if (target_kind == "python" and kind != "pypi") or (
                target_kind == "brew" and kind != "brew"
            ):
                raise ValueError("Incompatible package source")
            name = normalize(name) if target_kind == "python" else name
            key = target_kind, name
            if key in targets:
                raise ValueError("Ambiguous comparison target")
            targets.add(key)
            selected.append({"kind": target_kind, "name": name})
        result[source_id] = {"kind": kind, "project": project, "targets": selected}
    return result


def url(definition):
    kind, project = definition["kind"], definition["project"]
    return {
        "pypi": f"https://pypi.org/pypi/{project}/json",
        "github": f"https://api.github.com/repos/{project}/releases/latest",
        "brew": f"https://formulae.brew.sh/api/formula/{project}.json",
    }[kind]


def _stable_version(value):
    if not isinstance(value, str) or not value or len(value) > 160:
        raise ValueError("Invalid release version")
    parsed = Version(value)
    if parsed.is_prerelease or parsed.is_devrelease or parsed.local is not None:
        raise ValueError("Unsupported release channel")
    return parsed


def stable(value):
    _stable_version(value)
    return value


def _unique_release_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate release response member")
        result[key] = value
    return result


def fetch(client, definition, *, deadline=None):
    now = monotonic()
    deadline = (
        min(deadline, now + FETCH_BUDGET_SECONDS)
        if deadline is not None
        else now + FETCH_BUDGET_SECONDS
    )

    def check_deadline():
        if monotonic() >= deadline:
            raise ValueError("Release lookup elapsed budget exceeded")

    check_deadline()
    with client.stream(
        "GET",
        url(definition),
        headers={"Accept-Encoding": "identity"},
        timeout=min(20, deadline - monotonic()),
    ) as response:
        check_deadline()
        response.raise_for_status()
        # Decompression would expand a chunk before any size check could run;
        # refuse encoded or over-declared bodies before reading them.
        encoding = response.headers.get("Content-Encoding", "").strip().lower()
        if encoding not in ("", "identity"):
            raise ValueError("Unsupported release response encoding")
        declared = response.headers.get("Content-Length")
        if declared is not None:
            declared = declared.strip(" \t")
            if not (declared.isascii() and declared.isdecimal()):
                raise ValueError("Release response too large")
            digits = declared.lstrip("0") or "0"
            limit = str(MAX_BODY)
            # Compare decimal magnitudes without converting an untrusted header.
            if (len(digits), digits) > (len(limit), limit):
                raise ValueError("Release response too large")
        raw = bytearray()
        for chunk in response.iter_bytes():
            check_deadline()
            if len(chunk) > MAX_BODY - len(raw):
                raise ValueError("Release response too large")
            raw.extend(chunk)
    check_deadline()
    try:
        data = json.loads(raw, object_pairs_hook=_unique_release_object)
    except RecursionError as exc:
        raise ValueError("Release response nested too deeply") from exc
    check_deadline()
    if not isinstance(data, dict):
        raise ValueError("Invalid release response")
    kind = definition["kind"]
    if kind == "github":
        if data.get("draft") is not False or data.get("prerelease") is not False:
            raise ValueError("Non-stable GitHub release")
        return stable(data.get("tag_name"))
    if kind == "pypi":
        info = data.get("info")
        if not isinstance(info, dict):
            raise ValueError("PyPI project mismatch")
        name = info.get("name")
        if not isinstance(name, str) or normalize(name) != definition["project"]:
            raise ValueError("PyPI project mismatch")
        files = data.get("urls")
        if (
            info.get("yanked") is not False
            or not isinstance(files, list)
            or not any(isinstance(f, dict) and f.get("yanked") is False for f in files)
        ):
            raise ValueError("No non-yanked release artifact")
        return stable(info.get("version"))
    if data.get("name") != definition["project"] or data.get("disabled") is not False:
        raise ValueError("Unsupported Homebrew formula")
    # Revision and rebuild differences are not upstream version differences.
    return stable(data.get("versions", {}).get("stable"))


@dataclass(frozen=True)
class RefreshResult:
    failures: int = 0
    skipped: int = 0
    # A newer refresh took the registry: this run's remaining results never count.
    superseded: bool = False

    @property
    def complete(self):
        return self.skipped == 0 and not self.superseded


def refresh(data, force=False):
    deadline = monotonic() + REFRESH_BUDGET_SECONDS
    registry = definitions(data)  # Validate completely before changing cache policy.
    with transaction.atomic():
        InventoryReleaseRegistry.objects.get_or_create(pk=1)
        InventoryReleaseRegistry.objects.filter(pk=1).update(
            generation=F("generation") + 1
        )
        generation = InventoryReleaseRegistry.objects.get(pk=1).generation
    started = timezone.now()
    with transaction.atomic():
        # UPDATE both checks ownership and locks policy replacement on SQLite
        # and row-locking databases. No wall-clock ordering is required.
        if not InventoryReleaseRegistry.objects.filter(
            pk=1, generation=generation
        ).update(generation=F("generation")):
            return RefreshResult(skipped=len(registry), superseded=True)
        removed = InventoryRelease.objects.filter(enabled=True).exclude(
            source_id__in=registry
        )
        if removed.exists():
            removed.update(enabled=False)
        for key, definition in registry.items():
            row, created = InventoryRelease.objects.only(
                "definition", "enabled"
            ).get_or_create(source_id=key, defaults={"definition": definition})
            if created:
                continue
            if row.definition != definition:
                # Cached evidence belongs to the entire definition, including targets.
                InventoryRelease.objects.filter(pk=key).update(
                    definition=definition,
                    enabled=True,
                    version="",
                    checked_at=None,
                    attempted_at=None,
                    error="",
                )
            elif not row.enabled:
                InventoryRelease.objects.filter(pk=key).update(enabled=True)
    failures = skipped = 0
    superseded = False
    with httpx.Client(
        timeout=20,
        follow_redirects=False,
        trust_env=False,
        headers={
            "Accept": "application/json",
            "User-Agent": "Graphyard-inventory-release-check/1",
        },
    ) as client:

        def fresh(row):
            return (
                not force
                and not row.error
                and row.checked_at
                and timedelta(0) <= started - row.checked_at < REFRESH_AFTER
            )

        keys = list(registry)
        for index, (key, definition) in enumerate(registry.items()):
            row = InventoryRelease.objects.get(pk=key)
            if fresh(row):
                continue
            if monotonic() >= deadline:
                skipped += 1
                continue
            try:
                version = fetch(client, definition, deadline=deadline)
                values = {"version": version, "checked_at": timezone.now(), "error": ""}
            except (httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError):
                failures += 1
                values = {"error": "Release lookup failed"}
            # Failed attempts preserve the last success and never renew its timestamp.
            values["attempted_at"] = started
            with transaction.atomic():
                # Serialize with policy replacement, never across network work.
                # Generation also fences removal/re-enable and A -> B -> A edits.
                superseded = not InventoryReleaseRegistry.objects.filter(
                    pk=1, generation=generation
                ).update(generation=F("generation"))
                if not superseded:
                    # The owned generation orders publications, even if the
                    # wall clock has moved backwards since the previous attempt.
                    published = InventoryRelease.objects.filter(
                        pk=key, enabled=True, definition=definition
                    ).update(**values)
                    if not published:
                        # A failed lookup already counts as failed, not also as skipped.
                        skipped += "version" in values
            if superseded:
                # A newer refresh owns the registry: this result was discarded and
                # the remaining sources needing a lookup are not looked up at all.
                # A failed lookup already counts as failed, not also as skipped.
                remaining = InventoryRelease.objects.filter(pk__in=keys[index + 1 :])
                skipped += ("version" in values) + sum(
                    1 for later in remaining if not fresh(later)
                )
                break
    if not superseded:
        # Fresh cache hits publish nothing, so a run that only skipped them would
        # otherwise never notice that a newer refresh replaced the registry. Any
        # newer refresh before this one returns makes it incomplete; rerun it.
        superseded = not InventoryReleaseRegistry.objects.filter(
            pk=1, generation=generation
        ).exists()
    return RefreshResult(failures=failures, skipped=skipped, superseded=superseded)


def _cached_release_state(release, now):
    if release.error:
        return "failed", None
    if not release.checked_at or not release.version:
        return "missing", None
    if not timedelta(0) <= now - release.checked_at <= MAX_AGE:
        return "stale", None
    try:
        return "ok", _stable_version(release.version)
    except ValueError:
        return "invalid", None


def source_health(now):
    """Report cached public-source freshness without fetching or trusting host names."""
    rows = []
    for release in InventoryRelease.objects.filter(enabled=True).order_by("source_id"):
        state, _ = _cached_release_state(release, now)
        rows.append(
            {
                "source": release.source_id,
                "status": state,
                "checked_at": release.checked_at.isoformat()
                if release.checked_at
                else None,
                "attempted_at": release.attempted_at.isoformat()
                if release.attempted_at
                else None,
            }
        )
    attention = sum(row["status"] != "ok" for row in rows)
    return {
        "total": len(rows),
        "attention": attention,
        "status": "not_configured" if not rows else "attention" if attention else "ok",
        "max_age_seconds": int(MAX_AGE.total_seconds()),
        "sources": rows,
    }


def registry():
    result = {}
    for row in InventoryRelease.objects.filter(enabled=True):
        for target in row.definition["targets"]:
            result[target["kind"], target["name"]] = row
    return result


@dataclass(frozen=True)
class _ReleaseEvidence:
    version: str
    source_url: str
    checked_at: datetime | None
    attempted_at: datetime | None
    state: str
    latest: Version | None


def _project_release(release, now):
    if release is None:
        return None
    source_url = url(release.definition)
    state, latest = _cached_release_state(release, now)
    return _ReleaseEvidence(
        release.version,
        source_url,
        release.checked_at,
        release.attempted_at,
        state,
        latest,
    )


def release_projector(now):
    """Reuse evidence for registered sources from a single read snapshot."""
    return cache(partial(_project_release, now=now))


def compare(installed, release, observed_at, now, historical=False):
    return _compare(
        installed, _project_release(release, now), observed_at, now, historical
    )


def _compare(installed, release, observed_at, now, historical):
    result = {
        "installed": installed if isinstance(installed, str) else "Unknown",
        "available": release.version if release and release.version else "Unknown",
        "source_url": release.source_url if release else "",
        "checked_at": release.checked_at if release else None,
        "attempted_at": release.attempted_at if release else None,
        "observed_at": observed_at,
        "status": "unknown",
        "reason": "No registered release source",
    }
    if release is None:
        return result
    latest = release.latest
    if latest is None:
        result["reason"] = {
            "failed": "Latest release lookup failed; previous result retained",
            "missing": "Release information missing",
            "stale": "Release information stale",
            "invalid": "Cached release version is invalid",
        }[release.state]
        return result
    if (
        historical
        or not observed_at
        or not timedelta(0) <= now - observed_at <= MAX_AGE
    ):
        result["reason"] = "Installed-version observation missing or stale"
        return result
    try:
        current = _stable_version(installed)
    except ValueError:
        result["reason"] = "Installed and release versions are not comparable"
        return result
    result["status"] = (
        "update" if current < latest else "equal" if current == latest else "ahead"
    )
    result["reason"] = {
        "update": "Newer upstream release",
        "equal": "Matches checked release",
        "ahead": "Installed version is ahead of checked release",
    }[result["status"]]
    return result


def rows_for_host(host, sources, now, observations=None, *, project_release=None):
    """Keep individual package observations traceable to their exact snapshot.

    ``observations`` are the host's categories with pointers already resolved,
    for example from one batched read snapshot; otherwise they are loaded here.
    Pass a projector to share release evidence across hosts in that snapshot.
    """
    from .inventory_applications import applications, source
    from .inventory_sbom import packages, Unavailable

    if project_release is None:
        project_release = partial(_project_release, now=now)
    if observations is None:
        observations = host.inventorycategory_set.select_related(
            "latest_attempt", "latest_success"
        )
    categories = {c.name: c for c in observations}
    snapshot, basis, apps = source(categories.get("applications"))
    rows = []
    notes = []
    if basis != "successful":
        notes.append("Application evidence: " + basis)
    if snapshot:
        for app in applications(apps):
            name = app.get("id")
            if not isinstance(name, str):
                continue
            evidence = app.get("items")
            if not isinstance(evidence, dict):
                continue
            target = project_release(sources.get(("application", name)))
            rows.append(
                {
                    "application": name,
                    "component": name,
                    "kind": "application",
                    "snapshot": snapshot,
                    "basis": basis,
                    **_compare(
                        evidence.get("installed_version"),
                        target,
                        snapshot.observed_at,
                        now,
                        basis == "historical" or app.get("status") != "ok",
                    ),
                }
            )
            try:
                installed = packages(app)
            except Unavailable:
                installed = []
            for package in installed:
                rows.append(
                    {
                        "application": name,
                        "component": package["name"],
                        "kind": "python",
                        "snapshot": snapshot,
                        "basis": basis,
                        **_compare(
                            package["version"],
                            project_release(
                                sources.get(("python", normalize(package["name"])))
                            ),
                            snapshot.observed_at,
                            now,
                            basis == "historical",
                        ),
                    }
                )
            for candidate in (
                evidence.get("package_candidates", [])
                if isinstance(evidence.get("package_candidates"), list)
                else []
            ):
                if (
                    not isinstance(candidate, dict)
                    or not all(
                        isinstance(candidate.get(k), str)
                        for k in ("name", "installed", "candidate")
                    )
                    or type(candidate.get("update_available")) is not bool
                ):
                    continue
                rows.append(
                    {
                        "application": name,
                        "component": candidate["name"],
                        "kind": "apt-cache",
                        "snapshot": snapshot,
                        "basis": basis,
                        "installed": candidate["installed"],
                        "available": candidate["candidate"],
                        "observed_at": snapshot.observed_at,
                        "status": "unknown",
                        "reason": "Cached APT candidate; index age unknown"
                        + (
                            "; newer candidate recorded"
                            if candidate["update_available"]
                            else ""
                        ),
                        "source_url": "",
                        "checked_at": None,
                    }
                )
    category = categories.get("packages")
    package_basis = "successful"
    package_snapshot = category.latest_attempt if category else None
    if not package_snapshot:
        notes.append("Package observation missing")
    elif package_snapshot.report["categories"]["packages"]["status"] != "ok":
        notes.append(
            "Latest package observation: "
            + package_snapshot.report["categories"]["packages"]["status"]
        )
        package_snapshot = category.latest_success
        package_basis = "historical"
    if package_snapshot:
        packages_data = package_snapshot.report["categories"]["packages"]
        if packages_data["status"] == "ok":
            for package in packages_data["items"]:
                if package.get("kind") != "brew" or not isinstance(
                    package.get("name"), str
                ):
                    continue
                name = package["name"]
                # Homebrew rebuild suffix is not part of the upstream stable version.
                version = package.get("version")
                if isinstance(version, str):
                    version = re.sub(r"_\d+$", "", version)
                row = _compare(
                    version,
                    project_release(sources.get(("brew", name))),
                    package_snapshot.observed_at,
                    now,
                    package_basis == "historical",
                )
                rows.append(
                    {
                        "application": "Homebrew formulae",
                        "component": name,
                        "kind": "brew (upstream only)",
                        "snapshot": package_snapshot,
                        "basis": package_basis,
                        **row,
                    }
                )
    return rows, notes
