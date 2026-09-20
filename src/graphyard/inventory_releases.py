"""Explicit public source registry and cached release comparisons. No host access."""

import json
import re
from datetime import timedelta

import httpx
from django.db import transaction
from django.utils import timezone
from packaging.version import InvalidVersion, Version

from .models import InventoryRelease

MAX_AGE = timedelta(days=8)
REFRESH_AFTER = timedelta(days=6)
MAX_BODY = 16 * 1024 * 1024
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


def stable(value):
    if not isinstance(value, str) or not value or len(value) > 160:
        raise ValueError("Invalid release version")
    parsed = Version(value)
    if parsed.is_prerelease or parsed.is_devrelease or parsed.local is not None:
        raise ValueError("Unsupported release channel")
    return value


def fetch(client, definition):
    with client.stream("GET", url(definition)) as response:
        response.raise_for_status()
        raw = bytearray()
        for chunk in response.iter_bytes():
            raw.extend(chunk)
            if len(raw) > MAX_BODY:
                raise ValueError("Release response too large")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise ValueError("Invalid release response")
    kind = definition["kind"]
    if kind == "github":
        if data.get("draft") is not False or data.get("prerelease") is not False:
            raise ValueError("Non-stable GitHub release")
        return stable(data.get("tag_name"))
    if kind == "pypi":
        info = data.get("info", {})
        if (
            not isinstance(info, dict)
            or normalize(str(info.get("name", ""))) != definition["project"]
        ):
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


def refresh(data, force=False):
    registry = definitions(data)  # Validate completely before changing cache policy.
    started = timezone.now()
    with transaction.atomic():
        InventoryRelease.objects.exclude(source_id__in=registry).update(enabled=False)
        for key, definition in registry.items():
            InventoryRelease.objects.update_or_create(
                source_id=key, defaults={"definition": definition, "enabled": True}
            )
    failures = 0
    with httpx.Client(
        timeout=20,
        follow_redirects=False,
        trust_env=False,
        headers={
            "Accept": "application/json",
            "User-Agent": "Graphyard-inventory-release-check/1",
        },
    ) as client:
        for key, definition in registry.items():
            row = InventoryRelease.objects.get(pk=key)
            if (
                not force
                and not row.error
                and row.checked_at
                and timedelta(0) <= started - row.checked_at < REFRESH_AFTER
            ):
                continue
            try:
                version = fetch(client, definition)
                values = {"version": version, "checked_at": timezone.now(), "error": ""}
            except (httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError):
                failures += 1
                values = {"error": "Release lookup failed"}
            # Failed attempts preserve the last success and never renew its timestamp.
            values["attempted_at"] = started
            InventoryRelease.objects.filter(pk=key).update(**values)
    return failures


def registry():
    result = {}
    for row in InventoryRelease.objects.filter(enabled=True):
        for target in row.definition["targets"]:
            result[target["kind"], target["name"]] = row
    return result


def compare(installed, release, observed_at, now, historical=False):
    result = {
        "installed": installed if isinstance(installed, str) else "Unknown",
        "available": release.version if release and release.version else "Unknown",
        "source_url": url(release.definition) if release else "",
        "checked_at": release.checked_at if release else None,
        "attempted_at": release.attempted_at if release else None,
        "observed_at": observed_at,
        "status": "unknown",
        "reason": "No registered release source",
    }
    if release is None:
        return result
    if release.error:
        result["reason"] = "Latest release lookup failed; previous result retained"
        return result
    if (
        not release.checked_at
        or not timedelta(0) <= now - release.checked_at <= MAX_AGE
    ):
        result["reason"] = "Release information missing or stale"
        return result
    if (
        historical
        or not observed_at
        or not timedelta(0) <= now - observed_at <= MAX_AGE
    ):
        result["reason"] = "Installed-version observation missing or stale"
        return result
    try:
        current, latest = Version(stable(installed)), Version(stable(release.version))
    except (InvalidVersion, ValueError, TypeError):
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


def rows_for_host(host, sources, now):
    """Keep individual package observations traceable to their exact snapshot."""
    from .inventory_applications import applications, source
    from .inventory_sbom import packages, Unavailable

    categories = {
        c.name: c
        for c in host.inventorycategory_set.select_related(
            "latest_attempt", "latest_success"
        )
    }
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
            target = sources.get(("application", name))
            rows.append(
                {
                    "application": name,
                    "component": name,
                    "kind": "application",
                    "snapshot": snapshot,
                    "basis": basis,
                    **compare(
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
                        **compare(
                            package["version"],
                            sources.get(("python", normalize(package["name"]))),
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
                row = compare(
                    version,
                    sources.get(("brew", name)),
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
