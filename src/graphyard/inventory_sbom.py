"""CycloneDX projection of one immutable observation, with no collection or I/O."""

import hashlib
import json
import re
from urllib.parse import quote
from uuid import NAMESPACE_URL, uuid5


EXPORT_VERSION = "1"
EXPORT_ID = f"python-sbom/{EXPORT_VERSION}"


class Unavailable(ValueError):
    """The received evidence cannot support this export."""


def packages(app):
    """Fail closed on ambiguous identities or malformed package declarations."""
    evidence = app.get("items")
    python = evidence.get("python") if isinstance(evidence, dict) else None
    if not isinstance(python, dict) or python.get("status") != "ok":
        raise Unavailable("Successful Python package evidence is required.")
    entries = python.get("items")
    if not isinstance(entries, list):
        raise Unavailable("Python package evidence is malformed.")
    seen = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise Unavailable("Python package evidence is malformed.")
        name, version = entry.get("name"), entry.get("version")
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?", name)
            or not isinstance(version, str)
            or not version.strip()
            or len(version) > 1024
            or any(ord(c) < 32 for c in version)
        ):
            raise Unavailable("A package name or version is missing or malformed.")
        normalized = re.sub(r"[-_.]+", "-", name).lower()
        if normalized in seen:
            raise Unavailable("Duplicate normalized package names are ambiguous.")
        seen.add(normalized)
        for key in ("requires",):
            if key in entry and (
                not isinstance(entry[key], list)
                or any(not isinstance(value, str) for value in entry[key])
            ):
                raise Unavailable("Declared package requirements are malformed.")
    return entries


def eligible(app):
    if not isinstance(app.get("id"), str) or not app["id"]:
        return False
    try:
        packages(app)
    except Unavailable:
        return False
    return True


def properties(values):
    return [
        {"name": "graphyard:" + name, "value": value} for name, value in values.items()
    ]


def build(snapshot, application_id):
    report = snapshot.report
    if not isinstance(report, dict) or any(
        not isinstance(report.get(key), str) or not report[key]
        for key in ("host", "observed_at", "collector")
    ):
        raise Unavailable("Report provenance is missing or malformed.")
    categories = report.get("categories")
    category = categories.get("applications") if isinstance(categories, dict) else None
    if not isinstance(category, dict) or category.get("status") not in (
        "ok",
        "error",
        "unsupported",
    ):
        raise Unavailable("Application category evidence is missing or malformed.")
    entries = (
        category.get("items")
        if category["status"] == "ok"
        else category.get("partial_items", [])
    )
    if not isinstance(entries, list) or any(
        not isinstance(entry, dict) for entry in entries
    ):
        raise Unavailable("Application entries are malformed.")
    matches = [entry for entry in entries if entry.get("id") == application_id]
    if not application_id or len(matches) != 1:
        raise Unavailable(
            "Application evidence is missing or ambiguous in this snapshot."
        )
    app = matches[0]
    installed = packages(app)
    root_ref = "application"
    components = []
    for package in sorted(installed, key=lambda item: item["name"].casefold()):
        normalized = re.sub(r"[-_.]+", "-", package["name"]).lower()
        purl = (
            "pkg:pypi/"
            + quote(normalized, safe="")
            + "@"
            + quote(package["version"], safe="")
        )
        values = {
            "installed-metadata": json.dumps(
                package, sort_keys=True, ensure_ascii=False
            ),
            "requirements-assessment": "declared-only; optional activation and resolution not evaluated",
        }
        components.append(
            {
                "type": "library",
                "bom-ref": purl,
                "name": package["name"],
                "version": package["version"],
                "purl": purl,
                "properties": properties(values),
            }
        )
    # Serial identity is stable for this export format and exact source evidence.
    identity = json.dumps(
        [
            EXPORT_ID,
            snapshot.report["host"],
            str(snapshot.snapshot_id),
            snapshot.digest,
            application_id,
        ]
    )
    source = app["items"]
    return {
        "$schema": "http://cyclonedx.org/schema/bom-1.6.schema.json",
        "bomFormat": "CycloneDX",
        "specVersion": "1.6",
        "serialNumber": "urn:uuid:" + str(uuid5(NAMESPACE_URL, identity)),
        "version": 1,
        "metadata": {
            "tools": {
                "components": [
                    {
                        "type": "application",
                        "name": "graphyard-python-sbom",
                        "version": EXPORT_VERSION,
                    }
                ]
            },
            "component": {
                "type": "application",
                "bom-ref": root_ref,
                "name": application_id,
            },
            "properties": properties(
                {
                    "exporter": EXPORT_ID,
                    "host": snapshot.report["host"],
                    "snapshot-id": str(snapshot.snapshot_id),
                    "source-report-sha256": snapshot.digest,
                    "observed-at": snapshot.report["observed_at"],
                    "collector": snapshot.report["collector"],
                    "application-category-status": category["status"],
                    "application-probe-status": str(app.get("status", "unknown")),
                    "scope": "installed Python distribution metadata only",
                    "completeness": "incomplete: OS, native, frontend and container contents not assessed; artifact hashes and resolved dependency graph unavailable",
                    "source-evidence": json.dumps(
                        {
                            key: value
                            for key, value in source.items()
                            if key != "python"
                        },
                        sort_keys=True,
                        ensure_ascii=False,
                    ),
                }
            ),
        },
        "components": components,
        "compositions": [{"aggregate": "incomplete", "assemblies": [root_ref]}],
    }


def filename(application_id, snapshot_id):
    # No producer/request strings reach a response header directly.
    suffix = hashlib.sha256(application_id.encode()).hexdigest()[:16]
    return f"python-sbom-{snapshot_id}-{suffix}.cdx.json"
