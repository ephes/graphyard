"""Bounded presentation of received application evidence; no collection or lookups."""

import math
import time
from datetime import UTC, datetime

HEALTH_MAX_AGE_SECONDS = 1800


def text(value, limit=200, default="Unknown"):
    if not isinstance(value, str) or not value:
        return default
    return value[:limit] + ("…" if len(value) > limit else "")


def mapping(value):
    return value if isinstance(value, dict) else {}


def objects(value):
    return (
        [item for item in value if isinstance(item, dict)]
        if isinstance(value, list)
        else []
    )


def source(category):
    """Keep failed attempts distinct from historical successful evidence."""
    if category is None or category.latest_attempt is None:
        return None, "missing", []
    attempt = category.latest_attempt
    observation = attempt.report["categories"]["applications"]
    if observation["status"] == "ok":
        return attempt, "successful", observation["items"]
    if observation.get("partial_items"):
        return attempt, "partial", observation["partial_items"]
    if category.latest_success:
        success = category.latest_success
        return (
            success,
            "historical",
            success.report["categories"]["applications"]["items"],
        )
    return None, "missing", []


def applications(entries):
    """Flatten the collector's explicit macOS bundle wrapper, retaining provenance."""
    for app in entries:
        if app.get("id") == "macos-application-bundles" and isinstance(
            app.get("items"), list
        ):
            bundles = objects(app["items"])
            malformed = len(bundles) != len(app["items"])
            raw_error = app.get("error")
            invalid_error = raw_error is not None and not isinstance(raw_error, str)
            wrapper_error = text(raw_error, 256, "")
            if invalid_error:
                wrapper_error = "Malformed bundle error; see full report"
            if malformed:
                wrapper_error += (
                    "; " if wrapper_error else ""
                ) + "Malformed bundle evidence; see full report"
            if app.get("status") != "ok" or wrapper_error:
                yield {
                    "id": "macos-application-bundles",
                    "kind": "macOS bundle scan",
                    "status": app.get("status"),
                    "error": wrapper_error,
                    "items": {},
                }
            for bundle in bundles:
                yield {
                    "id": bundle.get("name"),
                    "status": app.get("status"),
                    "error": text(raw_error, 256, ""),
                    "items": {"installed_version": bundle.get("version")},
                    "bundle_path": bundle.get("path"),
                    "bundle_build": bundle.get("build"),
                    "kind": "macOS application bundle",
                }
        else:
            yield app


def health_projection(evidence, host_id, observed_at):
    """Monitoring results retain their own source time and restricted meaning."""
    if "software_health" not in evidence:
        return None
    unavailable = {"available": False}
    observation = mapping(evidence["software_health"])
    data = mapping(observation.get("items"))
    stamp = data.get("observed_at_epoch")
    if (
        not isinstance(host_id, str)
        or not host_id
        or observation.get("status") != "ok"
        or type(data.get("schema_version")) is not int
        or data["schema_version"] != 1
        or data.get("source") != "software-live/2"
        or data.get("host") != host_id
        or not isinstance(observed_at, datetime)
        or type(stamp) not in (int, float)
        or not 0 < stamp <= 253402300799
        or not math.isfinite(stamp)
        or not 0 <= observed_at.timestamp() - stamp <= HEALTH_MAX_AGE_SECONDS
        or type(data.get("max_age_seconds")) is not int
        or data["max_age_seconds"] != HEALTH_MAX_AGE_SECONDS
    ):
        return unavailable
    checks = mapping(data.get("checks"))
    if set(checks) != {"os", "postgresql", "traefik"}:
        return unavailable
    rows = []
    for key, label in (
        ("os", "Operating system support"),
        ("postgresql", "PostgreSQL"),
        ("traefik", "Traefik"),
    ):
        item = mapping(checks[key])
        if item.get("status") not in ("ok", "warning", "unknown") or any(
            type(item.get(k)) is not bool
            for k in ("observed", "expected", "issues_truncated")
        ):
            return unavailable
        if any(
            item.get(field) is not None
            and (not isinstance(item[field], str) or len(item[field]) > 160)
            for field in ("installed_version", "running_version", "upstream_version")
        ):
            return unavailable
        issues = item.get("issues")
        if (
            not isinstance(issues, list)
            or len(issues) > 20
            or any(not isinstance(i, str) for i in issues)
        ):
            return unavailable
        rows.append(
            {
                "name": label,
                "status": item["status"] if item["observed"] else "unknown",
                "expected": item["expected"],
                "installed": text(item.get("installed_version")),
                "running": text(item.get("running_version"), default="Not assessed"),
                "upstream": text(item.get("upstream_version"), default="Not assessed"),
                "issues": [text(i, 160) for i in issues if i],
                "truncated": item["issues_truncated"],
            }
        )
    apt = mapping(data.get("apt"))
    count = apt.get("pending_security_count")
    if (
        apt.get("status") not in ("ok", "unknown")
        or type(apt.get("indexes_fresh")) is not bool
        or (count is not None and (type(count) is not int or count < 0))
        or (apt.get("status") == "ok" and count is None)
    ):
        return unavailable
    return {
        "available": True,
        "observed_at": datetime.fromtimestamp(stamp, UTC),
        "historical": not 0 <= time.time() - stamp <= HEALTH_MAX_AGE_SECONDS,
        "checks": rows,
        "security_count": count
        if apt["status"] == "ok" and apt["indexes_fresh"]
        else None,
        "indexes_fresh": apt["indexes_fresh"],
    }


def project(app, host_id=None, observed_at=None):
    """Never render arbitrary nested report values or infer unsupported freshness."""
    from .inventory_sbom import eligible

    evidence = mapping(app.get("items"))
    raw_coverage = evidence.get("coverage")
    coverage_invalid = "coverage" in evidence and (
        not isinstance(raw_coverage, list)
        or any(not isinstance(item, str) for item in raw_coverage)
    )
    coverage = raw_coverage if isinstance(raw_coverage, list) else []
    python = mapping(evidence.get("python"))
    python_status = text(python.get("status"), 40, "Not assessed")
    raw_packages = python.get("items")
    valid_packages = isinstance(raw_packages, list) and all(
        isinstance(item, dict) for item in raw_packages
    )
    if python_status == "ok" and not valid_packages:
        python_status = "Invalid evidence"
    packages = raw_packages if python_status == "ok" and valid_packages else []
    dependencies = []
    for package in packages[:10]:
        requires = package.get("requires")
        valid_requires = isinstance(requires, list) and all(
            isinstance(item, str) for item in requires
        )
        requires = requires if valid_requires else []
        dependencies.append(
            {
                "name": text(package.get("name")),
                "version": text(package.get("version")),
                "requires": [text(value, 160) for value in requires[:5]],
                "requires_count": len(requires) if valid_requires else None,
            }
        )
    raw_candidates = evidence.get("package_candidates")
    candidates_invalid = "package_candidates" in evidence and (
        not isinstance(raw_candidates, list)
        or any(not isinstance(item, dict) for item in raw_candidates)
    )
    candidates = raw_candidates if isinstance(raw_candidates, list) else []
    updates = []
    for raw_candidate in candidates[:10]:
        candidate = mapping(raw_candidate)
        valid = all(
            isinstance(candidate.get(key), str) and candidate[key]
            for key in ("installed", "candidate")
        )
        flag = candidate.get("update_available")
        verdict = "Unknown"
        if valid and isinstance(flag, bool):
            verdict = "Update available" if flag else "No newer cached candidate"
        updates.append(
            {
                "name": text(candidate.get("name")),
                "installed": text(candidate.get("installed")),
                "candidate": text(candidate.get("candidate")),
                "verdict": verdict,
            }
        )
    git = mapping(evidence.get("git"))
    checkout = mapping(git.get("items")) if git.get("status") == "ok" else {}
    dirty = checkout.get("dirty")
    return {
        "health": health_projection(evidence, host_id, observed_at),
        "sbom_id": app["id"] if eligible(app) else None,
        "name": text(app.get("id"), default="Unnamed application"),
        "kind": text(app.get("kind"), default="Registered application probe"),
        "probe_status": text(app.get("status"), 40),
        "installed": text(evidence.get("installed_version")),
        "running": text(evidence.get("running_version"), default="Not verified"),
        "presence": text(evidence.get("presence")),
        "runtime": text(evidence.get("runtime")),
        "error": text(app.get("error"), 512, ""),
        "coverage": [text(value, 160) for value in coverage[:10]],
        "coverage_count": len(coverage),
        "coverage_invalid": coverage_invalid,
        "python_status": python_status,
        "package_count": len(packages),
        "packages": dependencies,
        "updates": updates,
        "candidate_count": len(candidates),
        "candidates_invalid": candidates_invalid,
        "commit": text(checkout.get("commit"), 64, "Not assessed"),
        "dirty": "Modified"
        if dirty is True
        else "Clean at observation"
        if dirty is False
        else "Unknown",
        "bundle_path": text(app.get("bundle_path"), 300, ""),
        "bundle_build": text(app.get("bundle_build"), 100, ""),
    }
