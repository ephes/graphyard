"""Push-only inventory boundary. Never contacts producers or executes commands."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import timedelta
from uuid import UUID

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.core.exceptions import RequestDataTooBig
from django.db import OperationalError, transaction
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from .models import (
    InventoryCategory,
    InventoryCredential,
    InventoryHost,
    InventorySnapshot,
)

logger = logging.getLogger(__name__)
CATEGORIES = ("packages", "units", "containers", "applications")
MAX_BYTES = 8 * 1024 * 1024


def canonical(report):
    return json.dumps(
        report,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def authenticate(request):
    value = request.headers.get("Authorization", "")
    if not value.startswith("Bearer "):
        return None
    try:
        identifier, secret = value[7:].split(".", 1)
        if (
            not identifier.isascii()
            or not identifier.isdigit()
            or len(identifier) > 18
            or not 32 <= len(secret) <= 128
        ):
            return None
        credential = InventoryCredential.objects.select_related("host__host").get(
            pk=int(identifier), enabled=True, host__host__enabled=True
        )
    except (ValueError, InventoryCredential.DoesNotExist):
        return None
    if not hmac.compare_digest(
        credential.digest, hashlib.sha256(secret.encode()).hexdigest()
    ):
        return None
    return credential


def validate(report, now):
    if not isinstance(report, dict) or set(report) != {
        "schema_version",
        "host",
        "snapshot_id",
        "observed_at",
        "collector",
        "categories",
        "gaps",
    }:
        raise ValueError("invalid envelope fields")
    if type(report["schema_version"]) is not int or report["schema_version"] != 1:
        raise ValueError("unsupported schema")
    for name, length in [
        ("host", 128),
        ("snapshot_id", 36),
        ("observed_at", 64),
        ("collector", 128),
    ]:
        if not isinstance(report[name], str) or not 1 <= len(report[name]) <= length:
            raise ValueError("invalid identity")
    UUID(report["snapshot_id"])
    observed = parse_datetime(report["observed_at"])
    if (
        observed is None
        or timezone.is_naive(observed)
        or observed > now + timedelta(minutes=5)
    ):
        raise ValueError("timestamp must have timezone and not be in the future")
    categories = report["categories"]
    if not isinstance(categories, dict) or set(categories) != set(CATEGORIES):
        raise ValueError("all categories must be reported explicitly")
    gaps = report["gaps"]
    if (
        not isinstance(gaps, list)
        or len(gaps) > 100
        or any(not isinstance(gap, str) or len(gap) > 512 for gap in gaps)
    ):
        raise ValueError("invalid coverage gaps")
    for name, category in categories.items():
        if (
            not isinstance(category, dict)
            or set(category) - {"status", "items", "error", "partial_items"}
            or not {"status", "items"} <= set(category)
        ):
            raise ValueError("invalid category")
        if category["status"] not in ("ok", "error", "unsupported"):
            raise ValueError("invalid category status")
        if (
            not isinstance(category["items"], list)
            or len(category["items"]) > 100000
            or any(not isinstance(item, dict) for item in category["items"])
        ):
            raise ValueError("invalid category items")
        if category["status"] != "ok" and category["items"]:
            raise ValueError("failed category cannot contain successful items")
        if "partial_items" in category:
            partial = category["partial_items"]
            if (
                name != "applications"
                or category["status"] != "error"
                or not isinstance(partial, list)
                or len(partial) > 100000
                or any(not isinstance(item, dict) for item in partial)
            ):
                raise ValueError("invalid partial application evidence")
        if "error" in category and (
            not isinstance(category["error"], str) or len(category["error"]) > 512
        ):
            raise ValueError("invalid error")
    # Bound nesting and reject non-JSON numbers before canonical hashing/storage.
    stack = [(report, 0)]
    while stack:
        item, depth = stack.pop()
        if depth > 16:
            raise ValueError("report nesting limit")
        if isinstance(item, dict):
            stack.extend((value, depth + 1) for value in item.values())
        elif isinstance(item, list):
            stack.extend((value, depth + 1) for value in item)
    return observed


@transaction.non_atomic_requests
@csrf_exempt
@require_POST
def ingest(request):
    try:
        credential = authenticate(request)
        if credential is None:
            return JsonResponse({"error": "invalid inventory credential"}, status=401)
        if request.content_type != "application/json":
            return JsonResponse({"error": "application/json required"}, status=415)
        # Limit this endpoint without lifting Django's global upload limit.
        length = request.META.get("CONTENT_LENGTH", "")
        if length and (not length.isdigit() or int(length) > MAX_BYTES):
            return JsonResponse({"error": "report too large"}, status=413)
        body = request.read(MAX_BYTES + 1)
        if len(body) > MAX_BYTES:
            return JsonResponse({"error": "report too large"}, status=413)
        report = json.loads(body)
        # Vector JSON encoding can frame one event as a one-element array.
        if isinstance(report, list) and len(report) == 1:
            report = report[0]
        observed = validate(report, timezone.now())
        if report["host"] != credential.host.host.host_id:
            return JsonResponse({"error": "credential host mismatch"}, status=403)
        digest = hashlib.sha256(canonical(report)).hexdigest()
        with transaction.atomic(durable=True):
            host = InventoryHost.objects.select_for_update().get(pk=credential.host_id)
            # Recheck revocation inside the write transaction.
            if not InventoryCredential.objects.filter(
                pk=credential.pk, enabled=True, host__host__enabled=True
            ).exists():
                return JsonResponse({"error": "credential revoked"}, status=401)
            previous = (
                InventorySnapshot.objects.only("digest")
                .filter(host=host, snapshot_id=report["snapshot_id"])
                .first()
            )
            if previous:
                if not hmac.compare_digest(previous.digest, digest):
                    return JsonResponse(
                        {"error": "snapshot identity reused with different content"},
                        status=409,
                    )
                duplicate = True
            else:
                snapshot = InventorySnapshot.objects.create(
                    host=host,
                    snapshot_id=report["snapshot_id"],
                    observed_at=observed,
                    digest=digest,
                    report=report,
                )
                observed_times = dict(
                    InventorySnapshot.objects.filter(host=host).values_list(
                        "pk", "observed_at"
                    )
                )
                for name, value in report["categories"].items():
                    category, _ = InventoryCategory.objects.get_or_create(
                        host=host, name=name
                    )
                    if (
                        category.latest_attempt_id is None
                        or observed > observed_times[category.latest_attempt_id]
                    ):
                        category.latest_attempt = snapshot
                    if value["status"] == "ok" and (
                        category.latest_success_id is None
                        or observed > observed_times[category.latest_success_id]
                    ):
                        category.latest_success = snapshot
                    category.save()
                # Retain recent arrivals plus every referenced last-known category.
                keep = list(
                    InventorySnapshot.objects.filter(host=host)
                    .order_by("-received_at")
                    .values_list("pk", flat=True)[:32]
                )
                for category in InventoryCategory.objects.filter(host=host):
                    keep.extend(
                        [category.latest_attempt_id, category.latest_success_id]
                    )
                InventorySnapshot.objects.filter(host=host).exclude(
                    pk__in=[pk for pk in keep if pk]
                ).delete()
                duplicate = False
        logger.info(
            "inventory_received host=%s duplicate=%s", host.host.host_id, duplicate
        )
        return JsonResponse(
            {
                "status": "stored",
                "snapshot_id": report["snapshot_id"],
                "duplicate": duplicate,
            }
        )
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError):
        return JsonResponse({"error": "invalid inventory report"}, status=400)
    except RequestDataTooBig:
        return JsonResponse({"error": "report too large"}, status=413)
    except OperationalError:
        logger.warning("inventory storage unavailable")
        return JsonResponse({"error": "inventory storage unavailable"}, status=503)


def host_state(host):
    """Decode each distinct referenced report once, including on the detail page."""
    latest_id = (
        InventorySnapshot.objects.filter(host=host)
        .order_by("-observed_at", "-received_at")
        .values_list("pk", flat=True)
        .first()
    )
    categories = list(host.inventorycategory_set.order_by("name"))
    identifiers = {latest_id} if latest_id else set()
    for category in categories:
        identifiers.update(
            pk for pk in (category.latest_attempt_id, category.latest_success_id) if pk
        )
    snapshots = InventorySnapshot.objects.filter(host=host).in_bulk(identifiers)
    for category in categories:
        category.latest_attempt = snapshots.get(category.latest_attempt_id)
        category.latest_success = snapshots.get(category.latest_success_id)
    return snapshots.get(latest_id), categories


def host_summary(host, now, state=None):
    latest, observations = host_state(host) if state is None else state
    categories = {}
    for category in observations:
        attempt, success = category.latest_attempt, category.latest_success
        categories[category.name] = {
            "status": attempt.report["categories"][category.name]["status"]
            if attempt
            else "missing",
            "observed_at": attempt.observed_at.isoformat() if attempt else None,
            "last_success_at": success.observed_at.isoformat() if success else None,
            "count": len(success.report["categories"][category.name]["items"])
            if success
            else None,
        }
    age = max(0, (now - latest.observed_at).total_seconds()) if latest else None
    stale = age is None or age > host.warning_after_seconds
    incomplete = len(categories) != len(CATEGORIES) or any(
        value["status"] == "error" for value in categories.values()
    )
    return {
        "host": host.host.host_id,
        "observed_at": latest.observed_at.isoformat() if latest else None,
        "received_at": latest.received_at.isoformat() if latest else None,
        "stale": stale,
        "alert": incomplete or (stale and host.alert_when_stale),
        "categories": categories,
        "gaps": latest.report["gaps"] if latest else ["never_reported"],
    }


def summaries():
    now = timezone.now()
    return [
        host_summary(host, now)
        for host in InventoryHost.objects.filter(host__enabled=True)
        .select_related("host")
        .order_by("host__host_id")
    ]


@require_GET
def status(request):
    configured = getattr(settings, "GRAPHYARD_INVENTORY_MONITOR_TOKEN", "")
    supplied = request.headers.get("Authorization", "")
    machine = bool(configured) and hmac.compare_digest(
        supplied.encode(), ("Bearer " + configured).encode()
    )
    if not request.user.is_authenticated and not machine:
        return JsonResponse({"error": "inventory read access required"}, status=401)
    hosts = summaries()
    return JsonResponse(
        {
            "summary": {
                "total": len(hosts),
                "attention": sum(host["alert"] for host in hosts),
            },
            "hosts": {host["host"]: host for host in hosts},
        }
    )


@login_required
@require_GET
def index(request):
    return render(request, "graphyard/inventory.html", {"hosts": summaries()})


def partial_application_preview(state):
    """Project untrusted nested evidence into a bounded, text-only page preview."""
    for category in state[1]:
        if category.name != "applications":
            continue
        report = category.latest_attempt.report["categories"]["applications"]
        partial = report.get("partial_items", [])
        if not partial:
            return None

        def text(value, limit, default=""):
            if not isinstance(value, str):
                return default
            return value[:limit] + ("…" if len(value) > limit else "")

        entries = []
        for app in partial[:50]:
            evidence = app.get("items")
            evidence = evidence if isinstance(evidence, dict) else {}
            gaps = evidence.get("coverage")
            gaps = gaps if isinstance(gaps, list) else []
            entries.append(
                {
                    "id": text(app.get("id"), 200, "Application"),
                    "status": text(app.get("status"), 40, "unknown"),
                    "version": text(evidence.get("installed_version"), 200),
                    "error": text(app.get("error"), 512),
                    "coverage": [
                        text(gap, 160, "unstructured gap; see full report")
                        for gap in gaps[:10]
                    ],
                }
            )
        return {
            "count": len(partial),
            "entries": entries,
            "snapshot": category.latest_attempt.snapshot_id,
        }
    return None


@login_required
@require_GET
def detail(request, host_id):
    host = get_object_or_404(InventoryHost, host__host_id=host_id)
    state = host_state(host)
    return render(
        request,
        "graphyard/inventory_detail.html",
        {
            "host": host_summary(host, timezone.now(), state),
            "categories": state[1],
            "latest": state[0],
            "partial_applications": partial_application_preview(state),
        },
    )


@login_required
@require_GET
def application_list(request, host_id):
    from . import inventory_applications as presentation

    host = get_object_or_404(InventoryHost, host__host_id=host_id)
    state = host_state(host)
    category = next((entry for entry in state[1] if entry.name == "applications"), None)
    snapshot, basis, entries = presentation.source(category)
    latest_category = (
        category.latest_attempt.report["categories"]["applications"]
        if category and category.latest_attempt
        else {}
    )
    query = request.GET.get("q", "")[:100].strip()
    rows = list(presentation.applications(entries))
    total = len(rows)
    if query:
        rows = [
            row
            for row in rows
            if isinstance(row.get("id"), str)
            and query.casefold() in row["id"].casefold()
        ]
    page = Paginator(rows, 20).get_page(request.GET.get("page"))
    now = timezone.now()
    return render(
        request,
        "graphyard/inventory_applications.html",
        {
            "host": host_summary(host, now, state),
            "snapshot": snapshot,
            "basis": basis,
            "observation_stale": snapshot is None
            or (now - snapshot.observed_at).total_seconds()
            > host.warning_after_seconds,
            "latest_attempt": category.latest_attempt if category else None,
            "attempt_status": presentation.text(
                latest_category.get("status"), 40, "missing"
            ),
            "attempt_error": presentation.text(latest_category.get("error"), 512, ""),
            "rows": [presentation.project(row) for row in page.object_list],
            "page": page,
            "total": total,
            "query": query,
        },
    )


@login_required
@require_GET
def download(request, host_id, snapshot_id):
    snapshot = get_object_or_404(
        InventorySnapshot, host__host__host_id=host_id, snapshot_id=snapshot_id
    )
    response = JsonResponse(snapshot.report)
    response["Content-Disposition"] = (
        f'attachment; filename="inventory-{snapshot.snapshot_id}.json"'
    )
    return response
