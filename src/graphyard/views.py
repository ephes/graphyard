from __future__ import annotations

import json
from datetime import UTC, datetime
import logging

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.db import connection
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, render
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from .auth import authenticate_ingest_token
from .influx import InfluxConfigurationError, MetricPoint, check_health as influx_health
from .influx import normalize_metric_point, write_points
from .models import (
    ConditionDefinition,
    HostRegistry,
    PipelineHeartbeat,
    ServiceRegistry,
    StatusLevel,
)
from .services import record_heartbeat, touch_registry_from_points

logger = logging.getLogger(__name__)


def _json_error(message: str, *, status: int) -> JsonResponse:
    return JsonResponse({"error": message}, status=status)


def _parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _parse_metrics_payload(payload: object) -> list[MetricPoint]:
    if isinstance(payload, dict):
        payload = payload.get("metrics")

    if not isinstance(payload, list):
        raise ValueError("Body must be a JSON list or object with a metrics list")

    points: list[MetricPoint] = []
    for idx, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError(f"Metric at index {idx} must be an object")

        ts_raw = item.get("ts")
        host = item.get("host")
        metric = item.get("metric")
        value = item.get("value")

        if not isinstance(ts_raw, str):
            raise ValueError(f"Metric at index {idx} has invalid ts")
        if host is not None and not isinstance(host, str):
            raise ValueError(f"Metric at index {idx} has invalid host")
        if not isinstance(metric, str) or not metric:
            raise ValueError(f"Metric at index {idx} has invalid metric")

        if value is None:
            raise ValueError(f"Metric at index {idx} has invalid value")
        try:
            parsed_value = float(str(value))
        except (TypeError, ValueError) as err:
            raise ValueError(f"Metric at index {idx} has invalid value") from err

        service = item.get("service")
        if service is not None and not isinstance(service, str):
            raise ValueError(f"Metric at index {idx} has invalid service")

        raw_tags = item.get("tags")
        if raw_tags is None:
            tags: dict[str, str] = {}
        elif isinstance(raw_tags, dict):
            tags = {str(key): str(tag_value) for key, tag_value in raw_tags.items()}
        else:
            raise ValueError(f"Metric at index {idx} has invalid tags")

        host_raw = host.strip() if isinstance(host, str) else ""
        service_raw = service.strip() if isinstance(service, str) else ""

        subject_type = item.get("subject_type")
        if subject_type is None:
            if host_raw:
                subject_type = "host"
            else:
                raise ValueError(
                    f"Metric at index {idx} missing subject_type and legacy host fallback"
                )
        if not isinstance(subject_type, str):
            raise ValueError(f"Metric at index {idx} has invalid subject_type")

        subject_id = item.get("subject_id")
        if subject_id is None:
            if host_raw:
                subject_id = host_raw
            else:
                raise ValueError(
                    f"Metric at index {idx} missing subject_id and legacy host fallback"
                )
        if not isinstance(subject_id, str):
            raise ValueError(f"Metric at index {idx} has invalid subject_id")

        source_system = item.get("source_system")
        if source_system is None:
            source_system = service_raw or "legacy"
        if not isinstance(source_system, str):
            raise ValueError(f"Metric at index {idx} has invalid source_system")

        source_instance = item.get("source_instance", "default")
        if source_instance is None:
            source_instance = "default"
        if not isinstance(source_instance, str):
            raise ValueError(f"Metric at index {idx} has invalid source_instance")

        source_entity_id = item.get("source_entity_id")
        if source_entity_id is not None and not isinstance(source_entity_id, str):
            raise ValueError(f"Metric at index {idx} has invalid source_entity_id")

        collector_service = item.get("collector_service")
        if collector_service is None:
            collector_service = service_raw or "graphyard-ingest"
        if not isinstance(collector_service, str):
            raise ValueError(f"Metric at index {idx} has invalid collector_service")

        collector_host = item.get("collector_host")
        if collector_host is None:
            collector_host = host_raw or str(subject_id)
        if not isinstance(collector_host, str):
            raise ValueError(f"Metric at index {idx} has invalid collector_host")

        try:
            normalized = normalize_metric_point(
                MetricPoint(
                    ts=_parse_timestamp(ts_raw),
                    metric=metric,
                    value=parsed_value,
                    subject_type=subject_type,
                    subject_id=subject_id,
                    source_system=source_system,
                    source_instance=source_instance,
                    source_entity_id=source_entity_id,
                    collector_service=collector_service,
                    collector_host=collector_host,
                    host=host_raw or None,
                    service=service_raw or None,
                    tags=tags,
                )
            )
        except ValueError as err:
            logger.warning(
                "Rejected metric payload at index=%s metric=%s: %s",
                idx,
                metric,
                err,
            )
            raise ValueError(f"Metric at index {idx} failed validation: {err}") from err

        points.append(normalized)

    return points


def _serialize_condition(condition: ConditionDefinition) -> dict[str, object]:
    return {
        "id": condition.id,
        "name": condition.name,
        "status": condition.status,
        "last_evaluated": (
            condition.last_evaluated.isoformat() if condition.last_evaluated else None
        ),
        "message": condition.message,
    }


@csrf_exempt
@require_POST
def metrics_ingest(request: HttpRequest) -> JsonResponse:
    token = authenticate_ingest_token(request)
    if token is None:
        return _json_error("Missing or invalid bearer token", status=401)

    try:
        payload = json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError:
        record_heartbeat(
            "metric_ingest",
            status=StatusLevel.WARNING,
            last_error="Invalid JSON payload",
            details={},
        )
        return _json_error("Invalid JSON payload", status=400)

    try:
        points = _parse_metrics_payload(payload)
    except ValueError as err:
        record_heartbeat(
            "metric_ingest",
            status=StatusLevel.WARNING,
            last_error=str(err),
            details={},
        )
        return _json_error(str(err), status=400)

    try:
        written = write_points(points)
    except InfluxConfigurationError as err:
        record_heartbeat(
            "metric_ingest",
            status=StatusLevel.CRITICAL,
            last_error=str(err),
            details={},
        )
        return _json_error("InfluxDB is not configured", status=503)
    except Exception as err:  # noqa: BLE001
        record_heartbeat(
            "metric_ingest",
            status=StatusLevel.CRITICAL,
            last_error=str(err),
            details={},
        )
        return _json_error("Failed to persist metrics", status=503)

    touch_registry_from_points(points)
    record_heartbeat(
        "metric_ingest",
        status=StatusLevel.OK,
        details={"ingested": written, "token": token.name},
        success=True,
    )
    return JsonResponse({"status": "accepted", "ingested": written}, status=202)


@require_GET
def conditions_list(request: HttpRequest) -> JsonResponse:
    del request
    conditions = ConditionDefinition.objects.filter(enabled=True).order_by("id")
    return JsonResponse(
        {"conditions": [_serialize_condition(item) for item in conditions]}
    )


@require_GET
def condition_detail(request: HttpRequest, condition_id: int) -> JsonResponse:
    del request
    condition = get_object_or_404(ConditionDefinition, id=condition_id, enabled=True)

    payload = _serialize_condition(condition)
    payload["config"] = {
        "metric_name": condition.metric_name,
        "host_filter": condition.host_filter,
        "subject_type_filter": condition.subject_type_filter,
        "subject_id_filter": condition.subject_id_filter,
        "service_filter": condition.service_filter,
        "tags_filter": condition.tags_filter,
        "operator": condition.operator,
        "warning_threshold": condition.warning_threshold,
        "critical_threshold": condition.critical_threshold,
        "window_minutes": condition.window_minutes,
        "breach_minutes": condition.breach_minutes,
    }
    payload["last_value"] = condition.last_value

    return JsonResponse(payload)


def _component_status_rank(status: str) -> int:
    if status == StatusLevel.CRITICAL:
        return 3
    if status == StatusLevel.WARNING:
        return 2
    return 1


def _heartbeat_component(name: str, now: datetime) -> dict[str, object]:
    heartbeat = PipelineHeartbeat.objects.filter(name=name).first()
    if heartbeat is None:
        return {
            "status": StatusLevel.WARNING,
            "message": "No heartbeat yet",
            "last_success": None,
            "age_seconds": None,
            "details": {},
        }

    if heartbeat.last_success is None:
        return {
            "status": heartbeat.status,
            "message": heartbeat.last_error or "No successful run yet",
            "last_success": None,
            "age_seconds": None,
            "details": heartbeat.details,
        }

    age_seconds = int((now - heartbeat.last_success).total_seconds())
    status = heartbeat.status
    if age_seconds >= settings.HEARTBEAT_CRITICAL_SECONDS:
        status = StatusLevel.CRITICAL
    elif age_seconds >= settings.HEARTBEAT_WARNING_SECONDS:
        status = StatusLevel.WARNING

    return {
        "status": status,
        "message": heartbeat.last_error,
        "last_success": heartbeat.last_success.isoformat(),
        "age_seconds": age_seconds,
        "details": heartbeat.details,
    }


@require_GET
def health(request: HttpRequest) -> JsonResponse:
    del request
    now = timezone.now()

    database_component: dict[str, object]
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        database_component = {"status": StatusLevel.OK, "message": "ok"}
    except Exception as err:  # noqa: BLE001
        database_component = {"status": StatusLevel.CRITICAL, "message": str(err)}

    influx_component: dict[str, object]
    try:
        influx_status = influx_health()
        mapped_status = (
            StatusLevel.OK
            if influx_status.get("status", "").lower() == "pass"
            else StatusLevel.WARNING
        )
        influx_component = {
            "status": mapped_status,
            "message": influx_status.get("message", ""),
            "name": influx_status.get("name", ""),
        }
    except Exception as err:  # noqa: BLE001
        influx_component = {"status": StatusLevel.CRITICAL, "message": str(err)}

    pipelines = {
        "metric_ingest": _heartbeat_component("metric_ingest", now),
        "metric_collectors": _heartbeat_component("metric_collectors", now),
        "condition_evaluator": _heartbeat_component("condition_evaluator", now),
    }

    status_values: list[str] = [
        str(database_component["status"]),
        str(influx_component["status"]),
        str(pipelines["metric_ingest"]["status"]),
        str(pipelines["metric_collectors"]["status"]),
        str(pipelines["condition_evaluator"]["status"]),
    ]
    overall = max(status_values, key=_component_status_rank)

    condition_counts = {
        StatusLevel.OK: ConditionDefinition.objects.filter(
            enabled=True, status=StatusLevel.OK
        ).count(),
        StatusLevel.WARNING: ConditionDefinition.objects.filter(
            enabled=True, status=StatusLevel.WARNING
        ).count(),
        StatusLevel.CRITICAL: ConditionDefinition.objects.filter(
            enabled=True, status=StatusLevel.CRITICAL
        ).count(),
    }

    return JsonResponse(
        {
            "status": overall,
            "checked_at": now.isoformat(),
            "components": {
                "database": database_component,
                "influxdb": influx_component,
                "pipelines": pipelines,
            },
            "conditions": {
                "total": sum(condition_counts.values()),
                "ok": condition_counts[StatusLevel.OK],
                "warning": condition_counts[StatusLevel.WARNING],
                "critical": condition_counts[StatusLevel.CRITICAL],
            },
        }
    )


@login_required
def host_service_index(request: HttpRequest) -> HttpResponse:
    hosts = (
        HostRegistry.objects.filter(enabled=True, services__enabled=True)
        .distinct()
        .order_by("host_id")
    )
    services = ServiceRegistry.objects.filter(enabled=True).select_related("host")

    context = {
        "hosts": hosts,
        "services": services,
        "grafana_base_url": settings.GRAFANA_BASE_URL,
    }
    return render(request, "graphyard/index.html", context)
