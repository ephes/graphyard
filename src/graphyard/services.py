from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import re
from typing import Callable

import httpx
from django.conf import settings
from django.utils import timezone

from . import influx
from .models import (
    ComparisonOperator,
    ConditionDefinition,
    HostRegistry,
    MetricCollectionSpec,
    MetricCollectionSpecType,
    PipelineHeartbeat,
    ServiceRegistry,
    StatusLevel,
)


@dataclass(frozen=True)
class ConditionEvaluation:
    status: str
    message: str
    last_value: float | None
    evaluated_at: datetime


@dataclass(frozen=True)
class ConditionEvaluationRun:
    total: int
    failed: int


@dataclass(frozen=True)
class MetricCollectionSpecRun:
    total: int
    failed: int
    warning: int
    ingested: int
    skipped: int


def _operator_fn(operator_name: str) -> Callable[[float, float], bool]:
    operators: dict[str, Callable[[float, float], bool]] = {
        ComparisonOperator.GT: lambda left, right: left > right,
        ComparisonOperator.GTE: lambda left, right: left >= right,
        ComparisonOperator.LT: lambda left, right: left < right,
        ComparisonOperator.LTE: lambda left, right: left <= right,
    }
    return operators[operator_name]


def _is_breached_for_duration(
    values: list[influx.MetricSample],
    operator_name: str,
    threshold: float,
    breach_minutes: int,
    now_utc: datetime,
) -> bool:
    if not values:
        return False

    compare = _operator_fn(operator_name)
    window_start = now_utc - timedelta(minutes=breach_minutes)
    window_values = [sample for sample in values if sample.ts >= window_start]
    if not window_values:
        return False

    grace_start = window_start + timedelta(minutes=1)
    if window_values[0].ts > grace_start:
        return False

    return all(compare(sample.value, threshold) for sample in window_values)


def evaluate_condition(
    condition: ConditionDefinition,
    *,
    now: datetime | None = None,
) -> ConditionEvaluation:
    now_utc = (now or datetime.now(UTC)).astimezone(UTC)
    samples = influx.query_condition_window(condition, now=now_utc)

    if not samples:
        return ConditionEvaluation(
            status=StatusLevel.WARNING,
            message="No samples available in condition window",
            last_value=None,
            evaluated_at=now_utc,
        )

    latest = samples[-1]
    stale_seconds = int((now_utc - latest.ts).total_seconds())
    if stale_seconds > settings.CONDITION_DATA_STALE_WARNING_SECONDS:
        return ConditionEvaluation(
            status=StatusLevel.WARNING,
            message=f"Latest sample is stale ({stale_seconds}s old)",
            last_value=latest.value,
            evaluated_at=now_utc,
        )

    if condition.critical_threshold is not None and _is_breached_for_duration(
        samples,
        condition.operator,
        condition.critical_threshold,
        condition.breach_minutes,
        now_utc,
    ):
        return ConditionEvaluation(
            status=StatusLevel.CRITICAL,
            message=(
                f"{condition.metric_name} {condition.operator} "
                f"{condition.critical_threshold} for {condition.breach_minutes}m"
            ),
            last_value=latest.value,
            evaluated_at=now_utc,
        )

    if condition.warning_threshold is not None and _is_breached_for_duration(
        samples,
        condition.operator,
        condition.warning_threshold,
        condition.breach_minutes,
        now_utc,
    ):
        return ConditionEvaluation(
            status=StatusLevel.WARNING,
            message=(
                f"{condition.metric_name} {condition.operator} "
                f"{condition.warning_threshold} for {condition.breach_minutes}m"
            ),
            last_value=latest.value,
            evaluated_at=now_utc,
        )

    return ConditionEvaluation(
        status=StatusLevel.OK,
        message="Condition is within thresholds",
        last_value=latest.value,
        evaluated_at=now_utc,
    )


def evaluate_conditions_once(
    *,
    condition_id: int | None = None,
) -> ConditionEvaluationRun:
    queryset = ConditionDefinition.objects.filter(enabled=True)
    if condition_id is not None:
        queryset = queryset.filter(id=condition_id)

    total = 0
    failed = 0

    for condition in queryset:
        total += 1
        try:
            result = evaluate_condition(condition)
            condition.status = result.status
            condition.message = result.message
            condition.last_value = result.last_value
            condition.last_evaluated = result.evaluated_at
            condition.save(
                update_fields=["status", "message", "last_value", "last_evaluated"]
            )
        except Exception as err:  # noqa: BLE001
            failed += 1
            condition.status = StatusLevel.CRITICAL
            condition.message = f"evaluation error: {err}"
            condition.save(update_fields=["status", "message"])

    heartbeat_status = StatusLevel.OK if failed == 0 else StatusLevel.WARNING
    record_heartbeat(
        "condition_evaluator",
        status=heartbeat_status,
        last_error="" if failed == 0 else f"{failed} condition(s) failed",
        details={"evaluated": total, "failed": failed},
        success=failed == 0,
    )

    return ConditionEvaluationRun(total=total, failed=failed)


def _normalize_home_assistant_sensor_state(
    payload: dict[str, object],
    *,
    host_id: str,
    service_id: str,
    metric_name_override: str = "",
    extra_tags: dict[str, str] | None = None,
) -> influx.MetricPoint | None:
    entity_id = str(payload.get("entity_id", "")).strip()
    if not entity_id:
        return None

    state_value = payload.get("state")
    if state_value is None:
        return None
    try:
        value = float(str(state_value))
    except (TypeError, ValueError):
        return None

    updated_raw = str(payload.get("last_updated") or "")
    ts = datetime.now(UTC)
    if updated_raw:
        normalized = updated_raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            ts = parsed.astimezone(UTC)
        except ValueError:
            ts = datetime.now(UTC)

    attributes = payload.get("attributes") or {}
    if not isinstance(attributes, dict):
        attributes = {}

    metric_name = metric_name_override or f"ha.{entity_id.replace(' ', '_')}"

    tags: dict[str, str] = {"entity_id": entity_id}
    if extra_tags:
        tags.update(extra_tags)

    unit = attributes.get("unit_of_measurement")
    if unit is not None:
        tags["unit"] = str(unit)

    device_class = attributes.get("device_class")
    if device_class is not None:
        tags["device_class"] = str(device_class)

    return influx.MetricPoint(
        ts=ts,
        host=host_id,
        service=service_id,
        metric=metric_name,
        value=value,
        tags=tags,
    )


def _resolve_json_path(payload: object, path: str) -> object | None:
    if path == "$":
        return payload

    if not path.startswith("$."):
        return None

    parts = [part for part in path[2:].split(".") if part]
    current: object = payload
    for part in parts:
        if isinstance(current, dict):
            if part not in current:
                return None
            current = current[part]
            continue

        if isinstance(current, list) and part.isdigit():
            index = int(part)
            if index < 0 or index >= len(current):
                return None
            current = current[index]
            continue

        return None

    return current


def _execute_home_assistant_sensor_spec(
    spec: MetricCollectionSpec,
) -> tuple[str, int, int, str]:
    if not isinstance(spec.config, dict):
        return StatusLevel.CRITICAL, 0, 0, "config must be an object"

    base_url = str(spec.config.get("base_url", "")).rstrip("/")
    access_token = str(spec.config.get("access_token", ""))
    entity_id = str(spec.config.get("entity_id", "")).strip()

    if not base_url:
        return StatusLevel.CRITICAL, 0, 0, "config.base_url is required"
    if not access_token:
        return StatusLevel.CRITICAL, 0, 0, "config.access_token is required"
    if not entity_id:
        return StatusLevel.CRITICAL, 0, 0, "config.entity_id is required"

    host_id = str(spec.config.get("host_id", "homeassistant"))
    service_id = str(spec.config.get("service_id", "homeassistant"))
    metric_name = str(spec.config.get("metric_name", ""))
    timeout_seconds = int(spec.config.get("request_timeout_seconds", 10))
    verify_tls = bool(spec.config.get("verify_tls", True))

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    try:
        with httpx.Client(
            timeout=timeout_seconds, headers=headers, verify=verify_tls
        ) as client:
            resp = client.get(f"{base_url}/api/states/{entity_id}")
            resp.raise_for_status()
            state_payload = resp.json()
            if not isinstance(state_payload, dict):
                return StatusLevel.WARNING, 0, 1, "invalid JSON payload"

        point = _normalize_home_assistant_sensor_state(
            state_payload,
            host_id=host_id,
            service_id=service_id,
            metric_name_override=metric_name,
            extra_tags={"spec_name": spec.name},
        )
        if point is None:
            return StatusLevel.WARNING, 0, 1, "state payload not numeric"

        written = influx.write_points([point])
        touch_registry_from_points([point])
        return StatusLevel.OK, written, 0, ""
    except Exception as err:  # noqa: BLE001
        return StatusLevel.CRITICAL, 0, 0, str(err)


def _execute_home_assistant_env_scan_spec(
    spec: MetricCollectionSpec,
) -> tuple[str, int, int, str]:
    if not isinstance(spec.config, dict):
        return StatusLevel.CRITICAL, 0, 0, "config must be an object"

    base_url = str(spec.config.get("base_url", "")).rstrip("/")
    access_token = str(spec.config.get("access_token", ""))
    if not base_url:
        return StatusLevel.CRITICAL, 0, 0, "config.base_url is required"
    if not access_token:
        return StatusLevel.CRITICAL, 0, 0, "config.access_token is required"

    host_id = str(spec.config.get("host_id", "homeassistant"))
    service_id = str(spec.config.get("service_id", "homeassistant"))
    metric_prefix = str(spec.config.get("metric_prefix", "ha."))
    timeout_seconds = int(spec.config.get("request_timeout_seconds", 10))
    verify_tls = bool(spec.config.get("verify_tls", True))

    include_device_classes = spec.config.get(
        "include_device_classes", ["temperature", "humidity"]
    )
    if not isinstance(include_device_classes, list):
        include_device_classes = ["temperature", "humidity"]
    include_device_classes_set = {
        str(item).strip().lower() for item in include_device_classes
    }

    entity_regex_value = str(
        spec.config.get("entity_id_regex", "(temperature|humidity)")
    )
    try:
        entity_pattern = re.compile(entity_regex_value, re.IGNORECASE)
    except re.error as err:
        return StatusLevel.CRITICAL, 0, 0, f"invalid entity_id_regex: {err}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    try:
        with httpx.Client(
            timeout=timeout_seconds, headers=headers, verify=verify_tls
        ) as client:
            resp = client.get(f"{base_url}/api/states")
            resp.raise_for_status()
            payload = resp.json()

        if not isinstance(payload, list):
            return StatusLevel.CRITICAL, 0, 0, "states response is not a list"

        points: list[influx.MetricPoint] = []
        skipped = 0
        for item in payload:
            if not isinstance(item, dict):
                continue
            entity_id = item.get("entity_id")
            if not isinstance(entity_id, str) or not entity_id.startswith("sensor."):
                continue

            attributes = item.get("attributes")
            if not isinstance(attributes, dict):
                attributes = {}
            device_class = str(attributes.get("device_class", "")).lower()

            include = False
            if device_class and device_class in include_device_classes_set:
                include = True
            elif entity_pattern.search(entity_id):
                include = True

            if not include:
                continue

            point = _normalize_home_assistant_sensor_state(
                item,
                host_id=host_id,
                service_id=service_id,
                metric_name_override=f"{metric_prefix}{entity_id}",
                extra_tags={"spec_name": spec.name},
            )
            if point is None:
                skipped += 1
                continue
            points.append(point)

        if not points:
            return StatusLevel.WARNING, 0, skipped, "no matching numeric states"

        written = influx.write_points(points)
        touch_registry_from_points(points)
        return StatusLevel.OK, written, skipped, ""
    except Exception as err:  # noqa: BLE001
        return StatusLevel.CRITICAL, 0, 0, str(err)


def _execute_http_json_metric_spec(
    spec: MetricCollectionSpec,
) -> tuple[str, int, int, str]:
    if not isinstance(spec.config, dict):
        return StatusLevel.CRITICAL, 0, 0, "config must be an object"

    url = str(spec.config.get("url", "")).strip()
    metric_path = str(spec.config.get("metric_path", "")).strip()
    metric_name = str(spec.config.get("metric_name", "")).strip()
    host_id = str(spec.config.get("host_id", "external"))
    service_id_raw = str(spec.config.get("service_id", "")).strip()
    timeout_seconds = int(spec.config.get("request_timeout_seconds", 10))
    verify_tls = bool(spec.config.get("verify_tls", True))

    if not url:
        return StatusLevel.CRITICAL, 0, 0, "config.url is required"
    if not metric_path:
        return StatusLevel.CRITICAL, 0, 0, "config.metric_path is required"
    if not metric_name:
        return StatusLevel.CRITICAL, 0, 0, "config.metric_name is required"

    tags: dict[str, str] = {"spec_name": spec.name}
    configured_tags = spec.config.get("tags", {})
    if isinstance(configured_tags, dict):
        tags.update({str(key): str(value) for key, value in configured_tags.items()})

    headers = {"Accept": "application/json"}
    bearer_token = str(spec.config.get("bearer_token", "")).strip()
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"

    basic_username = str(spec.config.get("basic_username", "")).strip()
    basic_password = str(spec.config.get("basic_password", "")).strip()
    auth = (
        httpx.BasicAuth(basic_username, basic_password)
        if basic_username and basic_password
        else None
    )

    try:
        with httpx.Client(
            timeout=timeout_seconds,
            headers=headers,
            verify=verify_tls,
            auth=auth,
        ) as client:
            resp = client.get(url)
            resp.raise_for_status()
            payload = resp.json()

        raw_value = _resolve_json_path(payload, metric_path)
        if raw_value is None:
            return (
                StatusLevel.WARNING,
                0,
                1,
                f"metric_path not found: {metric_path}",
            )

        try:
            metric_value = float(str(raw_value))
        except (TypeError, ValueError):
            return StatusLevel.WARNING, 0, 1, "metric value is not numeric"

        point = influx.MetricPoint(
            ts=datetime.now(UTC),
            host=host_id,
            service=service_id_raw or None,
            metric=metric_name,
            value=metric_value,
            tags=tags,
        )
        written = influx.write_points([point])
        touch_registry_from_points([point])
        return StatusLevel.OK, written, 0, ""
    except Exception as err:  # noqa: BLE001
        return StatusLevel.CRITICAL, 0, 0, str(err)


def _run_single_metric_collection_spec(
    spec: MetricCollectionSpec,
) -> tuple[str, int, int, str]:
    if spec.spec_type == MetricCollectionSpecType.HOME_ASSISTANT_SENSOR:
        return _execute_home_assistant_sensor_spec(spec)
    if spec.spec_type == MetricCollectionSpecType.HOME_ASSISTANT_ENV_SCAN:
        return _execute_home_assistant_env_scan_spec(spec)
    if spec.spec_type == MetricCollectionSpecType.HTTP_JSON_METRIC:
        return _execute_http_json_metric_spec(spec)

    return StatusLevel.CRITICAL, 0, 0, f"unsupported spec_type: {spec.spec_type}"


def run_metric_collection_specs_once(
    *,
    due_only: bool = False,
) -> MetricCollectionSpecRun:
    now = timezone.now()
    now_ts = int(now.timestamp())

    queryset = MetricCollectionSpec.objects.filter(enabled=True)
    if due_only:
        queryset = queryset.filter(next_run_time__lte=now_ts)

    specs = list(queryset)
    if not specs:
        if due_only:
            return MetricCollectionSpecRun(
                total=0, failed=0, warning=0, ingested=0, skipped=0
            )
        raise ValueError("No matching enabled metric collection specs")

    total = 0
    failed = 0
    warning = 0
    ingested = 0
    skipped = 0
    error_messages: list[str] = []

    for spec in specs:
        total += 1
        status, item_ingested, item_skipped, error_msg = (
            _run_single_metric_collection_spec(spec)
        )
        ingested += item_ingested
        skipped += item_skipped

        spec.last_run_at = now
        spec.last_status = status
        spec.last_error = error_msg
        spec.next_run_time = now_ts + max(1, int(spec.interval_seconds))
        spec.save(
            update_fields=["last_run_at", "last_status", "last_error", "next_run_time"]
        )

        if status == StatusLevel.CRITICAL:
            failed += 1
            error_messages.append(f"{spec.name}: {error_msg}")
        elif status == StatusLevel.WARNING:
            warning += 1
            if error_msg:
                error_messages.append(f"{spec.name}: {error_msg}")

    if failed > 0 and (total - failed) == 0:
        heartbeat_status = StatusLevel.CRITICAL
        heartbeat_success = False
    elif failed > 0 or warning > 0:
        heartbeat_status = StatusLevel.WARNING
        heartbeat_success = True
    else:
        heartbeat_status = StatusLevel.OK
        heartbeat_success = True

    record_heartbeat(
        "metric_collectors",
        status=heartbeat_status,
        last_error="; ".join(error_messages[:5]),
        details={
            "total": total,
            "failed": failed,
            "warning": warning,
            "ingested": ingested,
            "skipped": skipped,
        },
        success=heartbeat_success,
    )

    return MetricCollectionSpecRun(
        total=total,
        failed=failed,
        warning=warning,
        ingested=ingested,
        skipped=skipped,
    )


def record_heartbeat(
    name: str,
    *,
    status: str,
    last_error: str = "",
    details: dict | None = None,
    success: bool = False,
) -> PipelineHeartbeat:
    defaults = {
        "status": status,
        "last_error": last_error,
        "details": details or {},
    }
    if success:
        defaults["last_success"] = timezone.now()

    heartbeat, _ = PipelineHeartbeat.objects.update_or_create(
        name=name, defaults=defaults
    )
    return heartbeat


def touch_registry_from_points(points: list[influx.MetricPoint]) -> None:
    now = timezone.now()
    for point in points:
        host, _ = HostRegistry.objects.get_or_create(
            host_id=point.host,
            defaults={"display_name": point.host},
        )
        if host.last_seen_at is None or now > host.last_seen_at:
            host.last_seen_at = now
            host.save(update_fields=["last_seen_at"])

        if point.service:
            service, _ = ServiceRegistry.objects.get_or_create(
                service_id=point.service,
                defaults={"display_name": point.service, "host": host},
            )
            changed_fields: list[str] = []
            if service.host_id != host.id:
                service.host = host
                changed_fields.append("host")
            if service.last_seen_at is None or now > service.last_seen_at:
                service.last_seen_at = now
                changed_fields.append("last_seen_at")
            if changed_fields:
                service.save(update_fields=changed_fields)
