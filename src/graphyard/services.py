from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
import re
import time
from typing import Callable

import httpx
from django.conf import settings
from django.db import OperationalError
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
    SubjectRegistry,
    SubjectType,
)

logger = logging.getLogger(__name__)
_subject_mapping_warning_keys: set[str] = set()
_heartbeat_write_cache: dict[tuple[str, str, str, bool], float] = {}


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


def _warn_subject_mapping_once(key: str, message: str, *args: object) -> None:
    if key in _subject_mapping_warning_keys:
        return
    _subject_mapping_warning_keys.add(key)
    logger.warning(message, *args)


def _entity_name_slug(entity_id: str) -> str:
    normalized = entity_id.strip().lower()
    for prefix in ("sensor.", "binary_sensor."):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
            break
    return influx.normalize_subject_id(normalized)


def _resolve_subject_mapping(
    *,
    spec: MetricCollectionSpec,
    entity_id: str,
) -> tuple[str, str] | None:
    if not isinstance(spec.config, dict):
        return None

    subject_mapping = spec.config.get("subject_mapping")
    if subject_mapping is None:
        _warn_subject_mapping_once(
            f"{spec.name}:missing",
            "Spec %s missing config.subject_mapping; using default entity_name_slug mapping",
            spec.name,
        )
        return (SubjectType.ENVIRONMENT_SENSOR, _entity_name_slug(entity_id))

    if not isinstance(subject_mapping, dict):
        _warn_subject_mapping_once(
            f"{spec.name}:invalid_type",
            "Spec %s has invalid config.subject_mapping type=%s; using default mapping",
            spec.name,
            type(subject_mapping).__name__,
        )
        return (SubjectType.ENVIRONMENT_SENSOR, _entity_name_slug(entity_id))

    rules = subject_mapping.get("rules", [])
    if isinstance(rules, list):
        for index, rule in enumerate(rules):
            if not isinstance(rule, dict):
                logger.warning(
                    "Spec %s subject_mapping.rules[%s] is not an object; skipping",
                    spec.name,
                    index,
                )
                continue
            pattern_raw = str(rule.get("match_entity_id_regex", "")).strip()
            if not pattern_raw:
                continue
            try:
                pattern = re.compile(pattern_raw)
            except re.error as err:
                logger.warning(
                    "Spec %s has invalid subject mapping regex at rules[%s]: %s",
                    spec.name,
                    index,
                    err,
                )
                continue
            if pattern.fullmatch(entity_id) is None:
                continue

            subject_type = str(rule.get("subject_type", "")).strip().lower()
            if subject_type not in SubjectType.ALL:
                _warn_subject_mapping_once(
                    f"{spec.name}:rule:{index}:unknown_subject_type:{subject_type}",
                    "Spec %s subject mapping rules[%s] has unknown subject_type=%s",
                    spec.name,
                    index,
                    subject_type,
                )
                continue
            template = rule.get("subject_id_template")
            if not isinstance(template, str) or not template.strip():
                logger.warning(
                    "Spec %s subject mapping rules[%s] matched %s but has no static subject_id_template",
                    spec.name,
                    index,
                    entity_id,
                )
                return None
            try:
                subject_id = influx.normalize_subject_id(template)
            except ValueError as err:
                logger.warning(
                    "Spec %s subject mapping rules[%s] produced invalid subject_id for %s: %s",
                    spec.name,
                    index,
                    entity_id,
                    err,
                )
                return None
            return (subject_type, subject_id)

    default_mapping = subject_mapping.get("default", {})
    if not isinstance(default_mapping, dict):
        _warn_subject_mapping_once(
            f"{spec.name}:invalid_default",
            "Spec %s has invalid subject_mapping.default; using fallback",
            spec.name,
        )
        default_mapping = {}

    subject_type = (
        str(default_mapping.get("subject_type", SubjectType.ENVIRONMENT_SENSOR))
        .strip()
        .lower()
    )
    if subject_type not in SubjectType.ALL:
        _warn_subject_mapping_once(
            f"{spec.name}:unknown_default_subject_type:{subject_type}",
            "Spec %s has unknown default subject_type=%s; using %s",
            spec.name,
            subject_type,
            SubjectType.ENVIRONMENT_SENSOR,
        )
        subject_type = SubjectType.ENVIRONMENT_SENSOR
    subject_id_from = str(default_mapping.get("subject_id_from", "entity_name_slug"))
    if subject_id_from != "entity_name_slug":
        _warn_subject_mapping_once(
            f"{spec.name}:unsupported_subject_id_from:{subject_id_from}",
            "Spec %s has unsupported subject_id_from=%s; using entity_name_slug",
            spec.name,
            subject_id_from,
        )
    return (subject_type, _entity_name_slug(entity_id))


def _resolve_home_assistant_metric_mapping(
    *,
    spec: MetricCollectionSpec,
    entity_id: str,
    metric_name: str,
    value: float,
) -> tuple[str, float, dict[str, str]]:
    config = spec.config if isinstance(spec.config, dict) else {}
    metric_mapping = config.get("metric_mapping")
    if not isinstance(metric_mapping, dict):
        return metric_name, value, {}

    rules = metric_mapping.get("rules", [])
    if not isinstance(rules, list):
        _warn_subject_mapping_once(
            f"{spec.name}:invalid_metric_mapping_rules",
            "Spec %s has invalid config.metric_mapping.rules type=%s; ignoring metric mapping",
            spec.name,
            type(rules).__name__,
        )
        return metric_name, value, {}

    for index, rule in enumerate(rules):
        if not isinstance(rule, dict):
            _warn_subject_mapping_once(
                f"{spec.name}:invalid_metric_mapping_rule:{index}",
                "Spec %s metric_mapping.rules[%s] is not an object; skipping",
                spec.name,
                index,
            )
            continue

        pattern_raw = str(rule.get("match_entity_id_regex", "")).strip()
        if not pattern_raw:
            _warn_subject_mapping_once(
                f"{spec.name}:missing_metric_mapping_regex:{index}",
                "Spec %s metric_mapping.rules[%s] missing match_entity_id_regex; skipping",
                spec.name,
                index,
            )
            continue

        try:
            pattern = re.compile(pattern_raw)
        except re.error as err:
            _warn_subject_mapping_once(
                f"{spec.name}:invalid_metric_mapping_regex:{index}",
                "Spec %s metric_mapping.rules[%s] invalid regex %s: %s",
                spec.name,
                index,
                pattern_raw,
                err,
            )
            continue

        if pattern.search(entity_id) is None:
            continue

        mapped_metric_name = str(rule.get("metric_name", metric_name)).strip()
        if not mapped_metric_name:
            mapped_metric_name = metric_name

        multiplier_raw = rule.get("value_multiplier", 1)
        try:
            if isinstance(multiplier_raw, bool):
                raise TypeError
            multiplier = float(multiplier_raw)
        except (TypeError, ValueError):
            _warn_subject_mapping_once(
                f"{spec.name}:invalid_metric_mapping_multiplier:{index}",
                "Spec %s metric_mapping.rules[%s] has invalid value_multiplier=%r; using 1",
                spec.name,
                index,
                multiplier_raw,
            )
            multiplier = 1.0

        extra_tags_raw = rule.get("extra_tags", {})
        if isinstance(extra_tags_raw, dict):
            extra_tags = {str(key): str(item) for key, item in extra_tags_raw.items()}
        else:
            _warn_subject_mapping_once(
                f"{spec.name}:invalid_metric_mapping_tags:{index}",
                "Spec %s metric_mapping.rules[%s] has invalid extra_tags type=%s; ignoring",
                spec.name,
                index,
                type(extra_tags_raw).__name__,
            )
            extra_tags = {}

        return mapped_metric_name, value * multiplier, extra_tags

    return metric_name, value, {}


def _normalize_home_assistant_sensor_state(
    payload: dict[str, object],
    *,
    spec: MetricCollectionSpec,
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

    resolved_subject = _resolve_subject_mapping(spec=spec, entity_id=entity_id)
    if resolved_subject is None:
        logger.warning(
            "Spec %s failed subject resolution for entity_id=%s",
            spec.name,
            entity_id,
        )
        return None
    subject_type, subject_id = resolved_subject

    config = spec.config if isinstance(spec.config, dict) else {}
    source_system = str(config.get("source_system", "homeassistant")).strip().lower()
    source_instance = str(config.get("source_instance", "default")).strip() or "default"

    # Keep legacy defaults but allow canonical collector fields.
    legacy_host_id = (
        str(config.get("host_id", "homeassistant")).strip() or "homeassistant"
    )
    legacy_service_id = (
        str(config.get("service_id", "homeassistant")).strip() or "homeassistant"
    )
    collector_service = (
        str(config.get("collector_service", "graphyard-agent")).strip()
        or "graphyard-agent"
    )
    collector_host = str(config.get("collector_host", legacy_host_id)).strip()
    if not collector_host:
        collector_host = legacy_host_id

    metric_name = metric_name_override or f"ha.{entity_id.replace(' ', '_')}"
    metric_name, value, mapped_tags = _resolve_home_assistant_metric_mapping(
        spec=spec,
        entity_id=entity_id,
        metric_name=metric_name,
        value=value,
    )

    tags: dict[str, str] = {"entity_id": entity_id}
    if extra_tags:
        tags.update(extra_tags)
    if mapped_tags:
        tags.update(mapped_tags)

    unit = attributes.get("unit_of_measurement")
    if unit is not None:
        tags["unit"] = str(unit)

    device_class = attributes.get("device_class")
    if device_class is not None:
        tags["device_class"] = str(device_class)

    host_compat: str | None = None
    try:
        normalized_legacy_host = influx.normalize_subject_id(legacy_host_id)
    except ValueError:
        normalized_legacy_host = ""
    if subject_type == SubjectType.HOST and subject_id == normalized_legacy_host:
        host_compat = normalized_legacy_host

    return influx.MetricPoint(
        ts=ts,
        metric=metric_name,
        value=value,
        subject_type=subject_type,
        subject_id=subject_id,
        source_system=source_system,
        source_instance=source_instance,
        source_entity_id=entity_id,
        collector_service=collector_service,
        collector_host=collector_host,
        host=host_compat,
        service=legacy_service_id,
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
            spec=spec,
            metric_name_override=metric_name,
            extra_tags={"spec_name": spec.name},
        )
        if point is None:
            return (
                StatusLevel.WARNING,
                0,
                1,
                "state payload not numeric or mapping failed",
            )

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
            if not isinstance(entity_id, str) or not (
                entity_id.startswith("sensor.")
                or entity_id.startswith("binary_sensor.")
            ):
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
                spec=spec,
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
    subject_type = str(spec.config.get("subject_type", SubjectType.SERVICE)).strip()
    subject_id_raw = str(
        spec.config.get("subject_id", service_id_raw or host_id or "external")
    ).strip()
    source_system = str(spec.config.get("source_system", "http")).strip()
    source_instance = str(spec.config.get("source_instance", "default")).strip()
    collector_service = str(
        spec.config.get("collector_service", service_id_raw or "graphyard-agent")
    ).strip()
    collector_host = str(
        spec.config.get("collector_host", host_id or "external")
    ).strip()
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
            metric=metric_name,
            value=metric_value,
            subject_type=subject_type,
            subject_id=subject_id_raw,
            source_system=source_system,
            source_instance=source_instance or "default",
            collector_service=collector_service or "graphyard-agent",
            collector_host=collector_host or host_id or "external",
            host=host_id if subject_type.strip().lower() == SubjectType.HOST else None,
            service=service_id_raw or None,
            tags=tags,
        )
        written = influx.write_points([point])
        touch_registry_from_points([point])
        return StatusLevel.OK, written, 0, ""
    except Exception as err:  # noqa: BLE001
        return StatusLevel.CRITICAL, 0, 0, str(err)


def _build_http_page_probe_point(
    *,
    ts: datetime,
    metric: str,
    value: float,
    subject_id: str,
    service_id: str,
    source_system: str,
    source_instance: str,
    collector_service: str,
    collector_host: str,
    url: str,
    tags: dict[str, str],
) -> influx.MetricPoint:
    return influx.MetricPoint(
        ts=ts,
        metric=metric,
        value=value,
        subject_type=SubjectType.SERVICE,
        subject_id=subject_id,
        source_system=source_system,
        source_instance=source_instance,
        source_entity_id=url,
        collector_service=collector_service,
        collector_host=collector_host,
        service=service_id,
        tags=dict(tags),
    )


def _execute_http_page_probe_spec(
    spec: MetricCollectionSpec,
) -> tuple[str, int, int, str]:
    if not isinstance(spec.config, dict):
        return StatusLevel.CRITICAL, 0, 0, "config must be an object"

    url = str(spec.config.get("url", "")).strip()
    subject_id = str(spec.config.get("subject_id", "")).strip()
    service_id = str(spec.config.get("service_id", subject_id)).strip()
    source_system = str(spec.config.get("source_system", "http_probe")).strip()
    source_instance = str(spec.config.get("source_instance", "public_web")).strip()
    collector_service = str(
        spec.config.get("collector_service", "graphyard-agent")
    ).strip()
    collector_host = str(spec.config.get("collector_host", "external")).strip()
    timeout_raw = spec.config.get("request_timeout_seconds", 10)
    verify_tls = bool(spec.config.get("verify_tls", True))
    follow_redirects = bool(spec.config.get("follow_redirects", True))

    if not url:
        return StatusLevel.CRITICAL, 0, 0, "config.url is required"
    if not subject_id:
        return StatusLevel.CRITICAL, 0, 0, "config.subject_id is required"

    try:
        timeout_seconds = float(timeout_raw)
    except (TypeError, ValueError):
        return (
            StatusLevel.CRITICAL,
            0,
            0,
            f"invalid request_timeout_seconds: {timeout_raw!r}",
        )
    if timeout_seconds <= 0:
        return (
            StatusLevel.CRITICAL,
            0,
            0,
            "config.request_timeout_seconds must be greater than 0",
        )

    tags: dict[str, str] = {
        "spec_name": spec.name,
        "http_method": "get",
    }
    configured_tags = spec.config.get("tags", {})
    if isinstance(configured_tags, dict):
        tags.update({str(key): str(value) for key, value in configured_tags.items()})

    normalized_source_system = source_system or "http_probe"
    normalized_source_instance = source_instance or "public_web"
    normalized_collector_service = collector_service or "graphyard-agent"
    normalized_collector_host = collector_host or "external"

    def _point(*, ts: datetime, metric: str, value: float) -> influx.MetricPoint:
        return _build_http_page_probe_point(
            ts=ts,
            metric=metric,
            value=value,
            subject_id=subject_id,
            service_id=service_id,
            source_system=normalized_source_system,
            source_instance=normalized_source_instance,
            collector_service=normalized_collector_service,
            collector_host=normalized_collector_host,
            url=url,
            tags=tags,
        )

    try:
        with httpx.Client(
            timeout=timeout_seconds,
            verify=verify_tls,
            follow_redirects=follow_redirects,
        ) as client:
            started_at = time.perf_counter()
            with client.stream("GET", url) as response:
                headers_received_at = time.perf_counter()
                chunks = response.iter_bytes()
                try:
                    first_chunk = next(chunks)
                except StopIteration:
                    pass
                else:
                    del first_chunk
                    for _chunk in chunks:
                        del _chunk
                # TTFB is measured to response headers. With follow_redirects=true,
                # redirect round-trips are intentionally included in this end-to-end value.
                ttfb_seconds = headers_received_at - started_at
                total_seconds = time.perf_counter() - started_at
                status_code = response.status_code
                redirect_count = len(response.history)

        now = datetime.now(UTC)
        success_value = 1.0 if 200 <= status_code < 400 else 0.0
        points = [
            _point(
                ts=now,
                metric="service.http_page_ttfb_seconds",
                value=ttfb_seconds,
            ),
            _point(
                ts=now,
                metric="service.http_page_total_seconds",
                value=total_seconds,
            ),
            _point(
                ts=now,
                metric="service.http_page_status_code",
                value=float(status_code),
            ),
            _point(
                ts=now,
                metric="service.http_page_success",
                value=success_value,
            ),
            _point(
                ts=now,
                metric="service.http_page_redirect_count",
                value=float(redirect_count),
            ),
        ]
        written = influx.write_points(points)
        touch_registry_from_points(points)
        if success_value == 1.0:
            return StatusLevel.OK, written, 0, ""
        return StatusLevel.WARNING, written, 0, f"unexpected status code: {status_code}"
    except httpx.HTTPError as err:
        now = datetime.now(UTC)
        points = [
            _point(
                ts=now,
                metric="service.http_page_status_code",
                value=0.0,
            ),
            _point(
                ts=now,
                metric="service.http_page_success",
                value=0.0,
            ),
        ]
        written = influx.write_points(points)
        touch_registry_from_points(points)
        return StatusLevel.WARNING, written, 0, str(err)
    except Exception as err:  # noqa: BLE001
        return StatusLevel.CRITICAL, 0, 0, str(err)


def _find_unifi_device(
    devices: list[object],
    *,
    device_name: str,
    device_mac: str,
) -> dict[str, object] | None:
    normalized_mac = device_mac.strip().lower()
    normalized_name = device_name.strip()

    for item in devices:
        if not isinstance(item, dict):
            continue
        if normalized_name and str(item.get("name", "")).strip() == normalized_name:
            return item
        if (
            normalized_mac
            and str(item.get("mac", "")).strip().lower() == normalized_mac
        ):
            return item

    return None


def _resolve_unifi_interface_stats(
    device: dict[str, object],
    *,
    interface_selector: str,
) -> tuple[dict[str, object], dict[str, str]]:
    selector = interface_selector.strip() or "uplink"
    selector_lower = selector.lower()
    port_table_raw = device.get("port_table")
    port_table = port_table_raw if isinstance(port_table_raw, list) else []

    if selector_lower == "uplink":
        uplink_raw = device.get("uplink")
        if not isinstance(uplink_raw, dict):
            raise ValueError("device uplink stats missing")

        tags: dict[str, str] = {"traffic_scope": "uplink"}
        interface_name = str(uplink_raw.get("name", "")).strip()
        if interface_name:
            tags["interface"] = interface_name
        port_idx_raw = uplink_raw.get("port_idx") or uplink_raw.get("num_port")
        if port_idx_raw is not None:
            tags["port_idx"] = str(port_idx_raw)
            for port in port_table:
                if not isinstance(port, dict):
                    continue
                if port.get("port_idx") == port_idx_raw:
                    port_name = str(port.get("name", "")).strip()
                    if port_name:
                        tags["port_name"] = port_name
                    break
        speed_raw = uplink_raw.get("speed")
        if speed_raw is not None:
            tags["speed_mbps"] = str(speed_raw)
        return uplink_raw, tags

    if selector_lower.startswith("port_idx:"):
        raw_port_idx = selector.split(":", 1)[1].strip()
        if not raw_port_idx.isdigit():
            raise ValueError(f"invalid port_idx selector: {selector}")
        target_port_idx = int(raw_port_idx)
        for port in port_table:
            if not isinstance(port, dict):
                continue
            if port.get("port_idx") != target_port_idx:
                continue
            tags = {
                "traffic_scope": "port",
                "port_idx": str(target_port_idx),
            }
            port_name = str(port.get("name", "")).strip()
            if port_name:
                tags["port_name"] = port_name
            speed_raw = port.get("speed")
            if speed_raw is not None:
                tags["speed_mbps"] = str(speed_raw)
            return port, tags
        raise ValueError(f"port_idx {target_port_idx} not found on device")

    if selector_lower.startswith("port_name:"):
        target_name = selector.split(":", 1)[1].strip()
        if not target_name:
            raise ValueError(f"invalid port_name selector: {selector}")
        for port in port_table:
            if not isinstance(port, dict):
                continue
            if str(port.get("name", "")).strip() != target_name:
                continue
            tags = {
                "traffic_scope": "port",
                "port_name": target_name,
            }
            port_idx_raw = port.get("port_idx")
            if port_idx_raw is not None:
                tags["port_idx"] = str(port_idx_raw)
            speed_raw = port.get("speed")
            if speed_raw is not None:
                tags["speed_mbps"] = str(speed_raw)
            return port, tags
        raise ValueError(f"port_name {target_name!r} not found on device")

    raise ValueError(f"unsupported interface_selector: {selector}")


def _execute_unifi_device_traffic_spec(
    spec: MetricCollectionSpec,
) -> tuple[str, int, int, str]:
    if not isinstance(spec.config, dict):
        return StatusLevel.CRITICAL, 0, 0, "config must be an object"

    base_url = str(spec.config.get("base_url", "")).rstrip("/")
    username = str(spec.config.get("username", "")).strip()
    password = str(spec.config.get("password", "")).strip()
    site_id = str(spec.config.get("site_id", "default")).strip() or "default"
    device_name = str(spec.config.get("device_name", "")).strip()
    device_mac = str(spec.config.get("device_mac", "")).strip()
    interface_selector = str(spec.config.get("interface_selector", "uplink")).strip()
    subject_type = (
        str(spec.config.get("subject_type", SubjectType.NETWORK_DEVICE)).strip()
        or SubjectType.NETWORK_DEVICE
    )
    subject_id = str(spec.config.get("subject_id", "")).strip()
    source_system = str(spec.config.get("source_system", "unifi")).strip() or "unifi"
    source_instance = (
        str(spec.config.get("source_instance", site_id)).strip() or site_id
    )
    service_id_raw = str(spec.config.get("service_id", "unifi")).strip() or "unifi"
    collector_service = (
        str(spec.config.get("collector_service", "graphyard-agent")).strip()
        or "graphyard-agent"
    )
    collector_host = str(spec.config.get("collector_host", "macmini")).strip()
    timeout_seconds = int(spec.config.get("request_timeout_seconds", 10))
    verify_tls = bool(spec.config.get("verify_tls", True))
    receive_metric_name = str(
        spec.config.get(
            "receive_metric_name", "network_device.network_receive_bytes_per_second"
        )
    ).strip()
    transmit_metric_name = str(
        spec.config.get(
            "transmit_metric_name",
            "network_device.network_transmit_bytes_per_second",
        )
    ).strip()

    if not base_url:
        return StatusLevel.CRITICAL, 0, 0, "config.base_url is required"
    if not username:
        return StatusLevel.CRITICAL, 0, 0, "config.username is required"
    if not password:
        return StatusLevel.CRITICAL, 0, 0, "config.password is required"
    if not (device_name or device_mac):
        return (
            StatusLevel.CRITICAL,
            0,
            0,
            "config.device_name or config.device_mac is required",
        )
    if not subject_id:
        return StatusLevel.CRITICAL, 0, 0, "config.subject_id is required"

    try:
        with httpx.Client(
            timeout=timeout_seconds,
            verify=verify_tls,
            follow_redirects=True,
        ) as client:
            login_resp = client.post(
                f"{base_url}/api/login",
                json={
                    "username": username,
                    "password": password,
                    "remember": True,
                },
                headers={"Accept": "application/json"},
            )
            login_resp.raise_for_status()

            resp = client.get(
                f"{base_url}/api/s/{site_id}/stat/device",
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            payload = resp.json()

        if not isinstance(payload, dict):
            return StatusLevel.CRITICAL, 0, 0, "device response is not an object"

        devices = payload.get("data")
        if not isinstance(devices, list):
            return StatusLevel.CRITICAL, 0, 0, "device response missing data list"

        device = _find_unifi_device(
            devices,
            device_name=device_name,
            device_mac=device_mac,
        )
        if device is None:
            return StatusLevel.WARNING, 0, 1, "configured UniFi device not found"

        interface_stats, interface_tags = _resolve_unifi_interface_stats(
            device,
            interface_selector=interface_selector,
        )

        device_mac_value = str(device.get("mac", "")).strip().lower()
        base_tags = {
            "device_class": "data_rate",
            "spec_name": spec.name,
        }
        base_tags.update(interface_tags)
        if device_mac_value:
            base_tags["device_mac"] = device_mac_value

        points: list[influx.MetricPoint] = []
        skipped = 0
        now = datetime.now(UTC)

        metric_specs = [
            (
                receive_metric_name,
                "rx_bytes-r",
                {"traffic_direction": "receive"},
            ),
            (
                transmit_metric_name,
                "tx_bytes-r",
                {"traffic_direction": "transmit"},
            ),
        ]
        for metric_name, field_name, direction_tags in metric_specs:
            raw_value = interface_stats.get(field_name)
            try:
                metric_value = float(str(raw_value))
            except (TypeError, ValueError):
                skipped += 1
                continue
            tags = dict(base_tags)
            tags.update(direction_tags)
            points.append(
                influx.MetricPoint(
                    ts=now,
                    metric=metric_name,
                    value=metric_value,
                    subject_type=subject_type,
                    subject_id=subject_id,
                    source_system=source_system,
                    source_instance=source_instance,
                    collector_service=collector_service,
                    collector_host=collector_host or "macmini",
                    service=service_id_raw,
                    source_entity_id=device_mac_value or None,
                    tags=tags,
                )
            )

        if not points:
            return StatusLevel.WARNING, 0, skipped, "no numeric traffic values found"

        written = influx.write_points(points)
        touch_registry_from_points(points)
        return StatusLevel.OK, written, skipped, ""
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
    if spec.spec_type == MetricCollectionSpecType.HTTP_PAGE_PROBE:
        return _execute_http_page_probe_spec(spec)
    if spec.spec_type == MetricCollectionSpecType.UNIFI_DEVICE_TRAFFIC:
        return _execute_unifi_device_traffic_spec(spec)

    return StatusLevel.CRITICAL, 0, 0, f"unsupported spec_type: {spec.spec_type}"


def run_metric_collection_specs_once(
    *,
    due_only: bool = False,
) -> MetricCollectionSpecRun:
    _subject_mapping_warning_keys.clear()
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
    min_update_interval_seconds: int = 0,
) -> PipelineHeartbeat:
    if min_update_interval_seconds > 0:
        cache_key = (name, status, last_error, success)
        now_monotonic = time.monotonic()
        cache_ttl = float(max(0, min_update_interval_seconds))
        last_write = _heartbeat_write_cache.get(cache_key)
        if last_write is not None and now_monotonic - last_write < cache_ttl:
            return PipelineHeartbeat(
                name=name,
                status=status,
                last_error=last_error,
                details=details or {},
            )
        _heartbeat_write_cache[cache_key] = now_monotonic

    defaults = {
        "status": status,
        "last_error": last_error,
        "details": details or {},
    }
    if success:
        defaults["last_success"] = timezone.now()

    for attempt in range(1, 4):
        try:
            heartbeat, _ = PipelineHeartbeat.objects.update_or_create(
                name=name, defaults=defaults
            )
            return heartbeat
        except OperationalError as err:
            if "database is locked" not in str(err).lower() or attempt == 3:
                raise
            # Small bounded backoff to ride through short write locks.
            time.sleep(0.05 * attempt)

    raise RuntimeError("unreachable")


def touch_registry_from_points(points: list[influx.MetricPoint]) -> None:
    now = timezone.now()
    for point in points:
        normalized = influx.normalize_metric_point(point)

        subject, _ = SubjectRegistry.objects.get_or_create(
            subject_type=normalized.subject_type,
            subject_id=normalized.subject_id,
            defaults={
                "display_name": normalized.subject_id,
                "source_system": normalized.source_system,
            },
        )
        subject_changed_fields: list[str] = []
        if subject.last_seen_at is None or now > subject.last_seen_at:
            subject.last_seen_at = now
            subject_changed_fields.append("last_seen_at")
        if (
            normalized.source_system
            and subject.source_system != normalized.source_system
        ):
            subject.source_system = normalized.source_system
            subject_changed_fields.append("source_system")
        if subject_changed_fields:
            subject.save(update_fields=subject_changed_fields)

        host: HostRegistry | None = None
        if normalized.subject_type == SubjectType.HOST:
            host, _ = HostRegistry.objects.get_or_create(
                host_id=normalized.subject_id,
                defaults={"display_name": normalized.subject_id},
            )
            if host.last_seen_at is None or now > host.last_seen_at:
                host.last_seen_at = now
                host.save(update_fields=["last_seen_at"])

        if normalized.service:
            service_host: HostRegistry | None = host
            if service_host is None:
                service_host = HostRegistry.objects.filter(
                    host_id=normalized.collector_host
                ).first()
            service, _ = ServiceRegistry.objects.get_or_create(
                service_id=normalized.service,
                defaults={
                    "display_name": normalized.service,
                    "host": service_host,
                },
            )
            changed_fields: list[str] = []
            if service_host is not None and service.host_id != service_host.id:
                service.host = service_host
                changed_fields.append("host")
            if service.last_seen_at is None or now > service.last_seen_at:
                service.last_seen_at = now
                changed_fields.append("last_seen_at")
            if changed_fields:
                service.save(update_fields=changed_fields)
