from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
import logging
import re
from typing import TYPE_CHECKING, Any

from django.conf import settings
import httpx
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from .models import SubjectType

if TYPE_CHECKING:
    from .models import ConditionDefinition


class InfluxConfigurationError(RuntimeError):
    pass


logger = logging.getLogger(__name__)

_SUBJECT_ID_PATTERN = re.compile(r"[a-z0-9]+(?:_[a-z0-9]+)*")
_DIMENSION_VALUE_PATTERN = re.compile(r"[a-z0-9][a-z0-9_.-]*")
_RESERVED_TAG_KEYS = {
    "metric",
    "host",
    "service",
    "subject_type",
    "subject_id",
    "source_system",
    "source_instance",
    "source_entity_id",
    "collector_service",
    "collector_host",
}


@dataclass(frozen=True)
class MetricPoint:
    ts: datetime
    metric: str
    value: float
    subject_type: str
    subject_id: str
    source_system: str
    collector_service: str
    collector_host: str
    source_instance: str = "default"
    source_entity_id: str | None = None
    host: str | None = None
    service: str | None = None
    tags: dict[str, str] | None = None


@dataclass(frozen=True)
class MetricSample:
    ts: datetime
    value: float
    host: str
    metric: str
    service: str | None
    subject_type: str | None = None
    subject_id: str | None = None
    source_system: str | None = None
    source_instance: str | None = None
    source_entity_id: str | None = None
    collector_service: str | None = None
    collector_host: str | None = None
    tags: dict[str, str] = field(default_factory=dict)


def normalize_subject_id(raw: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", raw.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized or _SUBJECT_ID_PATTERN.fullmatch(normalized) is None:
        raise ValueError(
            f"Invalid subject_id {raw!r}: expected lowercase snake_case identifier"
        )
    return normalized


def _normalize_dimension_value(
    raw: str | None,
    *,
    field_name: str,
    fallback: str | None = None,
) -> str:
    value = (raw or "").strip().lower()
    if not value:
        if fallback is not None:
            value = fallback
        else:
            raise ValueError(f"{field_name} is required")
    value = re.sub(r"\s+", "_", value)
    if _DIMENSION_VALUE_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f"Invalid {field_name} {raw!r}: expected lowercase token with [a-z0-9_.-]"
        )
    return value


def normalize_metric_point(item: MetricPoint) -> MetricPoint:
    metric_name = str(item.metric).strip()
    if not metric_name:
        raise ValueError("metric is required")

    subject_type = str(item.subject_type).strip().lower()
    if subject_type not in SubjectType.ALL:
        raise ValueError(
            f"Unknown subject_type {item.subject_type!r}; expected one of "
            f"{sorted(SubjectType.ALL)}"
        )

    subject_id = normalize_subject_id(str(item.subject_id))
    source_system = _normalize_dimension_value(
        item.source_system,
        field_name="source_system",
    )
    source_instance = _normalize_dimension_value(
        item.source_instance,
        field_name="source_instance",
        fallback="default",
    )
    collector_service = _normalize_dimension_value(
        item.collector_service,
        field_name="collector_service",
    )
    collector_host = _normalize_dimension_value(
        item.collector_host,
        field_name="collector_host",
    )
    source_entity_id = (
        str(item.source_entity_id).strip()
        if item.source_entity_id is not None
        else None
    )
    if source_entity_id == "":
        source_entity_id = None

    host: str | None = item.host
    if subject_type == SubjectType.HOST:
        if host is None or not str(host).strip():
            host = subject_id
        else:
            normalized_host = normalize_subject_id(str(host))
            if normalized_host != subject_id:
                raise ValueError(
                    "host compatibility tag must equal subject_id when subject_type=host"
                )
            host = normalized_host
    else:
        host = None

    service = str(item.service).strip() if item.service is not None else None
    if service == "":
        service = None

    normalized_tags = (
        {str(key): str(value) for key, value in item.tags.items()} if item.tags else {}
    )

    return MetricPoint(
        ts=_ensure_utc(item.ts),
        metric=metric_name,
        value=float(item.value),
        subject_type=subject_type,
        subject_id=subject_id,
        source_system=source_system,
        source_instance=source_instance,
        source_entity_id=source_entity_id,
        collector_service=collector_service,
        collector_host=collector_host,
        host=host,
        service=service,
        tags=normalized_tags,
    )


def _build_client() -> InfluxDBClient:
    if not settings.INFLUX_URL:
        raise InfluxConfigurationError("Influx setting missing: INFLUX_URL is required")

    token = settings.INFLUX_TOKEN
    org = settings.INFLUX_ORG
    if settings.INFLUX_API_MODE == "v3":
        token = token or "dev-influxdb3-token"
        org = org or settings.INFLUX_BUCKET or "graphyard"
    elif not token or not org:
        raise InfluxConfigurationError(
            "Influx settings missing: INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG are required"
        )

    return InfluxDBClient(
        url=settings.INFLUX_URL,
        token=token,
        org=org,
        timeout=settings.INFLUX_TIMEOUT_MS,
    )


def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def _flux_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _flux_record_access(key: str) -> str:
    # Use bracket access to safely handle arbitrary tag keys.
    return f'r["{_flux_escape(key)}"]'


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _sql_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise InfluxConfigurationError(f"Unsupported SQL identifier: {name}")
    return f'"{name}"'


def _fmt_flux_ts(ts: datetime) -> str:
    return _ensure_utc(ts).isoformat().replace("+00:00", "Z")


def write_points(points: list[MetricPoint]) -> int:
    if not points:
        return 0

    influx_points: list[Point] = []
    rejected_points = 0
    for item in points:
        try:
            normalized = normalize_metric_point(item)
        except ValueError as err:
            logger.error(
                "Metric point validation failed metric=%s subject_type=%s subject_id=%s: %s",
                item.metric,
                item.subject_type,
                item.subject_id,
                err,
            )
            rejected_points += 1
            continue

        point = Point(settings.INFLUX_MEASUREMENT)
        point = (
            point.tag("metric", normalized.metric)
            .tag("subject_type", normalized.subject_type)
            .tag("subject_id", normalized.subject_id)
            .tag("source_system", normalized.source_system)
            .tag("source_instance", normalized.source_instance)
            .tag("collector_service", normalized.collector_service)
            .tag("collector_host", normalized.collector_host)
        )
        if normalized.source_entity_id:
            point = point.tag("source_entity_id", normalized.source_entity_id)
        if normalized.host:
            point = point.tag("host", normalized.host)
        if normalized.service:
            point = point.tag("service", normalized.service)
        if normalized.tags:
            for key, value in normalized.tags.items():
                if key in _RESERVED_TAG_KEYS:
                    logger.warning(
                        "Ignoring custom tag override for reserved key %s", key
                    )
                    continue
                point = point.tag(str(key), str(value))

        point = point.field("value", float(normalized.value))
        point = point.time(_ensure_utc(normalized.ts))
        influx_points.append(point)

    if not influx_points:
        if rejected_points:
            logger.warning(
                "Dropped %s invalid metric points; nothing written", rejected_points
            )
        return 0

    with _build_client() as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(
            bucket=settings.INFLUX_BUCKET,
            org=settings.INFLUX_ORG,
            record=influx_points,
        )

    if rejected_points:
        logger.warning(
            "Skipped %s invalid metric points while writing %s valid points",
            rejected_points,
            len(influx_points),
        )

    return len(influx_points)


def query_range(
    metric_name: str,
    start: datetime,
    stop: datetime,
    *,
    host: str | None = None,
    service: str | None = None,
    subject_type: str | None = None,
    subject_id: str | None = None,
    tags: dict[str, str] | None = None,
) -> list[MetricSample]:
    mode = settings.INFLUX_API_MODE
    if mode == "v3":
        return _query_range_v3_sql(
            metric_name,
            start,
            stop,
            host=host,
            service=service,
            subject_type=subject_type,
            subject_id=subject_id,
            tags=tags,
        )

    if mode == "auto":
        try:
            return _query_range_v2_flux(
                metric_name,
                start,
                stop,
                host=host,
                service=service,
                subject_type=subject_type,
                subject_id=subject_id,
                tags=tags,
            )
        except Exception as err:  # noqa: BLE001
            if not _is_v2_query_missing(err):
                raise
            return _query_range_v3_sql(
                metric_name,
                start,
                stop,
                host=host,
                service=service,
                subject_type=subject_type,
                subject_id=subject_id,
                tags=tags,
            )

    return _query_range_v2_flux(
        metric_name,
        start,
        stop,
        host=host,
        service=service,
        subject_type=subject_type,
        subject_id=subject_id,
        tags=tags,
    )


def _is_v2_query_missing(err: Exception) -> bool:
    if isinstance(err, ApiException):
        return err.status == 404
    return False


def _query_range_v2_flux(
    metric_name: str,
    start: datetime,
    stop: datetime,
    *,
    host: str | None = None,
    service: str | None = None,
    subject_type: str | None = None,
    subject_id: str | None = None,
    tags: dict[str, str] | None = None,
) -> list[MetricSample]:
    clauses = [
        f'r._measurement == "{_flux_escape(settings.INFLUX_MEASUREMENT)}"',
        'r._field == "value"',
        f'{_flux_record_access("metric")} == "{_flux_escape(metric_name)}"',
    ]
    if host:
        clauses.append(f'{_flux_record_access("host")} == "{_flux_escape(host)}"')
    if service:
        clauses.append(f'{_flux_record_access("service")} == "{_flux_escape(service)}"')
    if subject_type:
        clauses.append(
            f'{_flux_record_access("subject_type")} == "{_flux_escape(subject_type)}"'
        )
    if subject_id:
        clauses.append(
            f'{_flux_record_access("subject_id")} == "{_flux_escape(subject_id)}"'
        )
    if tags:
        for key, value in tags.items():
            clauses.append(
                f'{_flux_record_access(str(key))} == "{_flux_escape(str(value))}"'
            )

    filter_clause = " and ".join(clauses)
    flux = (
        f'from(bucket: "{_flux_escape(settings.INFLUX_BUCKET)}")\n'
        f"  |> range(start: {_fmt_flux_ts(start)}, stop: {_fmt_flux_ts(stop)})\n"
        f"  |> filter(fn: (r) => {filter_clause})\n"
        '  |> sort(columns: ["_time"], desc: false)'
    )

    samples: list[MetricSample] = []
    with _build_client() as client:
        tables = client.query_api().query(query=flux, org=settings.INFLUX_ORG)

    for table in tables:
        for record in table.records:
            value = record.get_value()
            if value is None:
                continue
            record_time = record.get_time()
            if record_time is None:
                continue
            samples.append(
                MetricSample(
                    ts=_ensure_utc(record_time),
                    value=float(value),
                    host=str(record.values.get("host", "")),
                    metric=str(record.values.get("metric", metric_name)),
                    service=(
                        str(record.values.get("service"))
                        if record.values.get("service") is not None
                        else None
                    ),
                    subject_type=(
                        str(record.values.get("subject_type"))
                        if record.values.get("subject_type") is not None
                        else None
                    ),
                    subject_id=(
                        str(record.values.get("subject_id"))
                        if record.values.get("subject_id") is not None
                        else None
                    ),
                    source_system=(
                        str(record.values.get("source_system"))
                        if record.values.get("source_system") is not None
                        else None
                    ),
                    source_instance=(
                        str(record.values.get("source_instance"))
                        if record.values.get("source_instance") is not None
                        else None
                    ),
                    source_entity_id=(
                        str(record.values.get("source_entity_id"))
                        if record.values.get("source_entity_id") is not None
                        else None
                    ),
                    collector_service=(
                        str(record.values.get("collector_service"))
                        if record.values.get("collector_service") is not None
                        else None
                    ),
                    collector_host=(
                        str(record.values.get("collector_host"))
                        if record.values.get("collector_host") is not None
                        else None
                    ),
                    tags={
                        str(key): str(tag_value)
                        for key, tag_value in record.values.items()
                        if key
                        not in {
                            "result",
                            "table",
                            "_start",
                            "_stop",
                            "_time",
                            "_value",
                            "_field",
                            "_measurement",
                            "host",
                            "service",
                            "metric",
                            "subject_type",
                            "subject_id",
                            "source_system",
                            "source_instance",
                            "source_entity_id",
                            "collector_service",
                            "collector_host",
                        }
                    },
                )
            )

    return samples


def _query_range_v3_sql(
    metric_name: str,
    start: datetime,
    stop: datetime,
    *,
    host: str | None = None,
    service: str | None = None,
    subject_type: str | None = None,
    subject_id: str | None = None,
    tags: dict[str, str] | None = None,
) -> list[MetricSample]:
    where_clauses = [
        f"time >= '{_sql_escape(_fmt_flux_ts(start))}'",
        f"time < '{_sql_escape(_fmt_flux_ts(stop))}'",
        f"metric = '{_sql_escape(metric_name)}'",
    ]
    if host:
        where_clauses.append(f"host = '{_sql_escape(host)}'")
    if service:
        where_clauses.append(f"service = '{_sql_escape(service)}'")
    if subject_type:
        where_clauses.append(f"subject_type = '{_sql_escape(subject_type)}'")
    if subject_id:
        where_clauses.append(f"subject_id = '{_sql_escape(subject_id)}'")
    if tags:
        for key, tag_value in tags.items():
            where_clauses.append(
                f"{_sql_identifier(str(key))} = '{_sql_escape(str(tag_value))}'"
            )

    measurement = _sql_identifier(settings.INFLUX_MEASUREMENT)
    sql = (
        "select time, value, host, metric, service, "
        "subject_type, subject_id, source_system, source_instance, "
        "source_entity_id, collector_service, collector_host "
        f"from {measurement} "
        f"where {' and '.join(where_clauses)} "
        "order by time asc"
    )

    headers = {"Accept": "application/json"}
    if settings.INFLUX_TOKEN:
        headers["Authorization"] = f"Bearer {settings.INFLUX_TOKEN}"

    payload = {"db": settings.INFLUX_BUCKET, "q": sql}
    try:
        response = httpx.post(
            f"{settings.INFLUX_URL.rstrip('/')}/api/v3/query_sql",
            json=payload,
            headers=headers,
            timeout=max(1.0, settings.INFLUX_TIMEOUT_MS / 1000),
        )
    except Exception as err:  # noqa: BLE001
        raise InfluxConfigurationError(str(err)) from err

    if response.status_code == 404:
        raise InfluxConfigurationError(
            "InfluxDB v3 SQL endpoint not found. Set INFLUX_API_MODE=v2 for InfluxDB 2."
        )
    response.raise_for_status()

    raw_rows = response.json()
    if not isinstance(raw_rows, list):
        raise InfluxConfigurationError("Invalid response from InfluxDB v3 query API")

    samples: list[MetricSample] = []
    for row in raw_rows:
        if not isinstance(row, dict):
            continue

        row_value = row.get("value")
        row_time = row.get("time")
        if row_value is None or row_time is None:
            continue

        try:
            ts = _parse_influx_ts(str(row_time))
            float_value = float(str(row_value))
        except (TypeError, ValueError):
            continue

        samples.append(
            MetricSample(
                ts=ts,
                value=float_value,
                host=str(row.get("host", "")),
                metric=str(row.get("metric", metric_name)),
                service=(
                    str(row.get("service")) if row.get("service") is not None else None
                ),
                subject_type=(
                    str(row.get("subject_type"))
                    if row.get("subject_type") is not None
                    else None
                ),
                subject_id=(
                    str(row.get("subject_id"))
                    if row.get("subject_id") is not None
                    else None
                ),
                source_system=(
                    str(row.get("source_system"))
                    if row.get("source_system") is not None
                    else None
                ),
                source_instance=(
                    str(row.get("source_instance"))
                    if row.get("source_instance") is not None
                    else None
                ),
                source_entity_id=(
                    str(row.get("source_entity_id"))
                    if row.get("source_entity_id") is not None
                    else None
                ),
                collector_service=(
                    str(row.get("collector_service"))
                    if row.get("collector_service") is not None
                    else None
                ),
                collector_host=(
                    str(row.get("collector_host"))
                    if row.get("collector_host") is not None
                    else None
                ),
                tags={},
            )
        )

    return samples


def _parse_influx_ts(raw: str) -> datetime:
    normalized = raw.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def query_condition_window(
    condition: ConditionDefinition,
    *,
    now: datetime | None = None,
) -> list[MetricSample]:
    now_utc = _ensure_utc(now or datetime.now(UTC))
    lookback_minutes = max(condition.window_minutes, condition.breach_minutes)
    start = now_utc - timedelta(minutes=lookback_minutes)
    tags = {k: str(v) for k, v in condition.tags_filter.items()}
    if condition.subject_type_filter:
        tags["subject_type"] = str(condition.subject_type_filter).strip().lower()
    if condition.subject_id_filter:
        tags["subject_id"] = normalize_subject_id(condition.subject_id_filter)

    host_filter: str | None = condition.host_filter or None
    if host_filter and condition.subject_type_filter:
        if str(condition.subject_type_filter).strip().lower() != SubjectType.HOST:
            logger.warning(
                "Ignoring host_filter for non-host condition id=%s name=%s",
                condition.id,
                condition.name,
            )
            host_filter = None

    return query_range(
        condition.metric_name,
        start,
        now_utc,
        host=host_filter,
        service=condition.service_filter or None,
        tags=tags,
    )


def check_health() -> dict[str, Any]:
    with _build_client() as client:
        health = client.health()

    return {
        "status": str(health.status).lower(),
        "message": health.message,
        "name": health.name,
    }
