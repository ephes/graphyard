from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import re
from typing import TYPE_CHECKING, Any

from django.conf import settings
import httpx
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

if TYPE_CHECKING:
    from .models import ConditionDefinition


class InfluxConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True)
class MetricPoint:
    ts: datetime
    host: str
    metric: str
    value: float
    service: str | None = None
    tags: dict[str, str] | None = None


@dataclass(frozen=True)
class MetricSample:
    ts: datetime
    value: float
    host: str
    metric: str
    service: str | None
    tags: dict[str, str]


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
    for item in points:
        point = Point(settings.INFLUX_MEASUREMENT)
        point = point.tag("host", item.host).tag("metric", item.metric)
        if item.service:
            point = point.tag("service", item.service)
        if item.tags:
            for key, value in item.tags.items():
                point = point.tag(str(key), str(value))

        point = point.field("value", float(item.value))
        point = point.time(_ensure_utc(item.ts))
        influx_points.append(point)

    with _build_client() as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(
            bucket=settings.INFLUX_BUCKET,
            org=settings.INFLUX_ORG,
            record=influx_points,
        )

    return len(influx_points)


def query_range(
    metric_name: str,
    start: datetime,
    stop: datetime,
    *,
    host: str | None = None,
    service: str | None = None,
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
                tags=tags,
            )

    return _query_range_v2_flux(
        metric_name,
        start,
        stop,
        host=host,
        service=service,
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
    if tags:
        for key, tag_value in tags.items():
            where_clauses.append(
                f"{_sql_identifier(str(key))} = '{_sql_escape(str(tag_value))}'"
            )

    measurement = _sql_identifier(settings.INFLUX_MEASUREMENT)
    sql = (
        "select time, value, host, metric, service "
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
    return query_range(
        condition.metric_name,
        start,
        now_utc,
        host=condition.host_filter or None,
        service=condition.service_filter or None,
        tags={k: str(v) for k, v in condition.tags_filter.items()},
    )


def check_health() -> dict[str, Any]:
    with _build_client() as client:
        health = client.health()

    return {
        "status": str(health.status).lower(),
        "message": health.message,
        "name": health.name,
    }
