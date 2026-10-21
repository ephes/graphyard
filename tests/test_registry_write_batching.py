"""Registry writes per ingest request stay bounded by distinct rows, not points.

Regression for bound-token ingest issuing one ``UPDATE service_registry`` per
point, each its own SQLite commit, which made a 50-point batch take seconds.
"""

from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace

import pytest
from django.db import connection
from django.urls import reverse
from django.utils import timezone

from graphyard import services
from graphyard.models import (
    HostRegistry,
    IngestToken,
    ServiceRegistry,
    SubjectRegistry,
)

pytestmark = pytest.mark.django_db

REGISTRY_TABLES = ("subject_registry", "host_registry", "service_registry")
SERVICES = [f"svc{idx}" for idx in range(5)]


@pytest.fixture(autouse=True)
def writes(monkeypatch):
    monkeypatch.setattr("graphyard.views.write_points", lambda points: len(points))
    monkeypatch.setattr("graphyard.views._record_heartbeat_safe", lambda *a, **k: None)


@pytest.fixture
def registry_sql(monkeypatch):
    """Record registry SQL and the registry transaction each write ran in."""
    state = {"depth": 0, "transactions": 0}
    log: list[dict[str, object]] = []
    real_atomic = services.transaction.atomic

    @contextmanager
    def atomic(*args, **kwargs):
        with real_atomic(*args, **kwargs):
            if state["depth"] == 0:
                state["transactions"] += 1
            state["depth"] += 1
            try:
                yield
            finally:
                state["depth"] -= 1

    monkeypatch.setattr(services, "transaction", SimpleNamespace(atomic=atomic))

    def wrapper(execute, sql, params, many, context):
        if any(f'"{table}"' in sql for table in REGISTRY_TABLES):
            verb = sql.lstrip().split(" ", 1)[0].upper()
            log.append(
                {
                    "verb": verb,
                    "sql": sql,
                    "txn": state["transactions"] if state["depth"] else None,
                }
            )
        return execute(sql, params, many, context)

    with connection.execute_wrapper(wrapper):
        yield log


def writes_in(log):
    return [entry for entry in log if entry["verb"] in {"INSERT", "UPDATE", "DELETE"}]


def make_token(binding):
    row = IngestToken(name="producer", bound_subject_id=binding)
    row.set_token("batching-test-credential")
    row.save()
    return row


def batch(size=50):
    now = timezone.now().isoformat()
    return [
        {
            "ts": now,
            "host": "edge",
            "metric": f"m{idx}",
            "value": idx,
            "service": SERVICES[idx % len(SERVICES)],
        }
        for idx in range(size)
    ]


def post(client, payload):
    return client.post(
        reverse("graphyard:metrics_ingest"),
        data=payload,
        content_type="application/json",
        HTTP_AUTHORIZATION="Bearer batching-test-credential",
    )


def age_registry(delta):
    for model in (SubjectRegistry, HostRegistry, ServiceRegistry):
        model.objects.update(last_seen_at=timezone.now() - delta)


@pytest.mark.parametrize("binding", ["edge", None])
def test_fifty_point_batch_writes_each_registry_row_once(client, registry_sql, binding):
    make_token(binding)

    assert post(client, batch()).status_code == 202
    first = writes_in(registry_sql)
    # One subject, one host, five services: one INSERT each, no UPDATEs.
    assert [entry["verb"] for entry in first] == ["INSERT"] * 7
    assert {entry["txn"] for entry in first} == {1}
    assert ServiceRegistry.objects.filter(host__host_id="edge").count() == 5

    registry_sql.clear()
    assert post(client, batch()).status_code == 202
    # Steady state: reads only, no write lock. The bound path's ownership
    # check adds one read in the view.
    assert writes_in(registry_sql) == []
    assert len(registry_sql) == (4 if binding else 3)


@pytest.mark.parametrize("binding", ["edge", None])
def test_stale_last_seen_refresh_is_one_update_per_table(client, registry_sql, binding):
    make_token(binding)
    assert post(client, batch()).status_code == 202
    age_registry(timedelta(minutes=5))
    old = timezone.now() - timedelta(minutes=4)

    registry_sql.clear()
    assert post(client, batch()).status_code == 202
    refresh = writes_in(registry_sql)
    assert [entry["verb"] for entry in refresh] == ["UPDATE"] * 3
    assert len({entry["txn"] for entry in refresh}) == 1
    assert None not in {entry["txn"] for entry in refresh}
    for model in (SubjectRegistry, HostRegistry, ServiceRegistry):
        assert not model.objects.filter(last_seen_at__lt=old).exists()


def test_recent_last_seen_is_not_rewritten(client, registry_sql):
    make_token("edge")
    assert post(client, batch()).status_code == 202
    age_registry(services.REGISTRY_LAST_SEEN_UPDATE_INTERVAL / 2)
    before = list(ServiceRegistry.objects.values_list("pk", "last_seen_at"))

    registry_sql.clear()
    assert post(client, batch()).status_code == 202
    assert writes_in(registry_sql) == []
    assert list(ServiceRegistry.objects.values_list("pk", "last_seen_at")) == before


def test_bound_stale_refresh_skips_service_taken_after_validation(client, monkeypatch):
    make_token("edge")
    victim = HostRegistry.objects.create(host_id="victim")
    old = timezone.now() - timedelta(days=1)
    service = ServiceRegistry.objects.create(service_id="mail", last_seen_at=old)

    def write(points):
        ServiceRegistry.objects.filter(pk=service.pk).update(host=victim)
        return len(points)

    monkeypatch.setattr("graphyard.views.write_points", write)
    # Non-host subject without a registered collector host: no re-parent
    # target, so only the throttled last_seen refresh would run.
    payload = [
        {
            "ts": timezone.now().isoformat(),
            "host": None,
            "subject_type": "service",
            "subject_id": "edge",
            "collector_host": "edge",
            "metric": "m",
            "value": 1,
            "service": "mail",
        }
    ]
    assert post(client, payload).status_code == 202
    service.refresh_from_db()
    assert service.host == victim
    assert service.last_seen_at == old


def test_batch_keeps_last_source_system_and_reparents_to_last_host():
    make_token(None)
    other = HostRegistry.objects.create(host_id="other")
    ServiceRegistry.objects.create(service_id="mail", host=other)
    now = timezone.now()
    points = [
        services.influx.MetricPoint(
            ts=now,
            metric="m",
            value=1.0,
            subject_type="host",
            subject_id=host,
            collector_service="vector",
            collector_host=host,
            host=host,
            source_system=source,
            service="mail",
        )
        for host, source in [("other", "a"), ("edge", "b"), ("edge", "c")]
    ]

    services.touch_registry_from_points(points)

    assert ServiceRegistry.objects.get(service_id="mail").host.host_id == "edge"
    edge = SubjectRegistry.objects.get(subject_type="host", subject_id="edge")
    assert edge.source_system == "c"
    assert SubjectRegistry.objects.get(subject_id="other").source_system == "a"


def sensor_point(service, collector):
    return services.influx.MetricPoint(
        ts=timezone.now(),
        metric="m",
        value=1.0,
        subject_type="environment_sensor",
        subject_id="office",
        source_system="homeassistant",
        collector_service="graphyard-agent",
        collector_host=collector,
        service=service,
    )


def host_point(host, service=None):
    return services.influx.MetricPoint(
        ts=timezone.now(),
        metric="m",
        value=1.0,
        subject_type="host",
        subject_id=host,
        source_system="vector",
        collector_service="vector",
        collector_host=host,
        host=host,
        service=service,
    )


@pytest.mark.parametrize(
    "order,expected",
    [
        # The collector host is registered only by a later point: no re-parent.
        ("collector_first", "other"),
        # Registered by an earlier point: the collector point re-parents.
        ("host_first", "edge"),
    ],
)
def test_collector_host_resolves_at_its_position_in_the_batch(order, expected):
    other = HostRegistry.objects.create(host_id="other")
    ServiceRegistry.objects.create(service_id="mail", host=other)
    collector, host = sensor_point("mail", "edge"), host_point("edge")
    points = [collector, host] if order == "collector_first" else [host, collector]

    services.touch_registry_from_points(points)

    assert ServiceRegistry.objects.get(service_id="mail").host.host_id == expected


def test_new_service_from_unknown_collector_host_has_no_host():
    services.touch_registry_from_points(
        [sensor_point("mail", "edge"), host_point("edge")]
    )
    assert ServiceRegistry.objects.get(service_id="mail").host is None
