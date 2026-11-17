"""Apply/prune uses one authoritative transaction, with synthetic split routing."""

import json
from contextlib import ExitStack
from io import StringIO

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import OperationalError, connections, router
from django.db.models.query import QuerySet
from django.test.utils import CaptureQueriesContext

from graphyard.models import MetricCollectionSpec

pytestmark = pytest.mark.django_db(
    transaction=True, databases=["default", "secondary", "spec_writer"]
)


class SplitRouter:
    def db_for_read(self, model, **hints):
        return "secondary" if model is MetricCollectionSpec else None

    def db_for_write(self, model, **hints):
        return "spec_writer" if model is MetricCollectionSpec else None


def desired(name, interval=60):
    return {
        "name": name,
        "spec_type": "http_page_probe",
        "interval_seconds": interval,
        "config": {"url": "https://example.test/"},
    }


def apply(tmp_path, specs, output):
    path = tmp_path / "synthetic.json"
    path.write_text(json.dumps(specs))
    call_command(
        "apply_metric_collection_specs", file=str(path), prune=True, stdout=output
    )


def snapshot(alias):
    return list(MetricCollectionSpec.objects.using(alias).values())


@pytest.mark.parametrize("first", ["create", "update"])
def test_second_definition_validation_rolls_back_writer(tmp_path, monkeypatch, first):
    writer = MetricCollectionSpec.objects.using("spec_writer")
    writer.create(**desired("keep-on-failure"))
    if first == "update":
        target = writer.create(**desired("first", interval=300), next_run_time=123)
        # Baseline must reach the second definition even through its stale read route.
        MetricCollectionSpec.objects.using("secondary").create(
            **writer.filter(pk=target.pk).values().get()
        )
        MetricCollectionSpec.objects.using("secondary").filter(pk=target.pk).update(
            created_at=target.created_at, updated_at=target.updated_at
        )
    before = {alias: snapshot(alias) for alias in connections}
    monkeypatch.setattr(router, "routers", [SplitRouter()])
    output = StringIO()
    invalid = desired("invalid-second")
    invalid["config"]["verify_tls"] = "false"
    with pytest.raises(CommandError, match="index 1 has invalid config.verify_tls"):
        apply(tmp_path, [desired("first"), invalid], output)
    assert output.getvalue() == ""
    assert {alias: snapshot(alias) for alias in connections} == before


def test_valid_apply_reads_updates_and_prunes_only_writer(tmp_path, monkeypatch):
    writer = MetricCollectionSpec.objects.using("spec_writer")
    writer.create(**desired("updated", interval=300), next_run_time=123)
    writer.create(**desired("unchanged"), next_run_time=456)
    writer.create(**desired("writer-stale"))
    MetricCollectionSpec.objects.using("secondary").create(
        **desired("updated", interval=120)
    )
    MetricCollectionSpec.objects.using("secondary").create(**desired("replica-only"))
    MetricCollectionSpec.objects.using("default").create(**desired("default-only"))
    monkeypatch.setattr(router, "routers", [SplitRouter()])
    output = StringIO()
    with ExitStack() as stack:
        untouched = [
            stack.enter_context(CaptureQueriesContext(connections[alias]))
            for alias in ("default", "secondary")
        ]
        apply(
            tmp_path,
            [desired(name) for name in ("created", "updated", "unchanged")],
            output,
        )
    assert all(len(queries) == 0 for queries in untouched)
    assert set(writer.values_list("name", flat=True)) == {
        "created",
        "updated",
        "unchanged",
    }
    assert writer.get(name="updated").interval_seconds == 60
    assert writer.get(name="updated").next_run_time == 0
    assert writer.get(name="unchanged").next_run_time == 456
    assert (
        "summary total=3 created=1 updated=1 unchanged=1 deleted=1 changed=3"
        in output.getvalue()
    )


@pytest.mark.parametrize("failure", ["prune", "commit"])
def test_late_writer_failure_rolls_back_without_success_output(
    tmp_path, monkeypatch, failure
):
    writer = MetricCollectionSpec.objects.using("spec_writer")
    writer.create(**desired("stale-a"))
    writer.create(**desired("stale-b"))
    before = snapshot("spec_writer")
    monkeypatch.setattr(router, "routers", [SplitRouter()])
    triggered = []
    if failure == "prune":
        original_delete = QuerySet.delete

        def fail_second_delete(rows):
            if rows.model is MetricCollectionSpec:
                triggered.append(rows.db)
                if len(triggered) == 2:
                    raise OperationalError("synthetic prune failure")
            return original_delete(rows)

        monkeypatch.setattr(QuerySet, "delete", fail_second_delete)
    else:

        def fail_commit():
            triggered.append("spec_writer")
            raise OperationalError("synthetic commit failure")

        monkeypatch.setattr(connections["spec_writer"], "commit", fail_commit)
    output = StringIO()
    with pytest.raises(CommandError, match="storage failure"):
        apply(tmp_path, [desired("created-before-failure")], output)
    assert triggered == ["spec_writer"] * (2 if failure == "prune" else 1)
    assert snapshot("spec_writer") == before
    assert snapshot("default") == snapshot("secondary") == []
    assert output.getvalue() == ""
