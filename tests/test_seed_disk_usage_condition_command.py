from __future__ import annotations

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard.models import ComparisonOperator, ConditionDefinition, SubjectType


@pytest.mark.django_db
def test_seed_disk_usage_condition_creates_default_host_mountpoint_condition():
    call_command("seed_disk_usage_condition", "--host", "macmini")

    condition = ConditionDefinition.objects.get()
    assert condition.name == "Host filesystem usage (macmini /)"
    assert condition.metric_name == "host.filesystem_used_ratio"
    assert condition.host_filter == "macmini"
    assert condition.subject_type_filter == SubjectType.HOST
    assert condition.subject_id_filter == "macmini"
    assert condition.tags_filter == {"mountpoint": "/"}
    assert condition.operator == ComparisonOperator.GTE
    assert condition.warning_threshold == 0.80
    assert condition.critical_threshold == 0.90
    assert condition.window_minutes == 30
    assert condition.breach_minutes == 5
    assert condition.enabled is True


@pytest.mark.django_db
def test_seed_disk_usage_condition_updates_existing_condition():
    condition = ConditionDefinition.objects.create(
        name="Root filesystem usage",
        enabled=False,
        metric_name="legacy.metric",
        host_filter="old",
        subject_type_filter="",
        subject_id_filter="",
        service_filter="legacy",
        tags_filter={},
        operator=ComparisonOperator.GT,
        warning_threshold=0.70,
        critical_threshold=0.85,
        window_minutes=15,
        breach_minutes=3,
    )

    call_command(
        "seed_disk_usage_condition",
        "--name",
        "Root filesystem usage",
        "--host",
        "macmini",
        "--mountpoint",
        "/",
        "--warning-threshold",
        "0.81",
        "--critical-threshold",
        "0.91",
    )

    condition.refresh_from_db()
    assert condition.metric_name == "host.filesystem_used_ratio"
    assert condition.host_filter == "macmini"
    assert condition.subject_type_filter == SubjectType.HOST
    assert condition.subject_id_filter == "macmini"
    assert condition.service_filter == ""
    assert condition.tags_filter == {"mountpoint": "/"}
    assert condition.operator == ComparisonOperator.GTE
    assert condition.warning_threshold == 0.81
    assert condition.critical_threshold == 0.91
    assert condition.enabled is True


@pytest.mark.django_db
def test_seed_disk_usage_condition_allows_host_only_scope_without_mountpoint():
    call_command(
        "seed_disk_usage_condition",
        "--host",
        "macmini",
        "--no-mountpoint-filter",
    )

    condition = ConditionDefinition.objects.get()
    assert condition.name == "Host filesystem usage (macmini *)"
    assert condition.tags_filter == {}


def test_seed_disk_usage_condition_rejects_invalid_thresholds():
    with pytest.raises(CommandError, match="warning-threshold must be between 0 and 1"):
        call_command(
            "seed_disk_usage_condition",
            "--host",
            "macmini",
            "--warning-threshold",
            "1.2",
        )

    with pytest.raises(
        CommandError,
        match="--warning-threshold must be less than or equal to --critical-threshold",
    ):
        call_command(
            "seed_disk_usage_condition",
            "--host",
            "macmini",
            "--warning-threshold",
            "0.95",
            "--critical-threshold",
            "0.90",
        )


def test_seed_disk_usage_condition_rejects_invalid_window_and_breach():
    with pytest.raises(CommandError, match="--window-minutes must be greater than 0"):
        call_command(
            "seed_disk_usage_condition",
            "--host",
            "macmini",
            "--window-minutes",
            "0",
        )

    with pytest.raises(CommandError, match="--breach-minutes must be greater than 0"):
        call_command(
            "seed_disk_usage_condition",
            "--host",
            "macmini",
            "--breach-minutes",
            "0",
        )

    with pytest.raises(
        CommandError,
        match="--breach-minutes must be less than or equal to --window-minutes",
    ):
        call_command(
            "seed_disk_usage_condition",
            "--host",
            "macmini",
            "--window-minutes",
            "5",
            "--breach-minutes",
            "6",
        )


@pytest.mark.django_db
def test_seed_disk_usage_condition_different_scope_creates_new_condition():
    call_command("seed_disk_usage_condition", "--host", "macmini", "--mountpoint", "/")
    call_command(
        "seed_disk_usage_condition",
        "--host",
        "macmini",
        "--mountpoint",
        "/data",
    )

    conditions = list(ConditionDefinition.objects.all())
    assert len(conditions) == 2
    mountpoints = {item.tags_filter.get("mountpoint") for item in conditions}
    assert mountpoints == {"/", "/data"}
