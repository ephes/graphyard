from __future__ import annotations

from django.contrib.auth.hashers import check_password, make_password
from django.core.exceptions import ValidationError
from django.db import models
from django.utils import timezone


class StatusLevel:
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"

    CHOICES = [
        (OK, "OK"),
        (WARNING, "Warning"),
        (CRITICAL, "Critical"),
    ]


class IngestToken(models.Model):
    name = models.CharField(max_length=128, unique=True)
    token_hash = models.CharField(max_length=255)
    enabled = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)
    revoked_at = models.DateTimeField(blank=True, null=True)
    last_used_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "ingest_token"
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name

    def set_token(self, token: str) -> None:
        self.token_hash = make_password(token)

    def check_token(self, token: str) -> bool:
        return check_password(token, self.token_hash)

    def mark_used(self) -> None:
        self.last_used_at = timezone.now()
        self.save(update_fields=["last_used_at"])

    def revoke(self) -> None:
        self.enabled = False
        self.revoked_at = timezone.now()
        self.save(update_fields=["enabled", "revoked_at"])


class HostRegistry(models.Model):
    host_id = models.CharField(max_length=128, unique=True)
    display_name = models.CharField(max_length=255, blank=True)
    grafana_url = models.URLField(blank=True)
    enabled = models.BooleanField(default=True)
    last_seen_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "host_registry"
        verbose_name_plural = "host registry"
        ordering = ["host_id"]

    def __str__(self) -> str:
        return self.display_name or self.host_id


class ServiceRegistry(models.Model):
    service_id = models.CharField(max_length=128, unique=True)
    host = models.ForeignKey(
        HostRegistry,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        related_name="services",
    )
    display_name = models.CharField(max_length=255, blank=True)
    grafana_url = models.URLField(blank=True)
    enabled = models.BooleanField(default=True)
    last_seen_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = "service_registry"
        verbose_name_plural = "service registry"
        ordering = ["service_id"]

    def __str__(self) -> str:
        return self.display_name or self.service_id


class MetricCollectionSpecType:
    HOME_ASSISTANT_SENSOR = "home_assistant_sensor"
    HOME_ASSISTANT_ENV_SCAN = "home_assistant_env_scan"
    HTTP_JSON_METRIC = "http_json_metric"

    CHOICES = [
        (HOME_ASSISTANT_SENSOR, "Home Assistant Sensor"),
        (HOME_ASSISTANT_ENV_SCAN, "Home Assistant Env Scan"),
        (HTTP_JSON_METRIC, "HTTP JSON Metric"),
    ]


class MetricCollectionSpec(models.Model):
    name = models.CharField(max_length=255, unique=True)
    enabled = models.BooleanField(default=True)
    spec_type = models.CharField(
        max_length=64,
        choices=MetricCollectionSpecType.CHOICES,
        default=MetricCollectionSpecType.HOME_ASSISTANT_SENSOR,
    )
    interval_seconds = models.PositiveIntegerField(default=60)
    next_run_time = models.PositiveIntegerField(
        default=0,
        help_text="Unix timestamp for next scheduled execution",
    )
    config = models.JSONField(default=dict, blank=True)

    last_run_at = models.DateTimeField(blank=True, null=True)
    last_status = models.CharField(
        max_length=16,
        choices=StatusLevel.CHOICES,
        default=StatusLevel.WARNING,
    )
    last_error = models.TextField(blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "metric_collection_spec"
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class ComparisonOperator:
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"

    CHOICES = [
        (GT, ">"),
        (GTE, ">="),
        (LT, "<"),
        (LTE, "<="),
    ]


class ConditionDefinition(models.Model):
    name = models.CharField(max_length=255, unique=True)
    enabled = models.BooleanField(default=True)

    metric_name = models.CharField(max_length=255)
    host_filter = models.CharField(max_length=128, blank=True)
    service_filter = models.CharField(max_length=128, blank=True)
    tags_filter = models.JSONField(default=dict, blank=True)

    operator = models.CharField(
        max_length=8,
        choices=ComparisonOperator.CHOICES,
        default=ComparisonOperator.GT,
    )
    warning_threshold = models.FloatField(blank=True, null=True)
    critical_threshold = models.FloatField(blank=True, null=True)
    window_minutes = models.PositiveIntegerField(default=30)
    breach_minutes = models.PositiveIntegerField(default=5)

    status = models.CharField(
        max_length=16,
        choices=StatusLevel.CHOICES,
        default=StatusLevel.OK,
    )
    last_evaluated = models.DateTimeField(blank=True, null=True)
    message = models.TextField(blank=True)
    last_value = models.FloatField(blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "condition_definition"
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name

    def clean(self) -> None:
        super().clean()
        if self.warning_threshold is None and self.critical_threshold is None:
            raise ValidationError(
                "ConditionDefinition requires warning_threshold or critical_threshold"
            )


class PipelineHeartbeat(models.Model):
    name = models.CharField(max_length=128, unique=True)
    status = models.CharField(
        max_length=16,
        choices=StatusLevel.CHOICES,
        default=StatusLevel.WARNING,
    )
    last_success = models.DateTimeField(blank=True, null=True)
    last_error = models.TextField(blank=True)
    details = models.JSONField(default=dict, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pipeline_heartbeat"
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name
