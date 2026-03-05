from __future__ import annotations

import copy
from typing import Any

from django.contrib import admin
from django.forms import ModelForm

from .models import (
    ConditionDefinition,
    HostRegistry,
    IngestToken,
    MetricCollectionSpec,
    PipelineHeartbeat,
    ServiceRegistry,
    SubjectRegistry,
)

_SECRET_CONFIG_KEYS = {
    "access_token",
    "bearer_token",
    "basic_password",
    "password",
    "token",
    "api_key",
}


class MetricCollectionSpecAdminForm(ModelForm):
    REDACTED = "********"

    class Meta:
        model = MetricCollectionSpec
        fields = "__all__"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._existing_config: dict[str, Any] = {}
        if (
            self.instance
            and self.instance.pk
            and isinstance(self.instance.config, dict)
        ):
            self._existing_config = copy.deepcopy(self.instance.config)
            self.initial["config"] = self._redact_config(self.instance.config)
            self.fields["config"].help_text = (
                "Sensitive keys are masked as ******** in existing specs. "
                "Leave masked values unchanged to keep existing secrets."
            )

    @classmethod
    def _redact_config(cls, config: dict[str, Any]) -> dict[str, Any]:
        redacted: dict[str, Any] = copy.deepcopy(config)
        for key in _SECRET_CONFIG_KEYS:
            if key in redacted and redacted[key]:
                redacted[key] = cls.REDACTED
        return redacted

    def clean_config(self) -> dict[str, Any]:
        config = self.cleaned_data["config"]
        if not isinstance(config, dict):
            return config

        merged = copy.deepcopy(config)
        for key in _SECRET_CONFIG_KEYS:
            if merged.get(key) == self.REDACTED and key in self._existing_config:
                merged[key] = self._existing_config[key]
        return merged


@admin.register(IngestToken)
class IngestTokenAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "enabled", "last_used_at", "created_at", "revoked_at")
    search_fields = ("name",)
    list_filter = ("enabled",)
    readonly_fields = ("token_hash", "created_at", "last_used_at", "revoked_at")


@admin.register(HostRegistry)
class HostRegistryAdmin(admin.ModelAdmin):
    list_display = ("host_id", "display_name", "enabled", "last_seen_at")
    search_fields = ("host_id", "display_name")
    list_filter = ("enabled",)


@admin.register(ServiceRegistry)
class ServiceRegistryAdmin(admin.ModelAdmin):
    list_display = ("service_id", "display_name", "host", "enabled", "last_seen_at")
    search_fields = ("service_id", "display_name")
    list_filter = ("enabled",)


@admin.register(SubjectRegistry)
class SubjectRegistryAdmin(admin.ModelAdmin):
    list_display = (
        "subject_type",
        "subject_id",
        "display_name",
        "source_system",
        "last_seen_at",
    )
    search_fields = ("subject_type", "subject_id", "display_name", "source_system")


@admin.register(ConditionDefinition)
class ConditionDefinitionAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "enabled",
        "metric_name",
        "status",
        "last_evaluated",
        "updated_at",
    )
    search_fields = (
        "name",
        "metric_name",
        "host_filter",
        "subject_type_filter",
        "subject_id_filter",
        "service_filter",
    )
    list_filter = ("enabled", "status", "operator")


@admin.register(MetricCollectionSpec)
class MetricCollectionSpecAdmin(admin.ModelAdmin):
    form = MetricCollectionSpecAdminForm
    list_display = (
        "id",
        "name",
        "spec_type",
        "enabled",
        "interval_seconds",
        "next_run_time",
        "last_status",
        "last_run_at",
    )
    search_fields = ("name", "spec_type")
    list_filter = ("enabled", "spec_type", "last_status")


@admin.register(PipelineHeartbeat)
class PipelineHeartbeatAdmin(admin.ModelAdmin):
    list_display = ("name", "status", "last_success", "updated_at")
    search_fields = ("name",)
    list_filter = ("status",)
    readonly_fields = ("updated_at",)
