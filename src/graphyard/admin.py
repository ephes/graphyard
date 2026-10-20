from __future__ import annotations

import copy
from typing import Any

from django.contrib import admin
from django import forms
from django.forms import ModelForm

from .models import (
    ConditionDefinition,
    HostRegistry,
    IngestToken,
    IngestTokenHostUsage,
    MetricCollectionSpec,
    PipelineHeartbeat,
    ServiceRegistry,
    SubjectRegistry,
)
from .token_scope import is_scoped, is_valid_stored_scope, parse_host_scope_text

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


class IngestTokenAdminForm(ModelForm):
    host_scope = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"rows": 4, "cols": 60}),
        help_text=(
            "Host identifiers this token may write metrics for, separated by "
            "commas, spaces or new lines. Exact names, or a prefix with one "
            "trailing '*' (for example 'web-*'). Empty means unrestricted. "
            "Checked against subject_id and collector_host of every point."
        ),
    )

    class Meta:
        model = IngestToken
        fields = "__all__"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        scope = self.instance.host_scope if self.instance else None
        if isinstance(scope, list):
            self.initial["host_scope"] = "\n".join(str(entry) for entry in scope)

    def clean_host_scope(self) -> list[str]:
        return parse_host_scope_text(self.cleaned_data.get("host_scope") or "")


@admin.register(IngestToken)
class IngestTokenAdmin(admin.ModelAdmin):
    form = IngestTokenAdminForm
    list_display = (
        "id",
        "name",
        "enabled",
        "host_scope_display",
        "bound_subject_id",
        "last_used_at",
        "created_at",
        "revoked_at",
    )
    search_fields = ("name",)
    list_filter = ("enabled",)
    readonly_fields = ("token_hash", "created_at", "last_used_at", "revoked_at")

    @admin.display(description="Host scope")
    def host_scope_display(self, obj: IngestToken) -> str:
        scope = obj.host_scope
        if not is_scoped(scope):
            return "unrestricted"
        if not is_valid_stored_scope(scope):
            return "invalid (allows nothing)"
        return ", ".join(str(entry) for entry in scope)


@admin.register(IngestTokenHostUsage)
class IngestTokenHostUsageAdmin(admin.ModelAdmin):
    list_display = ("token", "host_id", "first_seen_at", "last_seen_at")
    search_fields = ("token__name", "host_id")
    list_select_related = ("token",)

    def has_add_permission(self, request: Any) -> bool:
        return False

    def has_change_permission(self, request: Any, obj: Any = None) -> bool:
        return False


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
        "stale_after_seconds",
        "alert_when_stale",
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
    list_filter = ("enabled", "status", "operator", "alert_when_stale")


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
