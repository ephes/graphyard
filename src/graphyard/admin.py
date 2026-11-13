from __future__ import annotations

import copy
import json
from typing import Any

from django.contrib import admin
from django import forms
from django.forms import ModelForm
from django.utils.crypto import constant_time_compare, salted_hmac
from django.db import connections, router, transaction

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


class DefinitionAdminForm(ModelForm):
    """Signed, row-bound definition revision; never save runner-owned fields."""

    definition_revision = forms.CharField(widget=forms.HiddenInput, required=False)
    CONFLICT = "Definition changed. Reload this page before editing again."

    def _definition_fields(self) -> tuple[str, ...]:
        return tuple(name for name in self.fields if name != "definition_revision")

    def _revision(self, row: Any) -> str:
        values = {
            name: row._meta.get_field(name).to_python(getattr(row, name))
            for name in self._definition_fields()
        }
        values.update(pk=row.pk, updated_at=str(row.updated_at))
        # A keyed MAC, never a bare hash: the payload includes unredacted
        # secrets while every other input is visible on the change page.
        return salted_hmac(
            f"graphyard.admin.definition_revision:{row._meta.label_lower}",
            json.dumps(values, sort_keys=True, default=str),
            algorithm="sha256",
        ).hexdigest()

    def _revision_matches(self, submitted: Any, row: Any) -> bool:
        return isinstance(submitted, str) and constant_time_compare(
            submitted, self._revision(row)
        )

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if self.instance.pk:
            self.initial["definition_revision"] = self._revision(self.instance)

    def _current(self) -> Any:
        alias = router.db_for_write(type(self.instance), instance=self.instance)
        rows = type(self.instance).objects.using(alias)
        # Django admin holds its POST transaction from validation through save.
        if connections[alias].in_atomic_block:
            rows = rows.select_for_update()
        return rows.filter(pk=self.instance.pk).first()

    def clean(self) -> dict[str, Any]:
        data = super().clean()
        if self.instance.pk:
            current = self._current()
            if current is None or not self._revision_matches(
                data.get("definition_revision"), current
            ):
                raise forms.ValidationError(self.CONFLICT)
        return data

    def save(self, commit: bool = True) -> Any:
        if not commit or not self.instance.pk:
            return super().save(commit=commit)
        # Preserve ModelForm.save's refusal to persist invalid bound forms.
        super().save(commit=False)
        alias = router.db_for_write(type(self.instance), instance=self.instance)
        with transaction.atomic(using=alias):
            current = self._current()
            if current is None or not self._revision_matches(
                self.cleaned_data.get("definition_revision"), current
            ):
                raise forms.ValidationError(self.CONFLICT)
            self.instance.save(
                using=alias, update_fields=(*self._definition_fields(), "updated_at")
            )
        return self.instance


class DefinitionAdmin(admin.ModelAdmin):
    def save_model(self, request: Any, obj: Any, form: Any, change: bool) -> None:
        form.save()


class MetricCollectionSpecAdminForm(DefinitionAdminForm):
    REDACTED = "********"

    class Meta:
        model = MetricCollectionSpec
        fields = ("name", "enabled", "spec_type", "interval_seconds", "config")

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
        current = self._current() if self.instance.pk else None
        existing = current.config if current is not None else self._existing_config
        for key in _SECRET_CONFIG_KEYS:
            if merged.get(key) == self.REDACTED and key in existing:
                merged[key] = existing[key]
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


class ConditionDefinitionAdminForm(DefinitionAdminForm):
    class Meta:
        model = ConditionDefinition
        fields = (
            "name",
            "enabled",
            "metric_name",
            "host_filter",
            "subject_type_filter",
            "subject_id_filter",
            "service_filter",
            "tags_filter",
            "operator",
            "warning_threshold",
            "critical_threshold",
            "window_minutes",
            "breach_minutes",
            "stale_after_seconds",
            "alert_when_stale",
        )


@admin.register(ConditionDefinition)
class ConditionDefinitionAdmin(DefinitionAdmin):
    form = ConditionDefinitionAdminForm
    readonly_fields = (
        "status",
        "message",
        "last_value",
        "last_evaluated",
        "created_at",
        "updated_at",
    )
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
class MetricCollectionSpecAdmin(DefinitionAdmin):
    readonly_fields = (
        "next_run_time",
        "last_run_at",
        "last_status",
        "last_error",
        "created_at",
        "updated_at",
    )
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
