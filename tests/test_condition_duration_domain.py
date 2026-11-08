"""Synthetic admission tests, including definitions written without full_clean."""

from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from django.contrib.admin.sites import AdminSite
from django.core.exceptions import ValidationError
from django.core.management import call_command
from django.core.management.base import CommandError

from graphyard import influx, services
from graphyard.admin import ConditionDefinitionAdmin
from graphyard.models import ConditionDefinition

MAX_MINUTES = 10080  # one week, shared by the Python and Rust runtimes


def condition(**kwargs):
    return ConditionDefinition(
        name="synthetic", metric_name="synthetic.metric", warning_threshold=60, **kwargs
    )


@pytest.mark.parametrize("field", ["window_minutes", "breach_minutes"])
@pytest.mark.parametrize("value", [0, MAX_MINUTES + 1, 2_147_483_647])
def test_model_rejects_invalid_duration(field, value):
    item = condition(**{field: value})
    with pytest.raises(ValidationError) as error:
        item.full_clean(validate_unique=False, validate_constraints=False)
    assert field in error.value.message_dict


@pytest.mark.django_db
@pytest.mark.parametrize("field", ["window_minutes", "breach_minutes"])
@pytest.mark.parametrize("value", [0, MAX_MINUTES + 1])
def test_admin_rejects_invalid_duration(field, value):
    form_class = ConditionDefinitionAdmin(ConditionDefinition, AdminSite()).get_form(
        None
    )
    form = form_class(
        data={
            "name": "synthetic",
            "metric_name": "synthetic.metric",
            "operator": "gt",
            "warning_threshold": "60",
            "window_minutes": "30",
            "breach_minutes": "5",
            "status": "ok",
            field: str(value),
        }
    )
    assert not form.is_valid()
    assert field in form.errors


@pytest.mark.django_db
@pytest.mark.parametrize("field", ["window_minutes", "breach_minutes"])
def test_seed_overflow_leaves_existing_definition_unchanged(field):
    item = condition()
    item.save()
    with pytest.raises(CommandError):
        call_command(
            "seed_disk_usage_condition",
            host="synthetic",
            name=item.name,
            **{field: MAX_MINUTES + 1},
        )
    item.refresh_from_db()
    assert item.metric_name == "synthetic.metric"
    assert item.window_minutes == 30 and item.breach_minutes == 5


@pytest.mark.parametrize("field", ["window_minutes", "breach_minutes"])
@pytest.mark.parametrize("value", [0, MAX_MINUTES + 1, 2_147_483_647])
def test_invalid_duration_never_queries(field, value, monkeypatch):
    query = Mock(return_value=[])
    monkeypatch.setattr(influx, "query_range", query)
    item = condition(**{field: value})
    with pytest.raises(ValueError, match="minutes"):
        services.evaluate_condition(item, now=datetime(2026, 3, 4, tzinfo=UTC))
    query.assert_not_called()


@pytest.mark.django_db
@pytest.mark.parametrize("field", ["window_minutes", "breach_minutes"])
@pytest.mark.parametrize("value", [0, 2_147_483_647])
def test_historical_invalid_definition_fails_explicitly(field, value, monkeypatch):
    item = condition(**{field: value}, last_value=17)
    item.save()  # simulate a historical row bypassing configuration validation
    query = Mock(return_value=[])
    monkeypatch.setattr(influx, "query_range", query)
    result = services.evaluate_conditions_once(condition_id=item.pk)
    assert result.failed == 1
    query.assert_not_called()
    item.refresh_from_db()
    assert item.enabled and getattr(item, field) == value
    assert item.status == "critical" and item.message == "evaluation error: ValueError"
    assert item.last_value == 17


def test_maximum_and_extended_lookback(monkeypatch, settings):
    settings.CONDITION_DATA_STALE_WARNING_SECONDS = 600
    query = Mock(return_value=[])
    monkeypatch.setattr(influx, "query_range", query)
    now = datetime(2026, 3, 4, tzinfo=UTC)
    item = condition(window_minutes=1, breach_minutes=MAX_MINUTES)
    item.full_clean(validate_unique=False, validate_constraints=False)
    influx.query_condition_window(item, now=now)
    assert query.call_args.args[1] == now - timedelta(minutes=MAX_MINUTES + 10)


@pytest.mark.django_db
def test_seed_supports_breach_longer_than_window():
    call_command(
        "seed_disk_usage_condition",
        host="synthetic",
        window_minutes=1,
        breach_minutes=MAX_MINUTES,
    )
    item = ConditionDefinition.objects.get()
    assert item.window_minutes == 1 and item.breach_minutes == MAX_MINUTES
