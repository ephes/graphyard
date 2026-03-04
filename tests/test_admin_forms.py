from __future__ import annotations

from graphyard.admin import MetricCollectionSpecAdminForm
from graphyard.models import MetricCollectionSpec, MetricCollectionSpecType


def test_metric_collection_spec_admin_form_masks_secret_values(db):
    spec = MetricCollectionSpec.objects.create(
        name="ha spec",
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_SENSOR,
        config={
            "base_url": "https://ha.local",
            "access_token": "super-secret-token",
            "entity_id": "sensor.office_humidity",
        },
    )

    form = MetricCollectionSpecAdminForm(instance=spec)

    assert form.initial["config"]["access_token"] == "********"


def test_metric_collection_spec_admin_form_preserves_masked_secret_on_clean(db):
    spec = MetricCollectionSpec.objects.create(
        name="ha spec keep secret",
        spec_type=MetricCollectionSpecType.HOME_ASSISTANT_SENSOR,
        config={
            "base_url": "https://ha.local",
            "access_token": "super-secret-token",
            "entity_id": "sensor.office_humidity",
        },
    )

    form = MetricCollectionSpecAdminForm(instance=spec)
    form.cleaned_data = {
        "config": {
            "base_url": "https://ha.changed.local",
            "access_token": "********",
            "entity_id": "sensor.office_humidity",
        }
    }

    cleaned = form.clean_config()

    assert cleaned["access_token"] == "super-secret-token"
    assert cleaned["base_url"] == "https://ha.changed.local"
