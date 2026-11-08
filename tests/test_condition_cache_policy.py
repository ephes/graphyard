"""Response storage policy through a real login/logout session."""

import pytest
from django.urls import reverse

from graphyard.models import ConditionDefinition


@pytest.mark.django_db
def test_authenticated_condition_and_index_responses_are_not_stored(
    client, django_user_model
):
    user = django_user_model.objects.create_user(
        username="synthetic", password="synthetic"
    )
    item = ConditionDefinition.objects.create(
        name="public-name",
        metric_name="private-metric-sentinel",
        host_filter="private-host-sentinel",
        warning_threshold=1,
        message="private-message-sentinel",
        last_value=123.45,
        tags_filter={"private-tag-sentinel": "private-value-sentinel"},
    )
    paths = [
        reverse("graphyard:conditions_list"),
        reverse("graphyard:condition_detail", args=[item.pk]),
    ]
    public = [client.get(path).json() for path in paths]
    client.force_login(user)
    for path in [*paths, reverse("graphyard:index")]:
        response = client.get(path)
        assert response.status_code == 200
        directives = {
            part.strip()
            for part in response.headers.get("Cache-Control", "").split(",")
        }
        assert {"private", "no-store", "no-cache", "max-age=0"} <= directives
        assert "Expires" in response.headers
    for path in paths:
        assert "public-name" in client.get(path).content.decode()
    assert client.post(reverse("graphyard:logout")).status_code == 302
    for path, expected in zip(paths, public, strict=True):
        response = client.get(path)
        assert response.json() == expected
        assert "private-" not in response.content.decode()
        assert "config" not in response.json()
    assert client.get(reverse("graphyard:index")).status_code == 302
