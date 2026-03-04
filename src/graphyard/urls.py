from django.urls import path

from . import views

app_name = "graphyard"

urlpatterns = [
    path("", views.host_service_index, name="index"),
    path("v1/metrics", views.metrics_ingest, name="metrics_ingest"),
    path("v1/conditions", views.conditions_list, name="conditions_list"),
    path(
        "v1/conditions/<int:condition_id>",
        views.condition_detail,
        name="condition_detail",
    ),
    path("v1/health", views.health, name="health"),
]
