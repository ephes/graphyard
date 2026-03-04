from django.contrib.auth import views as auth_views
from django.urls import path

from . import views

app_name = "graphyard"

urlpatterns = [
    path(
        "login/",
        auth_views.LoginView.as_view(
            template_name="graphyard/login.html",
            redirect_authenticated_user=True,
        ),
        name="login",
    ),
    path(
        "logout/",
        auth_views.LogoutView.as_view(next_page="graphyard:login"),
        name="logout",
    ),
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
