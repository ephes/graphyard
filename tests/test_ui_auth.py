from __future__ import annotations

import pytest
from django.urls import reverse


@pytest.mark.django_db
def test_index_redirects_to_login_for_anonymous_client(client):
    response = client.get(reverse("graphyard:index"))

    assert response.status_code == 302
    assert response.url == "/login/?next=/"


@pytest.mark.django_db
def test_login_page_renders(client):
    response = client.get(reverse("graphyard:login"))

    assert response.status_code == 200
    assert b"Sign in to view hosts, services, and Grafana links." in response.content


@pytest.mark.django_db
def test_login_redirects_to_index_without_next(client, django_user_model):
    django_user_model.objects.create_user(
        username="demo-user",
        password="super-secret-pass",
    )

    response = client.post(
        reverse("graphyard:login"),
        data={"username": "demo-user", "password": "super-secret-pass"},
    )

    assert response.status_code == 302
    assert response.url == "/"


@pytest.mark.django_db
def test_logout_redirects_to_login(client, django_user_model):
    django_user_model.objects.create_user(
        username="logout-user",
        password="logout-pass",
    )
    client.login(username="logout-user", password="logout-pass")

    response = client.post(reverse("graphyard:logout"))

    assert response.status_code == 302
    assert response.url == "/login/"


@pytest.mark.django_db
def test_login_with_wrong_password_renders_form(client, django_user_model):
    django_user_model.objects.create_user(
        username="wrong-user",
        password="correct-pass",
    )

    response = client.post(
        reverse("graphyard:login"),
        data={"username": "wrong-user", "password": "wrong-pass"},
    )

    assert response.status_code == 200
    assert b"Please enter a correct username and password." in response.content


@pytest.mark.django_db
def test_login_page_redirects_authenticated_user(client, django_user_model):
    django_user_model.objects.create_user(
        username="auth-user",
        password="auth-pass",
    )
    client.login(username="auth-user", password="auth-pass")

    response = client.get(reverse("graphyard:login"))

    assert response.status_code == 302
    assert response.url == "/"
