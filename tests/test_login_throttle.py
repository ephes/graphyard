import pytest
from pathlib import Path
from django.core.cache import caches
from django.urls import reverse
from django.contrib.auth.backends import ModelBackend

pytestmark = pytest.mark.django_db


@pytest.fixture(autouse=True)
def throttle_cache(settings):
    settings.LOGIN_FAILURE_LIMIT = 2
    settings.LOGIN_ADDRESS_FAILURE_LIMIT = 50
    caches["login_throttle"].clear()
    yield
    caches["login_throttle"].clear()


@pytest.mark.parametrize("url", ["graphyard:login", "admin:login"])
def test_both_logins_lock_without_password_check(
    client, django_user_model, monkeypatch, url
):
    url = reverse(url)
    django_user_model.objects.create_user("operator", password="correct", is_staff=True)
    for _ in range(2):
        assert (
            client.post(url, {"username": "operator", "password": "wrong"}).status_code
            == 200
        )

    def forbidden(*args, **kwargs):
        pytest.fail("blocked login checked password")

    monkeypatch.setattr(ModelBackend, "authenticate", forbidden)
    response = client.post(url, {"username": "operator", "password": "correct"})
    assert response.status_code == 200
    assert "_auth_user_id" not in client.session


@pytest.mark.parametrize("url", ["graphyard:login", "admin:login"])
def test_success_resets_pair(client, django_user_model, url):
    url = reverse(url)
    django_user_model.objects.create_user("operator", password="correct", is_staff=True)
    client.post(url, {"username": "operator", "password": "wrong"})
    assert (
        client.post(url, {"username": "operator", "password": "correct"}).status_code
        == 302
    )
    client.logout()
    client.post(url, {"username": "operator", "password": "wrong"})
    assert (
        client.post(url, {"username": "operator", "password": "correct"}).status_code
        == 302
    )


@pytest.mark.parametrize("hops", [0, 1])
def test_forwarded_clients_and_spoofed_left_entries(
    client, django_user_model, settings, hops
):
    settings.LOGIN_THROTTLE_PROXY_HOPS = hops
    django_user_model.objects.create_user("operator", password="correct")
    for _ in range(2):
        client.post(
            "/login/",
            {"username": "operator", "password": "wrong"},
            HTTP_X_FORWARDED_FOR="198.51.100.9, 192.0.2.1",
        )
    response = client.post(
        "/login/",
        {"username": "operator", "password": "correct"},
        HTTP_X_FORWARDED_FOR="198.51.100.8, 192.0.2.1",
    )
    assert response.status_code == 200
    response = client.post(
        "/login/",
        {"username": "operator", "password": "correct"},
        HTTP_X_FORWARDED_FOR="192.0.2.2",
    )
    assert response.status_code == (302 if hops else 200)


def test_window_expiry_and_address_limit(
    client, django_user_model, settings, monkeypatch
):
    import time

    settings.LOGIN_FAILURE_WINDOW_S = 10
    settings.LOGIN_ADDRESS_FAILURE_LIMIT = 2
    django_user_model.objects.create_user("operator", password="correct")
    for name in ("other1", "other2"):
        client.post("/login/", {"username": name, "password": "wrong"})
    assert (
        client.post(
            "/login/", {"username": "operator", "password": "correct"}
        ).status_code
        == 200
    )
    future = time.time() + 11
    monkeypatch.setattr(time, "time", lambda: future)
    assert (
        client.post(
            "/login/", {"username": "operator", "password": "correct"}
        ).status_code
        == 302
    )


def test_locked_address_does_not_churn_or_evict_pair_keys(
    client, django_user_model, settings
):
    from graphyard.login_throttle import _keys, _count

    settings.CACHES = {
        **settings.CACHES,
        "login_throttle": {
            **settings.CACHES["login_throttle"],
            "OPTIONS": {"MAX_ENTRIES": 300},
        },
    }
    from django.test import RequestFactory

    django_user_model.objects.create_user("operator", password="correct")
    for _ in range(2):
        client.post("/login/", {"username": "operator", "password": "wrong"})
    for i in range(400):
        client.post(
            "/login/",
            {"username": f"unknown{i}", "password": "wrong"},
            REMOTE_ADDR="192.0.2.2",
        )
    assert (
        client.post(
            "/login/", {"username": "operator", "password": "correct"}
        ).status_code
        == 200
    )
    pair, _ = _keys(RequestFactory().post("/", REMOTE_ADDR="192.0.2.2"), "unknown399")
    assert _count(pair) == 0


def test_window_stays_fixed_after_midwindow_failure(
    client, django_user_model, settings, monkeypatch
):
    import time

    settings.LOGIN_FAILURE_WINDOW_S = 10
    start = time.time()
    monkeypatch.setattr(time, "time", lambda: start)
    django_user_model.objects.create_user("operator", password="correct")
    client.post("/login/", {"username": "operator", "password": "wrong"})
    monkeypatch.setattr(time, "time", lambda: start + 8)
    client.post("/login/", {"username": "operator", "password": "wrong"})
    assert (
        client.post(
            "/login/", {"username": "operator", "password": "correct"}
        ).status_code
        == 200
    )
    monkeypatch.setattr(time, "time", lambda: start + 11)
    assert (
        client.post(
            "/login/", {"username": "operator", "password": "correct"}
        ).status_code
        == 302
    )


@pytest.mark.parametrize(
    "hops,header,expected",
    [
        (1, "", "127.0.0.1"),
        (2, "192.0.2.1", "127.0.0.1"),
        (1, "bad", "127.0.0.1"),
        (2, "spoofed, 192.0.2.1, 192.0.2.2", "192.0.2.1"),
    ],
)
def test_proxy_fallbacks(settings, hops, header, expected):
    from django.test import RequestFactory
    from graphyard.login_throttle import client_address

    settings.LOGIN_THROTTLE_PROXY_HOPS = hops
    assert (
        client_address(RequestFactory().get("/", HTTP_X_FORWARDED_FOR=header))
        == expected
    )


@pytest.mark.parametrize(
    "name,value,valid",
    [
        ("GRAPHYARD_LOGIN_ADDRESS_FAILURE_LIMIT", "0", False),
        ("GRAPHYARD_LOGIN_ADDRESS_FAILURE_LIMIT", "1", True),
        ("GRAPHYARD_LOGIN_FAILURE_WINDOW_SECONDS", "0", False),
        ("GRAPHYARD_LOGIN_FAILURE_WINDOW_SECONDS", "1", True),
        ("GRAPHYARD_LOGIN_THROTTLE_PROXY_HOPS", "-1", False),
        ("GRAPHYARD_LOGIN_THROTTLE_PROXY_HOPS", "0", True),
    ],
)
def test_settings_reject_invalid_throttle_limits(name, value, valid):
    import os
    import subprocess
    import sys

    environment = {
        k: v for k, v in os.environ.items() if not k.startswith("GRAPHYARD_LOGIN_")
    }
    environment[name] = value
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; sys.path.insert(0, 'src/django'); import config.settings.base",
        ],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert (result.returncode == 0) == valid
    if not valid:
        assert name in result.stderr
