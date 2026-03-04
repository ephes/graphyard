"""Base settings for graphyard."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import environ

BASE_DIR = Path(__file__).resolve().parent.parent.parent

env = environ.Env(
    DEBUG=(bool, False),
)
environ.Env.read_env(os.path.join(BASE_DIR, ".env"))

SECRET_KEY = env(
    "DJANGO_SECRET_KEY",
    default="django-insecure-graphyard-dev-key-change-me",
)
DEBUG = env.bool("DJANGO_DEBUG", default=True)

ALLOWED_HOSTS = env.list("DJANGO_ALLOWED_HOSTS", default=["localhost", "127.0.0.1"])

DJANGO_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
]

THIRD_PARTY_APPS: list[str] = []

LOCAL_APPS = [
    "graphyard.apps.GraphyardConfig",
]

INSTALLED_APPS = DJANGO_APPS + THIRD_PARTY_APPS + LOCAL_APPS

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES: list[dict[str, Any]] = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
STATICFILES_DIRS: list[str] = []

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"},
}

ADMIN_URL = env("DJANGO_ADMIN_URL", default="admin/")
LOGIN_URL = env("DJANGO_LOGIN_URL", default="/admin/login/")

INFLUX_URL = env("INFLUX_URL", default="")
INFLUX_TOKEN = env("INFLUX_TOKEN", default="")
INFLUX_ORG = env("INFLUX_ORG", default="")
INFLUX_BUCKET = env("INFLUX_BUCKET", default="graphyard")
INFLUX_MEASUREMENT = env("INFLUX_MEASUREMENT", default="graphyard_metrics")
INFLUX_TIMEOUT_MS = env.int("INFLUX_TIMEOUT_MS", default=10000)
INFLUX_API_MODE = env("INFLUX_API_MODE", default="auto").lower()

HEARTBEAT_WARNING_SECONDS = env.int("GRAPHYARD_HEARTBEAT_WARNING_SECONDS", default=900)
HEARTBEAT_CRITICAL_SECONDS = env.int(
    "GRAPHYARD_HEARTBEAT_CRITICAL_SECONDS", default=3600
)

GRAFANA_BASE_URL = env("GRAFANA_BASE_URL", default="")

GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS = env.int(
    "GRAPHYARD_METRIC_COLLECTION_INTERVAL_SECONDS",
    default=env.int("GRAPHYARD_HA_POLL_INTERVAL_SECONDS", default=60),
)
GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS = env.int(
    "GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS", default=60
)

CONDITION_DATA_STALE_WARNING_SECONDS = env.int(
    "GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS", default=600
)
