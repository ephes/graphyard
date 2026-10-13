"""Test settings."""

from .base import *  # noqa

DEBUG = False
if len(TEMPLATES) > 0:  # noqa: F405
    TEMPLATES[0]["OPTIONS"]["debug"] = False  # noqa: F405

SECRET_KEY = env("DJANGO_SECRET_KEY", default="graphyard-test-key")  # noqa: F405

PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.MD5PasswordHasher",
]

CACHES = {
    "login_throttle": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "OPTIONS": {"MAX_ENTRIES": 10000},
        "LOCATION": "graphyard-login-throttle-test",
    },
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "",
    },
}
