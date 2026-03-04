# ruff: noqa: F405
from .base import *  # noqa

DEBUG = env.bool("DJANGO_DEBUG", default=True)
if len(TEMPLATES) > 0:
    TEMPLATES[0]["OPTIONS"]["debug"] = DEBUG

ALLOWED_HOSTS = env.list("DJANGO_ALLOWED_HOSTS", default=["*"])  # noqa
