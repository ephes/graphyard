"""Isolated PostgreSQL contract checks; never use a production database."""

from .test import *  # noqa: F403

DATABASES = {"default": env.db("GRAPHYARD_TEST_DATABASE_URL")}  # noqa: F405

DATABASES["secondary"] = {
    **DATABASES["default"],
    "TEST": {"NAME": "test_graphyard_secondary", "DEPENDENCIES": []},
}
