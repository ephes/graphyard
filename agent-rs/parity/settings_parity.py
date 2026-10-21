"""Django settings of the Python agent in the parity harness (PLAN.md 9.4).

The base settings with the production logging (a root ``StreamHandler``
without formatter at INFO, so records are bare stderr lines like the Rust
agent's) and ``DATABASES`` pointing at the harness's copy of the corpus
database (``PARITY_DATABASE``). Kept outside ``src/`` on purpose: nothing in
the app can import it. ``src/django/.env`` is not read, so a developer's
file cannot leak into the run (the Rust agent never reads it, D-9).
"""

# ruff: noqa: F403, F405
import os

import environ

environ.Env.read_env = classmethod(lambda cls, *args, **kwargs: None)  # type: ignore[method-assign,assignment]

from config.settings.base import *  # noqa: E402

DEBUG = False

DATABASES["default"]["NAME"] = os.environ["PARITY_DATABASE"]

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
}
