from __future__ import annotations

import logging

from django.apps import AppConfig
from django.conf import settings
from django.db.backends.signals import connection_created

logger = logging.getLogger(__name__)

_VALID_JOURNAL_MODES = {"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"}
_VALID_SYNCHRONOUS_MODES = {"OFF", "NORMAL", "FULL", "EXTRA"}


def _configure_sqlite_connection(sender, connection, **kwargs) -> None:  # noqa: ANN001
    del sender, kwargs
    if connection.vendor != "sqlite":
        return

    busy_timeout_ms = int(max(0, settings.GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS))
    journal_mode = str(settings.GRAPHYARD_SQLITE_JOURNAL_MODE).strip().upper()
    synchronous_mode = str(settings.GRAPHYARD_SQLITE_SYNCHRONOUS).strip().upper()

    if journal_mode not in _VALID_JOURNAL_MODES:
        logger.warning(
            "Invalid GRAPHYARD_SQLITE_JOURNAL_MODE=%s, falling back to WAL",
            journal_mode,
        )
        journal_mode = "WAL"
    if synchronous_mode not in _VALID_SYNCHRONOUS_MODES:
        logger.warning(
            "Invalid GRAPHYARD_SQLITE_SYNCHRONOUS=%s, falling back to NORMAL",
            synchronous_mode,
        )
        synchronous_mode = "NORMAL"

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            cursor.execute(f"PRAGMA journal_mode = {journal_mode}")
            cursor.execute(f"PRAGMA synchronous = {synchronous_mode}")
    except Exception:  # noqa: BLE001
        logger.exception("Failed to apply SQLite pragmas on new DB connection")


class GraphyardConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "graphyard"

    def ready(self) -> None:
        connection_created.connect(
            _configure_sqlite_connection,
            dispatch_uid="graphyard.sqlite_connection_pragmas",
        )
