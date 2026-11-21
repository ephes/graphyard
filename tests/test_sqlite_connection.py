import logging

import pytest
from django.db import connections


@pytest.mark.parametrize("in_memory", [True, False])
def test_sqlite_connection_reports_journal_mode_fallback(
    in_memory, tmp_path, settings, caplog, django_db_blocker
):
    settings.GRAPHYARD_SQLITE_JOURNAL_MODE = "wal"
    settings.GRAPHYARD_SQLITE_SYNCHRONOUS = "normal"
    db = connections["default"].copy(alias="synthetic")
    db.settings_dict["NAME"] = (
        "file:synthetic?mode=memory&cache=shared"
        if in_memory
        else str(tmp_path / "graphyard.sqlite3")
    )
    with (
        django_db_blocker.unblock(),
        caplog.at_level(logging.INFO, logger="graphyard.apps"),
    ):
        try:
            with db.cursor() as cursor:
                cursor.execute("PRAGMA journal_mode")
                assert cursor.fetchone()[0] == ("memory" if in_memory else "wal")
                cursor.execute("PRAGMA synchronous")
                assert cursor.fetchone()[0] == 1
        finally:
            db.close()

    diagnostics = [
        record for record in caplog.records if record.name == "graphyard.apps"
    ]
    if in_memory:
        [diagnostic] = diagnostics
        assert diagnostic.levelno == logging.INFO
        assert diagnostic.args == ("synthetic", "WAL", "MEMORY")
    else:
        assert diagnostics == []
