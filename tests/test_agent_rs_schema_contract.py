"""Schema contract of the Rust agent (agent-rs/PLAN.md section 5.1).

The Rust agent runs no DDL: it verifies the Django-migrated schema at startup
and every Rust store test builds its database from
``agent-rs/fixtures/schema/schema.sql``. These tests migrate a temporary
database and fail when the committed contract files no longer match, so a
migration cannot merge without regenerating them and consciously updating the
Rust contract (``graphyard-store``'s ``schema.rs`` asserts its constants
against ``agent_tables.json``).
"""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "agent-rs" / "fixtures"
STALE_MESSAGE = "run `just rs-fixtures` and update the Rust schema contract"


def _load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        f"agent_rs_{name}", FIXTURES_DIR / f"{name}.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def schema() -> ModuleType:
    return _load("gen_schema")


@pytest.fixture(scope="module")
def driver() -> ModuleType:
    return _load("generate")


@pytest.fixture(scope="module")
def rendered(schema: ModuleType) -> dict[str, Any]:
    result: dict[str, Any] = schema.render()
    return result


def _stale(driver: ModuleType, rendered: dict[str, object]) -> list[str]:
    stale = []
    for name, content in sorted(rendered.items()):
        path = FIXTURES_DIR / name
        current = path.read_text(encoding="utf-8") if path.is_file() else None
        if current != driver.render(content):
            stale.append(name)
    return stale


def test_committed_schema_contract_matches_the_migrations(
    driver: ModuleType, rendered: dict[str, Any]
) -> None:
    stale = _stale(driver, rendered)
    assert not stale, f"stale Rust schema contract {stale}: {STALE_MESSAGE}"


def _drop_agent_column(db_path: Path) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "ALTER TABLE condition_definition DROP COLUMN stale_after_seconds"
        )
    connection.close()


def _record_new_migration(db_path: Path) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "INSERT INTO django_migrations (app, name, applied) "
            "VALUES ('graphyard', '0099_simulated', '2026-01-01 00:00:00')"
        )
    connection.close()


def test_contract_detects_a_migration_that_drops_an_agent_column(
    schema: ModuleType, driver: ModuleType
) -> None:
    rendered = schema.render(prepare=_drop_agent_column)

    assert _stale(driver, rendered) == [
        "schema/agent_tables.json",
        "schema/schema.sql",
    ]
    tables: Any = rendered["schema/agent_tables.json"]
    assert "stale_after_seconds" not in tables["tables"]["condition_definition"]
    committed = json.loads(
        (FIXTURES_DIR / "schema" / "agent_tables.json").read_text(encoding="utf-8")
    )
    assert "stale_after_seconds" in committed["tables"]["condition_definition"]


def test_contract_detects_any_new_migration(
    schema: ModuleType, driver: ModuleType
) -> None:
    rendered = schema.render(prepare=_record_new_migration)

    assert _stale(driver, rendered) == ["schema/schema.sql"]


def _migration(name: str, *operations: tuple[str, list[str] | None]) -> dict[str, Any]:
    return {
        "name": name,
        "operations": [{"type": kind, "models": models} for kind, models in operations],
    }


def test_minimum_migration_is_the_last_one_touching_an_agent_model(
    schema: ModuleType,
) -> None:
    migrations = [
        _migration("0001_initial", ("CreateModel", ["pipelineheartbeat"])),
        _migration("0002_tokens", ("AddField", ["ingesttoken"])),
    ]
    assert schema.latest_agent_migration(migrations) == "0001_initial"

    migrations.append(
        _migration(
            "0003_heartbeat_note",
            ("CreateModel", ["inventoryrelease"]),
            ("AddField", ["pipelineheartbeat"]),
        )
    )
    assert schema.latest_agent_migration(migrations) == "0003_heartbeat_note"

    # An operation without a model (RunPython, RunSQL) needs a decision.
    migrations.append(_migration("0004_data", ("RunPython", None)))
    assert schema.latest_agent_migration(migrations) == "0004_data"

    migrations.append(
        _migration("0005_rename", ("RenameModel", ["hostregistry", "hosts"]))
    )
    assert schema.latest_agent_migration(migrations) == "0005_rename"


def test_minimum_migration_of_the_real_migrations(rendered: dict[str, Any]) -> None:
    contract = rendered["schema/agent_tables.json"]
    # 0011 and 0012 touch only the ingest token tables (agent-rs/PLAN.md 5.1).
    assert contract["min_graphyard_migration"] == [
        "graphyard",
        "0010_condition_stale_allowance",
    ]
