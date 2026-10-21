"""Fixtures ``schema/schema.sql`` and ``schema/agent_tables.json`` (slice S3a).

``agent-rs/PLAN.md`` section 5.1: Django migrations own the schema and the
Rust agent runs no DDL. This builder migrates a temporary database with the
test settings (in a subprocess with the same scrubbed environment as the
fixture driver) and renders

* ``schema/schema.sql``: the DDL of every table and index (``sqlite_master``
  order) plus the ``django_migrations`` rows; every Rust store test builds
  its database from it;
* ``schema/agent_tables.json``: the columns of the six agent tables in
  ``PRAGMA table_info`` order and the minimum graphyard migration of the Rust
  agent, which is the last graphyard migration whose operations touch an
  agent model (an operation the classifier cannot attribute to a model, such
  as ``RunPython`` or ``RunSQL``, counts as touching one).

``tests/test_agent_rs_schema_contract.py`` re-renders both and fails when they
differ from the committed files, so a migration cannot merge without a
conscious update of the Rust contract (``graphyard-store``'s ``schema.rs``
asserts its constants against ``agent_tables.json``).
"""

from __future__ import annotations

import functools
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

FIXTURES_DIR = Path(__file__).resolve().parent
REPO_ROOT = FIXTURES_DIR.parent.parent
DJANGO_DIR = REPO_ROOT / "src" / "django"
SCRUBBED_ENV_PREFIXES = ("DJANGO_", "GRAPHYARD_", "INFLUX_")

APP = "graphyard"

#: The agent tables and their Django model names (lower case), in the order
#: of PLAN.md 5.1.
AGENT_TABLES = {
    "metric_collection_spec": "metriccollectionspec",
    "condition_definition": "conditiondefinition",
    "pipeline_heartbeat": "pipelineheartbeat",
    "subject_registry": "subjectregistry",
    "host_registry": "hostregistry",
    "service_registry": "serviceregistry",
}

#: ``django_migrations.applied`` is wall-clock time; the fixture pins it.
APPLIED_AT = "2000-01-01 00:00:00"

_MIGRATE = r"""
import json
import os
import sys

for key in list(os.environ):
    if key.startswith(("DJANGO_", "GRAPHYARD_", "INFLUX_")):
        del os.environ[key]
os.environ["DJANGO_SETTINGS_MODULE"] = "config.settings.test"

import environ

environ.Env.read_env = classmethod(lambda cls, *args, **kwargs: None)

from django.conf import settings

settings.DATABASES["default"]["NAME"] = sys.argv[1]

import django

django.setup()

from django.core.management import call_command
from django.db.migrations.loader import MigrationLoader
from django.db.migrations.operations.base import Operation
from django.db.migrations.operations.models import ModelOperation, RenameModel

call_command("migrate", verbosity=0, interactive=False)


def models_of(operation: Operation) -> list[str] | None:
    if hasattr(operation, "model_name"):
        return [operation.model_name.lower()]
    if isinstance(operation, RenameModel):
        return [operation.old_name.lower(), operation.new_name.lower()]
    if isinstance(operation, ModelOperation):
        return [operation.name.lower()]
    return None


loader = MigrationLoader(None, ignore_no_migrations=True)
app = sys.argv[2]
leaves = loader.graph.leaf_nodes(app)
summaries = []
for key in loader.graph.forwards_plan(leaves[0]):
    if key[0] != app:
        continue
    migration = loader.graph.nodes[key]
    summaries.append(
        {
            "name": key[1],
            "operations": [
                {"type": type(op).__name__, "models": models_of(op)}
                for op in migration.operations
            ],
        }
    )
print(json.dumps({"leaves": [list(leaf) for leaf in leaves], "migrations": summaries}))
"""


def migrate(db_path: Path) -> dict[str, Any]:
    """Migrate a new database at ``db_path``; return the migration summary."""
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(SCRUBBED_ENV_PREFIXES)
    }
    env["PYTHONPATH"] = os.pathsep.join(
        [str(DJANGO_DIR), str(REPO_ROOT / "src"), env.get("PYTHONPATH", "")]
    )
    env["PYTHONHASHSEED"] = "0"
    result = subprocess.run(
        [sys.executable, "-c", _MIGRATE, str(db_path), APP],
        check=True,
        cwd=DJANGO_DIR,
        env=env,
        capture_output=True,
        text=True,
    )
    summary: dict[str, Any] = json.loads(result.stdout)
    if len(summary["leaves"]) != 1:
        raise RuntimeError(f"{APP} has several migration leaves: {summary['leaves']}")
    return summary


def latest_agent_migration(migrations: Sequence[dict[str, Any]]) -> str:
    """The last migration (in plan order) that touches an agent model.

    An operation without a model attribution (``models`` is ``None``) counts
    as touching one: such a migration needs a human decision.
    """
    agent_models = set(AGENT_TABLES.values())
    latest = None
    for migration in migrations:
        for operation in migration["operations"]:
            models = operation["models"]
            if models is None or agent_models.intersection(models):
                latest = migration["name"]
                break
    if latest is None:
        raise RuntimeError(f"no {APP} migration touches an agent table")
    return latest


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def render_from_db(db_path: Path, summary: dict[str, Any]) -> dict[str, object]:
    """Render both contract files from a migrated database."""
    with sqlite3.connect(db_path) as connection:
        ddl = [
            row[0]
            for row in connection.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid"
            )
        ]
        migrations = connection.execute(
            "SELECT app, name FROM django_migrations ORDER BY id"
        ).fetchall()
        tables = {
            table: [
                row[1]
                for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
            ]
            for table in AGENT_TABLES
        }
    connection.close()
    minimum = latest_agent_migration(summary["migrations"])
    if (APP, minimum) not in {tuple(row) for row in migrations}:
        raise RuntimeError(f"{APP}.{minimum} is not recorded in django_migrations")

    lines = [
        "-- The Django schema the Rust agent runs against (agent-rs/PLAN.md 5.1).",
        "-- Generated by agent-rs/fixtures/gen_schema.py from `manage.py migrate`;",
        "-- do not edit (`just rs-fixtures`).",
    ]
    lines.extend(f"{statement};" for statement in ddl)
    lines.extend(
        "INSERT INTO django_migrations (app, name, applied) VALUES "
        f"({_sql_literal(app)}, {_sql_literal(name)}, {_sql_literal(APPLIED_AT)});"
        for app, name in migrations
    )
    return {
        "schema/schema.sql": "\n".join(lines) + "\n",
        "schema/agent_tables.json": {
            "min_graphyard_migration": [APP, minimum],
            "tables": tables,
        },
    }


def render(prepare: Callable[[Path], None] | None = None) -> dict[str, object]:
    """Migrate a temporary database and render the contract files.

    Args:
        prepare: Optional hook run on the migrated database before rendering
            (the contract test simulates schema changes with it).
    """
    with tempfile.TemporaryDirectory() as directory:
        db_path = Path(directory) / "db.sqlite3"
        summary = migrate(db_path)
        if prepare is not None:
            prepare(db_path)
        return render_from_db(db_path, summary)


@functools.cache
def _rendered() -> dict[str, object]:
    return render()


GENERATORS = {
    "schema/agent_tables.json": lambda: _rendered()["schema/agent_tables.json"],
    "schema/schema.sql": lambda: _rendered()["schema/schema.sql"],
}
