#!/usr/bin/env python3
"""Record the history of a shadow run (agent-rs/PLAN.md 11.2, step 3).

The agent tables keep only the latest state, so this recorder opens the
production and the shadow database read-only (``mode=ro``) every
``--interval`` seconds and appends one JSON line per tick to ``--out``::

    {"ts": <epoch>, "utc": "...", "dbs": {"prod": {...}, "shadow": {...}},
     "units": {"prod": {...}, "shadow": {...}}}

Per database (one read transaction, so every table is the same snapshot):
the rows of ``metric_collection_spec`` (every column except ``config``,
which holds the spec secrets), ``condition_definition``,
``pipeline_heartbeat``, ``subject_registry``, ``host_registry`` and
``service_registry`` (whole rows), each as ``{"<table>": {"<id>": row}}``
with only the rows that changed since the previous successful tick of that
database (every row on the first one), plus ``"<table>_deleted": [ids]``.
A database that cannot be read gets ``{"error": "..."}``; its next tick
records every change since its last good one.

Per unit (``--prod-unit``, ``--shadow-unit``): ``systemctl show`` properties
(``ActiveState``, ``MainPID``, ``NRestarts``, ``MemoryCurrent``,
``MemoryPeak``, ``CPUUsageNSec``) and, for a running main process,
``VmRSS``, ``VmHWM``, ``RssAnon`` and ``RssFile`` (KiB) of
``/proc/<pid>/status``.

Stop with Ctrl-C or SIGTERM. Standard library only (runs with the host's
``python3``); run it as ``graphyard`` (both databases and both processes
belong to that user)::

    sudo -u graphyard python3 shadow_record.py \\
        --prod-db /opt/apps/graphyard/site/src/django/db.sqlite3 \\
        --shadow-db /var/lib/graphyard-shadow/db.sqlite3 \\
        --out /var/lib/graphyard-shadow/record.jsonl
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

#: Table -> columns left out of the record.
TABLES: dict[str, tuple[str, ...]] = {
    "metric_collection_spec": ("config",),
    "condition_definition": (),
    "pipeline_heartbeat": (),
    "subject_registry": (),
    "host_registry": (),
    "service_registry": (),
}
UNIT_PROPERTIES = (
    "ActiveState",
    "MainPID",
    "NRestarts",
    "MemoryCurrent",
    "MemoryPeak",
    "CPUUsageNSec",
)
PROC_KEYS = ("VmRSS", "VmHWM", "RssAnon", "RssFile")


class DatabaseRecorder:
    """The incremental view of one database."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.rows: dict[str, dict[str, list[Any]]] | None = None
        self.columns: dict[str, list[str]] = {}

    def tick(self) -> dict[str, Any]:
        try:
            connection = sqlite3.connect(
                f"file:{self.path}?mode=ro", uri=True, timeout=10
            )
        except sqlite3.Error as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}
        try:
            connection.execute("BEGIN")
            snapshot = {table: self._read(connection, table) for table in TABLES}
            connection.execute("COMMIT")
        except sqlite3.Error as exc:
            return {"error": f"{type(exc).__name__}: {exc}"}
        finally:
            connection.close()
        out: dict[str, Any] = {}
        previous = self.rows or {}
        for table, rows in snapshot.items():
            before = previous.get(table, {})
            out[table] = {
                key: dict(zip(self.columns[table], row))
                for key, row in rows.items()
                if before.get(key) != row
            }
            out[f"{table}_deleted"] = sorted(
                (key for key in before if key not in rows), key=int
            )
        self.rows = snapshot
        return out

    def _read(self, connection: sqlite3.Connection, table: str) -> dict[str, list[Any]]:
        columns = [
            row[1]
            for row in connection.execute(f'PRAGMA table_info("{table}")')
            if row[1] not in TABLES[table]
        ]
        if not columns:
            raise sqlite3.OperationalError(f"no such table: {table}")
        self.columns[table] = columns
        quoted = ", ".join(f'"{column}"' for column in columns)
        return {
            str(row[0]): list(row)
            for row in connection.execute(f'SELECT {quoted} FROM "{table}" ORDER BY id')
        }


def _blob(value: object) -> str:
    """A BLOB cell (only raw SQL stores one) as ``x'<hex>'``."""
    if isinstance(value, bytes):
        return f"x'{value.hex()}'"
    raise TypeError(f"cannot record {type(value).__name__}")


def unit_sample(unit: str, systemctl: str, proc: Path) -> dict[str, Any]:
    """``systemctl show`` properties of ``unit`` plus ``/proc`` memory."""
    if shutil.which(systemctl) is None:
        return {"error": f"{systemctl} not found"}
    completed = subprocess.run(
        [systemctl, "show", "-p", ",".join(UNIT_PROPERTIES), unit],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return {"error": completed.stderr.strip() or f"exit {completed.returncode}"}
    sample: dict[str, Any] = {}
    for line in completed.stdout.splitlines():
        key, _, value = line.partition("=")
        sample[key] = value
    pid = sample.get("MainPID", "0")
    if pid.isdigit() and pid != "0":
        try:
            status = (proc / pid / "status").read_text(encoding="utf-8")
        except OSError:
            status = ""
        for line in status.splitlines():
            key, _, value = line.partition(":")
            if key in PROC_KEYS:
                sample[key] = value.strip()
    return sample


def record(
    prod: DatabaseRecorder,
    shadow: DatabaseRecorder,
    units: dict[str, str],
    out: Path,
    interval: float,
    stop: threading.Event,
    ticks: int | None = None,
    systemctl: str = "systemctl",
    proc: Path = Path("/proc"),
) -> int:
    """Append ticks to ``out`` until ``stop`` is set (or ``ticks`` ran)."""
    done = 0
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as stream:
        while not stop.is_set():
            now = time.time()
            line = {
                "ts": round(now, 3),
                "utc": dt.datetime.fromtimestamp(now, dt.UTC).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "dbs": {"prod": prod.tick(), "shadow": shadow.tick()},
                "units": {
                    role: unit_sample(unit, systemctl, proc)
                    for role, unit in units.items()
                },
            }
            stream.write(json.dumps(line, separators=(",", ":"), default=_blob) + "\n")
            stream.flush()
            done += 1
            if ticks is not None and done >= ticks:
                break
            stop.wait(max(0.0, interval - (time.time() - now)))
    return done


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--prod-db", required=True, type=Path)
    parser.add_argument("--shadow-db", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path, help="JSONL (appended)")
    parser.add_argument("--interval", type=float, default=60.0, help="seconds")
    parser.add_argument("--prod-unit", default="graphyard-agent.service")
    parser.add_argument("--shadow-unit", default="graphyard-agent-shadow.service")
    parser.add_argument("--no-units", action="store_true", help="skip systemctl")
    parser.add_argument("--systemctl", default="systemctl", help=argparse.SUPPRESS)
    parser.add_argument(
        "--proc", type=Path, default=Path("/proc"), help=argparse.SUPPRESS
    )
    parser.add_argument("--ticks", type=int, help="stop after this many ticks")
    args = parser.parse_args(argv)
    for path in (args.prod_db, args.shadow_db):
        if not path.is_file():
            print(f"database not found: {path}", file=sys.stderr)
            return 1
    if args.prod_db.resolve() == args.shadow_db.resolve():
        print("production and shadow database are the same file", file=sys.stderr)
        return 1
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    units = (
        {} if args.no_units else {"prod": args.prod_unit, "shadow": args.shadow_unit}
    )
    ticks = record(
        DatabaseRecorder(args.prod_db),
        DatabaseRecorder(args.shadow_db),
        units,
        args.out,
        args.interval,
        stop,
        args.ticks,
        args.systemctl,
        args.proc,
    )
    print(f"recorded {ticks} tick(s) to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
