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
import math
import os
import shutil
import selectors
import signal
import sqlite3
import stat
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
UNIT_TIMEOUT = 2.0
UNIT_MAX_BYTES = 64 * 1024
MAX_INTERVAL = 86400

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
        out: dict[str, Any] = {"baseline": True} if self.rows is None else {}
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


def unit_sample(
    unit: str, systemctl: str, proc: Path, stop: threading.Event | None = None
) -> dict[str, Any]:
    """``systemctl show`` properties of ``unit`` plus ``/proc`` memory."""
    if shutil.which(systemctl) is None:
        return {"error": "unit probe unavailable"}
    sample: dict[str, Any] = {}
    try:
        with subprocess.Popen(
            [systemctl, "show", "-p", ",".join(UNIT_PROPERTIES), unit],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        ) as child:
            try:
                assert child.stdout is not None
                os.set_blocking(child.stdout.fileno(), False)
                captured = bytearray()
                deadline = time.monotonic() + UNIT_TIMEOUT
                with selectors.DefaultSelector() as selector:
                    selector.register(child.stdout, selectors.EVENT_READ)
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or (stop is not None and stop.is_set()):
                            raise ValueError("probe deadline")
                        if not selector.select(min(remaining, 0.05)):
                            continue
                        chunk = os.read(
                            child.stdout.fileno(),
                            min(8192, UNIT_MAX_BYTES + 1 - len(captured)),
                        )
                        if not chunk:
                            break
                        captured.extend(chunk)
                        if len(captured) > UNIT_MAX_BYTES:
                            raise ValueError("probe output limit")
                if child.wait(timeout=max(0.001, deadline - time.monotonic())) != 0:
                    raise ValueError("probe failed")
                for line in captured.decode("utf-8").splitlines():
                    key, _, value = line.partition("=")
                    if key in UNIT_PROPERTIES:
                        sample[key] = value
            finally:
                # The process group was created for this probe, including pipe holders.
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait()
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return {"error": "unit probe unavailable"}
    pid = sample.get("MainPID", "0")
    if pid.isdigit() and pid != "0":
        try:
            with (proc / pid / "status").open(encoding="utf-8") as status_file:
                status = status_file.read(UNIT_MAX_BYTES)
        except OSError:
            status = ""
        for line in status.splitlines():
            key, _, value = line.partition(":")
            if key in PROC_KEYS:
                sample[key] = value.strip()
    return sample


def validate_loop(interval: float, ticks: int | None) -> None:
    if not math.isfinite(interval) or not 0 < interval <= MAX_INTERVAL:
        raise ValueError("interval must be finite and in (0, 86400] seconds")
    if ticks is not None and (type(ticks) is not int or ticks <= 0):
        raise ValueError("ticks must be a positive integer")


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
    validate_loop(interval, ticks)
    done = 0
    # Hold admitted input descriptors until output identity has been checked.
    from contextlib import ExitStack

    with ExitStack() as inputs:
        identities = []
        for source in (prod.path, shadow.path):
            try:
                fd_in = os.open(source, os.O_RDONLY | os.O_NONBLOCK)
            except FileNotFoundError:
                continue  # The tick records an unavailable database, then retries.

            inputs.callback(os.close, fd_in)
            info = os.fstat(fd_in)
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("database must be a regular file")
            identities.append((info.st_dev, info.st_ino))
        if len(identities) == 2 and identities[0] == identities[1]:
            raise ValueError("production and shadow database are the same file")
        fd = os.open(
            out,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NONBLOCK | os.O_NOFOLLOW,
            0o600,
        )
        try:
            info = os.fstat(fd)
            if (info.st_dev, info.st_ino) in identities:
                raise ValueError("output aliases an input database")
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid()
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise ValueError(
                    "output must be a private regular file owned by the caller (0600)"
                )
        except BaseException:
            os.close(fd)
            raise
    with os.fdopen(fd, "a", encoding="utf-8") as stream:
        while not stop.is_set():
            started = time.monotonic()
            now = time.time()
            line = {
                "ts": round(now, 3),
                "utc": dt.datetime.fromtimestamp(now, dt.UTC).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "dbs": {"prod": prod.tick(), "shadow": shadow.tick()},
                "units": {
                    role: unit_sample(unit, systemctl, proc, stop)
                    for role, unit in units.items()
                },
            }
            stream.write(json.dumps(line, separators=(",", ":"), default=_blob) + "\n")
            stream.flush()
            done += 1
            if ticks is not None and done >= ticks:
                break
            stop.wait(max(0.0, interval - (time.monotonic() - started)))
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
    try:
        validate_loop(args.interval, args.ticks)
    except ValueError as err:
        parser.error(str(err))
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
    try:
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
    except (OSError, ValueError) as err:
        print(f"shadow_record: {err}", file=sys.stderr)
        return 1
    print(f"recorded {ticks} tick(s) to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
