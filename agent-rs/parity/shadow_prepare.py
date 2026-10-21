#!/usr/bin/env python3
"""Prepare the database copy of a shadow run (agent-rs/PLAN.md 11.2, step 1).

1. Copy the live database online with SQLite's backup API (the same as
   ``sqlite3 SRC ".backup DEST"``; consistent while the Python agent and the
   web app keep writing) into ``<shadow-dir>/db.sqlite3``. The shadow
   directory must exist, belong to the caller and have mode 0700 (the copy
   holds the spec secrets); the copy is created 0600 and must not exist yet.
   ``PRAGMA quick_check`` must pass on the copy.
2. With ``--disable-spec NAME`` (repeatable), set ``enabled = 0`` on those
   specs in the copy only (PLAN.md 13.1 R6: the UniFi spec when the
   controller limits logins; it is then compared through one ``--run-once``).
3. Scan the rows the agent loads from the copy for the documents and values
   on which the Rust agent and Python disagree (PLAN.md D-11, D-13): stored
   JSON (``config``, ``tags_filter``, heartbeat ``details``) that Python
   cannot load, integer tokens
   beyond CPython's 4300-digit limit, lone UTF-16 surrogate escapes, nesting
   at or beyond ``serde_json``'s 128 levels, ``NaN``/``Infinity`` tokens, and
   values of another SQLite storage class than the column's declared type.
4. Record T0 (UTC, before the copy started) and the summary in
   ``<shadow-dir>/shadow.json`` for ``shadow_record.py`` and
   ``shadow_compare.py``.

Exit status: 0 prepared, 2 prepared but the scan listed rows to look at
(the copy is kept), 1 nothing usable (bad arguments, copy failed).

Standard library only (runs with the host's ``python3`` on macmini). The
source is opened read-only. Run it as the user of the shadow agent
(``graphyard``), so the copy belongs to that user::

    install -d -o graphyard -g graphyard -m 0700 /var/lib/graphyard-shadow
    sudo -u graphyard python3 shadow_prepare.py \\
        --source /opt/apps/graphyard/site/src/django/db.sqlite3 \\
        --shadow-dir /var/lib/graphyard-shadow
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sqlite3
import stat
import sys
from pathlib import Path
from typing import Any

COPY_NAME = "db.sqlite3"
META_NAME = "shadow.json"

#: CPython's default ``sys.get_int_max_str_digits()``.
INT_MAX_STR_DIGITS = 4300
#: ``serde_json``'s nesting limit: 128 levels are rejected, 127 accepted.
SERDE_MAX_DEPTH = 128

#: The tables whose rows the agent loads (PLAN.md 5.1) and, for the spec and
#: condition tables, the rows it loads (``WHERE enabled``, any truthy value).
AGENT_TABLES = {
    "metric_collection_spec": "enabled",
    "condition_definition": "enabled",
    "pipeline_heartbeat": None,
    "subject_registry": None,
    "host_registry": None,
    "service_registry": None,
}
#: Stored JSON columns of D-11.
JSON_COLUMNS = {
    "metric_collection_spec": "config",
    "condition_definition": "tags_filter",
    # Never parsed by Rust, but Python loads it before its heartbeat update
    # and fails on such a document (D-11).
    "pipeline_heartbeat": "details",
}


def utc_text(moment: dt.datetime) -> str:
    return moment.astimezone(dt.UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def expected_classes(declared: str) -> set[str] | None:
    """The storage classes the Rust store reads for a declared column type
    (D-13), ``None`` for ``bool``/``datetime`` columns, which follow Python's
    converters whatever the class. NULL is always accepted (the schema's
    ``NOT NULL`` constraints keep it out where Django forbids it)."""
    name = declared.strip().lower()
    if name.startswith(("bool", "datetime")):
        return None
    if name.startswith(("integer", "bigint", "smallint")):
        return {"integer", "null"}
    if name.startswith("real"):
        return {"real", "integer", "null"}
    if name.startswith(("varchar", "text", "char")):
        return {"text", "null"}
    return None


def json_issues(text: str) -> list[str]:
    """Why the Rust agent (``serde_json``) and Python (``json.loads``) would
    treat a stored document differently, or why Python cannot load it."""
    issues: list[str] = []
    in_string = False
    escape = False
    depth = max_depth = 0
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if in_string:
            if escape:
                escape = False
                if char == "u":
                    code = _hex4(text, index + 1)
                    if code is not None and 0xD800 <= code <= 0xDBFF:
                        follow = (
                            _hex4(text, index + 7)
                            if text[index + 5 : index + 7] == "\\u"
                            else None
                        )
                        if follow is None or not 0xDC00 <= follow <= 0xDFFF:
                            issues.append(f"lone surrogate escape \\u{code:04x} (D-11)")
                        else:
                            index += 6
                    elif code is not None and 0xDC00 <= code <= 0xDFFF:
                        issues.append(f"lone surrogate escape \\u{code:04x} (D-11)")
                    index += 4
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
        elif char == '"':
            in_string = True
        elif char in "[{":
            depth += 1
            max_depth = max(max_depth, depth)
        elif char in "]}":
            depth -= 1
        elif char == "-" or char.isdigit():
            end = index + 1
            while end < length and (text[end].isdigit() or text[end] in ".eE+-"):
                end += 1
            token = text[index:end]
            digits = token.lstrip("-")
            if digits.isdigit() and len(digits) > INT_MAX_STR_DIGITS:
                issues.append(
                    f"integer of {len(digits)} digits beyond CPython's "
                    f"{INT_MAX_STR_DIGITS}-digit limit (D-11)"
                )
            index = end
            continue
        index += 1
    if max_depth >= SERDE_MAX_DEPTH:
        issues.append(
            f"nesting of {max_depth} levels at or beyond serde_json's limit of "
            f"{SERDE_MAX_DEPTH} (D-11)"
        )

    def reject_constant(name: str) -> Any:
        raise ValueError(f"non-standard JSON token {name} (D-4)")

    try:
        json.loads(text, parse_constant=reject_constant)
    except RecursionError:
        issues.append("Python cannot load it: nesting beyond its recursion limit")
    except ValueError as err:
        message = str(err)
        if "non-standard JSON token" in message:
            issues.append(message)
        elif not any("digits beyond" in issue for issue in issues):
            issues.append(f"Python cannot load it: {type(err).__name__}: {message}")
    return issues


def _hex4(text: str, start: int) -> int | None:
    chunk = text[start : start + 4]
    if len(chunk) != 4:
        return None
    try:
        return int(chunk, 16)
    except ValueError:
        return None


def scan(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """The rows of the copy to look at before the shadow starts."""
    findings: list[dict[str, Any]] = []
    for table, filter_column in AGENT_TABLES.items():
        columns = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
        if not columns:
            findings.append({"table": table, "issue": "table missing"})
            continue
        has_name = any(column[1] == "name" for column in columns)
        where = f' WHERE "{filter_column}"' if filter_column else ""
        label = '"name"' if has_name else "NULL"
        for column in columns:
            name, declared = column[1], column[2]
            classes = expected_classes(declared)
            if classes is None:
                continue
            for row_id, row_name, actual in connection.execute(
                f'SELECT id, {label}, typeof("{name}") FROM "{table}"{where} ORDER BY id'
            ):
                if actual not in classes:
                    findings.append(
                        {
                            "table": table,
                            "id": row_id,
                            "name": row_name,
                            "column": name,
                            "issue": f"stored as {actual}, the agent reads "
                            f"{'/'.join(sorted(classes - {'null'}))} (D-13)",
                        }
                    )
        json_column = JSON_COLUMNS.get(table)
        if json_column is None:
            continue
        for row_id, row_name, text in connection.execute(
            f'SELECT id, name, "{json_column}" FROM "{table}"{where} ORDER BY id'
        ):
            if not isinstance(text, str):
                continue  # reported by the storage class check
            for issue in json_issues(text):
                findings.append(
                    {
                        "table": table,
                        "id": row_id,
                        "name": row_name,
                        "column": json_column,
                        "issue": issue,
                    }
                )
    return findings


def check_shadow_dir(path: Path) -> str | None:
    """Why ``path`` cannot hold the copy, or ``None``."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return f"shadow directory does not exist: {path}"
    if not stat.S_ISDIR(info.st_mode):
        return f"not a directory: {path}"
    if info.st_uid != os.geteuid():
        return f"shadow directory {path} must belong to the current user"
    if stat.S_IMODE(info.st_mode) != 0o700:
        return (
            f"shadow directory {path} must have mode 0700, "
            f"has {stat.S_IMODE(info.st_mode):04o}"
        )
    for name in (COPY_NAME, f"{COPY_NAME}-wal", f"{COPY_NAME}-shm", META_NAME):
        if (path / name).exists():
            return f"refusing to overwrite {path / name}"
    return None


def backup(source: Path, dest: Path) -> None:
    src = sqlite3.connect(f"file:{source}?mode=ro", uri=True, timeout=30)
    try:
        fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(fd)
        out = sqlite3.connect(dest)
        try:
            src.backup(out)
        finally:
            out.close()
    finally:
        src.close()


def prepare(dest: Path, disable: list[str]) -> dict[str, Any]:
    connection = sqlite3.connect(dest, timeout=30)
    try:
        check = connection.execute("PRAGMA quick_check").fetchone()[0]
        if check != "ok":
            raise RuntimeError(f"quick_check of the copy failed: {check}")
        missing = []
        with connection:
            for name in disable:
                changed = connection.execute(
                    "UPDATE metric_collection_spec SET enabled = 0 WHERE name = ?",
                    (name,),
                ).rowcount
                if not changed:
                    missing.append(name)
        if missing:
            raise RuntimeError(f"no spec named {', '.join(map(repr, missing))}")
        specs = connection.execute(
            "SELECT spec_type, COUNT(*) FROM metric_collection_spec WHERE enabled"
            " GROUP BY spec_type ORDER BY spec_type"
        ).fetchall()
        conditions = connection.execute(
            "SELECT COUNT(*) FROM condition_definition WHERE enabled"
        ).fetchone()[0]
        findings = scan(connection)
    finally:
        connection.close()
    return {
        "enabled_specs": dict(specs),
        "enabled_conditions": conditions,
        "disabled_specs": sorted(disable),
        "findings": findings,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--source", required=True, type=Path, help="live database (read-only)"
    )
    parser.add_argument(
        "--shadow-dir",
        required=True,
        type=Path,
        help="existing 0700 directory of the caller; receives db.sqlite3 and shadow.json",
    )
    parser.add_argument(
        "--disable-spec",
        action="append",
        default=[],
        metavar="NAME",
        help="disable this spec in the copy (repeatable)",
    )
    args = parser.parse_args(argv)
    if not args.source.is_file():
        print(f"source database not found: {args.source}", file=sys.stderr)
        return 1
    problem = check_shadow_dir(args.shadow_dir)
    if problem:
        print(problem, file=sys.stderr)
        return 1
    dest = args.shadow_dir / COPY_NAME
    if args.source.resolve() == dest.resolve():
        print("source and copy are the same file", file=sys.stderr)
        return 1
    t0 = dt.datetime.now(dt.UTC)
    try:
        backup(args.source, dest)
        summary = prepare(dest, args.disable_spec)
    except (sqlite3.Error, RuntimeError) as err:
        print(f"prepare failed: {err} (copy: {dest})", file=sys.stderr)
        return 1
    meta = {
        "t0": utc_text(t0),
        "source": str(args.source),
        "copy": str(dest),
        **summary,
    }
    fd = os.open(
        args.shadow_dir / META_NAME, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
    )
    with os.fdopen(fd, "w", encoding="utf-8") as out:
        json.dump(meta, out, indent=1)
        out.write("\n")
    print(f"T0={meta['t0']}")
    print(f"copy {dest}")
    print(f"enabled specs by type: {meta['enabled_specs']}")
    print(f"enabled conditions: {meta['enabled_conditions']}")
    if meta["disabled_specs"]:
        print(f"disabled in the copy: {', '.join(meta['disabled_specs'])}")
    for finding in meta["findings"]:
        where = f"{finding['table']}"
        if "id" in finding:
            where += f" id={finding['id']}"
        if finding.get("name") is not None:
            where += f" name={finding['name']!r}"
        if "column" in finding:
            where += f" column={finding['column']}"
        print(f"look at: {where}: {finding['issue']}")
    if meta["findings"]:
        print(f"{len(meta['findings'])} row(s) to look at before the shadow starts")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
