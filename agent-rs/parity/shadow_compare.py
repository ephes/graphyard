#!/usr/bin/env python3
"""Compare a shadow run with production (agent-rs/PLAN.md 11.2, steps 4-5).

Inputs, all in the shadow directory by default:

- ``shadow.json`` of ``shadow_prepare.py``: T0 and the specs disabled in the
  copy (left out of every comparison and listed as excluded);
- ``points.lp``, the shadow agent's ``--influx-write-file``: the window ends
  at its last ``# write`` batch (``--stop`` overrides; ``--start`` overrides
  T0);
- ``record.jsonl`` of ``shadow_record.py``;
- the points the Python agent wrote to production over the window: queried
  with ``docker exec graphyard-influxdb influx query --raw`` (the container's
  CLI configuration, no token in this tool; ``--influx-command`` overrides)
  or read from ``--prod-csv`` (annotated CSV as ``influx query --raw``
  prints it; ``--save-prod-csv`` keeps a fetched copy).

Points are compared per spec (the ``spec_name`` tag every collector point
carries), time window ``[start, stop)`` on both sides:

- series keys (measurement and the whole tag set): exact;
- ``home_assistant_*`` specs: points by ``(series, timestamp)``, values
  exact (both agents stamp a point with the entity's ``last_updated``);
- ``http_json_metric`` and ``unifi_device_traffic``: each shadow point is
  paired with the production point of the same series nearest in time within
  ``--align-seconds``; a series whose median relative difference exceeds
  ``--tolerance`` is a finding, single pairs outside it are notes (P2);
- ``http_page_probe``: status code, success and redirect count as value
  sequences with repeats collapsed (exact), TTFB and total time as medians
  (notes);

and from the record:

- spec ``last_status`` sequences (one entry per run seen, repeats collapsed;
  exact) and ``last_error`` sequences (notes, P2 texts masked with
  ``--mask``);
- condition ``status`` sequences (exact), messages and the last value
  (notes);
- the ``metric_collectors`` and ``condition_evaluator`` heartbeats: status
  sequences (exact), ``details`` and ``last_error`` (notes);
- registry rows of the last tick by natural key, timestamps masked to
  null/non-null, a service's host by ``host_id`` (exact);
- the shadow unit: no restart, never inactive, the lean goals of PLAN.md 10
  (steady ``MemoryCurrent``, ``MemoryPeak``, ``RssAnon``, ``VmRSS``, CPU per
  day); the production unit's numbers are shown for comparison.

Every finding is a difference to explain (an endpoint that changed between
the two samples) or a bug. The report goes to stdout; ``--json PATH`` writes
everything. Exit status 0, 1 for unreadable input or, with
``--fail-on-findings``, when findings remain. Standard library only; run as
root on macmini (``docker`` and the 0700 shadow directory)::

    python3 shadow_compare.py --shadow-dir /var/lib/graphyard-shadow \\
        --json /root/graphyard-shadow-report.json
"""

from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import io
import json
import re
import shlex
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

HA_TYPES = {"home_assistant_sensor", "home_assistant_env_scan"}
ALIGNED_TYPES = {"http_json_metric", "unifi_device_traffic"}
PAGE_PROBE = "http_page_probe"
PAGE_PROBE_TIMINGS = {
    "service.http_page_ttfb_seconds",
    "service.http_page_total_seconds",
}
AGENT_HEARTBEATS = ("metric_collectors", "condition_evaluator")
#: Columns of an annotated CSV record that are not tags.
NON_TAG_COLUMNS = {
    "",
    "result",
    "table",
    "_start",
    "_stop",
    "_time",
    "_value",
    "_field",
}
#: PLAN.md 10: Rust targets.
MIB = 1024 * 1024
BUDGETS = {
    "MemoryCurrent (steady, median)": 16 * MIB,
    "MemoryPeak": 40 * MIB,
    "RssAnon": 12 * MIB,
    "VmRSS (ps RSS)": 25 * MIB,
}
CPU_BUDGET_MINUTES_PER_DAY = 1.5
#: Samples of the first minutes after the start are not "steady".
STEADY_AFTER_SECONDS = 600
#: Library-derived (P2) fragments of messages, replaced before comparing.
DEFAULT_MASKS = (
    r"line \d+ column \d+ \(char \d+\)",
    r"at line \d+ column \d+",
)
SHOW = 12

Series = tuple[str, tuple[tuple[str, str], ...]]


# --- time ------------------------------------------------------------------

_RFC3339 = re.compile(
    r"(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})?"
)


def rfc3339_ns(text: str) -> int:
    """Nanoseconds since the epoch of an RFC 3339 time (``Z`` or offset;
    none is UTC) or of a decimal Unix epoch in seconds."""
    text = text.strip()
    if re.fullmatch(r"\d+(\.\d+)?", text):
        whole, _, frac = text.partition(".")
        return int(whole) * 10**9 + int(frac.ljust(9, "0")[:9] or 0)
    match = _RFC3339.fullmatch(text)
    if match is None:
        raise ValueError(f"not an RFC 3339 time: {text!r}")
    seconds = calendar.timegm(
        time.strptime(f"{match[1]}T{match[2]}", "%Y-%m-%dT%H:%M:%S")
    )
    offset = match[4] or "Z"
    if offset != "Z":
        sign = 1 if offset[0] == "+" else -1
        seconds -= sign * (int(offset[1:3]) * 3600 + int(offset[4:6]) * 60)
    return seconds * 10**9 + int((match[3] or "").ljust(9, "0"))


def ns_text(ns: int) -> str:
    seconds, rest = divmod(ns, 10**9)
    moment = dt.datetime.fromtimestamp(seconds, dt.UTC)
    return f"{moment:%Y-%m-%dT%H:%M:%S}.{rest:09d}Z".replace(".000000000Z", "Z")


# --- points ----------------------------------------------------------------


@dataclass(frozen=True)
class Point:
    series: Series
    ts: int
    value: float

    @property
    def tags(self) -> dict[str, str]:
        return dict(self.series[1])

    @property
    def spec(self) -> str:
        return self.tags.get("spec_name", "")

    @property
    def metric(self) -> str:
        return self.tags.get("metric", "")


def series_text(series: Series) -> str:
    tags = ",".join(f"{key}={value}" for key, value in series[1])
    return f"{series[0]}{{{tags}}}"


def _split(
    text: str, separator: str, limit: int = -1, quotes: bool = False
) -> list[str]:
    """Split on ``separator`` outside backslash escapes and, with ``quotes``
    (the field section, where string values are quoted), double quotes."""
    parts: list[str] = []
    current: list[str] = []
    quoted = False
    index = 0
    while index < len(text):
        char = text[index]
        if char == "\\" and index + 1 < len(text):
            current.append(text[index : index + 2])
            index += 2
            continue
        if quotes and char == '"':
            quoted = not quoted
        if char == separator and not quoted and (limit < 0 or len(parts) < limit):
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
        index += 1
    parts.append("".join(current))
    return parts


def _unescape(text: str, specials: str) -> str:
    """InfluxDB's reading of an escaped name: ``\\`` before one of
    ``specials`` drops, any other backslash is literal."""
    return re.sub(rf"\\([{re.escape(specials)}])", r"\1", text)


def parse_line(line: str) -> tuple[str, dict[str, str], dict[str, Any], int | None]:
    """One line of InfluxDB line protocol."""
    # Quotes are literal in the measurement and the tags; only string field
    # values are quoted.
    head = _split(line, " ", 1)
    if len(head) < 2:
        raise ValueError(f"not line protocol: {line!r}")
    key = head[0]
    sections = _split(head[1], " ", 1, quotes=True)
    fields_text = sections[0]
    timestamp = int(sections[1]) if len(sections) > 1 and sections[1] else None
    parts = _split(key, ",")
    measurement = _unescape(parts[0], ", ")
    tags: dict[str, str] = {}
    for pair in parts[1:]:
        pieces = _split(pair, "=", 1)
        if len(pieces) != 2:
            raise ValueError(f"tag without value: {pair!r}")
        name, value = pieces
        tags[_unescape(name, ",= ")] = _unescape(value, ",= ")
    fields: dict[str, Any] = {}
    for pair in _split(fields_text, ",", quotes=True):
        pieces = _split(pair, "=", 1)
        if len(pieces) != 2:
            raise ValueError(f"field without value: {pair!r}")
        name, value = pieces
        if value.startswith('"'):
            parsed: Any = value[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        elif value in ("t", "T", "true", "True", "TRUE"):
            parsed = True
        elif value in ("f", "F", "false", "False", "FALSE"):
            parsed = False
        elif value.endswith(("i", "u")):
            parsed = int(value[:-1])
        else:
            parsed = float(value)
        fields[_unescape(name, ",= ")] = parsed
    return measurement, tags, fields, timestamp


def read_points_file(path: Path) -> tuple[list[Point], list[int]]:
    """The shadow's ``--influx-write-file``: its points and batch times."""
    points: list[Point] = []
    batches: list[int] = []
    with open(path, encoding="utf-8") as stream:
        for number, raw in enumerate(stream, 1):
            line = raw.rstrip("\n")
            if not line:
                continue
            if line.startswith("#"):
                match = re.match(r"# write (\S+) points=\d+$", line)
                if match:
                    batches.append(rfc3339_ns(match[1]))
                continue
            try:
                measurement, tags, fields, timestamp = parse_line(line)
            except ValueError as err:
                raise ValueError(f"{path}:{number}: {err}") from None
            if "value" not in fields or timestamp is None:
                raise ValueError(f"{path}:{number}: no value field or timestamp")
            points.append(
                Point(
                    (measurement, tuple(sorted((k, v) for k, v in tags.items() if v))),
                    timestamp,
                    float(fields["value"]),
                )
            )
    return points, batches


def parse_annotated_csv(text: str) -> list[dict[str, str]]:
    """The records of ``influx query --raw`` (annotated CSV, several tables,
    a new header whenever the columns change). An error table raises."""
    records: list[dict[str, str]] = []
    header: list[str] | None = None
    defaults: list[str] = []
    for row in csv.reader(io.StringIO(text)):
        if not row or all(cell == "" for cell in row):
            header = None
            defaults = []
            continue
        if row[0].startswith("#"):
            if row[0] == "#default":
                defaults = row
            continue
        if header is None:
            header = row
            if "error" in header and "_value" not in header:
                header = ["__error__", *header[1:]]
            continue
        if header[0] == "__error__":
            raise RuntimeError(f"InfluxDB query failed: {row}")
        record = {}
        for index, name in enumerate(header):
            if not name:
                continue
            value = row[index] if index < len(row) else ""
            if value == "" and index < len(defaults) and defaults[index]:
                value = defaults[index]
            record[name] = value
        records.append(record)
    return records


def influx_points(records: Iterable[dict[str, str]]) -> list[Point]:
    points = []
    for record in records:
        if record.get("_field", "value") != "value":
            continue
        tags = tuple(
            sorted(
                (key, value)
                for key, value in record.items()
                if key not in NON_TAG_COLUMNS and key != "_measurement" and value
            )
        )
        points.append(
            Point(
                (record.get("_measurement", ""), tags),
                rfc3339_ns(record["_time"]),
                float(record["_value"]),
            )
        )
    return points


def flux_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("${", "\\${")


def flux_query(bucket: str, measurement: str, start: int, stop: int) -> str:
    return (
        f'from(bucket: "{flux_escape(bucket)}")\n'
        f"  |> range(start: {ns_text(start)}, stop: {ns_text(stop)})\n"
        f'  |> filter(fn: (r) => r._measurement == "{flux_escape(measurement)}"'
        ' and r._field == "value" and exists r.spec_name)\n'
        '  |> drop(columns: ["_start", "_stop"])'
    )


def fetch_prod_csv(command: str, org: str | None, flux: str) -> str:
    argv = shlex.split(command)
    if org:
        argv += ["--org", org]
    completed = subprocess.run(
        [*argv, flux], capture_output=True, text=True, check=False
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"{argv[0]} exited {completed.returncode}: {completed.stderr.strip()}"
        )
    return completed.stdout


# --- record ----------------------------------------------------------------


@dataclass
class Event:
    ts: float
    values: tuple[Any, ...]


@dataclass
class DatabaseHistory:
    """The replayed record of one database."""

    state: dict[str, dict[str, dict[str, Any]]] = field(
        default_factory=lambda: defaultdict(dict)
    )
    #: (table, natural name) -> events after the first tick.
    events: dict[tuple[str, str], list[Event]] = field(
        default_factory=lambda: defaultdict(list)
    )
    errors: list[tuple[float, str]] = field(default_factory=list)
    ticks: int = 0

    #: Table -> (the column whose change marks a new run, the event columns).
    WATCH: ClassVar[dict[str, tuple[str, tuple[str, ...]]]] = {
        "metric_collection_spec": ("last_run_at", ("last_status", "last_error")),
        "condition_definition": (
            "last_evaluated",
            ("status", "message", "last_value"),
        ),
        "pipeline_heartbeat": ("updated_at", ("status", "last_error", "details")),
    }

    def add(self, ts: float, line: dict[str, Any]) -> None:
        if "error" in line:
            self.errors.append((ts, str(line["error"])))
            return
        first = self.ticks == 0
        self.ticks += 1
        for key, value in line.items():
            if key.endswith("_deleted"):
                table = key[: -len("_deleted")]
                for row_id in value:
                    self.state[table].pop(str(row_id), None)
                continue
            table = key
            watch = self.WATCH.get(table)
            for row_id, row in value.items():
                before = self.state[table].get(row_id)
                self.state[table][row_id] = row
                if first or watch is None:
                    continue
                marker, columns = watch
                if before is not None and before.get(marker) == row.get(marker):
                    continue
                self.events[(table, row["name"])].append(
                    Event(ts, tuple(row.get(column) for column in columns))
                )

    def rows(self, table: str) -> list[dict[str, Any]]:
        return list(self.state.get(table, {}).values())


@dataclass
class Record:
    dbs: dict[str, DatabaseHistory]
    units: dict[str, list[tuple[float, dict[str, Any]]]]
    first_ts: float | None
    last_ts: float | None


def read_record(path: Path) -> Record:
    dbs = {"prod": DatabaseHistory(), "shadow": DatabaseHistory()}
    units: dict[str, list[tuple[float, dict[str, Any]]]] = defaultdict(list)
    first = last = None
    with open(path, encoding="utf-8") as stream:
        for number, raw in enumerate(stream, 1):
            if not raw.strip():
                continue
            try:
                line = json.loads(raw)
            except ValueError as err:
                raise ValueError(f"{path}:{number}: {err}") from None
            ts = float(line["ts"])
            first = ts if first is None else first
            last = ts
            for role, history in dbs.items():
                if role in line.get("dbs", {}):
                    history.add(ts, line["dbs"][role])
            for role, sample in line.get("units", {}).items():
                units[role].append((ts, sample))
    return Record(dbs, dict(units), first, last)


# --- comparison ------------------------------------------------------------


@dataclass
class Report:
    window: dict[str, Any] = field(default_factory=dict)
    findings: list[dict[str, Any]] = field(default_factory=list)
    notes: list[dict[str, Any]] = field(default_factory=list)
    sections: dict[str, Any] = field(default_factory=dict)

    def finding(self, section: str, subject: str, detail: Any) -> None:
        self.findings.append({"section": section, "subject": subject, "detail": detail})

    def note(self, section: str, subject: str, detail: Any) -> None:
        self.notes.append({"section": section, "subject": subject, "detail": detail})


def collapse(values: Iterable[Any]) -> list[Any]:
    out: list[Any] = []
    for value in values:
        if not out or out[-1] != value:
            out.append(value)
    return out


def masked(text: Any, masks: list[re.Pattern[str]]) -> Any:
    if not isinstance(text, str):
        return text
    for mask in masks:
        text = mask.sub("<p2>", text)
    return text


def relative_difference(a: float, b: float) -> float:
    scale = max(abs(a), abs(b))
    return 0.0 if scale == 0 else abs(a - b) / scale


def _sequences(
    record: Record,
    table: str,
    name: str,
    column: int,
    masks: list[re.Pattern[str]],
) -> dict[str, list[Any]]:
    """Per database, the values of one event column, repeats collapsed."""
    return {
        role: collapse(
            masked(event.values[column], masks)
            for event in history.events.get((table, name), [])
        )
        for role, history in record.dbs.items()
    }


def _compare_sequences(
    report: Report,
    record: Record,
    section: str,
    table: str,
    name: str,
    masks: list[re.Pattern[str]],
    exact: str,
    noted: list[tuple[int, str]],
) -> None:
    """Event column 0 (``exact``) is a finding when the sequences differ,
    the ``noted`` columns are notes; a name the shadow never updated is a
    finding."""
    statuses = _sequences(record, table, name, 0, [])
    if statuses["prod"] != statuses["shadow"]:
        report.finding(
            section, name, {exact: {r: seq[:SHOW] for r, seq in statuses.items()}}
        )
    for column, label in noted:
        values = _sequences(record, table, name, column, masks)
        if values["prod"] != values["shadow"]:
            report.note(
                section, name, {label: {r: seq[:SHOW] for r, seq in values.items()}}
            )
    if not record.dbs["shadow"].events.get((table, name)):
        report.finding(section, name, "no shadow update recorded")


def compare_rows(
    report: Report,
    record: Record,
    excluded: set[str],
    masks: list[re.Pattern[str]],
) -> dict[str, str]:
    """Specs, conditions and heartbeats; returns the spec types by name."""
    spec_types: dict[str, str] = {}
    enabled: dict[str, set[str]] = {}
    for role, history in record.dbs.items():
        enabled[role] = set()
        for row in history.rows("metric_collection_spec"):
            spec_types.setdefault(row["name"], row["spec_type"])
            if row.get("enabled"):
                enabled[role].add(row["name"])
    names = sorted((enabled["prod"] | enabled["shadow"]) - excluded)
    for name in sorted(excluded):
        report.note("specs", name, "excluded: disabled in the copy")
    for name in names:
        for role in ("prod", "shadow"):
            if name not in enabled[role]:
                report.finding("specs", name, f"not enabled in {role}")
        _compare_sequences(
            report,
            record,
            "specs",
            "metric_collection_spec",
            name,
            masks,
            "last_status",
            [(1, "last_error")],
        )
    report.sections["specs"] = {
        "compared": len(names),
        "runs": {
            role: sum(
                len(history.events.get(("metric_collection_spec", name), []))
                for name in names
            )
            for role, history in record.dbs.items()
        },
    }

    conditions = sorted(
        {
            row["name"]
            for history in record.dbs.values()
            for row in history.rows("condition_definition")
            if row.get("enabled")
        }
    )
    for name in conditions:
        _compare_sequences(
            report,
            record,
            "conditions",
            "condition_definition",
            name,
            masks,
            "status",
            [(1, "message")],
        )
        last = {
            role: (events[-1].values[2] if events else None)
            for role, events in (
                (r, h.events.get(("condition_definition", name), []))
                for r, h in record.dbs.items()
            )
        }
        if last["prod"] != last["shadow"]:
            report.note("conditions", name, {"last_value (last run)": last})
    report.sections["conditions"] = {"compared": len(conditions)}

    for name in AGENT_HEARTBEATS:
        _compare_sequences(
            report,
            record,
            "heartbeats",
            "pipeline_heartbeat",
            name,
            masks,
            "status",
            [(1, "last_error")],
        )
        details = {
            role: sorted(
                {
                    str(e.values[2])
                    for e in h.events.get(("pipeline_heartbeat", name), [])
                }
            )
            for role, h in record.dbs.items()
        }
        if details["prod"] != details["shadow"]:
            report.note(
                "heartbeats",
                name,
                {"details seen": {r: d[:SHOW] for r, d in details.items()}},
            )
    for role, history in record.dbs.items():
        if history.errors:
            report.note(
                "record",
                role,
                f"{len(history.errors)} unreadable tick(s), first: {history.errors[0][1]}",
            )
    return {name: spec_types[name] for name in names}


def registry_view(history: DatabaseHistory) -> dict[str, dict[str, Any]]:
    hosts_by_id = {
        str(row["id"]): row["host_id"] for row in history.rows("host_registry")
    }
    view: dict[str, dict[str, Any]] = {}
    for row in history.rows("subject_registry"):
        view[f"subject {row['subject_type']}/{row['subject_id']}"] = {
            "display_name": row["display_name"],
            "source_system": row["source_system"],
            "last_seen_at": row["last_seen_at"] is not None,
        }
    for row in history.rows("host_registry"):
        view[f"host {row['host_id']}"] = {
            "display_name": row["display_name"],
            "grafana_url": row["grafana_url"],
            "enabled": row["enabled"],
            "last_seen_at": row["last_seen_at"] is not None,
        }
    for row in history.rows("service_registry"):
        host = row.get("host_id")
        view[f"service {row['service_id']}"] = {
            "display_name": row["display_name"],
            "grafana_url": row["grafana_url"],
            "enabled": row["enabled"],
            "host": hosts_by_id.get(str(host), host) if host is not None else None,
            "last_seen_at": row["last_seen_at"] is not None,
        }
    return view


def compare_registry(report: Report, record: Record) -> None:
    prod = registry_view(record.dbs["prod"])
    shadow = registry_view(record.dbs["shadow"])
    for key in sorted(prod.keys() | shadow.keys()):
        if prod.get(key) != shadow.get(key):
            report.finding(
                "registry", key, {"prod": prod.get(key), "shadow": shadow.get(key)}
            )
    report.sections["registry"] = {"rows": {"prod": len(prod), "shadow": len(shadow)}}


def compare_points(
    report: Report,
    prod_points: list[Point],
    shadow_points: list[Point],
    spec_types: dict[str, str],
    tolerance: float,
    align_ns: int,
) -> None:
    by_spec: dict[str, dict[str, list[Point]]] = defaultdict(
        lambda: {"prod": [], "shadow": []}
    )
    ignored = {"prod": 0, "shadow": 0}
    for role, points in (("prod", prod_points), ("shadow", shadow_points)):
        for point in points:
            if point.spec in spec_types:
                by_spec[point.spec][role].append(point)
            else:
                ignored[role] += 1
    for role, count in ignored.items():
        if count:
            report.note(
                "points", role, f"{count} point(s) of specs outside the comparison"
            )
    summary: dict[str, Any] = {}
    for spec in sorted(spec_types):
        spec_type = spec_types[spec]
        sides = by_spec.get(spec, {"prod": [], "shadow": []})
        series = {role: {p.series for p in pts} for role, pts in sides.items()}
        entry: dict[str, Any] = {
            "spec_type": spec_type,
            "points": {role: len(pts) for role, pts in sides.items()},
            "series": {role: len(s) for role, s in series.items()},
        }
        summary[spec] = entry
        for role, other in (("prod", "shadow"), ("shadow", "prod")):
            only = sorted(series[role] - series[other])
            if only:
                report.finding(
                    "series",
                    spec,
                    {
                        f"only in {role}": [series_text(s) for s in only[:SHOW]],
                        "count": len(only),
                    },
                )
        if spec_type in HA_TYPES:
            entry.update(_compare_exact(report, spec, sides))
        elif spec_type in ALIGNED_TYPES:
            entry.update(_compare_aligned(report, spec, sides, tolerance, align_ns))
        elif spec_type == PAGE_PROBE:
            entry.update(_compare_page_probe(report, spec, sides))
        compared = entry.get("matched", entry.get("pairs"))
        if compared == 0 and all(sides.values()):
            # Values were written on both sides but none could be compared:
            # the comparison proved nothing about them.
            report.finding("values", spec, "no value pair to compare (inconclusive)")
    report.sections["points"] = summary


def _compare_exact(
    report: Report, spec: str, sides: dict[str, list[Point]]
) -> dict[str, Any]:
    keyed = {
        role: {(p.series, p.ts): p.value for p in pts} for role, pts in sides.items()
    }
    common = keyed["prod"].keys() & keyed["shadow"].keys()
    mismatched = sorted(k for k in common if keyed["prod"][k] != keyed["shadow"][k])
    if mismatched:
        report.finding(
            "values",
            spec,
            {
                "mismatched points": [
                    f"{series_text(s)} @{ns_text(ts)}: prod {keyed['prod'][(s, ts)]!r}"
                    f" shadow {keyed['shadow'][(s, ts)]!r}"
                    for s, ts in mismatched[:SHOW]
                ],
                "count": len(mismatched),
            },
        )
    only = {
        role: len(keyed[role].keys() - keyed[other].keys())
        for role, other in (("prod", "shadow"), ("shadow", "prod"))
    }
    if any(only.values()):
        report.note("values", spec, {"(series, ts) in one side only": only})
    return {"matched": len(common), "mismatched": len(mismatched), "unpaired": only}


def _compare_aligned(
    report: Report,
    spec: str,
    sides: dict[str, list[Point]],
    tolerance: float,
    align_ns: int,
) -> dict[str, Any]:
    prod_by_series: dict[Series, list[Point]] = defaultdict(list)
    for point in sides["prod"]:
        prod_by_series[point.series].append(point)
    for points in prod_by_series.values():
        points.sort(key=lambda p: p.ts)
    differences: dict[Series, list[tuple[float, Point, Point]]] = defaultdict(list)
    unpaired = 0
    for point in sides["shadow"]:
        candidates = prod_by_series.get(point.series, [])
        best = min(candidates, key=lambda p: abs(p.ts - point.ts), default=None)
        if best is None or abs(best.ts - point.ts) > align_ns:
            unpaired += 1
            continue
        differences[point.series].append(
            (relative_difference(best.value, point.value), best, point)
        )
    pairs = sum(len(d) for d in differences.values())
    outside = 0
    for series, diffs in sorted(differences.items()):
        median = statistics.median(d for d, _, _ in diffs)
        beyond = [d for d in diffs if d[0] > tolerance]
        outside += len(beyond)
        if median > tolerance:
            report.finding(
                "values",
                spec,
                {
                    "series": series_text(series),
                    "median relative difference": round(median, 6),
                    "pairs": len(diffs),
                },
            )
        elif beyond:
            worst = max(beyond, key=lambda d: d[0])
            report.note(
                "values",
                spec,
                {
                    "series": series_text(series),
                    "pairs beyond tolerance": f"{len(beyond)}/{len(diffs)}",
                    "worst": f"prod {worst[1].value!r} @{ns_text(worst[1].ts)}, "
                    f"shadow {worst[2].value!r} @{ns_text(worst[2].ts)}",
                },
            )
    if unpaired:
        report.note(
            "values",
            spec,
            f"{unpaired} shadow point(s) without a production point within the alignment",
        )
    return {"pairs": pairs, "beyond_tolerance": outside, "unpaired_shadow": unpaired}


def _compare_page_probe(
    report: Report, spec: str, sides: dict[str, list[Point]]
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    grouped: dict[Series, dict[str, list[Point]]] = defaultdict(
        lambda: {"prod": [], "shadow": []}
    )
    for role, points in sides.items():
        for point in points:
            grouped[point.series][role].append(point)
    for series, by_role in sorted(grouped.items()):
        metric = dict(series[1]).get("metric", "")
        values = {
            role: [p.value for p in sorted(pts, key=lambda p: p.ts)]
            for role, pts in by_role.items()
        }
        if metric in PAGE_PROBE_TIMINGS:
            medians = {
                role: (round(statistics.median(v), 6) if v else None)
                for role, v in values.items()
            }
            out[metric] = medians
            report.note("page probe timings", spec, {metric: {"median": medians}})
            continue
        sequences = {role: collapse(v) for role, v in values.items()}
        if sequences["prod"] != sequences["shadow"] and all(values.values()):
            report.finding(
                "page probe",
                spec,
                {
                    "series": series_text(series),
                    "sequence": {role: seq[:SHOW] for role, seq in sequences.items()},
                },
            )
    return out


def _number(text: Any) -> int | None:
    if isinstance(text, int):
        return text
    if not isinstance(text, str):
        return None
    head = text.split()[0] if text.split() else ""
    return int(head) if head.isdigit() else None


def unit_summary(samples: list[tuple[float, dict[str, Any]]]) -> dict[str, Any]:
    """Lean-goal numbers (bytes, minutes per day) and restart evidence."""
    good = [(ts, s) for ts, s in samples if "error" not in s]
    summary: dict[str, Any] = {"samples": len(samples), "usable": len(good)}
    if not good:
        return summary
    first_ts = good[0][0]
    steady = [
        _number(s.get("MemoryCurrent"))
        for ts, s in good
        if ts - first_ts >= STEADY_AFTER_SECONDS
    ]
    steady_values = [v for v in steady if v is not None]

    def kib_max(key: str) -> int | None:
        values = [_number(s.get(key)) for _, s in good]
        present = [v for v in values if v is not None]
        return max(present) * 1024 if present else None

    peaks = [_number(s.get("MemoryPeak")) for _, s in good]
    present_peaks = [v for v in peaks if v is not None]
    summary["MemoryCurrent (steady, median)"] = (
        int(statistics.median(steady_values)) if steady_values else None
    )
    summary["MemoryPeak"] = max(present_peaks) if present_peaks else None
    summary["RssAnon"] = kib_max("RssAnon")
    summary["VmRSS (ps RSS)"] = kib_max("VmRSS")
    pids = collapse(s.get("MainPID") for _, s in good)
    summary["main_pids"] = pids
    summary["inactive_samples"] = sum(
        1 for _, s in good if s.get("ActiveState") != "active"
    )
    restarts = [_number(s.get("NRestarts")) for _, s in good]
    present_restarts = [r for r in restarts if r is not None]
    summary["restarts"] = (
        max(present_restarts) - min(present_restarts) if present_restarts else None
    )
    cpu = [
        (ts, _number(s.get("CPUUsageNSec")), s.get("MainPID"))
        for ts, s in good
        if _number(s.get("CPUUsageNSec")) is not None
    ]
    if len(cpu) >= 2 and cpu[-1][0] > cpu[0][0] and cpu[0][2] == cpu[-1][2]:
        used = (cpu[-1][1] or 0) - (cpu[0][1] or 0)
        elapsed = cpu[-1][0] - cpu[0][0]
        summary["cpu_minutes_per_day"] = round(used / 1e9 / 60 / elapsed * 86400, 3)
    else:
        summary["cpu_minutes_per_day"] = None
    return summary


def compare_units(report: Report, record: Record) -> None:
    summaries = {role: unit_summary(s) for role, s in record.units.items()}
    report.sections["units"] = summaries
    shadow = summaries.get("shadow")
    if not shadow or not shadow.get("usable"):
        report.finding("units", "shadow", "no usable unit samples")
        return
    if len(shadow["main_pids"]) > 1 or shadow["restarts"]:
        report.finding(
            "units",
            "shadow",
            {"main_pids": shadow["main_pids"], "restarts": shadow["restarts"]},
        )
    if shadow["inactive_samples"]:
        report.finding(
            "units", "shadow", f"{shadow['inactive_samples']} sample(s) not active"
        )
    for key, budget in BUDGETS.items():
        value = shadow.get(key)
        if value is None:
            report.note("lean goals", key, "not measured")
        elif value > budget:
            report.finding(
                "lean goals", key, f"{value / MIB:.1f} MiB > {budget / MIB:.0f} MiB"
            )
    cpu = shadow.get("cpu_minutes_per_day")
    if cpu is None:
        report.note("lean goals", "CPU", "not measured")
    elif cpu > CPU_BUDGET_MINUTES_PER_DAY:
        report.finding(
            "lean goals", "CPU", f"{cpu} min/day > {CPU_BUDGET_MINUTES_PER_DAY}"
        )
    prod = summaries.get("prod")
    if prod and prod.get("usable") and (len(prod["main_pids"]) > 1 or prod["restarts"]):
        report.note(
            "units",
            "prod",
            {"main_pids": prod["main_pids"], "restarts": prod["restarts"]},
        )


# --- output ----------------------------------------------------------------


def render(report: Report) -> str:
    lines = [
        (
            f"window: {report.window['start']} .. {report.window['stop']}"
            f" ({report.window['hours']} h)"
        ),
        f"points: prod {report.window['prod_points']}, shadow {report.window['shadow_points']}",
        "",
    ]
    for title, items in (("FINDINGS", report.findings), ("NOTES", report.notes)):
        lines.append(f"{title} ({len(items)})")
        for item in items:
            detail = item["detail"]
            if not isinstance(detail, str):
                detail = json.dumps(detail, ensure_ascii=False)
            lines.append(f"- [{item['section']}] {item['subject']}: {detail}")
        lines.append("")
    units = report.sections.get("units", {})
    for role in ("shadow", "prod"):
        summary = units.get(role)
        if not summary:
            continue
        parts = []
        for key in BUDGETS:
            value = summary.get(key)
            parts.append(
                f"{key}={'n/a' if value is None else f'{value / MIB:.1f} MiB'}"
            )
        parts.append(f"CPU={summary.get('cpu_minutes_per_day')} min/day")
        lines.append(f"{role} unit: " + ", ".join(parts))
    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--shadow-dir", type=Path, default=Path("/var/lib/graphyard-shadow")
    )
    parser.add_argument("--meta", type=Path, help="default: <shadow-dir>/shadow.json")
    parser.add_argument("--points", type=Path, help="default: <shadow-dir>/points.lp")
    parser.add_argument(
        "--record", type=Path, help="default: <shadow-dir>/record.jsonl"
    )
    parser.add_argument(
        "--prod-csv", type=Path, help="annotated CSV instead of a query"
    )
    parser.add_argument("--save-prod-csv", type=Path, help="keep the fetched CSV")
    parser.add_argument(
        "--influx-command",
        default="docker exec graphyard-influxdb influx query --raw",
        help="command that takes the Flux query as its last argument",
    )
    parser.add_argument("--org", help="passed as --org to the influx command")
    parser.add_argument("--bucket", default="graphyard")
    parser.add_argument("--measurement", default="graphyard_metrics")
    parser.add_argument("--start", help="RFC 3339 or epoch (default: T0)")
    parser.add_argument("--stop", help="RFC 3339 or epoch (default: last shadow batch)")
    parser.add_argument("--tolerance", type=float, default=0.05)
    parser.add_argument("--align-seconds", type=float, default=60.0)
    parser.add_argument("--mask", action="append", default=[], metavar="REGEX")
    parser.add_argument("--json", type=Path, help="write the full report here")
    parser.add_argument("--fail-on-findings", action="store_true")
    args = parser.parse_args(argv)
    meta_path = args.meta or args.shadow_dir / "shadow.json"
    points_path = args.points or args.shadow_dir / "points.lp"
    record_path = args.record or args.shadow_dir / "record.jsonl"
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        shadow_all, batches = read_points_file(points_path)
        record = read_record(record_path)
        masks = [re.compile(p) for p in (*DEFAULT_MASKS, *args.mask)]
        start = rfc3339_ns(args.start or meta["t0"])
        if args.stop:
            stop = rfc3339_ns(args.stop)
        elif batches:
            stop = max(batches)
        else:
            raise ValueError(f"{points_path} holds no batch; pass --stop")
        if stop <= start:
            raise ValueError("the window is empty (stop <= start)")
        if args.prod_csv:
            csv_text = args.prod_csv.read_text(encoding="utf-8")
        else:
            csv_text = fetch_prod_csv(
                args.influx_command,
                args.org,
                flux_query(args.bucket, args.measurement, start, stop),
            )
            if args.save_prod_csv:
                args.save_prod_csv.write_text(csv_text, encoding="utf-8")
        prod_points = [
            p
            for p in influx_points(parse_annotated_csv(csv_text))
            if p.series[0] == args.measurement and start <= p.ts < stop
        ]
    except (OSError, ValueError, KeyError, RuntimeError, re.error) as err:
        print(f"shadow_compare: {err}", file=sys.stderr)
        return 1
    shadow_points = [
        p
        for p in shadow_all
        if p.series[0] == args.measurement and start <= p.ts < stop
    ]
    report = Report()
    report.window = {
        "start": ns_text(start),
        "stop": ns_text(stop),
        "hours": round((stop - start) / 3.6e12, 2),
        "prod_points": len(prod_points),
        "shadow_points": len(shadow_points),
        "shadow_batches": len(batches),
    }
    excluded = set(meta.get("disabled_specs", []))
    spec_types = compare_rows(report, record, excluded, masks)
    compare_registry(report, record)
    compare_points(
        report,
        prod_points,
        shadow_points,
        spec_types,
        args.tolerance,
        int(args.align_seconds * 1e9),
    )
    compare_units(report, record)
    sys.stdout.write(render(report))
    if args.json:
        args.json.write_text(
            json.dumps(
                {
                    "window": report.window,
                    "findings": report.findings,
                    "notes": report.notes,
                    "sections": report.sections,
                },
                indent=1,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
    return 1 if args.fail_on_findings and report.findings else 0


if __name__ == "__main__":
    sys.exit(main())
