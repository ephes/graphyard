"""Shadow-run tooling of the Rust agent (agent-rs/parity, PLAN.md 11.2).

``shadow_prepare.py`` makes the database copy, ``shadow_record.py`` records
both databases over time, ``shadow_compare.py`` compares the record and the
shadow's ``--influx-write-file`` with the production points. These tests run
the three on two databases built from the schema fixture, with a fake
``systemctl``, a fake ``/proc`` and a fake ``influx`` command.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import stat
import sys
import threading
from pathlib import Path
from types import ModuleType

import pytest

REPO = Path(__file__).resolve().parents[1]
PARITY = REPO / "agent-rs" / "parity"
SCHEMA = REPO / "agent-rs" / "fixtures" / "schema" / "schema.sql"


def _load(name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, PARITY / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


prepare = _load("shadow_prepare")
recorder = _load("shadow_record")
compare = _load("shadow_compare")

CREATED = "2026-10-01 00:00:00"
SPECS = (
    # name, spec_type, config
    (
        "ha env",
        "home_assistant_env_scan",
        '{"base_url": "http://ha", "access_token": "secret"}',
    ),
    ("json temp", "http_json_metric", '{"url": "http://x/value", "metric_path": "v"}'),
    ("page site", "http_page_probe", '{"url": "https://example.org/"}'),
    ("unifi", "unifi_device_traffic", '{"base_url": "https://127.0.0.1:8443"}'),
)


def _database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript(SCHEMA.read_text())
    for name, spec_type, config in SPECS:
        connection.execute(
            "INSERT INTO metric_collection_spec (name, enabled, spec_type, interval_seconds,"
            " next_run_time, config, last_run_at, last_status, last_error, created_at,"
            " updated_at) VALUES (?, 1, ?, 60, 0, ?, NULL, 'ok', '', ?, ?)",
            (name, spec_type, config, CREATED, CREATED),
        )
    connection.execute(
        "INSERT INTO condition_definition (name, enabled, metric_name, host_filter,"
        " service_filter, tags_filter, operator, warning_threshold, critical_threshold,"
        " window_minutes, breach_minutes, status, message, created_at, updated_at,"
        " subject_id_filter, subject_type_filter, alert_when_stale) VALUES ('disk', 1,"
        " 'host.filesystem_used_ratio', 'macmini', '', '{}', 'gte', 0.8, 0.9, 30, 5,"
        " 'ok', '', ?, ?, '', '', 1)",
        (CREATED, CREATED),
    )
    connection.execute(
        "INSERT INTO host_registry (host_id, display_name, grafana_url, enabled,"
        " last_seen_at) VALUES ('macmini', 'macmini', '', 1, ?)",
        (CREATED,),
    )
    connection.execute(
        "INSERT INTO pipeline_heartbeat (name, status, last_success, last_error, details,"
        " updated_at) VALUES ('metric_ingest', 'ok', NULL, '', '{}', ?)",
        (CREATED,),
    )
    connection.commit()
    return connection


def _shadow_dir(tmp_path: Path) -> Path:
    path = tmp_path / "shadow"
    path.mkdir()
    path.chmod(0o700)
    return path


# --- shadow_prepare ----------------------------------------------------------


def test_json_issues_lists_what_python_and_serde_json_disagree_on():
    deep = "[" * 128 + "]" * 128
    assert prepare.json_issues('{"a": [1, {"b": "x"}]}') == []
    assert prepare.json_issues("[" * 127 + "]" * 127) == []
    assert prepare.json_issues(deep) == [
        "nesting of 128 levels at or beyond serde_json's limit of 128 (D-11)"
    ]
    assert prepare.json_issues('{"k": "\\ud800"}') == [
        "lone surrogate escape \\ud800 (D-11)"
    ]
    assert prepare.json_issues('{"k": "\\udc00x"}') == [
        "lone surrogate escape \\udc00 (D-11)"
    ]
    # A pair is one character in both.
    assert prepare.json_issues('{"k": "\\ud83d\\ude00"}') == []
    # Escapes inside strings are not structure.
    assert prepare.json_issues('{"k": "[[[\\" 9999"}') == []
    assert prepare.json_issues("[" + "9" * 4300 + "]") == []
    assert prepare.json_issues('{"n": -' + "9" * 4301 + "}") == [
        "integer of 4301 digits beyond CPython's 4300-digit limit (D-11)"
    ]
    # A long float is no integer: no limit in either.
    assert prepare.json_issues("[1." + "9" * 5000 + "]") == []
    assert prepare.json_issues('{"v": NaN}') == ["non-standard JSON token NaN (D-4)"]
    issues = prepare.json_issues("{")
    assert len(issues) == 1 and issues[0].startswith(
        "Python cannot load it: JSONDecodeError: "
    )


def test_expected_storage_classes_follow_the_declared_types():
    assert prepare.expected_classes("integer") == {"integer", "null"}
    assert prepare.expected_classes("integer unsigned") == {"integer", "null"}
    assert prepare.expected_classes("bigint") == {"integer", "null"}
    assert prepare.expected_classes("real") == {"real", "integer", "null"}
    assert prepare.expected_classes("varchar(255)") == {"text", "null"}
    assert prepare.expected_classes("text") == {"text", "null"}
    assert prepare.expected_classes("bool") is None
    assert prepare.expected_classes("datetime") is None


def test_prepare_copies_disables_and_lists_rows_to_look_at(tmp_path, capsys):
    source = tmp_path / "prod.sqlite3"
    connection = _database(source)
    connection.execute(
        "UPDATE metric_collection_spec SET config = ? WHERE name = 'json temp'",
        ('{"url": "http://x", "note": "\\ud800"}',),
    )
    # D-13: raw SQL past Django's conversion.
    connection.execute(
        "UPDATE metric_collection_spec SET interval_seconds = '60s' WHERE name = 'unifi'"
    )
    connection.execute(
        "UPDATE condition_definition SET warning_threshold = 'high' WHERE name = 'disk'"
    )
    # A disabled spec is not loaded, so not scanned.
    connection.execute(
        "INSERT INTO metric_collection_spec (name, enabled, spec_type, interval_seconds,"
        " next_run_time, config, last_status, last_error, created_at, updated_at)"
        " VALUES ('off', 0, 'http_json_metric', 60, 0, ?, 'ok', '', ?, ?)",
        ("[" * 200 + "]" * 200, CREATED, CREATED),
    )
    connection.commit()
    connection.close()
    shadow = _shadow_dir(tmp_path)

    code = prepare.main(
        [
            "--source",
            str(source),
            "--shadow-dir",
            str(shadow),
            "--disable-spec",
            "unifi",
        ]
    )

    assert code == 2
    out = capsys.readouterr().out
    copy = shadow / "db.sqlite3"
    assert stat.S_IMODE(copy.stat().st_mode) == 0o600
    meta = json.loads((shadow / "shadow.json").read_text())
    assert stat.S_IMODE((shadow / "shadow.json").stat().st_mode) == 0o600
    assert meta["copy"] == str(copy)
    assert meta["t0"].endswith("Z") and f"T0={meta['t0']}" in out
    assert meta["disabled_specs"] == ["unifi"]
    assert meta["enabled_specs"] == {
        "home_assistant_env_scan": 1,
        "http_json_metric": 1,
        "http_page_probe": 1,
    }
    assert meta["enabled_conditions"] == 1
    issues = {
        (f["table"], f.get("name"), f["column"], f["issue"]) for f in meta["findings"]
    }
    assert issues == {
        (
            "metric_collection_spec",
            "json temp",
            "config",
            "lone surrogate escape \\ud800 (D-11)",
        ),
        (
            "condition_definition",
            "disk",
            "warning_threshold",
            "stored as text, the agent reads integer/real (D-13)",
        ),
    }
    assert "2 row(s) to look at" in out
    # The copy is changed, the source is not.
    enabled = sqlite3.connect(copy).execute(
        "SELECT enabled FROM metric_collection_spec WHERE name = 'unifi'"
    )
    assert enabled.fetchone() == (0,)
    assert sqlite3.connect(source).execute(
        "SELECT enabled FROM metric_collection_spec WHERE name = 'unifi'"
    ).fetchone() == (1,)

    # A second run refuses to overwrite.
    assert prepare.main(["--source", str(source), "--shadow-dir", str(shadow)]) == 1
    assert "refusing to overwrite" in capsys.readouterr().err


def test_prepare_succeeds_on_a_clean_database(tmp_path, capsys):
    source = tmp_path / "prod.sqlite3"
    _database(source).close()
    shadow = _shadow_dir(tmp_path)
    assert prepare.main(["--source", str(source), "--shadow-dir", str(shadow)]) == 0
    assert "look at" not in capsys.readouterr().out
    assert json.loads((shadow / "shadow.json").read_text())["findings"] == []


def test_prepare_checks_the_directory_and_the_spec_names(tmp_path, capsys):
    source = tmp_path / "prod.sqlite3"
    _database(source).close()
    missing = tmp_path / "missing"
    assert prepare.main(["--source", str(source), "--shadow-dir", str(missing)]) == 1
    assert "does not exist" in capsys.readouterr().err
    loose = tmp_path / "loose"
    loose.mkdir()
    loose.chmod(0o755)
    assert prepare.main(["--source", str(source), "--shadow-dir", str(loose)]) == 1
    assert "must have mode 0700, has 0755" in capsys.readouterr().err
    assert not (loose / "db.sqlite3").exists()
    shadow = _shadow_dir(tmp_path)
    code = prepare.main(
        ["--source", str(source), "--shadow-dir", str(shadow), "--disable-spec", "nope"]
    )
    assert code == 1
    assert "no spec named 'nope'" in capsys.readouterr().err
    assert not (shadow / "shadow.json").exists()
    assert (
        prepare.main(["--source", str(tmp_path / "x"), "--shadow-dir", str(shadow)])
        == 1
    )


# --- shadow_record -------------------------------------------------------------


def _fake_systemctl(tmp_path: Path, pids: dict[str, str]) -> str:
    script = tmp_path / "systemctl"
    lines = ["#!/bin/sh", 'case "$4" in']
    for unit, properties in pids.items():
        lines.append(f"  {unit}) cat <<'EOF'\n{properties}\nEOF\n  ;;")
    lines.append('  *) echo "Unit $4 not found." >&2; exit 1 ;;')
    lines.append("esac")
    script.write_text("\n".join(lines) + "\n")
    script.chmod(0o755)
    return str(script)


def _unit(pid: int, current: int, peak: int, cpu: int, state: str = "active") -> str:
    return (
        f"ActiveState={state}\nMainPID={pid}\nNRestarts=0\nMemoryCurrent={current}\n"
        f"MemoryPeak={peak}\nCPUUsageNSec={cpu}"
    )


def _proc(tmp_path: Path, pid: int, rss_kib: int, anon_kib: int) -> Path:
    root = tmp_path / "proc"
    (root / str(pid)).mkdir(parents=True, exist_ok=True)
    (root / str(pid) / "status").write_text(
        f"Name:\tx\nVmHWM:\t{rss_kib} kB\nVmRSS:\t{rss_kib} kB\n"
        f"RssAnon:\t{anon_kib} kB\nRssFile:\t1000 kB\n"
    )
    return root


def test_recorder_writes_incremental_snapshots_without_secrets(tmp_path):
    prod_path, shadow_path = tmp_path / "prod.sqlite3", tmp_path / "shadow.sqlite3"
    prod_db, shadow_db = _database(prod_path), _database(shadow_path)
    systemctl = _fake_systemctl(
        tmp_path,
        {
            "graphyard-agent.service": _unit(10, 80_000_000, 83_000_000, 10**9),
            "graphyard-agent-shadow.service": _unit(20, 9_000_000, 20_000_000, 10**8),
        },
    )
    proc = _proc(tmp_path, 20, 15_000, 6_000)
    out = tmp_path / "record.jsonl"
    prod_rec = recorder.DatabaseRecorder(prod_path)
    shadow_rec = recorder.DatabaseRecorder(shadow_path)
    units = {
        "prod": "graphyard-agent.service",
        "shadow": "graphyard-agent-shadow.service",
    }
    stop = threading.Event()

    def tick() -> dict:
        recorder.record(
            prod_rec, shadow_rec, units, out, 0.001, stop, 1, systemctl, proc
        )
        return json.loads(out.read_text().splitlines()[-1])

    first = tick()
    assert stat.S_IMODE(out.stat().st_mode) == 0o600
    specs = first["dbs"]["prod"]["metric_collection_spec"]
    assert len(specs) == 4
    assert all("config" not in row for row in specs.values())
    assert "secret" not in out.read_text()
    assert set(first["dbs"]["shadow"]) >= {"condition_definition", "host_registry"}
    assert first["units"]["shadow"]["RssAnon"] == "6000 kB"
    assert first["units"]["shadow"]["MemoryCurrent"] == "9000000"
    assert "VmRSS" not in first["units"]["prod"]  # no /proc entry for pid 10

    shadow_db.execute(
        "UPDATE metric_collection_spec SET last_run_at = '2026-10-21 10:00:00',"
        " last_status = 'warning' WHERE name = 'json temp'"
    )
    shadow_db.execute("DELETE FROM host_registry")
    shadow_db.commit()
    second = tick()
    assert second["dbs"]["prod"]["metric_collection_spec"] == {}
    changed = second["dbs"]["shadow"]["metric_collection_spec"]
    assert [row["last_status"] for row in changed.values()] == ["warning"]
    assert second["dbs"]["shadow"]["host_registry_deleted"] == ["1"]

    # An unreadable database is an error line; the next tick catches up.
    prod_db.close()
    moved = tmp_path / "moved.sqlite3"
    os.rename(prod_path, moved)
    assert "error" in tick()["dbs"]["prod"]
    os.rename(moved, prod_path)
    prod_db = sqlite3.connect(prod_path)
    prod_db.execute("UPDATE condition_definition SET status = 'warning'")
    prod_db.commit()
    caught_up = tick()["dbs"]["prod"]
    assert [row["status"] for row in caught_up["condition_definition"].values()] == [
        "warning"
    ]


def test_recorder_main_refuses_the_same_database_twice(tmp_path, capsys):
    path = tmp_path / "db.sqlite3"
    _database(path).close()
    args = [
        "--prod-db",
        str(path),
        "--shadow-db",
        str(path),
        "--out",
        str(tmp_path / "r"),
    ]
    assert recorder.main(args) == 1
    assert "same file" in capsys.readouterr().err
    other = tmp_path / "copy.sqlite3"
    _database(other).close()
    code = recorder.main(
        [
            "--prod-db",
            str(path),
            "--shadow-db",
            str(other),
            "--out",
            str(tmp_path / "r"),
            "--ticks",
            "1",
            "--no-units",
        ]
    )
    assert code == 0
    line = json.loads((tmp_path / "r").read_text())
    assert line["units"] == {}


# --- shadow_compare: parsers ---------------------------------------------------


def test_line_protocol_parser_reads_influxdb_escapes():
    measurement, tags, fields, ts = compare.parse_line(
        r"graphyard_metrics,metric=a\,b,spec_name=page\ site,x\=y=1\=2 value=0.25 1700000000000000000"
    )
    assert measurement == "graphyard_metrics"
    assert tags == {"metric": "a,b", "spec_name": "page site", "x=y": "1=2"}
    assert fields == {"value": 0.25}
    assert ts == 1700000000000000000
    _, _, fields, _ = compare.parse_line('m,t=1 value=2,n=3i,s="a b,c",ok=t 5')
    assert fields == {"value": 2.0, "n": 3, "s": "a b,c", "ok": True}
    # A backslash before another character is literal, as in InfluxDB.
    _, tags, _, _ = compare.parse_line(r"m,path=C:\temp value=1 1")
    assert tags == {"path": "C:\\temp"}
    with pytest.raises(ValueError):
        compare.parse_line("m,t value=1 1")


def test_annotated_csv_with_several_tables_and_an_error_table():
    text = (
        "#group,false,false,false,false,true,true\r\n"
        "#datatype,string,long,dateTime:RFC3339,double,string,string\r\n"
        "#default,_result,,,,,\r\n"
        ",result,table,_time,_value,_field,metric\r\n"
        ",,0,2026-10-21T10:00:00Z,1.5,value,a\r\n"
        ",,0,2026-10-21T10:01:00.5Z,2,value,a\r\n"
        "\r\n"
        "#group,false,false,false,false,true,true,true\r\n"
        "#datatype,string,long,dateTime:RFC3339,double,string,string,string\r\n"
        "#default,_result,,,,,,\r\n"
        ",result,table,_time,_value,_field,metric,name\r\n"
        ',,1,2026-10-21T10:00:00Z,3,value,b,"x,y"\r\n'
    )
    records = compare.parse_annotated_csv(text)
    assert [r["result"] for r in records] == ["_result"] * 3
    assert records[2]["name"] == "x,y"
    points = compare.influx_points(records)
    assert points[1].ts == compare.rfc3339_ns("2026-10-21T10:01:00.500000000Z")
    assert points[2].series == ("", (("metric", "b"), ("name", "x,y")))
    with pytest.raises(RuntimeError, match="InfluxDB query failed"):
        compare.parse_annotated_csv(
            "#datatype,string,string\n#group,true,true\n#default,,\n"
            ",error,reference\n,bucket not found,\n"
        )
    assert compare.parse_annotated_csv("\n") == []


def test_rfc3339_and_epoch_times():
    assert compare.rfc3339_ns("1970-01-01T00:00:01Z") == 10**9
    assert compare.rfc3339_ns("1970-01-01T01:00:01.000000001+01:00") == 10**9 + 1
    assert compare.rfc3339_ns("1700000000.25") == 1700000000250000000
    assert compare.ns_text(1700000000250000000) == "2023-11-14T22:13:20.250000000Z"
    assert compare.ns_text(10**9) == "1970-01-01T00:00:01Z"
    with pytest.raises(ValueError):
        compare.rfc3339_ns("yesterday")


def test_flux_query_names_the_window_and_the_agent_points():
    flux = compare.flux_query('graph"yard', "graphyard_metrics", 10**9, 2 * 10**9)
    assert flux == (
        'from(bucket: "graph\\"yard")\n'
        "  |> range(start: 1970-01-01T00:00:01Z, stop: 1970-01-01T00:00:02Z)\n"
        '  |> filter(fn: (r) => r._measurement == "graphyard_metrics" and'
        ' r._field == "value" and exists r.spec_name)\n'
        '  |> drop(columns: ["_start", "_stop"])'
    )


# --- shadow_compare: a whole run ---------------------------------------------

T0 = 1_792_000_000  # 2026-10-14T...Z
BASE_TAGS = {
    "collector_host": "macmini",
    "collector_service": "graphyard-agent",
    "source_instance": "default",
    "subject_type": "service",
}


def _line(spec: str, metric: str, value: float, ts: int, **extra: str) -> str:
    tags = {**BASE_TAGS, "metric": metric, "spec_name": spec, **extra}
    rendered = ",".join(
        f"{key}={value.replace(' ', chr(92) + ' ')}"
        for key, value in sorted(tags.items())
    )
    number = repr(value).removesuffix(".0")
    return f"graphyard_metrics,{rendered} value={number} {ts * 10**9}"


def _csv(points: list[tuple[str, str, float, int, dict]]) -> str:
    keys = sorted(
        {*BASE_TAGS, "metric", "spec_name", *(k for p in points for k in p[4])}
    )
    rows = [
        "#group,false,false,false,false,true,true" + ",true" * len(keys),
        "#datatype,string,long,dateTime:RFC3339,double,string,string"
        + ",string" * len(keys),
        "#default,_result,,,,,," + "," * (len(keys) - 1),
        ",result,table,_time,_value,_field,_measurement," + ",".join(keys),
    ]
    for spec, metric, value, ts, extra in points:
        tags = {**BASE_TAGS, "metric": metric, "spec_name": spec, **extra}
        stamp = compare.ns_text(ts * 10**9)
        rows.append(
            f",,0,{stamp},{value},value,graphyard_metrics,"
            + ",".join(tags.get(key, "") for key in keys)
        )
    return "\r\n".join(rows) + "\r\n\r\n"


def _record_line(ts: float, dbs: dict, units: dict) -> str:
    return json.dumps({"ts": ts, "dbs": dbs, "units": units})


def _spec_row(
    row_id: int,
    name: str,
    spec_type: str,
    run: str | None,
    status: str,
    error: str = "",
) -> dict:
    return {
        "id": row_id,
        "name": name,
        "enabled": 1,
        "spec_type": spec_type,
        "last_run_at": run,
        "last_status": status,
        "last_error": error,
    }


def _heartbeat(name: str, updated: str, status: str, details: str) -> dict:
    return {
        "id": 1 if name == "metric_collectors" else 2,
        "name": name,
        "status": status,
        "last_success": updated,
        "last_error": "",
        "details": details,
        "updated_at": updated,
    }


def _units(pid: int, current: int, cpu: int) -> dict:
    return {
        "ActiveState": "active",
        "MainPID": str(pid),
        "NRestarts": "0",
        "MemoryCurrent": str(current),
        "MemoryPeak": str(current + 5_000_000),
        "CPUUsageNSec": str(cpu),
        "VmRSS": "15000 kB",
        "RssAnon": "6000 kB",
    }


def _write_run(tmp_path: Path, shadow_page_status: list[int]) -> Path:
    shadow = tmp_path / "shadow"
    shadow.mkdir()
    (shadow / "shadow.json").write_text(
        json.dumps({"t0": compare.ns_text(T0 * 10**9), "disabled_specs": ["unifi"]})
    )
    # Points: HA by (series, ts); JSON aligned; page probe sequences.
    prod: list[tuple[str, str, float, int, dict]] = []
    lp = [f"# write {compare.ns_text((T0 + 10) * 10**9)} points=3"]
    for minute in range(10):
        ts = T0 + 60 * minute
        prod.append(
            ("ha env", "ha.sensor.t", 20.0 + minute, ts, {"entity_id": "sensor.t"})
        )
        lp.append(
            _line("ha env", "ha.sensor.t", 20.0 + minute, ts, entity_id="sensor.t")
        )
        prod.append(("json temp", "host.temp", 50.0, ts + 5, {}))
        lp.append(
            _line("json temp", "host.temp", 50.0 if minute != 4 else 60.0, ts + 20)
        )
        prod.append(("page site", "service.http_page_status_code", 200.0, ts + 1, {}))
        prod.append(("page site", "service.http_page_total_seconds", 0.2, ts + 1, {}))
        lp.append(
            _line(
                "page site",
                "service.http_page_status_code",
                shadow_page_status[minute],
                ts + 30,
            )
        )
        lp.append(_line("page site", "service.http_page_total_seconds", 0.1, ts + 30))
    # HA: one value differs at the same (series, ts); a series only in prod.
    prod[-4] = ("ha env", "ha.sensor.t", 99.0, T0 + 540, {"entity_id": "sensor.t"})
    prod.append(
        (
            "ha env",
            "ha.sensor.only_prod",
            1.0,
            T0 + 60,
            {"entity_id": "sensor.only_prod"},
        )
    )
    # Outside the window and of an excluded spec: ignored.
    prod.append(("ha env", "ha.sensor.t", 1.0, T0 - 60, {"entity_id": "sensor.t"}))
    prod.append(("unifi", "network_device.rx", 1.0, T0 + 60, {}))
    lp.append(f"# write {compare.ns_text((T0 + 600) * 10**9)} points=1")
    (shadow / "points.lp").write_text("\n".join(lp) + "\n")
    (tmp_path / "prod.csv").write_text(_csv(prod))

    specs = [
        ("ha env", "home_assistant_env_scan"),
        ("json temp", "http_json_metric"),
        ("page site", "http_page_probe"),
        ("unifi", "unifi_device_traffic"),
    ]
    lines = []
    for tick in range(12):
        ts = T0 + 60 * tick
        stamp = f"2026-10-14 00:{tick:02d}:00"
        dbs = {}
        for role in ("prod", "shadow"):
            status = "warning" if (role == "shadow" and tick == 5) else "ok"
            error = ""
            if role == "prod" and tick == 5:
                error = "Expecting value: line 1 column 1 (char 0)"
            elif role == "shadow" and tick == 5:
                error = "expected value at line 1 column 1"
            rows = {
                str(i + 1): _spec_row(
                    i + 1, name, spec_type, stamp if tick else None, "ok"
                )
                for i, (name, spec_type) in enumerate(specs)
            }
            rows["2"] = _spec_row(
                2,
                "json temp",
                "http_json_metric",
                stamp if tick else None,
                status if role == "shadow" else "ok",
                error,
            )
            hb_status = "warning" if role == "shadow" and tick == 5 else "ok"
            dbs[role] = {
                "metric_collection_spec": rows,
                "condition_definition": {
                    "1": {
                        "id": 1,
                        "name": "disk",
                        "enabled": 1,
                        "status": "ok",
                        "message": "ok at 0.5",
                        "last_value": 0.5 if role == "prod" else 0.51,
                        "last_evaluated": stamp,
                    }
                },
                "pipeline_heartbeat": {
                    "1": _heartbeat(
                        "metric_collectors",
                        stamp,
                        hb_status,
                        '{"total": 4, "failed": 0, "warning": 0, "ingested": 3, "skipped": 0}',
                    ),
                    "2": _heartbeat(
                        "condition_evaluator",
                        stamp,
                        "ok",
                        '{"evaluated": 1, "failed": 0}',
                    ),
                },
                "subject_registry": {
                    "1": {
                        "id": 1,
                        "subject_type": "service",
                        "subject_id": "site",
                        "display_name": "site",
                        "source_system": "page_probe",
                        "last_seen_at": stamp,
                    }
                },
                "host_registry": {
                    "1": {
                        "id": 1,
                        "host_id": "macmini",
                        "display_name": "macmini",
                        "grafana_url": "",
                        "enabled": 1,
                        "last_seen_at": stamp,
                    }
                },
                "service_registry": {
                    "1": {
                        "id": 1,
                        "service_id": "site",
                        "display_name": "site",
                        "grafana_url": "",
                        "enabled": 1,
                        "last_seen_at": stamp,
                        "host_id": 1 if role == "prod" or tick < 11 else None,
                    }
                },
            }
        units = {
            "prod": _units(10, 80_000_000, 10**9 + tick * 2 * 10**8),
            "shadow": _units(20, 9_000_000 if tick < 11 else 30_000_000, tick * 10**7),
        }
        lines.append(_record_line(ts, dbs, units))
    (shadow / "record.jsonl").write_text("\n".join(lines) + "\n")
    return shadow


def test_compare_reports_every_kind_of_difference(tmp_path, capsys):
    status = [200] * 10
    status[6] = 503
    shadow = _write_run(tmp_path, status)
    report_path = tmp_path / "report.json"
    code = compare.main(
        [
            "--shadow-dir",
            str(shadow),
            "--prod-csv",
            str(tmp_path / "prod.csv"),
            "--json",
            str(report_path),
            "--fail-on-findings",
        ]
    )
    out = capsys.readouterr().out
    assert code == 1, out
    report = json.loads(report_path.read_text())
    assert report["window"]["start"] == compare.ns_text(T0 * 10**9)
    assert report["window"]["stop"] == compare.ns_text((T0 + 600) * 10**9)
    findings = {(f["section"], f["subject"]) for f in report["findings"]}
    assert findings == {
        ("series", "ha env"),  # ha.sensor.only_prod
        ("values", "ha env"),  # 99.0 vs 29.0 at the same timestamp
        ("page probe", "page site"),  # 200 vs 200, 503, 200
        ("specs", "json temp"),  # ok vs ok, warning, ok
        ("heartbeats", "metric_collectors"),
        ("registry", "service site"),  # host lost in the shadow only
        ("lean goals", "MemoryCurrent (steady, median)"),  # 19.5 MB > 16 MiB
    }
    by_subject = {(f["section"], f["subject"]): f["detail"] for f in report["findings"]}
    assert by_subject[("series", "ha env")] == {
        "only in prod": [
            "graphyard_metrics{collector_host=macmini,collector_service=graphyard-agent,"
            "entity_id=sensor.only_prod,metric=ha.sensor.only_prod,source_instance=default,"
            "spec_name=ha env,subject_type=service}"
        ],
        "count": 1,
    }
    assert by_subject[("page probe", "page site")]["sequence"] == {
        "prod": [200.0],
        "shadow": [200.0, 503.0, 200.0],
    }
    assert by_subject[("specs", "json temp")] == {
        "last_status": {"prod": ["ok"], "shadow": ["ok", "warning", "ok"]}
    }
    assert by_subject[("registry", "service site")]["shadow"]["host"] is None
    assert by_subject[("registry", "service site")]["prod"]["host"] == "macmini"
    notes = {(n["section"], n["subject"]) for n in report["notes"]}
    # The JSON value at minute 4 is beyond the tolerance, the median is not.
    assert ("values", "json temp") in notes
    assert ("specs", "unifi") in notes  # excluded
    assert ("conditions", "disk") in notes  # last_value 0.5 vs 0.51
    assert ("page probe timings", "page site") in notes
    assert ("points", "prod") in notes  # the excluded spec's point
    # P2 positions are masked; the texts still differ and stay a note.
    spec_note = next(
        n
        for n in report["notes"]
        if n["section"] == "specs" and n["subject"] == "json temp"
    )
    assert spec_note["detail"]["last_error"]["prod"] == [
        "",
        "Expecting value: <p2>",
        "",
    ]
    points = report["sections"]["points"]
    assert points["ha env"]["matched"] == 10 and points["ha env"]["mismatched"] == 1
    assert points["json temp"]["pairs"] == 10
    assert report["sections"]["units"]["shadow"]["cpu_minutes_per_day"] == round(
        11 * 10**7 / 1e9 / 60 / 660 * 86400, 3
    )
    assert "FINDINGS (7)" in out


def test_compare_is_clean_on_identical_runs_and_exit_codes(tmp_path, capsys):
    shadow = _write_run(tmp_path, [200] * 10)
    code = compare.main(
        ["--shadow-dir", str(shadow), "--prod-csv", str(tmp_path / "prod.csv")]
    )
    assert code == 0  # findings, but no --fail-on-findings
    capsys.readouterr()
    (shadow / "points.lp").write_text("")
    assert (
        compare.main(
            ["--shadow-dir", str(shadow), "--prod-csv", str(tmp_path / "prod.csv")]
        )
        == 1
    )
    assert "holds no batch" in capsys.readouterr().err


def test_compare_queries_influx_through_the_command(tmp_path, capsys):
    shadow = _write_run(tmp_path, [200] * 10)
    calls = tmp_path / "calls.json"
    fake = tmp_path / "influx"
    fake.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        f"open({str(calls)!r}, 'w').write(json.dumps(sys.argv[1:]))\n"
        f"sys.stdout.write(open({str(tmp_path / 'prod.csv')!r}).read())\n"
    )
    fake.chmod(0o755)
    saved = tmp_path / "saved.csv"
    code = compare.main(
        [
            "--shadow-dir",
            str(shadow),
            "--influx-command",
            f"{fake} query --raw",
            "--org",
            "graphyard",
            "--save-prod-csv",
            str(saved),
            "--json",
            str(tmp_path / "r.json"),
        ]
    )
    assert code == 0
    argv = json.loads(calls.read_text())
    assert argv[:4] == ["query", "--raw", "--org", "graphyard"]
    assert argv[4] == compare.flux_query(
        "graphyard", "graphyard_metrics", T0 * 10**9, (T0 + 600) * 10**9
    )
    assert saved.read_text() == (tmp_path / "prod.csv").read_text()
    report = json.loads((tmp_path / "r.json").read_text())
    assert report["window"]["prod_points"] == 42
    fake.write_text("#!/bin/sh\necho 'Error: unauthorized' >&2\nexit 1\n")
    code = compare.main(
        ["--shadow-dir", str(shadow), "--influx-command", f"{fake} query --raw"]
    )
    assert code == 1
    assert "production CSV command failed" in capsys.readouterr().err


def _simulate_run(connection: sqlite3.Connection, stamp: str, status: str) -> None:
    """What one agent run writes: spec rows, a condition, both heartbeats and
    a registry row."""
    connection.execute(
        "UPDATE metric_collection_spec SET last_run_at = ?, last_status = ?, last_error = ''",
        (stamp, status),
    )
    connection.execute(
        "UPDATE condition_definition SET status = 'ok', message = 'fine', last_value = 0.5,"
        " last_evaluated = ?",
        (stamp,),
    )
    for name, details in (
        (
            "metric_collectors",
            '{"total": 4, "failed": 0, "warning": 0, "ingested": 4, "skipped": 0}',
        ),
        ("condition_evaluator", '{"evaluated": 1, "failed": 0}'),
    ):
        connection.execute(
            "INSERT INTO pipeline_heartbeat (name, status, last_success, last_error, details,"
            " updated_at) VALUES (?, 'ok', ?, '', ?, ?) ON CONFLICT (name) DO UPDATE SET"
            " last_success = excluded.last_success, details = excluded.details,"
            " updated_at = excluded.updated_at",
            (name, stamp, details, stamp),
        )
    connection.execute(
        "INSERT INTO service_registry (service_id, display_name, grafana_url, enabled,"
        " last_seen_at, host_id) VALUES ('site', 'site', '', 1, ?, 1)"
        " ON CONFLICT (service_id) DO UPDATE SET last_seen_at = excluded.last_seen_at",
        (stamp,),
    )
    connection.commit()


def test_prepare_record_compare_chain_on_two_databases(tmp_path, capsys):
    prod_path = tmp_path / "prod.sqlite3"
    prod_db = _database(prod_path)
    shadow_dir = _shadow_dir(tmp_path)
    assert (
        prepare.main(["--source", str(prod_path), "--shadow-dir", str(shadow_dir)]) == 0
    )
    meta = json.loads((shadow_dir / "shadow.json").read_text())
    t0 = compare.rfc3339_ns(meta["t0"]) // 10**9
    shadow_db = sqlite3.connect(shadow_dir / "db.sqlite3")
    systemctl = _fake_systemctl(
        tmp_path,
        {
            "graphyard-agent.service": _unit(10, 80_000_000, 83_000_000, 10**9),
            "graphyard-agent-shadow.service": _unit(20, 9_000_000, 20_000_000, 10**8),
        },
    )
    proc = _proc(tmp_path, 20, 15_000, 6_000)
    record_path = shadow_dir / "record.jsonl"
    prod_rec = recorder.DatabaseRecorder(prod_path)
    shadow_rec = recorder.DatabaseRecorder(shadow_dir / "db.sqlite3")
    units = {
        "prod": "graphyard-agent.service",
        "shadow": "graphyard-agent-shadow.service",
    }
    for minute, status in enumerate(["ok", "ok", "warning", "ok"]):
        stamp = f"2026-10-21 10:0{minute}:00"
        for connection in (prod_db, shadow_db):
            _simulate_run(connection, stamp, status)
        recorder.record(
            prod_rec,
            shadow_rec,
            units,
            record_path,
            0.001,
            threading.Event(),
            1,
            systemctl,
            proc,
        )
    points = [
        (name, "host.temp", 50.0, t0 + 30 + 60 * minute, {})
        for minute in range(4)
        for name, _, _ in SPECS
    ]
    (tmp_path / "prod.csv").write_text(_csv(points))
    (shadow_dir / "points.lp").write_text(
        f"# write {compare.ns_text((t0 + 300) * 10**9)} points=4\n"
        + "\n".join(_line(s, m, v, ts) for s, m, v, ts, _ in points)
        + "\n"
    )
    report_path = tmp_path / "report.json"
    code = compare.main(
        [
            "--shadow-dir",
            str(shadow_dir),
            "--prod-csv",
            str(tmp_path / "prod.csv"),
            "--json",
            str(report_path),
            "--fail-on-findings",
        ]
    )
    out = capsys.readouterr().out
    report = json.loads(report_path.read_text())
    # The recorder samples within a second: no steady memory, no CPU rate.
    assert {(f["section"], f["subject"]) for f in report["findings"]} == set(), out
    assert code == 0
    assert report["sections"]["specs"] == {
        "compared": 4,
        "runs": {"prod": 12, "shadow": 12},
    }
    assert report["sections"]["points"]["json temp"]["pairs"] == 4
    assert report["sections"]["registry"]["rows"] == {"prod": 2, "shadow": 2}
    # A difference in one database only is found.
    shadow_db.execute(
        "UPDATE condition_definition SET status = 'critical', last_evaluated = 'x'"
    )
    shadow_db.commit()
    recorder.record(
        prod_rec,
        shadow_rec,
        units,
        record_path,
        0.001,
        threading.Event(),
        1,
        systemctl,
        proc,
    )
    assert (
        compare.main(
            [
                "--shadow-dir",
                str(shadow_dir),
                "--prod-csv",
                str(tmp_path / "prod.csv"),
                "--fail-on-findings",
            ]
        )
        == 1
    )
    assert "[conditions] disk" in capsys.readouterr().out


def test_quotes_are_literal_in_the_series_part_of_a_line():
    measurement, tags, fields, ts = compare.parse_line(
        'm"x,spec_name=sensor"one,t="a\\ b value=1.5,s="x y" 7'
    )
    assert measurement == 'm"x'
    assert tags == {"spec_name": 'sensor"one', "t": '"a b'}
    assert fields == {"value": 1.5, "s": "x y"}
    assert ts == 7


def test_prepare_scans_heartbeat_details(tmp_path, capsys):
    source = tmp_path / "prod.sqlite3"
    connection = _database(source)
    connection.execute(
        "UPDATE pipeline_heartbeat SET details = ? WHERE name = 'metric_ingest'",
        ('{"n": ' + "9" * 4301 + "}",),
    )
    connection.commit()
    connection.close()
    shadow = _shadow_dir(tmp_path)
    assert prepare.main(["--source", str(source), "--shadow-dir", str(shadow)]) == 2
    findings = json.loads((shadow / "shadow.json").read_text())["findings"]
    assert [(f["table"], f["name"], f["column"]) for f in findings] == [
        ("pipeline_heartbeat", "metric_ingest", "details")
    ]


def test_values_that_cannot_be_paired_are_an_inconclusive_finding(tmp_path, capsys):
    shadow = _write_run(tmp_path, [200] * 10)
    # Shift every shadow point by 20 minutes: same series, no pair in reach.
    lines = []
    for line in (shadow / "points.lp").read_text().splitlines():
        if line.startswith("#"):
            lines.append(line)
            continue
        head, ts = line.rsplit(" ", 1)
        lines.append(f"{head} {int(ts) - 1200 * 10**9}")
    (shadow / "points.lp").write_text("\n".join(lines) + "\n")
    report_path = tmp_path / "r.json"
    compare.main(
        [
            "--shadow-dir",
            str(shadow),
            "--prod-csv",
            str(tmp_path / "prod.csv"),
            "--start",
            compare.ns_text((T0 - 1200) * 10**9),
            "--json",
            str(report_path),
        ]
    )
    report = json.loads(report_path.read_text())
    inconclusive = {
        f["subject"]
        for f in report["findings"]
        if f["detail"] == "no value pair to compare (inconclusive)"
    }
    assert inconclusive == {"ha env", "json temp"}


@pytest.mark.parametrize("role", ["prod", "shadow"])
@pytest.mark.parametrize("alias", ["same", "hard", "symlink"])
def test_recorder_output_alias_is_refused(tmp_path, role, alias):
    paths = {r: tmp_path / f"{r}.sqlite3" for r in ("prod", "shadow")}
    for path in paths.values():
        _database(path).close()
    before = {r: p.read_bytes() for r, p in paths.items()}
    out = paths[role] if alias == "same" else tmp_path / "record"
    if alias == "hard":
        os.link(paths[role], out)
    elif alias == "symlink":
        out.symlink_to(paths[role])
    with pytest.raises((OSError, ValueError)):
        recorder.record(
            *(recorder.DatabaseRecorder(paths[r]) for r in ("prod", "shadow")),
            {},
            out,
            1,
            threading.Event(),
            1,
        )
    assert {r: p.read_bytes() for r, p in paths.items()} == before


def test_recorder_restart_replaces_baseline(tmp_path):
    paths = [tmp_path / f"{r}.sqlite3" for r in ("prod", "shadow")]
    for path in paths:
        _database(path).close()
    out = tmp_path / "record"

    def session():
        recorder.record(
            *(recorder.DatabaseRecorder(p) for p in paths),
            {},
            out,
            1,
            threading.Event(),
            1,
        )

    session()
    with sqlite3.connect(paths[1]) as db:
        db.execute("DELETE FROM host_registry")
    session()
    history = compare.read_record(out).dbs["shadow"]
    assert history.rows("host_registry") == []
    assert history.ticks == 2
    assert len(history.rows("metric_collection_spec")) == 4


@pytest.mark.parametrize(
    "option,value",
    [
        ("--interval", "nan"),
        ("--interval", "inf"),
        ("--interval", "0"),
        ("--interval", "-1"),
        ("--interval", "86401"),
        ("--ticks", "0"),
        ("--ticks", "-1"),
    ],
)
def test_recorder_rejects_loop_options_before_io(tmp_path, option, value):
    with pytest.raises(SystemExit) as exc:
        recorder.main(
            [
                "--prod-db",
                str(tmp_path / "absent"),
                "--shadow-db",
                "absent2",
                "--out",
                str(tmp_path / "out"),
                option,
                value,
            ]
        )
    assert exc.value.code == 2
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("mode", ["hang", "flood", "descendant"])
def test_unit_probe_has_owned_deadline_and_capture(tmp_path, mode):
    import subprocess

    helper = tmp_path / "helper"
    helper.write_text(
        f"#!{sys.executable}\n"
        + """import os, sys, time
mode = """
        + repr(mode)
        + """
if mode == 'descendant':
    if os.fork():
        sys.exit(0)
if mode == 'flood':
    while True:
        os.write(1, b'private-helper-output' * 1000)
time.sleep(30)
"""
    )
    helper.chmod(0o755)
    script = f"""import sys
sys.path.insert(0, {str(PARITY)!r})
import shadow_record as r
from pathlib import Path
r.UNIT_TIMEOUT = 0.15
sample = r.unit_sample('fake', {str(helper)!r}, Path('/nonexistent'))
assert sample == {{'error': 'unit probe unavailable'}}
"""
    import signal

    process = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        _, stderr = process.communicate(timeout=3)
        assert process.returncode == 0, stderr.decode()
    finally:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def test_prepare_mixed_disable_rolls_back(tmp_path):
    path = tmp_path / "db"
    _database(path).close()
    with pytest.raises(RuntimeError, match="no spec named"):
        prepare.prepare(path, ["unifi", "absent"])
    with sqlite3.connect(path) as db:
        assert db.execute(
            "SELECT enabled FROM metric_collection_spec WHERE name='unifi'"
        ).fetchone() == (1,)


def test_backup_progress_has_elapsed_budget(tmp_path, monkeypatch):
    path = tmp_path / "db"
    _database(path).close()
    times = iter([0.0, 100.0])
    monkeypatch.setattr(prepare.time, "monotonic", lambda: next(times))
    dest = tmp_path / "copy"
    with pytest.raises(RuntimeError, match="backup deadline"):
        prepare.backup(path, dest)
    assert not dest.exists()


def test_backup_busy_source_exits_and_removes_owned_copy(tmp_path, monkeypatch):
    import time

    source = tmp_path / "source"
    db = _database(source)
    db.execute("BEGIN EXCLUSIVE")
    monkeypatch.setattr(prepare, "BACKUP_TIMEOUT", 0.1)
    dest = tmp_path / "copy"
    started = time.monotonic()
    try:
        with pytest.raises(RuntimeError, match="backup deadline"):
            prepare.backup(source, dest)
    finally:
        db.close()
    assert time.monotonic() - started < 2
    assert not dest.exists()


@pytest.mark.parametrize("phase", ["backup", "disable", "metadata"])
def test_prepare_process_death_never_completes_half_generation(tmp_path, phase):
    import select
    import subprocess

    source = tmp_path / "db"
    _database(source).close()
    shadow = _shadow_dir(tmp_path)
    module, name = (
        ("p.json", "dump")
        if phase == "metadata"
        else ("p", "backup" if phase == "backup" else "prepare")
    )
    script = f"""import sys, time
sys.path.insert(0, {str(PARITY)!r})
import shadow_prepare as p
original = {module}.{name}
def paused(*a, **kw):
    result = original(*a, **kw)
    print('paused', flush=True)
    time.sleep(30)
    return result
{module}.{name} = paused
p.main(['--source', {str(source)!r}, '--shadow-dir', {str(shadow)!r}, '--disable-spec', 'unifi'])
"""
    child = subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        assert select.select([child.stdout], [], [], 5)[0]
        assert child.stdout.readline() == b"paused\n"
        child.kill()
        child.wait(timeout=3)
    finally:
        if child.poll() is None:
            child.kill()
        child.communicate()
    assert not (shadow / "shadow.json").exists()
    again = subprocess.run(
        [
            sys.executable,
            str(PARITY / "shadow_prepare.py"),
            "--source",
            str(source),
            "--shadow-dir",
            str(shadow),
        ],
        capture_output=True,
        timeout=5,
    )
    assert again.returncode == 1
    assert b"incomplete preparation" in again.stderr
    assert b"fresh 0700 directory" in again.stderr


def test_prepare_publishes_synced_completion_last(tmp_path, monkeypatch):
    source = tmp_path / "db"
    _database(source).close()
    shadow = _shadow_dir(tmp_path)
    events = []
    real_link, real_sync = os.link, os.fsync

    def link(src, dest):
        events.append(Path(dest).name)
        return real_link(src, dest)

    def sync(fd):
        events.append("sync")
        return real_sync(fd)

    monkeypatch.setattr(prepare.os, "link", link)
    monkeypatch.setattr(prepare.os, "fsync", sync)
    assert prepare.main(["--source", str(source), "--shadow-dir", str(shadow)]) == 0
    meta = json.loads((shadow / "shadow.json").read_text())
    assert meta["preparation_version"] == 2 and meta["complete"] is True
    assert events.index("db.sqlite3") < events.index("shadow.json")
    assert events[events.index("db.sqlite3") + 1] == "sync"
    assert events[-1] == "sync"
    assert list(shadow.glob(".prepare-*")) == []


def test_compare_readers_bound_individual_and_combined_inputs(tmp_path, monkeypatch):
    monkeypatch.setattr(compare, "MAX_FILE_BYTES", 40)
    monkeypatch.setattr(compare, "MAX_TOTAL_BYTES", 60)
    path = tmp_path / "points"
    path.write_bytes(b"#" + b"x" * 39)
    budget = compare.EvidenceBudget()
    assert compare.read_points_file(path, budget) == ([], [])
    with pytest.raises(ValueError, match="evidence byte limit"):
        compare.read_points_file(path, budget)
    path.write_bytes(b"x" * 41)
    with pytest.raises(ValueError, match="evidence byte limit"):
        compare.read_points_file(path)
    with path.open("wb") as stream:
        stream.truncate(10**8)
    with pytest.raises(ValueError, match="evidence byte limit"):
        compare.read_record(path)


def test_compare_bounds_rows_and_json_history(tmp_path, monkeypatch):
    monkeypatch.setattr(compare, "MAX_ROWS", 2)
    path = tmp_path / "record"
    path.write_text('{"ts":1}\n' * 2)
    assert compare.read_record(path).last_ts == 1
    path.write_text('{"ts":1}\n' * 3)
    with pytest.raises(ValueError, match="evidence row limit"):
        compare.read_record(path)
    monkeypatch.setattr(compare, "MAX_JSON_NODES", 3)
    path.write_text('{"ts":1,"dbs":{"prod":{}}}\n')
    with pytest.raises(ValueError, match="evidence history limit"):
        compare.read_record(path)


def test_compare_detects_growing_input_during_bounded_reads(tmp_path, monkeypatch):
    path = tmp_path / "growing"
    path.write_text("#one\n")
    monkeypatch.setattr(compare, "MAX_FILE_BYTES", 10)
    budget = compare.EvidenceBudget()
    lines = compare.evidence_lines(path, budget)
    assert next(lines) == "#one\n"
    with path.open("a") as stream:
        stream.write("#two\n#three\n")
    with pytest.raises(ValueError, match="evidence byte limit"):
        list(lines)


def test_compare_bounds_fetched_csv(tmp_path, monkeypatch):
    import shlex

    script = tmp_path / "csv-helper"
    script.write_text("print('x' * 10000)\n")
    monkeypatch.setattr(compare, "MAX_FILE_BYTES", 100)
    with pytest.raises(ValueError, match="evidence byte limit"):
        compare.fetch_prod_csv(
            f"{shlex.quote(sys.executable)} {shlex.quote(str(script))}", None, "unused"
        )


@pytest.mark.parametrize(
    "option,value",
    [
        ("--tolerance", "nan"),
        ("--tolerance", "inf"),
        ("--tolerance", "-1"),
        ("--align-seconds", "nan"),
        ("--align-seconds", "inf"),
        ("--align-seconds", "0"),
        ("--align-seconds", "-1"),
        ("--align-seconds", "86401"),
    ],
)
def test_compare_rejects_invalid_options_before_evidence(
    tmp_path, capsys, option, value
):
    out = tmp_path / "report"
    assert (
        compare.main(
            [
                "--shadow-dir",
                str(tmp_path / "missing"),
                "--json",
                str(out),
                option,
                value,
            ]
        )
        == 1
    )
    assert "must be finite" in capsys.readouterr().err
    assert not out.exists()


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", "1e999"])
def test_compare_refuses_nonfinite_observations(tmp_path, value):
    path = tmp_path / "lp"
    path.write_text(f"m value={value} 1\n")
    with pytest.raises(ValueError):
        compare.read_points_file(path)
    with pytest.raises(ValueError):
        compare.influx_points([{"_time": "1", "_value": value}])
    path.write_text('{"ts":' + value + "}\n")
    with pytest.raises(ValueError):
        compare.read_record(path)


def test_compare_finite_negative_zero_and_extreme_values(tmp_path):
    path = tmp_path / "lp"
    path.write_text("m value=-2 1\nm value=0 2\n")
    assert [p.value for p in compare.read_points_file(path)[0]] == [-2, 0]
    assert compare.relative_difference(-1e308, 1e308) == 2
    assert compare.relative_difference(0, 0) == 0


@pytest.mark.parametrize("timestamp", [str(2**63), str(-(2**63) - 1)])
def test_compare_refuses_unrepresentable_point_times(tmp_path, timestamp):
    path = tmp_path / "lp"
    path.write_text(f"m value=1 {timestamp}\n")
    with pytest.raises(ValueError):
        compare.read_points_file(path)


def test_nearest_alignment_preserves_exhaustive_ties_duplicates_and_reuse(monkeypatch):
    import random

    randomizer = random.Random(97)
    series = ("m", (("spec_name", "s"),))
    prod = [
        compare.Point(series, ts, float(i + 1))
        for i, ts in enumerate(
            [10, 10, 20, 30] + [randomizer.randrange(100) for _ in range(40)]
        )
    ]
    ordered = sorted(prod, key=lambda p: p.ts)
    shadow = [
        compare.Point(series, ts, min(ordered, key=lambda p: abs(p.ts - ts)).value)
        for ts in [-1, 15, 15, 25, 101] + list(range(100))
    ]
    report = compare.Report()
    result = compare._compare_aligned(
        report, "s", {"prod": prod, "shadow": shadow}, 0, 1000
    )
    assert result == {"pairs": len(shadow), "beyond_tolerance": 0, "unpaired_shadow": 0}
    assert report.findings == []


def test_nearest_alignment_work_does_not_multiply_series_lengths(monkeypatch):
    import builtins

    series = ("m", (("spec_name", "s"),))
    size = 2000
    prod = [compare.Point(series, ts * 10, 1) for ts in range(size)]
    shadow = [compare.Point(series, ts * 10 + 4, 1) for ts in range(size)]
    calls = 0

    def counted_abs(value):
        nonlocal calls
        calls += 1
        return builtins.abs(value)

    monkeypatch.setattr(compare, "abs", counted_abs, raising=False)
    result = compare._compare_aligned(
        compare.Report(), "s", {"prod": prod, "shadow": shadow}, 0, 10
    )
    assert result["pairs"] == size
    assert calls <= size * 10


@pytest.mark.parametrize(
    "spec_type", [*compare.HA_TYPES, *compare.ALIGNED_TYPES, compare.PAGE_PROBE]
)
@pytest.mark.parametrize("side", ["neither", "prod", "shadow"])
def test_required_spec_without_observations_is_inconclusive(spec_type, side):
    point = compare.Point(("m", (("spec_name", "s"),)), 10, 1)
    report = compare.Report()
    compare.compare_points(
        report,
        [point] if side == "prod" else [],
        [point] if side == "shadow" else [],
        {"s": spec_type},
        0.05,
        60,
    )
    assert any(
        f["section"] == "values" and "inconclusive" in str(f["detail"])
        for f in report.findings
    )
    excluded_report = compare.Report()
    compare.compare_points(excluded_report, [], [], {}, 0.05, 60)
    assert not excluded_report.findings


def test_no_samples_fail_cli_qualification(tmp_path):
    shadow = _write_run(tmp_path, [200] * 10)
    (shadow / "points.lp").write_text(
        f"# write {compare.ns_text((T0 + 600) * 10**9)} points=0\n"
    )
    (tmp_path / "prod.csv").write_text("")
    report = tmp_path / "report"
    assert (
        compare.main(
            [
                "--shadow-dir",
                str(shadow),
                "--prod-csv",
                str(tmp_path / "prod.csv"),
                "--json",
                str(report),
                "--fail-on-findings",
            ]
        )
        == 1
    )
    assert any(
        "inconclusive" in str(f["detail"])
        for f in json.loads(report.read_text())["findings"]
    )


def test_history_work_is_refused_before_json_materialization(monkeypatch):
    monkeypatch.setattr(compare, "MAX_JSON_NODES", 3)

    def unexpected_decode(*args, **kwargs):
        pytest.fail("over-budget history reached the JSON decoder")

    monkeypatch.setattr(compare.json, "loads", unexpected_decode)
    with pytest.raises(ValueError, match="evidence history limit"):
        compare.EvidenceBudget().json("[0,0,0]", "synthetic:1")


def test_json_history_limits_count_escaped_strings_and_exact_depth(monkeypatch):
    monkeypatch.setattr(compare, "MAX_JSON_NODES", 5)
    assert compare.EvidenceBudget().json('{"x":["a\\"[",0]}', "synthetic") == {
        "x": ['a"[', 0]
    }
    monkeypatch.setattr(compare, "MAX_JSON_DEPTH", 2)
    assert compare.EvidenceBudget().json("[[0]]", "synthetic") == [[0]]
    with pytest.raises(ValueError, match="evidence history limit"):
        compare.EvidenceBudget().json("[[[0]]]", "synthetic")


def test_completion_sync_failure_preserves_published_artifacts(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "db"
    _database(source).close()
    shadow = _shadow_dir(tmp_path)
    original = prepare.sync_directory

    def fail_final_sync(path):
        if (shadow / "shadow.json").exists():
            raise OSError("synthetic sync failure")
        original(path)

    monkeypatch.setattr(prepare, "sync_directory", fail_final_sync)
    assert prepare.main(["--source", str(source), "--shadow-dir", str(shadow)]) == 1
    assert (shadow / "db.sqlite3").exists()
    assert (shadow / "shadow.json").exists()
    assert "uncertain publication" in capsys.readouterr().err
