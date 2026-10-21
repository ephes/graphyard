"""Helpers of the Rust agent's parity harness (agent-rs/parity, PLAN.md 9.4).

The harness itself runs in CI's ``parity`` job (``just rs-parity``). These
tests pin the parts whose silent breakage would let it pass vacuously: the
fakes answer by request index and the real Python query code reads the fake
InfluxDB's answers as samples at the scripted ages, the masks hide only the
documented values, and the comparison reports what differs.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import sqlite3
import sys
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType

import pytest

from graphyard import influx

PARITY = Path(__file__).resolve().parents[1] / "agent-rs" / "parity"


def _load(name: str) -> ModuleType:
    if str(PARITY) not in sys.path:
        sys.path.insert(0, str(PARITY))
    spec = importlib.util.spec_from_file_location(name, PARITY / f"{name}.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


fakes = _load("fakes")
run_parity = _load("run_parity")


def test_scripts_answer_by_request_index_and_repeat_the_last_response():
    script = fakes.Script()
    script.set("/a", ["first", "second"])
    assert [script.next("/a") for _ in range(4)] == [
        "first",
        "second",
        "second",
        "second",
    ]
    assert script.next("/unknown") is None
    with pytest.raises(ValueError):
        script.set("/a", ["again"])


def test_http_fake_serves_its_script_and_logs_every_request():
    http = fakes.HttpFake()
    try:
        http.script.set(
            "/p",
            [
                run_parity.reply(
                    200, {"a": 1}, [("Set-Cookie", "x=1"), ("Set-Cookie", "y=2")]
                ),
                run_parity.reply(201, "zipped", gzip=True),
            ],
        )
        url = f"http://127.0.0.1:{http.port}/p?q=1"
        with urllib.request.urlopen(url) as response:
            assert response.status == 200
            assert response.headers.get_all("Set-Cookie") == ["x=1", "y=2"]
            assert json.loads(response.read()) == {"a": 1}
        request = urllib.request.Request(url, data=b"body", method="POST")
        with urllib.request.urlopen(request) as response:
            assert response.status == 201
            assert response.headers["Content-Encoding"] == "gzip"
            assert gzip.decompress(response.read()) == b"zipped"
        with pytest.raises(urllib.error.HTTPError) as unknown:
            urllib.request.urlopen(f"http://127.0.0.1:{http.port}/nothing")
        assert unknown.value.code == 404
        logged = http.requests()
        assert [(r.method, r.target, r.body) for r in logged] == [
            ("GET", "/p?q=1", b""),
            ("POST", "/p?q=1", b"body"),
            ("GET", "/nothing", b""),
        ]
        assert logged[0].path == "/p"
    finally:
        http.close()


def _expected_samples(stop: datetime) -> list[tuple[datetime, float, dict[str, str]]]:
    return [
        (stop - timedelta(seconds=300), 0.25, {"mountpoint": "/data"}),
        (stop - timedelta(seconds=90), 0.5, {"mountpoint": "/"}),
        (stop - timedelta(seconds=30), 0.75, {"mountpoint": "/data"}),
    ]


@pytest.mark.parametrize("mode", ["v2", "v3", "auto"])
def test_the_python_query_code_reads_the_fake_influxdb_samples(settings, mode):
    fake = fakes.InfluxFake("graphyard_metrics")
    try:
        script = [
            {"age": 30, "value": 0.75, "tags": {"mountpoint": "/data"}},
            {"age": 90, "value": 0.5, "tags": {"mountpoint": "/"}},
            {"age": 300, "value": 0.25, "tags": {"mountpoint": "/data"}},
        ]
        fake.queries.set("host.disk", [script, {"status": 500, "body": "boom"}])
        fake.v2_missing = mode == "auto"
        settings.INFLUX_URL = f"http://127.0.0.1:{fake.port}"
        settings.INFLUX_TOKEN = "t"
        settings.INFLUX_ORG = "o"
        settings.INFLUX_BUCKET = "b"
        settings.INFLUX_MEASUREMENT = "graphyard_metrics"
        settings.INFLUX_API_MODE = mode
        stop = datetime(2026, 10, 21, 12, 0, 0, 123456, tzinfo=UTC)
        start = stop - timedelta(minutes=30)
        samples = influx.query_range("host.disk", start, stop)
        got = sorted((s.ts, s.value, s.tags) for s in samples)
        assert got == _expected_samples(stop)
        # The second answer is the scripted error.
        with pytest.raises(Exception):  # noqa: B017 - ApiException or the SQL error
            influx.query_range("host.disk", start, stop)
        queries = [r for r in fake.requests() if r.path != "/api/v2/query"]
        if mode == "v2":
            queries = fake.requests()
        assert [fakes.query_window(r) for r in queries] == [(start, stop)] * 2
    finally:
        fake.close()


def test_query_bounds_are_masked_and_the_window_kept():
    flux = 'from(bucket: "b")\n  |> range(start: 2026-10-21T11:30:00Z, stop: 2026-10-21T12:00:00.5Z)\n'
    sql = "select * from \"m\" where time >= '2026-10-21T11:30:00Z' and time < '2026-10-21T12:00:00Z' and metric = 'x'"
    assert fakes.mask_query_bounds(flux) == (
        'from(bucket: "b")\n  |> range(start: <start>, stop: <stop>)\n'
    )
    assert fakes.mask_query_bounds(sql) == (
        "select * from \"m\" where time >= '<start>' and time < '<stop>' and metric = 'x'"
    )
    request = fakes.Request(
        "POST", "/api/v2/query?org=o", (), json.dumps({"query": flux}).encode()
    )
    view = run_parity.request_view(request, [], (0, 0))
    assert view["window"] == "0:30:00.500000"
    assert "range(start: <start>, stop: <stop>)" in view["body"]


def test_written_lines_mask_only_now_based_timestamps_and_timings():
    window = (1_000, 2_000)
    line = "m,metric=service.queue_depth,spec_name=a\\ b value=12 1500"
    assert run_parity.line_view(line, [], window) == {
        "series": "m,metric=service.queue_depth,spec_name=a\\ b",
        "fields": "value=12",
        "ts": "<now>",
    }
    # A timestamp from the payload (outside the run) is compared exactly.
    assert run_parity.line_view(line.replace("1500", "999"), [], window)["ts"] == 999
    timing = "m,metric=service.http_page_ttfb_seconds value=0.0123 1500"
    assert run_parity.line_view(timing, [], window)["fields"] == "value=<duration>"
    status = "m,metric=service.http_page_status_code value=200 1500"
    assert run_parity.line_view(status, [], window)["fields"] == "value=200"
    tokens = [("127.0.0.1:4711", "127.0.0.1:<http>")]
    url = "m,metric=x,source_entity_id=http://127.0.0.1:4711/p value=1 1"
    assert run_parity.line_view(url, tokens, window)["series"] == (
        "m,metric=x,source_entity_id=http://127.0.0.1:<http>/p"
    )


def test_operator_lines_keep_the_documented_lines_only():
    stderr = "\n".join(
        [
            "graphyard-agent 0.1.0 (abc)",
            'HTTP Request: GET http://127.0.0.1:4711/x "HTTP/1.1 200 OK"',
            "Metric collection spec 'p' (http_page_probe) raised",
            "Traceback (most recent call last):",
            "Metric collection spec 'p' (http_page_probe) raised: E: boom",
            "Ignoring host_filter for non-host condition id=7 name=n",
            "Spec s missing config.subject_mapping; using default entity_name_slug mapping",
        ]
    )
    assert run_parity.operator_lines(stderr, [("127.0.0.1:4711", "<http>")]) == [
        'HTTP Request: GET http://<http>/x "HTTP/1.1 200 OK"',
        "Metric collection spec 'p' (http_page_probe) raised",
        "Metric collection spec 'p' (http_page_probe) raised",
        "Ignoring host_filter for non-host condition id=7 name=n",
        "Spec s missing config.subject_mapping; using default entity_name_slug mapping",
    ]


def test_differences_report_values_key_order_and_missing_items():
    same = [{"name": "a", "x": 1}]
    assert run_parity.diff_lists("t", same, [{"name": "a", "x": 1}]) == []
    (value,) = run_parity.diff_lists("t", same, [{"name": "a", "x": 2}])
    assert "x: python=1" in value and "rust  =2" in value
    assert run_parity.diff_lists("t", [{"a": 1, "b": 2}], [{"b": 2, "a": 1}])
    assert run_parity.diff_lists("t", ["line"], [])


def test_table_views_mask_now_based_columns_only(tmp_path):
    db = tmp_path / "db.sqlite3"
    http = fakes.HttpFake()
    https = fakes.HttpFake()
    fake = fakes.InfluxFake("graphyard_metrics")
    try:
        corpus = run_parity.build_corpus(http, https, fake)
        run_parity.seed(db, corpus)
    finally:
        for server in (http, https, fake):
            server.close()
    now_ts = int(datetime(2026, 10, 21, 12, 0, 0, tzinfo=UTC).timestamp())
    connection = sqlite3.connect(db)
    connection.execute(
        "UPDATE metric_collection_spec SET last_run_at = '2026-10-21 12:00:00.5',"
        " next_run_time = ?, last_error = 'url http://127.0.0.1:4711/x'"
        " WHERE id = 1",
        (now_ts + 60,),
    )
    connection.commit()
    connection.close()
    tokens = [("127.0.0.1:4711", "127.0.0.1:<http>")]
    specs = run_parity.table_view(db, "metric_collection_spec", tokens)
    assert specs[0]["last_run_at"] == "<set>"
    assert specs[0]["next_run_time"] == "last_run_at + 60"
    assert specs[0]["last_error"] == "url http://127.0.0.1:<http>/x"
    assert specs[0]["created_at"] == run_parity.SEEDED_AT
    assert specs[1]["last_run_at"] is None and specs[1]["next_run_time"] == 0
    heartbeats = run_parity.table_view(db, "pipeline_heartbeat", tokens)
    assert [(h["name"], h["last_success"], h["updated_at"]) for h in heartbeats] == [
        ("condition_evaluator", "<set>", "<set>"),
        ("ha_poller", "2026-03-01 10:00:00.5", "2026-03-01 10:00:00.5"),
    ]
    hosts = run_parity.table_view(db, "host_registry", tokens)
    assert [h["last_seen_at"] for h in hosts] == ["<set>", "<set>"]


def test_every_spec_type_and_corpus_case_is_covered():
    http, https = fakes.HttpFake(), fakes.HttpFake()
    fake = fakes.InfluxFake("graphyard_metrics")
    try:
        corpus = run_parity.build_corpus(http, https, fake)
    finally:
        for server in (http, https, fake):
            server.close()
    rounds = len(run_parity.ROUNDS)
    types = {spec.spec_type for spec in corpus.specs if spec.enabled}
    assert {
        "home_assistant_sensor",
        "home_assistant_env_scan",
        "http_json_metric",
        "http_page_probe",
        "unifi_device_traffic",
    } < types
    assert any(not spec.enabled for spec in corpus.specs)
    assert any(not condition.enabled for condition in corpus.conditions)
    for item in [*corpus.specs, *corpus.conditions]:
        assert item.expect is None or len(item.expect) == rounds, item.name
    statuses = {s for spec in corpus.specs for s in spec.expect or []}
    statuses |= {s for c in corpus.conditions for s in c.expect or []}
    assert statuses == {"ok", "warning", "critical"}
    modes = {round_.env.get("INFLUX_API_MODE") for round_ in run_parity.ROUNDS}
    assert modes == {"v2", "v3", "auto"}
