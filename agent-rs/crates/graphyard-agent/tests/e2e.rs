//! Process e2e of the binary (PLAN.md 9.3): `graphyard-agent` against a
//! database built from the schema fixture and fake HTTP servers (collector
//! endpoints and InfluxDB). `process.rs` (S8) holds the CLI validation,
//! `--check-schema`, missing-database and signal basics; this file adds a
//! run over every collector family with all rows and lines checked, the
//! loop's due selection, signals while a spec waits on a stalled server,
//! the write file that cannot be opened and the schema check's naming of
//! missing tables. The blocked `getaddrinfo` case needs a resolver stub and
//! runs one level below the binary (`shutdown.rs`), on the same `run_agent`
//! as `main`.

mod common;

use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use chrono::{SubsecRound as _, Utc};
use rusqlite::types::Value as SqlValue;
use serde_json::{Value, json};

use common::TestDb;

/// One request the fake server received.
#[derive(Clone, Debug)]
struct Request {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

enum Reply {
    Respond {
        status: &'static str,
        content_type: &'static str,
        body: String,
    },
    /// Keep the connection open without an answer.
    Stall,
}

fn respond(status: &'static str, content_type: &'static str, body: impl Into<String>) -> Reply {
    Reply::Respond {
        status,
        content_type,
        body: body.into(),
    }
}

type Handler = dyn Fn(&Request) -> Reply + Send + Sync;

/// A fake HTTP/1.1 server answering every connection on its own thread.
struct Fake {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<Request>>>,
}

impl Fake {
    fn start(handler: impl Fn(&Request) -> Reply + Send + Sync + 'static) -> Fake {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = requests.clone();
        let handler: Arc<Handler> = Arc::new(handler);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let recorded = recorded.clone();
                let handler = handler.clone();
                std::thread::spawn(move || serve(stream, &recorded, &*handler));
            }
        });
        Fake { addr, requests }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }

    /// Wait until a request for `path` arrived.
    fn wait_for(&self, path: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.requests().iter().any(|r| r.target == path) {
            assert!(Instant::now() < deadline, "no request for {path}");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

fn serve(mut stream: TcpStream, recorded: &Mutex<Vec<Request>>, handler: &Handler) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let mut headers = Vec::new();
    let mut length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 || line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.trim_end().split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap_or(0);
            }
            headers.push((name.to_owned(), value.trim().to_owned()));
        }
    }
    let mut body = vec![0; length];
    let _ = reader.read_exact(&mut body);
    let mut parts = request_line.split_whitespace();
    let request = Request {
        method: parts.next().unwrap_or_default().to_owned(),
        target: parts.next().unwrap_or_default().to_owned(),
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    };
    recorded.lock().unwrap().push(request.clone());
    match handler(&request) {
        Reply::Respond {
            status,
            content_type,
            body,
        } => {
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
        }
        Reply::Stall => std::thread::sleep(Duration::from_secs(60)),
    }
}

/// Annotated CSV of InfluxDB v2 with one sample per minute over the last
/// 20 minutes (relative to the request), all at `value`.
fn flux_csv(metric: &str, value: f64) -> String {
    let mut csv = String::from(
        "#datatype,string,long,dateTime:RFC3339,double,string,string,string,string\r\n\
         #group,false,false,false,false,true,true,true,true\r\n\
         #default,_result,,,,,,,\r\n\
         ,result,table,_time,_value,_field,_measurement,metric,host\r\n",
    );
    let now = Utc::now().trunc_subsecs(0);
    for minute in (1..=20).rev() {
        let ts = now - chrono::Duration::minutes(minute);
        csv.push_str(&format!(
            ",,0,{},{value},value,graphyard_metrics,{metric},macmini\r\n",
            ts.format("%Y-%m-%dT%H:%M:%SZ")
        ));
    }
    csv.push_str("\r\n");
    csv
}

fn command(args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_graphyard-agent"));
    command.args(args).env_clear().envs(env.iter().copied());
    command
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn spec(db: &TestDb, name: &str, spec_type: &str, enabled: bool, next_run: i64, config: &Value) {
    db.raw()
        .execute(
            "INSERT INTO metric_collection_spec (name, enabled, spec_type, interval_seconds, \
             next_run_time, config, last_run_at, last_status, last_error, created_at, \
             updated_at) VALUES (?, ?, ?, 300, ?, ?, NULL, 'ok', '', \
             '2026-01-02 03:04:05', '2026-01-02 03:04:05')",
            rusqlite::params![name, enabled, spec_type, next_run, config.to_string()],
        )
        .unwrap();
}

fn condition(db: &TestDb, name: &str, metric: &str, host_filter: &str, subject_type: &str) {
    db.raw()
        .execute(
            "INSERT INTO condition_definition (name, enabled, metric_name, host_filter, \
             service_filter, tags_filter, operator, warning_threshold, critical_threshold, \
             window_minutes, breach_minutes, status, message, created_at, updated_at, \
             subject_id_filter, subject_type_filter, alert_when_stale) VALUES (?, 1, ?, ?, \
             '', '{}', 'gte', 0.8, 0.9, 30, 5, 'ok', '', '2026-01-01 00:00:00', \
             '2026-01-01 00:00:00', '', ?, 1)",
            rusqlite::params![name, metric, host_filter, subject_type],
        )
        .unwrap();
}

fn influx_env(fake: &Fake) -> Vec<(&'static str, String)> {
    vec![
        ("INFLUX_URL", fake.url("")),
        ("INFLUX_TOKEN", "tok".to_owned()),
        ("INFLUX_ORG", "graphyard".to_owned()),
        ("INFLUX_API_MODE", "v2".to_owned()),
    ]
}

fn run(args: &[&str], env: &[(&'static str, String)]) -> Output {
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    command(args, &env).output().unwrap()
}

fn integer(db: &TestDb, table: &str, name: &str, column: &str) -> i64 {
    match db.value(table, name, column) {
        SqlValue::Integer(value) => value,
        other => panic!("{table}.{column} of {name}: {other:?}"),
    }
}

/// The collectors' fake: a JSON metric, a page, a Home Assistant state, and
/// InfluxDB (Flux samples; a write would be answered but must never come).
fn collectors_fake() -> Fake {
    Fake::start(|request| match request.target.as_str() {
        "/value" => respond("200 OK", "application/json", r#"{"data": {"temp": 41.5}}"#),
        "/page" => respond("200 OK", "text/html", "<html>ok</html>"),
        "/api/states/sensor.living_room_temp" => {
            if request.header("authorization") == Some("Bearer ha-token") {
                respond(
                    "200 OK",
                    "application/json",
                    r#"{"entity_id": "sensor.living_room_temp", "state": "21.5",
                        "attributes": {"unit_of_measurement": "C", "device_class": "temperature"},
                        "last_updated": "2026-10-21T10:00:00.250000+00:00"}"#,
                )
            } else {
                respond("401 Unauthorized", "text/plain", "")
            }
        }
        target if target.starts_with("/api/v2/query") => respond(
            "200 OK",
            "text/csv; charset=utf-8",
            flux_csv("host.disk_ratio", 0.85),
        ),
        _ => respond("404 Not Found", "text/plain", ""),
    })
}

#[test]
fn run_once_writes_the_rows_and_lines_of_every_collector_family() {
    let fake = collectors_fake();
    let db = TestDb::new();
    spec(
        &db,
        "a json",
        "http_json_metric",
        true,
        0,
        &json!({
            "url": fake.url("/value"), "metric_path": "$.data.temp",
            "metric_name": "host.temp", "subject_type": "host", "subject_id": "fractal",
            "host_id": "fractal", "collector_host": "macmini",
        }),
    );
    spec(
        &db,
        "b page",
        "http_page_probe",
        true,
        0,
        &json!({"url": fake.url("/page"), "subject_id": "example_site",
                "collector_host": "macmini"}),
    );
    spec(
        &db,
        "c ha",
        "home_assistant_sensor",
        true,
        0,
        &json!({"base_url": fake.url(""), "access_token": "ha-token",
                "entity_id": "sensor.living_room_temp"}),
    );
    spec(
        &db,
        "d broken",
        "http_json_metric",
        true,
        0,
        &json!({"metric_path": "x"}),
    );
    spec(&db, "e legacy", "legacy_type", true, 0, &json!({}));
    spec(&db, "f disabled", "http_json_metric", false, 0, &json!({}));
    condition(&db, "disk", "host.disk_ratio", "macmini", "");
    condition(&db, "svc", "host.disk_ratio", "macmini", "service");
    let writes = db.dir.path().join("points.lp");
    let before = Utc::now().timestamp();

    let output = run(
        &[
            "--run-once",
            "--db",
            db.path().to_str().unwrap(),
            "--influx-write-file",
            writes.to_str().unwrap(),
        ],
        &influx_env(&fake),
    );

    let after = Utc::now().timestamp();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        text(&output.stdout),
        "Starting graphyard-agent (metrics=True@60s, conditions=True@60s)\n\
         Metric specs: total=5 failed=2 warning=0 ingested=7 skipped=0\n\
         Condition eval: evaluated=2 failed=0\n"
    );
    let stderr = text(&output.stderr);
    for line in [
        format!(
            "HTTP Request: GET {} \"HTTP/1.1 200 OK\"",
            fake.url("/value")
        ),
        format!(
            "HTTP Request: GET {} \"HTTP/1.1 200 OK\"",
            fake.url("/page")
        ),
        format!(
            "HTTP Request: GET {} \"HTTP/1.1 200 OK\"",
            fake.url("/api/states/sensor.living_room_temp")
        ),
        "Spec c ha missing config.subject_mapping; using default entity_name_slug mapping"
            .to_owned(),
        "Ignoring host_filter for non-host condition id=2 name=svc".to_owned(),
    ] {
        assert!(stderr.lines().any(|l| l == line), "{line}\n{stderr}");
    }

    // Collector requests in spec order, then one Flux query per condition;
    // the shadow mode sends no write request.
    let requests = fake.requests();
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(
        targets,
        [
            "/value",
            "/page",
            "/api/states/sensor.living_room_temp",
            "/api/v2/query?org=graphyard",
            "/api/v2/query?org=graphyard",
        ]
    );
    let query = &requests[3];
    assert_eq!(query.method, "POST");
    assert_eq!(query.header("authorization"), Some("Token tok"));
    assert!(
        query
            .body
            .contains(r#"r[\"metric\"] == \"host.disk_ratio\""#),
        "{}",
        query.body
    );

    // Spec rows: status, error, next run (`now_ts + interval`), untouched
    // `updated_at`; the disabled spec is not touched at all.
    for (name, status, error) in [
        ("a json", "ok", ""),
        ("b page", "ok", ""),
        ("c ha", "ok", ""),
        ("d broken", "critical", "config.url is required"),
        ("e legacy", "critical", "unsupported spec_type: legacy_type"),
    ] {
        assert_eq!(
            db.text("metric_collection_spec", name, "last_status"),
            status
        );
        assert_eq!(db.text("metric_collection_spec", name, "last_error"), error);
        let next = integer(&db, "metric_collection_spec", name, "next_run_time");
        assert!(
            (before + 300..=after + 300).contains(&next),
            "{name}: {next}"
        );
        assert_eq!(
            db.text("metric_collection_spec", name, "updated_at"),
            "2026-01-02 03:04:05"
        );
        let run_at = db.text("metric_collection_spec", name, "last_run_at");
        assert!(run_at.len() >= 19 && run_at.starts_with("20"), "{run_at}");
    }
    assert_eq!(
        db.value("metric_collection_spec", "f disabled", "last_run_at"),
        SqlValue::Null
    );
    assert_eq!(
        integer(&db, "metric_collection_spec", "f disabled", "next_run_time"),
        0
    );

    // Heartbeats: some specs failed, so `warning` with success and the
    // first errors; the conditions all evaluated.
    assert_eq!(
        db.text("pipeline_heartbeat", "metric_collectors", "status"),
        "warning"
    );
    assert_eq!(
        db.text("pipeline_heartbeat", "metric_collectors", "last_error"),
        "d broken: config.url is required; e legacy: unsupported spec_type: legacy_type"
    );
    assert_eq!(
        db.text("pipeline_heartbeat", "metric_collectors", "details"),
        r#"{"total": 5, "failed": 2, "warning": 0, "ingested": 7, "skipped": 0}"#
    );
    assert_ne!(
        db.value("pipeline_heartbeat", "metric_collectors", "last_success"),
        SqlValue::Null
    );
    assert_eq!(
        db.text("pipeline_heartbeat", "condition_evaluator", "details"),
        r#"{"evaluated": 2, "failed": 0}"#
    );
    assert_eq!(
        db.text("pipeline_heartbeat", "condition_evaluator", "status"),
        "ok"
    );

    // Conditions: twenty samples at 0.85 breach the warning threshold.
    for name in ["disk", "svc"] {
        assert_eq!(db.text("condition_definition", name, "status"), "warning");
        assert_eq!(
            db.value("condition_definition", name, "last_value"),
            SqlValue::Real(0.85)
        );
        assert_ne!(
            db.value("condition_definition", name, "last_evaluated"),
            SqlValue::Null
        );
    }

    // Registry: the JSON spec's host, the page's service (parented to its
    // collector host, which no host point registered) and the HA subject.
    let subjects: Vec<(String, String, String)> = db
        .raw()
        .prepare("SELECT subject_type, subject_id, source_system FROM subject_registry ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        subjects,
        [
            ("host".into(), "fractal".into(), "http".into()),
            ("service".into(), "example_site".into(), "http_probe".into()),
            (
                "environment_sensor".into(),
                "living_room_temp".into(),
                "homeassistant".into()
            ),
        ]
    );
    let hosts: Vec<String> = db
        .raw()
        .prepare("SELECT host_id FROM host_registry ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(hosts, ["fractal"]);
    let services: Vec<(String, Option<i64>)> = db
        .raw()
        .prepare("SELECT service_id, host_id FROM service_registry ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        services,
        [
            ("example_site".into(), None),
            ("homeassistant".into(), None)
        ]
    );

    // The write file: one batch per collector, every point once.
    let written = std::fs::read_to_string(&writes).unwrap();
    let comments: Vec<&str> = written.lines().filter(|l| l.starts_with('#')).collect();
    assert_eq!(comments.len(), 3, "{written}");
    assert!(comments[0].starts_with("# write 20") && comments[0].ends_with("Z points=1"));
    assert!(comments[1].ends_with(" points=5") && comments[2].ends_with(" points=1"));
    let lines: Vec<&str> = written.lines().filter(|l| !l.starts_with('#')).collect();
    assert_eq!(lines.len(), 7, "{written}");
    assert!(lines[0].starts_with(
        "graphyard_metrics,collector_host=macmini,collector_service=graphyard-agent,\
         host=fractal,metric=host.temp,source_instance=default,source_system=http,\
         spec_name=a\\ json,subject_id=fractal,subject_type=host value=41.5 "
    ));
    let page_metrics: Vec<&str> = lines[1..6]
        .iter()
        .map(|line| {
            line.split(',')
                .find_map(|tag| tag.strip_prefix("metric="))
                .unwrap()
        })
        .collect();
    assert_eq!(
        page_metrics,
        [
            "service.http_page_ttfb_seconds",
            "service.http_page_total_seconds",
            "service.http_page_status_code",
            "service.http_page_success",
            "service.http_page_redirect_count",
        ]
    );
    assert!(lines[3].contains(" value=200 ") && lines[4].contains(" value=1 "));
    // The HA point carries the state's `last_updated` as its time.
    assert_eq!(
        lines[6],
        "graphyard_metrics,collector_host=homeassistant,collector_service=graphyard-agent,\
         device_class=temperature,entity_id=sensor.living_room_temp,\
         metric=ha.sensor.living_room_temp,service=homeassistant,\
         source_entity_id=sensor.living_room_temp,source_instance=default,\
         source_system=homeassistant,spec_name=c\\ ha,subject_id=living_room_temp,\
         subject_type=environment_sensor,unit=C value=21.5 1792576800250000000"
    );
}

#[test]
fn the_loop_runs_only_due_specs() {
    let fake = collectors_fake();
    let db = TestDb::new();
    let config = json!({"url": fake.url("/value"), "metric_path": "$.data.temp",
                        "metric_name": "host.temp", "subject_type": "host",
                        "subject_id": "fractal", "host_id": "fractal"});
    spec(&db, "due", "http_json_metric", true, 0, &config);
    spec(
        &db,
        "later",
        "http_json_metric",
        true,
        i64::from(i32::MAX),
        &config,
    );
    let writes = db.dir.path().join("points.lp");
    let env: Vec<(&str, String)> = influx_env(&fake);
    let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let mut child = command(
        &[
            "--disable-conditions",
            "--db",
            db.path().to_str().unwrap(),
            "--influx-write-file",
            writes.to_str().unwrap(),
        ],
        &env,
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()
    .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut lines = Vec::new();
    while !lines
        .last()
        .is_some_and(|l: &String| l.starts_with("Metric specs:"))
    {
        let mut line = String::new();
        assert!(stdout.read_line(&mut line).unwrap() > 0, "{lines:?}");
        lines.push(line.trim_end().to_owned());
    }
    kill(child.id(), "-INT");
    let mut rest = String::new();
    stdout.read_to_string(&mut rest).unwrap();
    assert_eq!(child.wait().unwrap().code(), Some(0));
    assert_eq!(
        lines,
        [
            "Starting graphyard-agent (metrics=True@60s, conditions=False@60s)",
            "Metric specs: total=1 failed=0 warning=0 ingested=1 skipped=0",
        ]
    );
    assert_eq!(rest, "graphyard-agent interrupted, exiting\n");
    assert_eq!(
        db.text("metric_collection_spec", "due", "last_status"),
        "ok"
    );
    assert_eq!(
        db.value("metric_collection_spec", "later", "last_run_at"),
        SqlValue::Null
    );
    assert_eq!(fake.requests().len(), 1);
}

fn kill(pid: u32, signal: &str) {
    let status = Command::new("kill")
        .args([signal, &pid.to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}

/// A signal while a spec waits on a server that never answers: the agent
/// exits 0 at once, the spec row is not written and no heartbeat exists.
#[cfg(unix)]
#[test]
fn a_signal_during_a_slow_spec_exits_promptly_without_a_partial_row() {
    for (signal, last_line) in [
        ("-TERM", None),
        ("-INT", Some("graphyard-agent interrupted, exiting")),
    ] {
        let fake = Fake::start(|request| {
            if request.target == "/slow" {
                Reply::Stall
            } else {
                respond("200 OK", "application/json", r#"{"v": 1}"#)
            }
        });
        let db = TestDb::new();
        spec(
            &db,
            "slow",
            "http_json_metric",
            true,
            0,
            &json!({"url": fake.url("/slow"), "metric_path": "$.v", "metric_name": "x.y",
                    "request_timeout_seconds": 30}),
        );
        let writes = db.dir.path().join("points.lp");
        let env = influx_env(&fake);
        let env: Vec<(&str, &str)> = env.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let child = command(
            &[
                "--run-once",
                "--db",
                db.path().to_str().unwrap(),
                "--influx-write-file",
                writes.to_str().unwrap(),
            ],
            &env,
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
        fake.wait_for("/slow");
        let signalled = Instant::now();
        kill(child.id(), signal);
        let (done, finished) = mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let output = child.wait_with_output().unwrap();
            let _ = done.send(());
            output
        });
        finished
            .recv_timeout(Duration::from_secs(5))
            .expect("the agent did not exit within 5 s of the signal");
        let output = waiter.join().unwrap();
        assert!(
            signalled.elapsed() < Duration::from_secs(3),
            "{signal}: {:?}",
            signalled.elapsed()
        );
        assert_eq!(output.status.code(), Some(0), "{signal}: {output:?}");
        let stdout = text(&output.stdout);
        let mut expected =
            "Starting graphyard-agent (metrics=True@60s, conditions=True@60s)\n".to_owned();
        if let Some(line) = last_line {
            expected.push_str(line);
            expected.push('\n');
        }
        assert_eq!(stdout, expected, "{signal}");
        assert_eq!(
            db.value("metric_collection_spec", "slow", "last_run_at"),
            SqlValue::Null,
            "{signal}"
        );
        assert_eq!(
            db.text("metric_collection_spec", "slow", "last_status"),
            "ok"
        );
        assert_eq!(db.count("pipeline_heartbeat"), 0, "{signal}");
        assert_eq!(std::fs::read_to_string(&writes).unwrap(), "", "{signal}");
    }
}

#[test]
fn a_write_file_that_cannot_be_opened_exits_1_before_the_first_run() {
    let db = TestDb::new();
    let target = db.dir.path().join("missing-dir").join("points.lp");
    let output = command(
        &[
            "--run-once",
            "--db",
            db.path().to_str().unwrap(),
            "--influx-write-file",
            target.to_str().unwrap(),
        ],
        &[],
    )
    .output()
    .unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stderr = text(&output.stderr);
    assert!(
        stderr.starts_with(&format!(
            "cannot open the --influx-write-file {}: ",
            target.display()
        )),
        "{stderr}"
    );
    assert_eq!(db.count("pipeline_heartbeat"), 0);
    assert!(!target.exists());
    // An existing file is appended to, never truncated.
    let existing = db.dir.path().join("existing.lp");
    std::fs::write(&existing, "# earlier\n").unwrap();
    let output = command(
        &[
            "--run-once",
            "--disable-metrics",
            "--db",
            db.path().to_str().unwrap(),
            "--influx-write-file",
            existing.to_str().unwrap(),
        ],
        &[],
    )
    .output()
    .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(std::fs::read_to_string(&existing).unwrap(), "# earlier\n");
}

#[test]
fn check_schema_names_missing_tables_and_accepts_extra_columns() {
    let db = TestDb::new();
    db.execute("ALTER TABLE host_registry ADD COLUMN future_column text");
    let output = command(
        &["--check-schema", "--db", db.path().to_str().unwrap()],
        &[],
    )
    .output()
    .unwrap();
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(text(&output.stdout).starts_with("database schema satisfies the agent contract"));

    db.execute("DROP TABLE service_registry; DROP TABLE pipeline_heartbeat");
    let output = command(
        &["--check-schema", "--db", db.path().to_str().unwrap()],
        &[],
    )
    .output()
    .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = text(&output.stderr);
    assert!(
        stderr.ends_with('\n') && stderr.lines().count() == 1,
        "{stderr}"
    );
    assert!(
        stderr.contains("missing tables: pipeline_heartbeat, service_registry"),
        "{stderr}"
    );
    assert!(output.stdout.is_empty());
}
