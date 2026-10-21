//! The binary (PLAN.md 3.1, 3.6, 5.1, D-1, D-2): `test_agent_command.py`,
//! CLI validation and exit codes, `--check-schema`, `--run-once` against
//! fakes with `--influx-write-file`, and the signals.

mod common;

use std::io::{BufRead as _, BufReader};
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::json;

use common::{FakeServer, TestDb};

fn command(args: &[&str], env: &[(&str, &str)]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_graphyard-agent"));
    command.args(args).env_clear().envs(env.iter().copied());
    command
}

fn agent(args: &[&str], env: &[(&str, &str)]) -> Output {
    command(args, env).output().unwrap()
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn db_arg(db: &TestDb) -> &str {
    db.path().to_str().unwrap()
}

#[test]
fn start_agent_requires_at_least_one_loop_enabled() {
    let output = agent(
        &["--disable-metrics", "--disable-conditions", "--run-once"],
        &[],
    );
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        text(&output.stderr),
        "CommandError: At least one loop must be enabled\n"
    );
    assert!(output.stdout.is_empty());
}

#[test]
fn validation_errors_exit_1_and_usage_errors_exit_2() {
    for (args, message) in [
        (
            &["--metrics-interval", "0"][..],
            "--metrics-interval must be greater than 0",
        ),
        (
            &["--condition-interval", "-1"],
            "--condition-interval must be greater than 0",
        ),
    ] {
        let output = agent(args, &[]);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        assert_eq!(text(&output.stderr), format!("CommandError: {message}\n"));
    }
    // An interval from the environment is validated the same way.
    let output = agent(&[], &[("GRAPHYARD_CONDITION_EVAL_INTERVAL_SECONDS", "0")]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        text(&output.stderr),
        "CommandError: --condition-interval must be greater than 0\n"
    );
    for args in [
        &["--metrics-interval", "x"][..],
        &["--bogus"],
        &["positional"],
    ] {
        let output = agent(args, &[]);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn an_invalid_environment_exits_1_before_the_flags_are_checked() {
    let env = [("GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS", "30s")];
    for args in [&[][..], &["--bogus"]] {
        let output = agent(args, &env);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        let stderr = text(&output.stderr);
        assert!(
            stderr.starts_with(
                "invalid environment variable GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS: ValueError: "
            ),
            "{stderr}"
        );
    }
}

#[test]
fn check_schema_reports_in_one_line() {
    let db = TestDb::new();
    let output = agent(&["--check-schema", "--db", db_arg(&db)], &[]);
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        text(&output.stdout),
        "database schema satisfies the agent contract \
         (graphyard.0010_condition_stale_allowance, journal_mode=delete)\n"
    );

    db.execute(
        "ALTER TABLE condition_definition DROP COLUMN stale_after_seconds; \
         DELETE FROM django_migrations WHERE name >= '0010'",
    );
    let output = agent(&["--check-schema", "--db", db_arg(&db)], &[]);
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        text(&output.stderr),
        "database schema is older than the agent contract; missing columns: \
         condition_definition.stale_after_seconds; missing migration: \
         graphyard.0010_condition_stale_allowance; run `manage.py migrate` before starting \
         the agent\n"
    );
    // The same check fails the agent before its first run (D-2).
    let output = agent(&["--run-once", "--db", db_arg(&db)], &[]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        text(&output.stderr).contains("missing columns: condition_definition.stale_after_seconds")
    );
    assert_eq!(db.count("pipeline_heartbeat"), 0);
}

#[test]
fn a_missing_database_exits_1_and_is_never_created() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("db.sqlite3");
    let path = missing.to_str().unwrap();
    for args in [
        &["--check-schema", "--db", path][..],
        &["--run-once", "--db", path],
    ] {
        let output = agent(args, &[]);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        assert!(
            text(&output.stderr).starts_with(&format!("cannot open database {path}: ")),
            "{output:?}"
        );
    }
    assert!(!missing.exists());
    // The default is db.sqlite3 in the working directory.
    let output = command(&["--run-once"], &[])
        .current_dir(dir.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(text(&output.stderr).starts_with("cannot open database db.sqlite3: "));
    assert!(!missing.exists());
}

#[test]
fn run_once_calls_both_loops_and_writes_only_to_the_file() {
    let server = FakeServer::start();
    let db = TestDb::new();
    common::insert_spec(
        &db,
        "json",
        "http_json_metric",
        &json!({
            "url": server.url("/value"),
            "metric_path": "$.value",
            "metric_name": "service.example_value",
            "service_id": "example",
        }),
    );
    db.execute(
        "INSERT INTO condition_definition (name, enabled, metric_name, host_filter, \
         service_filter, tags_filter, operator, warning_threshold, critical_threshold, \
         window_minutes, breach_minutes, status, message, created_at, updated_at, \
         subject_id_filter, subject_type_filter, alert_when_stale) VALUES ('cond', 1, \
         'service.example_value', '', '', '{}', 'gte', 5, 10, 30, 5, 'ok', '', \
         '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '', 1)",
    );
    let writes = db.dir.path().join("writes.lp");
    let url = server.url("");
    let env = [
        ("INFLUX_URL", url.as_str()),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
        ("INFLUX_API_MODE", "v2"),
    ];
    let output = agent(
        &[
            "--run-once",
            "--db",
            db_arg(&db),
            "--influx-write-file",
            writes.to_str().unwrap(),
        ],
        &env,
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        text(&output.stdout),
        "Starting graphyard-agent (metrics=True@60s, conditions=True@60s)\n\
         Metric specs: total=1 failed=0 warning=0 ingested=1 skipped=0\n\
         Condition eval: evaluated=1 failed=0\n"
    );
    let stderr = text(&output.stderr);
    assert!(
        stderr.contains(&format!(
            "HTTP Request: GET {} \"HTTP/1.1 200 OK\"\n",
            server.url("/value")
        )),
        "{stderr}"
    );
    // The query reached the fake InfluxDB, no write request did.
    assert_eq!(
        server.requests(),
        ["GET /value", "POST /api/v2/query?org=o"]
    );
    let written = std::fs::read_to_string(&writes).unwrap();
    assert_eq!(written.lines().count(), 2, "{written}");
    assert!(written.starts_with("# write ") && written.contains(" points=1\n"));

    assert_eq!(
        db.text("metric_collection_spec", "json", "last_status"),
        "ok"
    );
    assert_eq!(
        db.text("pipeline_heartbeat", "metric_collectors", "status"),
        "ok"
    );
    assert_eq!(
        db.text("condition_definition", "cond", "message"),
        "No samples available in condition window"
    );
    assert_eq!(
        db.text("pipeline_heartbeat", "condition_evaluator", "details"),
        r#"{"evaluated": 1, "failed": 0}"#
    );
}

#[test]
fn a_failed_run_is_reported_and_run_once_still_exits_0() {
    let db = TestDb::new();
    let output = agent(
        &["--run-once", "--disable-conditions", "--db", db_arg(&db)],
        &[],
    );
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        text(&output.stdout),
        "Starting graphyard-agent (metrics=True@60s, conditions=False@60s)\n"
    );
    let stderr = text(&output.stderr);
    assert!(
        stderr.ends_with(
            "Metric collection failed: ValueError: No matching enabled metric collection specs\n\
         Metric collection failed: No matching enabled metric collection specs\n"
        ),
        "{stderr}"
    );
    assert_eq!(db.count("pipeline_heartbeat"), 0);
}

/// Start the loop, wait for its first conditions run, send `signal`.
fn signalled(db: &TestDb, signal: &str) -> (Output, Vec<String>) {
    let mut child: Child = command(&["--db", db_arg(db)], &[])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (lines, received) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            let _ = lines.send(line.clone());
        }
    });
    let mut seen = Vec::new();
    while !seen
        .iter()
        .any(|line: &String| line.starts_with("Condition eval:"))
    {
        seen.push(received.recv_timeout(Duration::from_secs(10)).unwrap());
    }
    let status = Command::new("kill")
        .args([signal, &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let output = child.wait_with_output().unwrap();
    reader.join().unwrap();
    seen.extend(received.try_iter());
    (output, seen)
}

#[cfg(unix)]
#[test]
fn sigint_prints_the_interrupted_line_and_exits_0() {
    let db = TestDb::new();
    let (output, lines) = signalled(&db, "-INT");
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        lines,
        [
            "Starting graphyard-agent (metrics=True@60s, conditions=True@60s)",
            "Metric specs: total=0 failed=0 warning=0 ingested=0 skipped=0",
            "Condition eval: evaluated=0 failed=0",
            "graphyard-agent interrupted, exiting",
        ]
    );
}

#[cfg(unix)]
#[test]
fn sigterm_exits_0_silently() {
    let db = TestDb::new();
    let (output, lines) = signalled(&db, "-TERM");
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        lines.last().map(String::as_str),
        Some("Condition eval: evaluated=0 failed=0")
    );
    assert_eq!(db.count("pipeline_heartbeat"), 1);
    assert!(Path::new(db.path()).exists());
}

#[cfg(unix)]
#[test]
fn sigterm_during_a_locked_startup_exits_0() {
    let db = TestDb::new();
    let holder = db.raw();
    holder.execute_batch("BEGIN EXCLUSIVE").unwrap();
    let mut child = command(
        &["--db", db_arg(&db)],
        &[("GRAPHYARD_SQLITE_BUSY_TIMEOUT_MS", "20000")],
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut start = String::new();
    stdout.read_line(&mut start).unwrap();
    assert!(start.starts_with("Starting graphyard-agent"), "{start}");
    // The handlers are registered right after the start line, before the
    // database is opened (where the agent now waits on the lock).
    std::thread::sleep(Duration::from_millis(500));
    let killed = std::time::Instant::now();
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let status = child.wait().unwrap();
    assert!(
        killed.elapsed() < Duration::from_secs(5),
        "{:?}",
        killed.elapsed()
    );
    assert_eq!(status.code(), Some(0), "{status:?}");
    holder.execute_batch("COMMIT").unwrap();
}
