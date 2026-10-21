//! `graphyard-agent evaluate-conditions` (PLAN.md 11.4, slice S15a), the
//! port of `manage.py evaluate_conditions`: the counterparts of
//! `test_evaluate_conditions_command.py` (summary lines, `CommandError`,
//! exit status, `--condition-id` passed through) on the binary, plus the
//! failure paths of the command. The run itself is pinned by `runs.json`
//! (`runs.rs`), the whole command with real samples by `e2e.rs`.

mod common;

use std::process::{Command, Output};

use common::{FakeServer, TestDb};

fn agent(args: &[&str], env: &[(&str, &str)]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_graphyard-agent"))
        .args(args)
        .env_clear()
        .envs(env.iter().copied())
        .output()
        .unwrap()
}

fn evaluate(db: &TestDb, args: &[&str], env: &[(&str, &str)]) -> Output {
    let db = db.path().to_str().unwrap();
    agent(
        &[&["evaluate-conditions", "--db", db][..], args].concat(),
        env,
    )
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn condition(db: &TestDb, id: i64, name: &str, enabled: bool) {
    db.raw()
        .execute(
            "INSERT INTO condition_definition (id, name, enabled, metric_name, host_filter, \
             service_filter, tags_filter, operator, warning_threshold, critical_threshold, \
             window_minutes, breach_minutes, status, message, created_at, updated_at, \
             subject_id_filter, subject_type_filter, alert_when_stale) VALUES (?, ?, ?, \
             'host.cpu_percent', '', '', '{}', 'gte', 80, 90, 30, 5, 'ok', '', \
             '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', '', 1)",
            rusqlite::params![id, name, enabled],
        )
        .unwrap();
}

/// Three enabled conditions (ids 1, 2, 4) and a disabled one (id 3).
fn conditions() -> TestDb {
    let db = TestDb::new();
    condition(&db, 1, "b cpu", true);
    condition(&db, 2, "a cpu", true);
    condition(&db, 3, "c disabled", false);
    condition(&db, 4, "d cpu", true);
    db
}

fn influx_env(server: &FakeServer) -> Vec<(&'static str, String)> {
    vec![
        ("INFLUX_URL", server.url("")),
        ("INFLUX_TOKEN", "t".to_owned()),
        ("INFLUX_ORG", "o".to_owned()),
        ("INFLUX_API_MODE", "v2".to_owned()),
    ]
}

fn pairs<'a>(env: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    env.iter()
        .map(|(key, value)| (*key, value.as_str()))
        .collect()
}

fn heartbeat_details(db: &TestDb) -> String {
    db.text("pipeline_heartbeat", "condition_evaluator", "details")
}

/// `test_evaluate_conditions_command_handle_reports_summary` and
/// `test_evaluate_conditions_command_cli_exit_status` without failures.
#[test]
fn a_run_without_failures_reports_the_summary_and_exits_0() {
    let server = FakeServer::start();
    let db = conditions();
    let env = influx_env(&server);
    let output = evaluate(&db, &[], &pairs(&env));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        text(&output.stdout),
        "Condition evaluation completed (3).\n"
    );
    // No start line, no startup log lines; v2 queries are not logged.
    assert_eq!(text(&output.stderr), "");
    // One query per enabled condition, ordered by name.
    assert_eq!(server.requests(), ["POST /api/v2/query?org=o"; 3]);
    for name in ["a cpu", "b cpu", "d cpu"] {
        assert_eq!(
            db.text("condition_definition", name, "message"),
            "No samples available in condition window"
        );
        assert_eq!(db.text("condition_definition", name, "status"), "warning");
    }
    assert_eq!(db.text("condition_definition", "c disabled", "message"), "");
    assert_eq!(heartbeat_details(&db), r#"{"evaluated": 3, "failed": 0}"#);
    assert_eq!(
        db.text("pipeline_heartbeat", "condition_evaluator", "status"),
        "ok"
    );
}

/// The failure branch of both Python tests: the summary on stdout, then
/// `CommandError: ...` on stderr and exit 1.
#[test]
fn failed_conditions_report_a_command_error_and_exit_1() {
    let db = conditions();
    // No InfluxDB settings: every query is a configuration error.
    let output = evaluate(&db, &[], &[("INFLUX_API_MODE", "v2")]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        text(&output.stdout),
        "Condition evaluation completed with failures (3/3).\n"
    );
    assert_eq!(
        text(&output.stderr),
        "CommandError: 3 condition evaluation(s) failed.\n"
    );
    assert_eq!(
        db.text("condition_definition", "a cpu", "message"),
        "evaluation error: Influx setting missing: INFLUX_URL is required"
    );
    assert_eq!(
        db.text("condition_definition", "a cpu", "status"),
        "critical"
    );
    assert_eq!(heartbeat_details(&db), r#"{"evaluated": 3, "failed": 3}"#);
    assert_eq!(
        db.text("pipeline_heartbeat", "condition_evaluator", "last_error"),
        "3 condition(s) failed"
    );
}

/// `evaluate.assert_called_once_with(condition_id=7)`: the id selects the
/// one enabled condition; the heartbeat is written for it.
#[test]
fn condition_id_evaluates_only_that_condition() {
    let server = FakeServer::start();
    let db = conditions();
    let env = influx_env(&server);
    let output = evaluate(&db, &["--condition-id", "4"], &pairs(&env));
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        text(&output.stdout),
        "Condition evaluation completed (1).\n"
    );
    assert_eq!(server.requests().len(), 1);
    assert_eq!(db.text("condition_definition", "a cpu", "message"), "");
    assert_eq!(
        db.text("condition_definition", "d cpu", "message"),
        "No samples available in condition window"
    );
    assert_eq!(heartbeat_details(&db), r#"{"evaluated": 1, "failed": 0}"#);

    // A disabled, unknown or out-of-range id evaluates nothing; the
    // heartbeat records the empty run.
    for id in ["3", "-7", "99999999999999999999"] {
        let output = evaluate(&db, &["--condition-id", id], &pairs(&env));
        assert_eq!(output.status.code(), Some(0), "{id}: {output:?}");
        assert_eq!(
            text(&output.stdout),
            "Condition evaluation completed (0).\n"
        );
        assert_eq!(heartbeat_details(&db), r#"{"evaluated": 0, "failed": 0}"#);
    }
    assert_eq!(server.requests().len(), 1);
}

/// A failure outside a condition (here a stored value of another storage
/// class, D-13, failing the load): one stderr line, exit 1, no heartbeat.
#[test]
fn a_failure_outside_a_condition_exits_1_with_one_line() {
    let db = conditions();
    db.execute("UPDATE condition_definition SET window_minutes = 'thirty' WHERE id = 1");
    let output = evaluate(&db, &[], &[]);
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(output.stdout.is_empty());
    let stderr = text(&output.stderr);
    assert!(
        stderr.starts_with("Condition evaluation failed: "),
        "{stderr}"
    );
    assert_eq!(stderr.lines().count(), 1, "{stderr}");
    assert_eq!(db.count("pipeline_heartbeat"), 0);
    assert_eq!(db.text("condition_definition", "a cpu", "message"), "");
}

/// D-2 as at the agent's startup: a missing database is never created, an
/// old schema is named in one line.
#[test]
fn the_database_is_checked_before_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("db.sqlite3");
    let path = missing.to_str().unwrap();
    let output = agent(&["evaluate-conditions", "--db", path], &[]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        text(&output.stderr).starts_with(&format!("cannot open database {path}: ")),
        "{output:?}"
    );
    assert!(output.stdout.is_empty());
    // The default is db.sqlite3 in the working directory.
    let output = Command::new(env!("CARGO_BIN_EXE_graphyard-agent"))
        .arg("evaluate-conditions")
        .env_clear()
        .current_dir(dir.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(text(&output.stderr).starts_with("cannot open database db.sqlite3: "));
    assert!(!missing.exists());

    let db = conditions();
    db.execute(
        "ALTER TABLE condition_definition DROP COLUMN stale_after_seconds; \
         DELETE FROM django_migrations WHERE name >= '0010'",
    );
    let output = evaluate(&db, &[], &[]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        text(&output.stderr).contains("missing columns: condition_definition.stale_after_seconds"),
        "{output:?}"
    );
    assert_eq!(db.count("pipeline_heartbeat"), 0);
}

/// The environment is read before the flags (PLAN.md 3.1); bad flags are
/// usage errors (exit 2) and touch nothing.
#[test]
fn environment_and_usage_errors() {
    let db = conditions();
    let env = [("GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS", "ten")];
    for args in [&[][..], &["--bogus"]] {
        let output = evaluate(&db, args, &env);
        assert_eq!(output.status.code(), Some(1), "{args:?}");
        assert!(
            text(&output.stderr).starts_with(
                "invalid environment variable GRAPHYARD_CONDITION_DATA_STALE_WARNING_SECONDS: "
            ),
            "{output:?}"
        );
    }
    for args in [
        &["--condition-id", "x"][..],
        &["--condition-id"],
        &["--no-color"],
        &["--run-once"],
    ] {
        let output = evaluate(&db, args, &[]);
        assert_eq!(output.status.code(), Some(2), "{args:?}");
        assert!(output.stdout.is_empty());
    }
    let output = agent(&["--run-once", "evaluate-conditions"], &[]);
    assert_eq!(output.status.code(), Some(2));
    assert_eq!(db.count("pipeline_heartbeat"), 0);
}
