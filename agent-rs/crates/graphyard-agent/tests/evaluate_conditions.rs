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
    // No startup lines; one bounded v2 transfer log per query.
    assert_eq!(
        text(&output.stderr),
        "Influx HTTP response: method=POST status=200\n"
            .to_owned()
            .repeat(3)
    );
    // One query per enabled condition.
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
        "Condition failure condition_id=1 stage=evaluation category=InfluxConfigurationError failure_published=true\n\
         Condition failure condition_id=2 stage=evaluation category=InfluxConfigurationError failure_published=true\n\
         Condition failure condition_id=4 stage=evaluation category=InfluxConfigurationError failure_published=true\n\
         CommandError: 3 condition evaluation(s) failed.\n"
    );
    assert_eq!(
        db.text("condition_definition", "a cpu", "message"),
        "evaluation error: InfluxConfigurationError"
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
/// one enabled condition and preserves the global warning and timestamps.
#[test]
fn condition_id_evaluates_only_that_condition() {
    let server = FakeServer::start();
    let db = conditions();
    db.execute(
        "INSERT INTO pipeline_heartbeat (name, status, last_success, last_error, details, updated_at) \
         VALUES ('condition_evaluator', 'warning', '2026-01-01 00:00:00', \
                 '2 condition(s) failed', '{\"evaluated\":5,\"failed\":2}', '2026-01-01 00:01:00')",
    );
    let before = db.rows("pipeline_heartbeat");
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
    assert_eq!(db.rows("pipeline_heartbeat"), before);

    // A disabled, unknown or out-of-range id also leaves the heartbeat alone.
    for id in ["3", "-7", "99999999999999999999"] {
        let output = evaluate(&db, &["--condition-id", id], &pairs(&env));
        assert_eq!(output.status.code(), Some(1), "{id}: {output:?}");
        assert!(output.stdout.is_empty());
        assert_eq!(
            text(&output.stderr),
            "CommandError: No matching enabled condition.\n"
        );
        assert_eq!(db.rows("pipeline_heartbeat"), before);
    }
    assert_eq!(server.requests().len(), 1);
}

#[test]
fn only_the_exact_enabled_flag_admits_conditions() {
    let server = FakeServer::start();
    let db = TestDb::new();
    condition(&db, 1, "legacy flag", true);
    condition(&db, 2, "enabled", true);
    db.execute("UPDATE condition_definition SET enabled = 2 WHERE id = 1");
    let env = influx_env(&server);

    let targeted = evaluate(&db, &["--condition-id", "1"], &pairs(&env));
    let sweep = evaluate(&db, &[], &pairs(&env));
    assert_eq!(targeted.status.code(), Some(1), "{targeted:?}");
    assert_eq!(
        text(&targeted.stderr),
        "CommandError: No matching enabled condition.\n"
    );
    assert_eq!(sweep.status.code(), Some(0), "{sweep:?}");
    assert_eq!(text(&sweep.stdout), "Condition evaluation completed (1).\n");
    assert_eq!(server.requests(), ["POST /api/v2/query?org=o"]);
    assert_eq!(
        db.text("condition_definition", "legacy flag", "message"),
        ""
    );
    assert_eq!(
        db.text("condition_definition", "enabled", "message"),
        "No samples available in condition window"
    );
    assert_eq!(heartbeat_details(&db), r#"{"evaluated": 1, "failed": 0}"#);
}

#[test]
fn agent_records_empty_sweeps_as_liveness_without_advancing_success() {
    let server = FakeServer::start();
    let db = TestDb::new();
    let env = influx_env(&server);
    let sweep = || {
        let output = agent(
            &[
                "--db",
                db.path().to_str().unwrap(),
                "--run-once",
                "--disable-metrics",
            ],
            &pairs(&env),
        );
        assert_eq!(output.status.code(), Some(0), "{output:?}");
    };
    let last_success = || db.value("pipeline_heartbeat", "condition_evaluator", "last_success");
    let last_update = || db.text("pipeline_heartbeat", "condition_evaluator", "updated_at");

    sweep();
    assert_eq!(last_success(), rusqlite::types::Value::Null);
    assert_eq!(
        heartbeat_details(&db),
        r#"{"evaluated": 0, "failed": 0, "no_work": 1}"#
    );
    let empty_update = last_update();

    condition(&db, 1, "synthetic", false);
    sweep();
    assert!(last_update() > empty_update);
    assert_eq!(last_success(), rusqlite::types::Value::Null);
    assert!(server.requests().is_empty());

    db.execute("UPDATE condition_definition SET enabled = 1");
    sweep();
    assert_eq!(server.requests().len(), 1);
    assert_eq!(heartbeat_details(&db), r#"{"evaluated": 1, "failed": 0}"#);
    let success = last_success();
    assert_ne!(success, rusqlite::types::Value::Null);
    let successful_update = last_update();

    db.execute("UPDATE condition_definition SET enabled = 0");
    sweep();
    assert!(last_update() > successful_update);
    assert_eq!(last_success(), success);
}

/// Unreadable stored rows (D-13, D-15): a sweep fails only those conditions
/// (stage `admission`, rows unchanged), still publishes the healthy ones and
/// writes a warning heartbeat; `--condition-id` on such a row fails with one
/// stderr line, exit 1 and no heartbeat.
#[test]
fn unreadable_conditions_fail_locally_and_healthy_siblings_publish() {
    let server = FakeServer::start();
    let db = conditions();
    db.execute("UPDATE condition_definition SET created_at = '2026-13-01 00:00:00' WHERE id = 2");
    condition(&db, 5, "e unreadable", true);
    db.execute("UPDATE condition_definition SET window_minutes = 'thirty' WHERE id = 5");
    let env = influx_env(&server);

    let output = evaluate(&db, &[], &pairs(&env));
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        text(&output.stdout),
        "Condition evaluation completed with failures (2/4).\n"
    );
    assert_eq!(server.requests(), ["POST /api/v2/query?org=o"; 2]);
    for name in ["b cpu", "d cpu"] {
        assert_eq!(
            db.text("condition_definition", name, "message"),
            "No samples available in condition window"
        );
    }
    for name in ["a cpu", "e unreadable"] {
        assert_eq!(db.text("condition_definition", name, "message"), "");
    }
    let stderr = text(&output.stderr);
    assert!(stderr.contains("condition_id=2 stage=admission"));
    assert!(stderr.contains("condition_id=5 stage=admission"));
    assert_eq!(heartbeat_details(&db), r#"{"evaluated": 4, "failed": 2}"#);
    assert_eq!(
        db.text("pipeline_heartbeat", "condition_evaluator", "status"),
        "warning"
    );

    let before = db.rows("pipeline_heartbeat");
    for id in ["2", "5"] {
        let output = evaluate(&db, &["--condition-id", id], &pairs(&env));
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(output.stdout.is_empty());
        let stderr = text(&output.stderr);
        assert!(
            stderr.starts_with("Condition evaluation failed: "),
            "{stderr}"
        );
        assert_eq!(stderr.lines().count(), 1, "{stderr}");
    }
    assert_eq!(server.requests().len(), 2);
    assert_eq!(db.rows("pipeline_heartbeat"), before);
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

#[test]
fn invalid_historical_durations_fail_before_query_without_rewriting_definitions() {
    for field in ["window_minutes", "breach_minutes"] {
        for value in [0_i64, 10081, 2_147_483_647] {
            let server = FakeServer::start();
            let db = TestDb::new();
            condition(&db, 1, "synthetic", true);
            db.raw()
                .execute(
                    &format!(
                        "UPDATE condition_definition SET {field} = ?, last_value = 17 WHERE id = 1"
                    ),
                    [value],
                )
                .unwrap();
            let env = influx_env(&server);
            let output = evaluate(&db, &[], &pairs(&env));
            assert_eq!(output.status.code(), Some(1), "{field}={value}: {output:?}");
            assert!(server.requests().is_empty(), "{field}={value}");
            assert_eq!(
                db.text("condition_definition", "synthetic", "message"),
                "evaluation error: ValueError"
            );
            let stored: (i64, i64, f64) = db
                .raw()
                .query_row(
                    &format!(
                        "SELECT {field}, enabled, last_value FROM condition_definition WHERE id = 1"
                    ),
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .unwrap();
            assert_eq!(stored, (value, 1, 17.0));
        }
    }
}
