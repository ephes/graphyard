//! Final production stderr projection, independent of semantic message values.
mod common;

use std::fmt;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

use common::{FakeServer, TestDb};
use graphyard_agent::logging::{self, MAX_RECORD_BYTES};

const CONTROLS: &str = "synthetic\x1b[2J\r\nforged\t\0\x7f\u{85}\u{9b}\u{2028}\u{2029}";
const CHILD: &str = "GRAPHYARD_TEST_LOG_RECORD_CHILD";
static VISITED: AtomicUsize = AtomicUsize::new(0);

struct LargeDisplay;
impl fmt::Display for LargeDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for _ in 0..10_000_000 {
            VISITED.fetch_add(1, Ordering::Relaxed);
            f.write_str("雪")?;
        }
        Ok(())
    }
}

// Run in a fresh process: install exactly the production log subscriber and
// capture the OS stderr stream, including the direct command diagnostic path.
#[test]
fn stderr_projection_child() {
    if std::env::var_os(CHILD).is_none() {
        return;
    }
    logging::install();
    log::warn!(target: "graphyard_influx", "condition={CONTROLS}");
    log::warn!(target: "graphyard_agent", "large={LargeDisplay}");
    graphyard_agent::app::print_stderr(CONTROLS);
    logging::shutdown();
    assert!(VISITED.load(Ordering::Relaxed) < MAX_RECORD_BYTES);
}

fn assert_safe_records(bytes: &[u8]) -> Vec<&str> {
    let text = std::str::from_utf8(bytes).unwrap();
    assert!(text.ends_with('\n'));
    let lines: Vec<_> = text.split_terminator('\n').collect();
    for line in &lines {
        assert!(
            line.len() < MAX_RECORD_BYTES,
            "record exceeded byte envelope"
        );
        assert!(
            !line
                .chars()
                .any(|ch| ch.is_control() || matches!(ch, '\u{2028}' | '\u{2029}')),
            "active control in stderr: {line:?}"
        );
    }
    lines
}

#[test]
fn subscriber_escapes_controls_and_streams_large_display_with_finite_utf8_bytes() {
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "stderr_projection_child", "--nocapture"])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let lines = assert_safe_records(&output.stderr);
    assert_eq!(lines.len(), 3, "one physical line per event");
    assert!(lines[0].starts_with(
        "condition=synthetic\\x1b[2J\\x0d\\x0aforged\\x09\\x00\\x7f\\x85\\x9b\\u{2028}\\u{2029}"
    ));
    assert!(lines[1].starts_with("large=雪"));
    assert!(lines[1].ends_with("...[truncated]"));
    assert_eq!(lines[2], lines[0].strip_prefix("condition=").unwrap());
}

#[test]
fn actual_agent_projects_condition_name_without_changing_stored_semantics() {
    let db = TestDb::new();
    let server = FakeServer::start();
    let name = format!("{CONTROLS}{}", "雪".repeat(10_000));
    db.raw()
        .execute(
            "INSERT INTO condition_definition (name, enabled, metric_name, host_filter,
         service_filter, tags_filter, operator, warning_threshold, critical_threshold,
         window_minutes, breach_minutes, status, message, created_at, updated_at,
         subject_id_filter, subject_type_filter, alert_when_stale) VALUES (?, 1,
         'service.test', 'synthetic', '', '{}', 'gte', 5, 10, 30, 5, 'ok', '',
         '2026-01-01 00:00:00', '2026-01-01 00:00:00', '', 'service', 1)",
            [&name],
        )
        .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_graphyard-agent"))
        .env_clear()
        .envs([
            ("INFLUX_URL", server.url("").as_str()),
            ("INFLUX_TOKEN", "synthetic"),
            ("INFLUX_ORG", "synthetic"),
            ("INFLUX_API_MODE", "v2"),
        ])
        .args([
            "--run-once",
            "--disable-metrics",
            "--db",
            db.path().to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    let lines = assert_safe_records(&output.stderr);
    let warnings: Vec<_> = lines
        .iter()
        .filter(|line| line.starts_with("Ignoring host_filter"))
        .collect();
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("synthetic\\x1b[2J\\r\\nforged\\x09\\x00\\x7f\\x85\\x9b"));
    assert!(warnings[0].ends_with("...[truncated]"));
    assert_eq!(db.text("condition_definition", &name, "name"), name);
    assert_eq!(
        db.text("condition_definition", &name, "message"),
        "No samples available in condition window"
    );
}
