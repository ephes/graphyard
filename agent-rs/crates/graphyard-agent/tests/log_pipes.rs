//! Production binary, current-thread runtime and genuinely unread OS pipes.
#![cfg(unix)]
mod common;

use std::io::{PipeReader, PipeWriter, Write as _, pipe};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::{FakeServer, TestDb};
use serde_json::json;

// Fill a pipe before the agent starts, so neither a large OS buffer nor the
// scheduler's one-second minimum interval can hide synchronous output.
fn full_pipe() -> (PipeReader, PipeWriter, std::thread::JoinHandle<()>) {
    let (reader, mut writer) = pipe().unwrap();
    let child_writer = writer.try_clone().unwrap();
    let written = Arc::new(AtomicUsize::new(0));
    let progress = written.clone();
    let filler = std::thread::spawn(move || {
        while writer.write_all(&[b'x'; 1024]).is_ok() {
            progress.fetch_add(1024, Ordering::Relaxed);
        }
    });
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let before = written.load(Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(100));
        if before > 0 && before == written.load(Ordering::Relaxed) {
            break;
        }
        assert!(Instant::now() < deadline, "pipe never filled");
    }
    (reader, child_writer, filler)
}

fn unread_pipe_does_not_stop_agent(stderr: bool) {
    let db = TestDb::new();
    let server = FakeServer::start();
    common::insert_spec(
        &db,
        "synthetic",
        "http_json_metric",
        &json!({"url": server.url("/value"), "metric_path": "$.value", "metric_name": "test.value", "service_id": "synthetic"}),
    );
    db.execute("UPDATE metric_collection_spec SET interval_seconds = 1");
    let (reader, writer, filler) = full_pipe();
    let mut command = Command::new(env!("CARGO_BIN_EXE_graphyard-agent"));
    command
        .env_clear()
        .envs([
            ("INFLUX_URL", "http://127.0.0.1:1"),
            ("INFLUX_TOKEN", "synthetic"),
            ("INFLUX_ORG", "synthetic"),
        ])
        .args([
            "--db",
            db.path().to_str().unwrap(),
            "--disable-conditions",
            "--metrics-interval",
            "1",
            "--influx-write-file",
            db.dir.path().join("writes.lp").to_str().unwrap(),
        ]);
    if stderr {
        command.stderr(Stdio::from(writer)).stdout(Stdio::null());
    } else {
        command.stdout(Stdio::from(writer)).stderr(Stdio::null());
    }
    let mut child = command.spawn().unwrap();
    drop(command);
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut advanced = false;
    while Instant::now() < deadline {
        // Two actual collector requests plus a committed healthy heartbeat
        // prove that the runtime continued after the first blocked report.
        if server.requests().len() >= 2 && db.count("pipeline_heartbeat") == 1 {
            advanced = db.text("pipeline_heartbeat", "metric_collectors", "status") == "ok";
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let signalled = Instant::now();
    let signal = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break Some(status);
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            break None;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    // Release the filler even on the regression path; never leave a child or
    // a test thread stuck on the intentionally blocked sink.
    drop(reader);
    filler.join().unwrap();
    assert!(
        advanced,
        "collector/heartbeat stalled on unread pipe (stderr={stderr}): requests={:?}, rows={:?}, specs={:?}",
        server.requests(),
        db.rows("pipeline_heartbeat"),
        db.rows("metric_collection_spec")
    );
    assert!(signal.success());
    assert_eq!(status.and_then(|s| s.code()), Some(0), "SIGTERM stalled");
    assert!(signalled.elapsed() < Duration::from_secs(2));
}

#[test]
fn unread_stdout_keeps_collection_heartbeat_and_sigterm_live() {
    unread_pipe_does_not_stop_agent(false);
}

#[test]
fn unread_stderr_keeps_collection_heartbeat_and_sigterm_live() {
    unread_pipe_does_not_stop_agent(true);
}
