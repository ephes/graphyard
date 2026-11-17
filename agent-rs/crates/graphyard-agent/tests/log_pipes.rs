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

const PANIC_CHILD: &str = "GRAPHYARD_TEST_PANIC_CHILD";

// Fresh process with the production hook: a caught panic (as a collector's
// `catch_unwind` sees it) must not write synchronously to stderr.
#[test]
fn panic_child() {
    if std::env::var_os(PANIC_CHILD).is_none() {
        return;
    }
    graphyard_agent::logging::install();
    let caught = std::panic::catch_unwind(|| panic!("synthetic\x1b[2J\nforged"));
    assert!(caught.is_err());
    graphyard_agent::logging::shutdown();
}

const UNCAUGHT_CHILD: &str = "GRAPHYARD_TEST_UNCAUGHT_PANIC_CHILD";

// The production binary's main thread: an uncaught panic leaves `main_entry`.
#[test]
fn uncaught_panic_child() {
    let Some(ready) = std::env::var_os(UNCAUGHT_CHILD) else {
        return;
    };
    let unwound = std::panic::catch_unwind(|| {
        graphyard_agent::app::run_with_output(move || {
            std::fs::write(&ready, b"").unwrap();
            panic!("synthetic fatal\x1b")
        })
    });
    assert!(unwound.is_err(), "the uncaught panic must keep unwinding");
    // Exit at once, as the binary does when the panic leaves `main`: only
    // output drained by `run_with_output` itself reaches the parent.
    std::process::exit(101);
}

#[test]
fn uncaught_panic_message_is_drained_before_exit() {
    use std::io::Read as _;
    use std::sync::atomic::AtomicBool;

    // Prefill stderr so the panic record blocks in the worker; release the
    // pipe only after the child is about to panic. Without the exit drain the
    // child would leave `main` while its worker is still blocked.
    let (mut reader, mut writer) = pipe().unwrap();
    let child_writer = writer.try_clone().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicUsize::new(0));
    let (halt, progress) = (stop.clone(), written.clone());
    let filler = std::thread::spawn(move || {
        while !halt.load(Ordering::Relaxed) && writer.write_all(&[b'x'; 1024]).is_ok() {
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
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "uncaught_panic_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(UNCAUGHT_CHILD, &ready)
        .stdout(Stdio::null())
        .stderr(Stdio::from(child_writer))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    stop.store(true, Ordering::Relaxed);
    let mut captured = Vec::new();
    reader.read_to_end(&mut captured).unwrap();
    filler.join().unwrap();
    let status = child.wait().unwrap();
    assert_eq!(status.code(), Some(101), "{status:?}");
    let text = String::from_utf8(captured).unwrap();
    let panics: Vec<_> = text.lines().filter(|l| l.contains("panicked at")).collect();
    assert_eq!(panics.len(), 1, "panic message lost at exit");
    assert!(panics[0].ends_with("synthetic fatal\\x1b"));
}

fn run_panic_child(stderr: Stdio) -> std::process::Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "panic_child", "--nocapture", "--test-threads=1"])
        .env(PANIC_CHILD, "1")
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .unwrap()
}

#[test]
fn caught_panic_is_one_escaped_record() {
    let output = run_panic_child(Stdio::piped()).wait_with_output().unwrap();
    assert!(output.status.success(), "{output:?}");
    let text = String::from_utf8(output.stderr).unwrap();
    let panics: Vec<_> = text.lines().filter(|l| l.contains("panicked at")).collect();
    assert_eq!(panics.len(), 1, "{text:?}");
    assert!(
        panics[0].contains("synthetic\\x1b[2J\\x0aforged"),
        "{text:?}"
    );
    assert!(
        !text.contains("\nforged") && !text.contains('\x1b'),
        "{text:?}"
    );
}

#[test]
fn caught_panic_does_not_block_on_unread_stderr() {
    let (reader, writer, filler) = full_pipe();
    let mut child = run_panic_child(Stdio::from(writer));
    let deadline = Instant::now() + Duration::from_secs(5);
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
    drop(reader);
    filler.join().unwrap();
    assert!(
        status.is_some_and(|s| s.success()),
        "panic blocked on stderr: {status:?}"
    );
}
