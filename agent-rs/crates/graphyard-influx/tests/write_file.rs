//! `--influx-write-file` (PLAN.md 3.1): the shadow writer appends every
//! batch to a file, counts it as written and never sends a write request.

use std::net::TcpListener;

use chrono::{DateTime, Utc};
use graphyard_core::MetricPoint;
use graphyard_core::settings::Settings;
use graphyard_influx::{InfluxWriter, WriteError};
use influxdb_line_protocol::parse_lines;

fn settings(env: &[(&str, &str)]) -> Settings {
    Settings::from_lookup(|key| {
        env.iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| (*value).to_owned())
    })
    .expect("settings")
}

fn point(subject_id: &str, value: f64) -> MetricPoint {
    let ts: DateTime<Utc> = "2026-10-21T08:15:30.123456Z".parse().unwrap();
    MetricPoint::new(
        ts,
        "host.cpu_usage",
        value,
        "host",
        subject_id,
        "http",
        "graphyard",
        "macmini",
    )
}

/// A loopback port that accepts connections but is never read: a write
/// request would have to connect to it.
fn untouched_port() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    (listener, url)
}

#[tokio::test]
async fn batches_are_appended_and_counted_without_a_request() {
    let (listener, url) = untouched_port();
    let settings = settings(&[
        ("INFLUX_URL", &url),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
        ("INFLUX_API_MODE", "v2"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    std::fs::write(&path, "# earlier content\n").unwrap();
    let writer = InfluxWriter::to_file(&path).unwrap();

    // The invalid point is logged and skipped as in a real write.
    let invalid = point("", 1.0);
    let written = writer
        .write_points(
            &settings,
            &[point("macmini", 0.5), invalid, point("fractal", 2.0)],
        )
        .await
        .unwrap();
    assert_eq!(written, 2);
    assert_eq!(
        writer
            .write_points(&settings, &[point("macmini", 0.25)])
            .await
            .unwrap(),
        1
    );
    // Nothing valid: no record at all.
    assert_eq!(writer.write_points(&settings, &[]).await.unwrap(), 0);

    let text = std::fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 8, "{text}");
    assert_eq!(lines[0], "# earlier content");
    for (comment, points) in [(lines[1], 2), (lines[5], 1)] {
        let rest = comment.strip_prefix("# write ").expect(comment);
        let (stamp, count) = rest.split_once(' ').unwrap();
        assert!(stamp.parse::<DateTime<Utc>>().is_ok(), "{stamp}");
        assert!(stamp.ends_with('Z'), "{stamp}");
        assert!(count.starts_with(&format!("points={points} bytes=")));
    }
    let body = [lines[2], lines[3], lines[6]].join("\n");
    let parsed: Vec<_> = parse_lines(&body).collect::<Result<_, _>>().unwrap();
    assert_eq!(parsed.len(), 3);
    assert!(lines[2].starts_with("graphyard_metrics,"), "{}", lines[2]);
    assert!(lines[2].contains("subject_id=macmini"));
    assert!(lines[3].contains("subject_id=fractal"));
    assert!(lines[2].ends_with(" value=0.5 1792570530123456000"));

    // No connection ever reached the InfluxDB address.
    assert!(listener.accept().is_err());
}

#[tokio::test]
async fn configuration_errors_still_fail_the_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    let writer = InfluxWriter::to_file(&path).unwrap();

    let missing = settings(&[("INFLUX_API_MODE", "v2")]);
    let err = writer
        .write_points(&missing, &[point("macmini", 1.0)])
        .await
        .unwrap_err();
    assert!(matches!(err, WriteError::Configuration(_)), "{err:?}");
    assert_eq!(
        err.to_string(),
        "Influx setting missing: INFLUX_URL is required"
    );

    let negative = settings(&[
        ("INFLUX_URL", "http://127.0.0.1:9"),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
        ("INFLUX_TIMEOUT_MS", "-1"),
    ]);
    let err = writer
        .write_points(&negative, &[point("macmini", 1.0)])
        .await
        .unwrap_err();
    assert_eq!(err.python_class(), "ValueError");

    let scheme = settings(&[
        ("INFLUX_URL", "ftp://127.0.0.1"),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
    ]);
    let err = writer
        .write_points(&scheme, &[point("macmini", 1.0)])
        .await
        .unwrap_err();
    assert_eq!(err.python_class(), "URLSchemeUnknown");

    // A token the HTTP stack refuses fails both writers the same way.
    let (_listener, url) = untouched_port();
    let token = settings(&[
        ("INFLUX_URL", &url),
        ("INFLUX_TOKEN", "t\nX-Injected: 1"),
        ("INFLUX_ORG", "o"),
    ]);
    let shadow = writer
        .write_points(&token, &[point("macmini", 1.0)])
        .await
        .unwrap_err();
    let real = InfluxWriter::new()
        .unwrap()
        .write_points(&token, &[point("macmini", 1.0)])
        .await
        .unwrap_err();
    assert_eq!(shadow.python_class(), "ValueError");
    assert_eq!(real.python_class(), "ValueError");
    assert_eq!(shadow.to_string(), real.to_string());

    assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
}

#[test]
fn an_unwritable_path_fails_to_open() {
    let dir = tempfile::tempdir().unwrap();
    assert!(InfluxWriter::to_file(&dir.path().join("missing/writes.lp")).is_err());
}

#[tokio::test]
async fn oversized_batch_keeps_shadow_file_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    std::fs::write(&path, "# earlier content\n").unwrap();
    let writer = InfluxWriter::to_file(&path).unwrap();
    let mut p = point("synthetic", 1.0);
    p.source_entity_id = Some("x".repeat(256));
    let result = writer.write_points(&settings(&[]), &vec![p; 25000]).await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("request byte limit")
    );
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "# earlier content\n"
    );
}

#[tokio::test]
async fn auto_timestamp_rejection_precedes_shadow_append() {
    let (listener, url) = untouched_port();
    let settings = settings(&[
        ("INFLUX_URL", &url),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
    ]);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    let writer = InfluxWriter::to_file(&path).unwrap();
    let mut invalid = point("synthetic", 1.0);
    invalid.ts = "2262-04-11T23:40:00Z".parse().unwrap();
    assert_eq!(
        writer
            .write_points(&settings, &[invalid.clone()])
            .await
            .unwrap(),
        0
    );
    assert!(std::fs::read(&path).unwrap().is_empty());
    let mut valid = invalid.clone();
    valid.ts = "2262-04-11T23:39:59.999999Z".parse().unwrap();
    assert_eq!(
        writer
            .write_points(&settings, &[invalid, valid])
            .await
            .unwrap(),
        1
    );
    let text = std::fs::read_to_string(&path).unwrap();
    assert_eq!(text.lines().count(), 3);
    assert!(text.contains(" 9223371599999999000\n# commit points=1 bytes="));
    assert!(listener.accept().is_err());
}

#[cfg(unix)]
#[test]
fn r102_shadow_open_rejects_links_and_makes_file_private() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("outside");
    std::fs::write(&target, "sentinel").unwrap();
    let link = dir.path().join("link");
    symlink(&target, &link).unwrap();
    assert!(
        InfluxWriter::to_file(&link).is_err(),
        "final symlink must be refused"
    );
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "sentinel");
    let path = dir.path().join("private");
    InfluxWriter::to_file(&path).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();
    InfluxWriter::to_file(&path).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[tokio::test]
async fn r102_partial_batch_refuses_reuse_without_changing_prior_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    let config = settings(&[
        ("INFLUX_URL", "http://127.0.0.1:9"),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
    ]);
    let writer = InfluxWriter::to_file(&path).unwrap();
    writer
        .write_points(&config, &[point("synthetic", 1.0)])
        .await
        .unwrap();
    let complete = std::fs::read(&path).unwrap();
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"# write 2026-10-21T08:15:30Z points=2 bytes=100\npartial\n")
        .unwrap();
    let partial = std::fs::read(&path).unwrap();
    let reopened = InfluxWriter::to_file(&path).unwrap();
    let result = reopened
        .write_points(&config, &[point("synthetic", 2.0)])
        .await;
    assert!(
        result.is_err(),
        "incomplete evidence must not be appended to"
    );
    assert_eq!(std::fs::read(&path).unwrap(), partial);
    assert!(partial.starts_with(&complete));
}

#[tokio::test]
async fn r102_shadow_child() {
    let Ok(path) = std::env::var("GRAPHYARD_TEST_SHADOW_PATH") else {
        return;
    };
    if std::env::var_os("GRAPHYARD_TEST_HOLD_LOCK").is_some() {
        use std::io::Write;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .unwrap();
        file.lock().unwrap();
        println!("locked");
        std::io::stdout().flush().unwrap();
        std::thread::sleep(std::time::Duration::from_secs(30));
        return;
    }
    let writer = InfluxWriter::to_file(std::path::Path::new(&path)).unwrap();
    let config = settings(&[
        ("INFLUX_URL", "http://127.0.0.1:9"),
        ("INFLUX_TOKEN", "t"),
        ("INFLUX_ORG", "o"),
    ]);
    for _ in 0..20 {
        writer
            .write_points(&config, &vec![point("synthetic", 1.0); 100])
            .await
            .unwrap();
    }
}

#[test]
fn r102_cross_process_shadow_lock_has_a_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    let file = std::fs::File::create(&path).unwrap();
    file.lock().unwrap();
    let result = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "r102_shadow_child", "--nocapture"])
        .env("GRAPHYARD_TEST_SHADOW_PATH", &path)
        .output()
        .unwrap();
    assert!(
        !result.status.success(),
        "another process must not write through a held lock"
    );
    assert!(std::fs::read(&path).unwrap().is_empty());
    file.unlock().unwrap();
}

#[test]
fn r102_two_processes_append_whole_batches_and_death_releases_lock() {
    use std::io::BufRead;
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("writes.lp");
    let executable = std::env::current_exe().unwrap();
    let spawn = || {
        Command::new(&executable)
            .args(["--exact", "r102_shadow_child", "--nocapture"])
            .env("GRAPHYARD_TEST_SHADOW_PATH", &path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut holder = Command::new(&executable)
        .args(["--exact", "r102_shadow_child", "--nocapture"])
        .env("GRAPHYARD_TEST_SHADOW_PATH", &path)
        .env("GRAPHYARD_TEST_HOLD_LOCK", "1")
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let output = holder.stdout.take().unwrap();
    let mut ready = false;
    for line in std::io::BufReader::new(output).lines() {
        if line.unwrap() == "locked" {
            ready = true;
            break;
        }
    }
    assert!(ready);
    holder.kill().unwrap();
    holder.wait().unwrap();
    let first = spawn();
    let second = spawn();
    for child in [first, second] {
        let result = child.wait_with_output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines = text.lines();
    for _ in 0..40 {
        let header = lines.next().unwrap();
        assert!(header.starts_with("# write ") && header.contains(" points=100 bytes="));
        let expected_bytes: usize = header.split(" bytes=").nth(1).unwrap().parse().unwrap();
        let mut bytes = 0;
        for _ in 0..100 {
            let point = lines.next().unwrap();
            assert!(point.starts_with("graphyard_metrics,"));
            bytes += point.len() + 1;
        }
        assert_eq!(bytes, expected_bytes);
        assert_eq!(
            lines.next().unwrap(),
            format!("# commit points=100 bytes={bytes}")
        );
    }
    assert!(lines.next().is_none());
}

#[cfg(unix)]
#[test]
fn r102_shadow_fifo_open_never_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fifo");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&path)
            .status()
            .unwrap()
            .success()
    );
    assert!(InfluxWriter::to_file(&path).is_err());
}
