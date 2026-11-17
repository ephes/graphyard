//! Bounded stdout/report and stderr/log workers (PLAN.md 3.6).
//! Submission never waits for a sink; a full queue drops the new record.

use std::fmt::{self, Write as _};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant};

use log::{Level, LevelFilter, Log, Metadata, Record};

/// Maximum encoded bytes per queued record, including the final newline.
pub const MAX_RECORD_BYTES: usize = 8192;
/// Per stream, plus at most one record being written by its worker.
pub const QUEUE_RECORDS: usize = 128;
/// Shared stdout/stderr drain deadline at process exit.
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(200);
const TRUNCATED: &str = "...[truncated]";

struct RecordBuffer {
    text: String,
    // Sticky: a `Display` that ignores one `fmt::Error` and keeps writing must
    // neither splice later, shorter chunks into the record nor lose the marker.
    overflowed: bool,
}

impl RecordBuffer {
    fn push(&mut self, text: &str) -> fmt::Result {
        if self.overflowed || self.text.len() + text.len() >= MAX_RECORD_BYTES {
            self.overflowed = true;
            return Err(fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}

impl fmt::Write for RecordBuffer {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        // Escape incrementally at the final projection, before charging bytes.
        // No full Display rendering or full escaped copy is ever allocated.
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for ch in text.chars() {
            match ch {
                '\u{00}'..='\u{1f}' | '\u{7f}'..='\u{9f}' => {
                    let value = ch as usize;
                    let escaped = [b'\\', b'x', HEX[value >> 4], HEX[value & 15]];
                    self.push(std::str::from_utf8(&escaped).expect("ASCII escape"))?;
                }
                '\u{2028}' => self.push("\\u{2028}")?,
                '\u{2029}' => self.push("\\u{2029}")?,
                _ => self.push(ch.encode_utf8(&mut [0; 4]))?,
            }
        }
        Ok(())
    }
}

fn format_record(args: fmt::Arguments<'_>) -> String {
    let mut buffer = RecordBuffer {
        text: String::with_capacity(MAX_RECORD_BYTES),
        overflowed: false,
    };
    // A `Display` error unrelated to the envelope also ends the record here.
    if buffer.write_fmt(args).is_err() || buffer.overflowed {
        let mut end = buffer
            .text
            .len()
            .min(MAX_RECORD_BYTES - 1 - TRUNCATED.len());
        while !buffer.text.is_char_boundary(end) {
            end -= 1;
        }
        buffer.text.truncate(end);
        buffer.text.push_str(TRUNCATED);
    }
    buffer.text.push('\n');
    buffer.text
}

struct Sink {
    // `None` once stopping: dropping the only sender lets the worker drain the
    // queue and see `Disconnected`, so an idle worker blocks without polling.
    sender: Mutex<Option<mpsc::SyncSender<String>>>,
    dropped: Arc<AtomicU64>,
    finished: Mutex<mpsc::Receiver<()>>,
}

impl Sink {
    fn new(name: &'static str, mut writer: impl std::io::Write + Send + 'static) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<String>(QUEUE_RECORDS);
        let (finished, completion) = mpsc::channel();
        let dropped = Arc::new(AtomicU64::new(0));
        let losses = dropped.clone();
        // Failure to start a worker disconnects its channel: submissions are
        // counted as lost instead of falling back to synchronous sink writes.
        let _ = std::thread::Builder::new()
            .name(format!("agent-{name}"))
            .spawn(move || {
                let mut reported = 0;
                // Blocks until a record arrives; ends after the sender is
                // dropped at shutdown and the queued records are written.
                while let Ok(line) = receiver.recv() {
                    if writer
                        .write_all(line.as_bytes())
                        .and_then(|()| writer.flush())
                        .is_err()
                    {
                        losses.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let total = losses.load(Ordering::Relaxed);
                    if total != reported {
                        // Fixed metadata, written by the sink owner, never
                        // recursively submitted to the saturated logger.
                        let notice =
                            format!("graphyard-agent output_dropped stream={name} total={total}\n");
                        if writer
                            .write_all(notice.as_bytes())
                            .and_then(|()| writer.flush())
                            .is_ok()
                        {
                            reported = total;
                        }
                    }
                }
                let _ = finished.send(());
            });
        Self {
            sender: Mutex::new(Some(sender)),
            dropped,
            finished: Mutex::new(completion),
        }
    }

    fn submit(&self, args: fmt::Arguments<'_>) {
        // Format before taking the lock; the lock only guards a nonblocking
        // `try_send`, so submission never waits for the sink.
        let record = format_record(args);
        let sent = self
            .sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|sender| sender.try_send(record).is_ok());
        if !sent {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Refuse new records and let the worker finish once the queue is empty.
    fn stop(&self) {
        self.sender
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    fn wait_until(&self, deadline: Instant) {
        let _ = self
            .finished
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv_timeout(deadline.saturating_duration_since(Instant::now()));
    }
}

struct Outputs {
    stdout: Sink,
    stderr: Sink,
}
static OUTPUTS: OnceLock<Outputs> = OnceLock::new();
fn outputs() -> &'static Outputs {
    OUTPUTS.get_or_init(|| Outputs {
        stdout: Sink::new("stdout", std::io::stdout()),
        stderr: Sink::new("stderr", std::io::stderr()),
    })
}

/// Submit a report without waiting for stdout.
pub fn stdout(args: fmt::Arguments<'_>) {
    outputs().stdout.submit(args);
}

/// Submit a diagnostic without waiting for stderr.
pub fn stderr(args: fmt::Arguments<'_>) {
    outputs().stderr.submit(args);
}

/// Cumulative losses (queue full, stopped/disconnected worker or sink error).
/// Reading these counters performs no logging or I/O.
pub fn dropped_records() -> (u64, u64) {
    let output = outputs();
    (
        output.stdout.dropped.load(Ordering::Relaxed),
        output.stderr.dropped.load(Ordering::Relaxed),
    )
}

/// Stop accepting output and allow at most 200 ms total to drain both streams.
/// A blocked worker is detached; process exit never joins it. Call once, after
/// the command/runtime has ended, including one-shot and validation failures.
pub fn shutdown() {
    if let Some(output) = OUTPUTS.get() {
        let deadline = Instant::now() + SHUTDOWN_TIMEOUT;
        output.stdout.stop();
        output.stderr.stop();
        output.stdout.wait_until(deadline);
        output.stderr.wait_until(deadline);
    }
}

struct StderrLogger;

fn level_for(target: &str) -> Level {
    if target.starts_with("graphyard") || target == "httpx" {
        Level::Info
    } else {
        Level::Warn
    }
}

impl Log for StderrLogger {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.level() <= level_for(metadata.target())
    }

    fn log(&self, record: &Record<'_>) {
        if self.enabled(record.metadata()) {
            stderr(*record.args());
        }
    }

    // Even explicit flushes must never wait for a pipe on the runtime thread.
    fn flush(&self) {}
}

static LOGGER: StderrLogger = StderrLogger;

/// Install the logger (once per process; later calls are ignored).
pub fn install() {
    let _ = outputs();
    if log::set_logger(&LOGGER).is_ok() {
        log::set_max_level(LevelFilter::Info);
        // Std's default hook writes synchronously to fd 2 before a collector's
        // `catch_unwind` sees the unwind; an unread stderr would block the
        // runtime thread there. Route panics through the bounded worker, so
        // they get the same escaping, envelope and drop-new policy.
        std::panic::set_hook(Box::new(|info| {
            let thread = std::thread::current();
            stderr(format_args!(
                "thread '{}' {info}",
                thread.name().unwrap_or("<unnamed>")
            ));
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct PausedWriter {
        started: Option<mpsc::Sender<()>>,
        release: mpsc::Receiver<()>,
        bytes: Arc<Mutex<Vec<u8>>>,
    }

    impl std::io::Write for PausedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if let Some(started) = self.started.take() {
                started.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.bytes.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn saturation_bounds_bytes_counts_losses_and_shutdown_does_not_join_sink() {
        let (started, entered) = mpsc::channel();
        let (release, paused) = mpsc::channel();
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let sink = Sink::new(
            "test",
            PausedWriter {
                started: Some(started),
                release: paused,
                bytes: bytes.clone(),
            },
        );
        let large = "é".repeat(MAX_RECORD_BYTES);
        sink.submit(format_args!("{large}"));
        entered.recv_timeout(Duration::from_secs(2)).unwrap();
        for _ in 0..QUEUE_RECORDS + 7 {
            sink.submit(format_args!("{large}"));
        }
        assert_eq!(sink.dropped.load(Ordering::Relaxed), 7);
        sink.stop();
        let start = Instant::now();
        sink.wait_until(start + Duration::from_millis(20));
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(bytes.lock().unwrap().is_empty());
        release.send(()).unwrap();
        sink.wait_until(Instant::now() + Duration::from_secs(2));
        let captured = bytes.lock().unwrap();
        let text = std::str::from_utf8(&captured).unwrap();
        assert!(text.contains("output_dropped stream=test total=7\n"));
        assert_eq!(text.lines().count(), QUEUE_RECORDS + 2);
        assert!(text.lines().all(|line| line.len() < MAX_RECORD_BYTES));
        assert!(captured.len() <= (QUEUE_RECORDS + 1) * MAX_RECORD_BYTES + 100);
    }

    struct IgnoresErrors;
    impl fmt::Display for IgnoresErrors {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let _ = f.write_str(&"a".repeat(MAX_RECORD_BYTES - 3));
            // Rejected near the limit; the shorter chunk after it must not land.
            let _ = f.write_str("\x1b");
            let _ = f.write_str("b");
            Ok(())
        }
    }

    #[test]
    fn overflow_is_sticky_and_marked_even_when_display_ignores_errors() {
        let record = format_record(format_args!("{IgnoresErrors}"));
        assert!(record.ends_with("...[truncated]\n"), "{record:?}");
        assert!(!record.contains('b'));
        assert!(record.len() <= MAX_RECORD_BYTES);
    }

    #[test]
    fn graphyard_targets_log_info_others_only_warnings() {
        let info = |target| {
            StderrLogger.enabled(
                &Metadata::builder()
                    .level(Level::Info)
                    .target(target)
                    .build(),
            )
        };
        let warn = |target| {
            StderrLogger.enabled(
                &Metadata::builder()
                    .level(Level::Warn)
                    .target(target)
                    .build(),
            )
        };
        assert!(info("graphyard_collect::http::client"));
        assert!(info("graphyard_agent"));
        assert!(!info("reqwest::connect"));
        assert!(warn("rustls::conn"));
        assert!(
            !StderrLogger.enabled(
                &Metadata::builder()
                    .level(Level::Debug)
                    .target("graphyard_store")
                    .build()
            )
        );
    }
}
