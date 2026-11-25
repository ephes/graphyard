//! Complete shadow batches under a cooperating local-filesystem lock.
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use graphyard_core::line_protocol::WRITE_MAX_REQUEST_BYTES;
use tokio::sync::Mutex;

// Per validation invocation, including every header, point, trailer and comment.
#[derive(Clone, Copy, Debug)]
struct ValidationLimits {
    bytes: u64,
    lines: usize,
    elapsed: Duration,
}

const VALIDATION_LIMITS: ValidationLimits = ValidationLimits {
    bytes: 64 * 1024 * 1024,
    lines: 250_000,
    elapsed: Duration::from_secs(5),
};

#[derive(Debug)]
pub(crate) struct ShadowFile {
    pub file: File,
    validated_end: u64,
    validation_limits: ValidationLimits,
}

impl ShadowFile {
    /// Validate the existing capture once, before collectors start, so the
    /// first append of a process only scans what cooperating writers added
    /// since. Invalid, over-budget or locked captures remain nonqualified at
    /// offset 0; append must still validate them and cannot treat them as empty.
    pub fn new(file: File) -> Self {
        Self::with_limits(file, VALIDATION_LIMITS)
    }

    fn with_limits(file: File, validation_limits: ValidationLimits) -> Self {
        let validated_end = validated_at_open(&file, validation_limits).unwrap_or(0);
        Self {
            file,
            validated_end,
            validation_limits,
        }
    }
}

fn validated_at_open(file: &File, limits: ValidationLimits) -> Option<u64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match file.try_lock() {
            Ok(()) => break,
            Err(std::fs::TryLockError::WouldBlock) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(_) => return None,
        }
    }
    let _locked = Locked(file);
    validate(file, 0, limits).ok()
}

// The descriptor stays open; never unlink/replace the inode while writers run.
struct Locked<'a>(&'a File);
impl Drop for Locked<'_> {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

fn incomplete(offset: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "incomplete shadow batch; last complete offset={offset}; preserve file and recover explicitly"
        ),
    )
}

fn bounded_line(reader: &mut impl BufRead, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_until(b'\n', &mut bytes)?;
    if bytes.len() > limit {
        return Err(io::Error::other("shadow line byte limit"));
    }
    Ok(bytes)
}

fn validation_budget(kind: &str, complete: u64) -> io::Error {
    io::Error::other(format!(
        "shadow validation {kind} budget exceeded; last complete offset={complete}; preserve file and rotate or qualify offline"
    ))
}

struct ValidationWork {
    limits: ValidationLimits,
    deadline: Instant,
    bytes: u64,
    lines: usize,
}

impl ValidationWork {
    fn check_time(&self, complete: u64) -> io::Result<()> {
        if Instant::now() >= self.deadline {
            return Err(validation_budget("time", complete));
        }
        Ok(())
    }

    fn line(
        &mut self,
        reader: &mut impl BufRead,
        limit: usize,
        complete: u64,
    ) -> io::Result<Vec<u8>> {
        self.check_time(complete)?;
        if self.lines >= self.limits.lines {
            return Err(validation_budget("lines", complete));
        }
        let bytes = bounded_line(reader, limit)?;
        self.bytes += bytes.len() as u64;
        self.lines += 1;
        self.check_time(complete)?;
        Ok(bytes)
    }
}

fn validate(file: &File, start: u64, limits: ValidationLimits) -> io::Result<u64> {
    validate_with(file, start, limits, file_len)
}

fn file_len(file: &File) -> io::Result<u64> {
    Ok(file.metadata()?.len())
}

/// Validate only the suffix added since this descriptor last admitted a batch.
/// Legacy headers are checked by point count; v2 headers require a commit trailer.
fn validate_with(
    file: &File,
    start: u64,
    limits: ValidationLimits,
    read_len: impl FnOnce(&File) -> io::Result<u64>,
) -> io::Result<u64> {
    let mut work = ValidationWork {
        limits,
        deadline: Instant::now() + limits.elapsed,
        bytes: 0,
        lines: 0,
    };
    work.check_time(start)?;
    let end = read_len(file)?;
    if end < start {
        return Err(incomplete(start));
    }
    let suffix_bytes = end - start;
    if suffix_bytes > limits.bytes {
        return Err(validation_budget("bytes", start));
    }
    let mut source = file;
    source.seek(SeekFrom::Start(start))?;
    // Admit metadata before reading and never read beyond this locked snapshot.
    let mut input = BufReader::new(source.take(suffix_bytes));
    let mut complete = start;
    while work.bytes < suffix_bytes {
        let header = work.line(&mut input, 4096, complete)?;
        if !header.ends_with(b"\n") || header.starts_with(b"# commit ") {
            return Err(incomplete(complete));
        }
        if !header.starts_with(b"# write ") {
            if header.starts_with(b"#") || header == b"\n" {
                complete = start + work.bytes;
                continue;
            }
            return Err(incomplete(complete));
        }
        let text = std::str::from_utf8(&header).map_err(|_| incomplete(complete))?;
        let fields: Vec<_> = text.split_whitespace().collect();
        if !(4..=5).contains(&fields.len())
            || fields[2].parse::<chrono::DateTime<chrono::Utc>>().is_err()
        {
            return Err(incomplete(complete));
        }
        let points = fields[3]
            .strip_prefix("points=")
            .and_then(|n| n.parse::<usize>().ok())
            .filter(|n| *n > 0 && *n <= WRITE_MAX_REQUEST_BYTES)
            .ok_or_else(|| incomplete(complete))?;
        let expected_bytes = if fields.len() == 5 {
            Some(
                fields[4]
                    .strip_prefix("bytes=")
                    .and_then(|n| n.parse::<usize>().ok())
                    .filter(|n| *n <= WRITE_MAX_REQUEST_BYTES + 1)
                    .ok_or_else(|| incomplete(complete))?,
            )
        } else {
            None
        };
        // A v2 row cut off at the declared body boundary is incomplete.
        // Legacy bodies keep the existing frame cap enforced by work.line.
        let mut body = (&mut input).take(expected_bytes.map_or(u64::MAX, |bytes| bytes as u64));
        let mut bytes = 0usize;
        for _ in 0..points {
            let row = work.line(
                &mut body,
                (WRITE_MAX_REQUEST_BYTES + 1).saturating_sub(bytes),
                complete,
            )?;
            if row.len() <= 1 || row.starts_with(b"#") || !row.ends_with(b"\n") {
                return Err(incomplete(complete));
            }
            bytes += row.len();
        }
        if let Some(expected) = expected_bytes {
            if bytes != expected {
                return Err(incomplete(complete));
            }
            let trailer = work.line(&mut input, 4096, complete)?;
            if trailer != format!("# commit points={points} bytes={expected}\n").as_bytes() {
                return Err(incomplete(complete));
            }
        }
        complete = start + work.bytes;
    }
    work.check_time(complete)?;
    Ok(complete)
}

fn admit_record(record: &[u8], limits: ValidationLimits) -> io::Result<()> {
    if record.len() as u64 > limits.bytes {
        return Err(record_budget("bytes"));
    }
    let lines = record.iter().filter(|&&byte| byte == b'\n').count()
        + usize::from(!record.is_empty() && !record.ends_with(b"\n"));
    if lines > limits.lines {
        return Err(record_budget("lines"));
    }
    Ok(())
}

fn record_budget(kind: &str) -> io::Error {
    io::Error::other(format!(
        "shadow record exceeds validation {kind} budget; refused before write"
    ))
}

pub(crate) async fn append(file: &Arc<Mutex<ShadowFile>>, record: &[u8]) -> io::Result<()> {
    append_with(file, record, write_and_sync, file_len).await
}

fn write_and_sync(file: &File, record: &[u8]) -> io::Result<()> {
    let mut output = file;
    output.write_all(record)?;
    output.flush()?;
    file.sync_data()
}

async fn append_with(
    file: &Arc<Mutex<ShadowFile>>,
    record: &[u8],
    write: impl FnOnce(&File, &[u8]) -> io::Result<()> + Send + 'static,
    read_len: impl Fn(&File) -> io::Result<u64> + Send + 'static,
) -> io::Result<()> {
    // One running/queued blocking job per writer. The job owns this guard,
    // descriptor and record even if the async caller stops waiting.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut state = tokio::time::timeout_at(deadline, file.clone().lock_owned())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shadow lock deadline"))?;
    let record = record.to_vec();
    tokio::task::spawn_blocking(move || {
        // A record no cooperating scan could validate is refused before writing.
        admit_record(&record, state.validation_limits)?;
        // Pool queueing, in-process admission and OS-lock wait share a deadline.
        let deadline = deadline.into_std();
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "shadow lock deadline",
                ));
            }
            match state.file.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(std::fs::TryLockError::Error(err)) => return Err(err),
            }
        }
        let ShadowFile {
            file,
            validated_end,
            validation_limits,
        } = &mut *state;
        let _locked = Locked(file);
        *validated_end = validate_with(file, *validated_end, *validation_limits, &read_len)?;
        let record_end = validated_end
            .checked_add(record.len() as u64)
            .ok_or_else(|| io::Error::other("shadow append offset overflow"))?;
        write(file, &record)?;
        *validated_end = record_end;
        Ok(())
    })
    .await
    .map_err(|_| io::Error::other("shadow blocking job failed"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(value: usize) -> String {
        let body = format!("m,host=synthetic value={value} 123\n");
        format!(
            "# write 2026-10-21T08:15:30Z points=1 bytes={}\n{body}# commit points=1 bytes={}\n",
            body.len(),
            body.len()
        )
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_io_does_not_stop_runtime_heartbeat() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let state = Arc::new(Mutex::new(ShadowFile::new(tempfile::tempfile().unwrap())));
        let beats = Arc::new(AtomicUsize::new(0));
        let heartbeat = tokio::spawn({
            let beats = beats.clone();
            async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    beats.fetch_add(1, Ordering::SeqCst);
                }
            }
        });
        let (entered, ready) = std::sync::mpsc::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let observer = std::thread::spawn(move || {
            ready.recv_timeout(Duration::from_secs(3)).unwrap();
            std::thread::sleep(Duration::from_millis(150));
            let observed = beats.load(Ordering::SeqCst);
            release.send(()).unwrap();
            observed
        });
        append_with(
            &state,
            record(1).as_bytes(),
            move |file, record| {
                entered.send(()).unwrap();
                gate.recv_timeout(Duration::from_secs(3)).unwrap();
                write_and_sync(file, record)
            },
            file_len,
        )
        .await
        .unwrap();
        heartbeat.abort();
        assert!(
            observer.join().unwrap() > 0,
            "disk I/O blocked the runtime heartbeat"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_waiter_retains_job_capacity_and_os_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("capture.lp");
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&path)
            .unwrap();
        let state = Arc::new(Mutex::new(ShadowFile::new(file)));
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let waiter = tokio::spawn({
            let state = state.clone();
            async move {
                append_with(
                    &state,
                    record(1).as_bytes(),
                    move |file, record| {
                        entered.send(()).unwrap();
                        gate.recv_timeout(Duration::from_secs(3)).unwrap();
                        write_and_sync(file, record)
                    },
                    file_len,
                )
                .await
            }
        });
        ready.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(
            state.try_lock().is_err(),
            "cancellation freed the running job slot"
        );
        let other = File::open(&path).unwrap();
        assert!(matches!(
            other.try_lock(),
            Err(std::fs::TryLockError::WouldBlock)
        ));
        assert_eq!(
            append(&state, record(2).as_bytes())
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        assert!(std::fs::read(&path).unwrap().is_empty());
        release.send(()).unwrap();
        append(&state, record(3).as_bytes()).await.unwrap();
        let text = std::fs::read_to_string(path).unwrap();
        assert_eq!(text, format!("{}{}", record(1), record(3)));
        assert_eq!(state.lock().await.validated_end, text.len() as u64);
    }

    fn tiny_limits(bytes: u64, lines: usize) -> ValidationLimits {
        ValidationLimits {
            bytes,
            lines,
            elapsed: Duration::from_secs(5),
        }
    }

    #[tokio::test]
    async fn failed_write_retains_admitted_peer_suffix_and_rechecks_partial_record() {
        let capture = tempfile::NamedTempFile::new().unwrap();
        let open_writer = || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .open(capture.path())
                .unwrap();
            Arc::new(Mutex::new(ShadowFile::with_limits(
                file,
                tiny_limits(100_000, 30),
            )))
        };
        let state = open_writer();
        let peer = open_writer();
        let peer_suffix = record(1).repeat(10);
        append(&peer, peer_suffix.as_bytes()).await.unwrap();

        let next = record(2);
        let partial = next.find("# commit").unwrap();
        let failure = append_with(
            &state,
            next.as_bytes(),
            move |mut file, record| {
                file.write_all(&record[..partial])?;
                Err(io::Error::other("injected write failure"))
            },
            file_len,
        )
        .await
        .unwrap_err();
        assert_eq!(failure.to_string(), "injected write failure");

        // Rescanning the peer suffix would exhaust the line budget before the tail.
        let retry = append(&state, next.as_bytes()).await.unwrap_err();
        assert_eq!(retry.kind(), io::ErrorKind::InvalidData, "{retry}");
        let peer_end = peer_suffix.len();
        assert!(
            retry
                .to_string()
                .contains(&format!("last complete offset={peer_end}"))
        );
        assert_eq!(state.lock().await.validated_end, peer_end as u64);
        assert_eq!(
            std::fs::read_to_string(capture.path()).unwrap(),
            format!("{peer_suffix}{}", &next[..partial]),
        );
    }

    #[tokio::test]
    async fn synced_append_succeeds_without_a_second_length_query() {
        let capture = tempfile::NamedTempFile::new().unwrap();
        let open_writer = || {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .append(true)
                .open(capture.path())
                .unwrap();
            Arc::new(Mutex::new(ShadowFile::new(file)))
        };
        let peer = open_writer();
        let state = open_writer();
        let prefix = record(1);
        append(&peer, prefix.as_bytes()).await.unwrap();

        let next = record(2);
        let read = std::cell::Cell::new(false);
        append_with(&state, next.as_bytes(), write_and_sync, move |file| {
            if read.replace(true) {
                return Err(io::Error::other("injected post-sync metadata failure"));
            }
            file_len(file)
        })
        .await
        .unwrap();
        assert_eq!(
            state.lock().await.validated_end,
            (prefix.len() + next.len()) as u64
        );

        let last = record(3);
        // The peer has to validate the newly written suffix before its own append.
        append(&peer, last.as_bytes()).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(capture.path()).unwrap(),
            format!("{prefix}{next}{last}"),
        );
    }

    #[tokio::test]
    async fn reopen_total_line_budget_refuses_without_qualifying_a_prefix() {
        let text = record(1).repeat(20);
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        let state = Arc::new(Mutex::new(ShadowFile::with_limits(
            file,
            tiny_limits(text.len() as u64, 9),
        )));
        assert_eq!(
            state.lock().await.validated_end,
            0,
            "over-budget reopen was qualified"
        );
        for _ in 0..2 {
            let error = append(&state, record(2).as_bytes())
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("shadow validation lines budget"), "{error}");
            assert!(error.contains(&format!("last complete offset={}", 3 * record(1).len())));
            assert_eq!(state.lock().await.validated_end, 0);
        }
        let mut state = state.lock().await;
        state.file.seek(SeekFrom::Start(0)).unwrap();
        let mut after = String::new();
        state.file.read_to_string(&mut after).unwrap();
        assert_eq!(after, text);
    }

    #[test]
    fn exact_total_bytes_and_lines_pass_but_one_more_byte_is_not_read() {
        let text = record(1).repeat(3);
        let limits = tiny_limits(text.len() as u64, 9);
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        assert_eq!(validate(&file, 0, limits).unwrap(), text.len() as u64);
        file.write_all(b"\n").unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let error = validate(&file, 0, limits).unwrap_err().to_string();
        assert!(error.contains("shadow validation bytes budget"), "{error}");
        assert_eq!(
            file.stream_position().unwrap(),
            0,
            "byte admission must precede reads"
        );
    }

    #[tokio::test]
    async fn appended_suffix_budget_preserves_verified_offset_and_capture() {
        for limits in [
            tiny_limits(100_000, 9),
            tiny_limits((record(1).len() * 3) as u64, 100),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("capture.lp");
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(&path)
                .unwrap();
            let state = Arc::new(Mutex::new(ShadowFile::with_limits(file, limits)));
            append(&state, record(1).as_bytes()).await.unwrap();
            let verified = state.lock().await.validated_end;
            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(record(2).repeat(4).as_bytes())
                .unwrap();
            let before = std::fs::read(&path).unwrap();
            let error = append(&state, record(3).as_bytes())
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("shadow validation"), "{error}");
            assert_eq!(state.lock().await.validated_end, verified);
            assert_eq!(std::fs::read(&path).unwrap(), before);
        }
    }

    #[tokio::test]
    async fn a_record_outside_the_scan_envelope_is_refused_before_write() {
        let one = record(1);
        for (limits, kind) in [
            (tiny_limits(100_000, 9), "lines"),
            (tiny_limits((one.len() * 3) as u64, 100), "bytes"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("capture.lp");
            let file = std::fs::OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(&path)
                .unwrap();
            let state = Arc::new(Mutex::new(ShadowFile::with_limits(file, limits)));
            append(&state, one.as_bytes()).await.unwrap();
            let verified = state.lock().await.validated_end;
            let error = append(&state, one.repeat(4).as_bytes())
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("shadow record exceeds validation {kind} budget")),
                "{error}"
            );
            assert_eq!(state.lock().await.validated_end, verified);
            assert_eq!(std::fs::read_to_string(&path).unwrap(), one);
            // A record exactly at the envelope is accepted and stays scannable.
            append(&state, one.repeat(3).as_bytes()).await.unwrap();
            let suffix = File::open(&path).unwrap();
            assert_eq!(
                validate(&suffix, verified, limits).unwrap(),
                (one.len() * 4) as u64
            );
        }
    }

    #[test]
    fn comments_and_elapsed_work_share_the_total_budget() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(b"# one\n# two\n# three\n# four\n").unwrap();
        let error = validate(&file, 0, tiny_limits(100, 3))
            .unwrap_err()
            .to_string();
        assert!(error.contains("shadow validation lines budget"), "{error}");
        let mut limits = tiny_limits(100, 100);
        limits.elapsed = Duration::ZERO;
        let error = validate(&file, 0, limits).unwrap_err().to_string();
        assert!(error.contains("shadow validation time budget"), "{error}");
    }

    #[test]
    fn an_existing_capture_is_validated_once_at_open() {
        let mut file = tempfile::tempfile().unwrap();
        for value in 0..20_000 {
            file.write_all(record(value).as_bytes()).unwrap();
        }
        let len = file.metadata().unwrap().len();
        // The first append then only scans what other writers added since.
        assert_eq!(ShadowFile::new(file).validated_end, len);
    }

    #[test]
    fn an_incomplete_capture_at_open_is_left_for_the_append_path_to_refuse() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(record(1).as_bytes()).unwrap();
        file.write_all(b"# write 2026-10-21T08:15:30Z points=2 bytes=100\npartial\n")
            .unwrap();
        let state = ShadowFile::new(file);
        assert_eq!(state.validated_end, 0);
        assert!(validate(&state.file, state.validated_end, VALIDATION_LIMITS).is_err());
    }

    #[test]
    fn orphan_commit_preserves_the_last_complete_boundary() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(record(1).as_bytes()).unwrap();
        file.write_all(b"# ordinary comment\n\n").unwrap();
        file.write_all(b"# write 2026-10-21T08:15:30Z points=1\nm value=2 123\n")
            .unwrap();
        let complete = file.metadata().unwrap().len();
        assert_eq!(validate(&file, 0, VALIDATION_LIMITS).unwrap(), complete);

        file.write_all(b"# commit points=1 bytes=14\n").unwrap();
        file.write_all(record(3).as_bytes()).unwrap();
        for start in [0, complete] {
            let error = validate(&file, start, VALIDATION_LIMITS).unwrap_err();
            assert_eq!(error.to_string(), incomplete(complete).to_string());
        }
    }

    #[test]
    fn framed_body_exceeding_its_byte_count_stops_before_the_next_row() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(record(1).as_bytes()).unwrap();
        let complete = file.metadata().unwrap().len();

        let row = "m value=2 123\n";
        let declared = row.len() + 2;
        write!(
            file,
            "# write 2026-10-21T08:15:30Z points=3 bytes={declared}\n{row}{row}{row}# commit points=3 bytes={declared}\n"
        )
        .unwrap();
        // Allow the header and two rows. Trying the third would report a
        // line-budget failure instead of the second row's framing failure.
        let error = validate(&file, complete, tiny_limits(1024, 3)).unwrap_err();
        assert_eq!(error.to_string(), incomplete(complete).to_string());
    }

    #[test]
    fn every_interrupted_prefix_preserves_the_last_complete_boundary() {
        let body = "m,host=synthetic value=1 123\n";
        let record = format!(
            "# write 2026-10-21T08:15:30Z points=1 bytes={}\n{body}# commit points=1 bytes={}\n",
            body.len(),
            body.len()
        );
        let mut file = tempfile::tempfile().unwrap();
        for end in 0..=record.len() {
            file.set_len(0).unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();
            file.write_all(record.as_bytes()).unwrap();
            file.write_all(&record.as_bytes()[..end]).unwrap();
            let result = validate(&file, 0, VALIDATION_LIMITS);
            if end == 0 || end == record.len() {
                assert_eq!(result.unwrap(), (record.len() + end) as u64);
            } else {
                let error = result.unwrap_err().to_string();
                assert!(
                    error.contains(&format!("last complete offset={}", record.len())),
                    "prefix {end}: {error}"
                );
            }
        }
    }
}
