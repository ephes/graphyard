//! Complete shadow batches under a cooperating local-filesystem lock.
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::Duration;

use graphyard_core::line_protocol::WRITE_MAX_REQUEST_BYTES;
use tokio::sync::Mutex;

#[derive(Debug)]
pub(crate) struct ShadowFile {
    pub file: File,
    validated_end: u64,
}

impl ShadowFile {
    /// Validate the existing capture once, before collectors start, so the
    /// first append of a process only scans what cooperating writers added
    /// since. An invalid capture, or a lock that cannot be taken in time,
    /// keeps offset 0: the append path then validates (and refuses) as before.
    pub fn new(file: File) -> Self {
        let validated_end = validated_at_open(&file).unwrap_or(0);
        Self {
            file,
            validated_end,
        }
    }
}

fn validated_at_open(file: &File) -> Option<u64> {
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
    validate(file, 0).ok()
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

fn line(reader: &mut impl BufRead, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .take(limit as u64 + 1)
        .read_until(b'\n', &mut bytes)?;
    if bytes.len() > limit {
        return Err(io::Error::other("shadow line byte limit"));
    }
    Ok(bytes)
}

/// Validate only the suffix added since this descriptor last admitted a batch.
/// Legacy headers are checked by point count; v2 headers require a commit trailer.
fn validate(file: &File, start: u64) -> io::Result<u64> {
    if file.metadata()?.len() < start {
        return Err(incomplete(start));
    }
    let mut input = BufReader::new(file);
    input.seek(SeekFrom::Start(start))?;
    let mut complete = start;
    loop {
        let header = line(&mut input, 4096)?;
        if header.is_empty() {
            return Ok(complete);
        }
        if !header.ends_with(b"\n") {
            return Err(incomplete(complete));
        }
        if !header.starts_with(b"# write ") {
            if header.starts_with(b"#") || header == b"\n" {
                complete = input.stream_position()?;
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
        let mut bytes = 0usize;
        for _ in 0..points {
            let row = line(
                &mut input,
                (WRITE_MAX_REQUEST_BYTES + 1).saturating_sub(bytes),
            )?;
            if row.len() <= 1 || row.starts_with(b"#") || !row.ends_with(b"\n") {
                return Err(incomplete(complete));
            }
            bytes += row.len();
        }
        if let Some(expected) = expected_bytes {
            let trailer = line(&mut input, 4096)?;
            if bytes != expected
                || trailer != format!("# commit points={points} bytes={bytes}\n").as_bytes()
            {
                return Err(incomplete(complete));
            }
        }
        complete = input.stream_position()?;
    }
}

pub(crate) async fn append(file: &Arc<Mutex<ShadowFile>>, record: &[u8]) -> io::Result<()> {
    append_with(file, record, || {}).await
}

async fn append_with(
    file: &Arc<Mutex<ShadowFile>>,
    record: &[u8],
    before_write: impl FnOnce() + Send + 'static,
) -> io::Result<()> {
    // One running/queued blocking job per writer. The job owns this guard,
    // descriptor and record even if the async caller stops waiting.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut state = tokio::time::timeout_at(deadline, file.clone().lock_owned())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shadow lock deadline"))?;
    let record = record.to_vec();
    tokio::task::spawn_blocking(move || {
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
        let end = {
            let _locked = Locked(&state.file);
            validate(&state.file, state.validated_end)?;
            before_write();
            let mut output = &state.file;
            output.write_all(&record)?;
            output.flush()?;
            state.file.sync_data()?;
            state.file.metadata()?.len()
        };
        state.validated_end = end;
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
        append_with(&state, record(1).as_bytes(), move || {
            entered.send(()).unwrap();
            gate.recv_timeout(Duration::from_secs(3)).unwrap();
        })
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
                append_with(&state, record(1).as_bytes(), move || {
                    entered.send(()).unwrap();
                    gate.recv_timeout(Duration::from_secs(3)).unwrap();
                })
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
        assert!(validate(&state.file, state.validated_end).is_err());
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
            let result = validate(&file, 0);
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
