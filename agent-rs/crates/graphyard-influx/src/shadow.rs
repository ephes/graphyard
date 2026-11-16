//! Complete shadow batches under a cooperating local-filesystem lock.
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::time::Duration;

use graphyard_core::line_protocol::WRITE_MAX_REQUEST_BYTES;
use tokio::sync::Mutex;

#[derive(Debug)]
pub(crate) struct ShadowFile {
    pub file: File,
    validated_end: u64,
}

impl ShadowFile {
    pub fn new(file: File) -> Self {
        Self {
            file,
            validated_end: 0,
        }
    }
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

pub(crate) async fn append(file: &Mutex<ShadowFile>, record: &[u8]) -> io::Result<()> {
    // Both in-process admission and OS-lock waiting share one elapsed deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let mut state = tokio::time::timeout_at(deadline, file.lock())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "shadow lock deadline"))?;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "shadow lock deadline",
            ));
        }
        match state.file.try_lock() {
            Ok(()) => break,
            Err(std::fs::TryLockError::WouldBlock) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(std::fs::TryLockError::Error(err)) => return Err(err),
        }
    }
    let end = {
        let _locked = Locked(&state.file);
        validate(&state.file, state.validated_end)?;
        let mut output = &state.file;
        output.write_all(record)?;
        output.flush()?;
        state.file.sync_data()?;
        state.file.metadata()?.len()
    };
    state.validated_end = end;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
