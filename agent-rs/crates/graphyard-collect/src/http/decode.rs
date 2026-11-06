//! `Content-Encoding` decoding as httpx 0.28.1 does it (`httpx/_decoders.py`,
//! `Response._get_content_decoder`): `gzip`, `deflate` (zlib, or raw
//! deflate when the first chunk is not zlib) and `identity`; anything else
//! (`br`, `zstd`: not installed with the Python agent) passes through. The
//! decoded bytes are what `iter_bytes()` yields and what `max_body_bytes`
//! counts. When a limit is installed, every intermediate stage also shares
//! that output allowance, before its vector can grow.
//!
//! Like `zlib.decompressobj`, a truncated stream is not an error and bytes
//! after the end of a stream are ignored. Corrupt data is a
//! `DecodingError` (an `httpx.HTTPError`, P2 text); the gzip trailer's CRC
//! is not checked (flate2 checks it only when finishing, which would also
//! reject truncated streams; zlib checks it inline, P2).

use std::io::{self, Write};

use flate2::write::{DeflateDecoder, GzDecoder, ZlibDecoder};

use super::error::{ErrorKind, HttpError};

#[derive(Debug)]
struct OutputLimit(u64);
impl std::fmt::Display for OutputLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "decoder output limit")
    }
}
impl std::error::Error for OutputLimit {}

fn is_limit(err: &io::Error) -> bool {
    err.get_ref().is_some_and(|inner| inner.is::<OutputLimit>())
}

fn decoding_error(err: &io::Error) -> HttpError {
    if let Some(limit) = err
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<OutputLimit>())
    {
        HttpError::body_limit(limit.0)
    } else {
        HttpError::new(ErrorKind::DecodingError, err.to_string())
    }
}

#[derive(Default)]
struct Output {
    bytes: Vec<u8>,
    remaining: Option<u64>,
    limit: u64,
}
impl Output {
    fn set_limit(&mut self, limit: u64) {
        self.limit = limit;
        self.remaining = Some(limit);
    }
}
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Some(remaining) = self.remaining {
            if bytes.len() as u64 > remaining {
                return Err(io::Error::other(OutputLimit(self.limit)));
            }
            self.remaining = Some(remaining - bytes.len() as u64);
        }
        self.bytes.reserve_exact(bytes.len());
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Write `data` until the decoder stops taking input (end of stream).
fn feed<W: Write>(decoder: &mut W, mut data: &[u8]) -> io::Result<()> {
    while !data.is_empty() {
        let written = decoder.write(data)?;
        if written == 0 {
            break;
        }
        data = &data[written..];
    }
    decoder.flush()
}

enum Stage {
    Gzip(Box<GzDecoder<Output>>),
    /// `DeflateDecoder.first_attempt`: zlib until the first chunk fails.
    Zlib(Box<ZlibDecoder<Output>>, bool),
    RawDeflate(Box<DeflateDecoder<Output>>),
    Identity(Output),
}

impl Stage {
    fn set_limit(&mut self, limit: u64) {
        match self {
            Self::Gzip(d) => d.get_mut().set_limit(limit),
            Self::Zlib(d, _) => d.get_mut().set_limit(limit),
            Self::RawDeflate(d) => d.get_mut().set_limit(limit),
            Self::Identity(d) => d.set_limit(limit),
        }
    }

    fn decode(&mut self, data: &[u8]) -> Result<Vec<u8>, HttpError> {
        match self {
            Self::Gzip(decoder) => {
                feed(decoder.as_mut(), data).map_err(|err| decoding_error(&err))?;
                Ok(std::mem::take(&mut decoder.get_mut().bytes))
            }
            Self::Zlib(decoder, first_attempt) => {
                let first = std::mem::replace(first_attempt, false);
                match feed(decoder.as_mut(), data) {
                    Ok(()) => Ok(std::mem::take(&mut decoder.get_mut().bytes)),
                    Err(err) if first && !is_limit(&err) => {
                        let mut output = Output::default();
                        if decoder.get_ref().remaining.is_some() {
                            output.set_limit(decoder.get_ref().limit);
                        }
                        *self = Self::RawDeflate(Box::new(DeflateDecoder::new(output)));
                        self.decode(data)
                    }
                    Err(err) => Err(decoding_error(&err)),
                }
            }
            Self::RawDeflate(decoder) => {
                feed(decoder.as_mut(), data).map_err(|err| decoding_error(&err))?;
                Ok(std::mem::take(&mut decoder.get_mut().bytes))
            }
            Self::Identity(output) => {
                output.write_all(data).map_err(|err| decoding_error(&err))?;
                Ok(std::mem::take(&mut output.bytes))
            }
        }
    }
}

/// The decoder of one response body.
pub(crate) struct BodyDecoder {
    /// In decoding order (the reverse of the header order).
    stages: Vec<Stage>,
    identity: Output,
}

impl BodyDecoder {
    /// `_get_content_decoder` for the comma-split `Content-Encoding` values.
    pub(crate) fn new<'a>(encodings: impl IntoIterator<Item = &'a str>) -> Self {
        let mut stages: Vec<Stage> = encodings
            .into_iter()
            .filter_map(|value| match value.trim().to_ascii_lowercase().as_str() {
                "identity" => Some(Stage::Identity(Output::default())),
                "gzip" => Some(Stage::Gzip(Box::new(GzDecoder::new(Output::default())))),
                "deflate" => Some(Stage::Zlib(
                    Box::new(ZlibDecoder::new(Output::default())),
                    true,
                )),
                _ => None,
            })
            .collect();
        stages.reverse();
        Self {
            stages,
            identity: Output::default(),
        }
    }

    pub(crate) fn set_limit(&mut self, limit: u64) {
        self.identity.set_limit(limit);
        for stage in &mut self.stages {
            stage.set_limit(limit);
        }
    }

    /// Decode one raw chunk.
    ///
    /// # Errors
    /// `DecodingError` for corrupt compressed data, or `LimitExceeded` when
    /// any stage exceeds its installed cumulative output allowance.
    pub(crate) fn decode(&mut self, data: &[u8]) -> Result<Vec<u8>, HttpError> {
        if self.stages.is_empty() {
            self.identity
                .write_all(data)
                .map_err(|err| decoding_error(&err))?;
            return Ok(std::mem::take(&mut self.identity.bytes));
        }
        // Do not copy raw input before the first stage applies its allowance.
        let mut stages = self.stages.iter_mut();
        let mut data = stages.next().expect("nonempty").decode(data)?;
        for stage in stages {
            data = stage.decode(&data)?;
        }
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{
        Compression,
        write::{DeflateEncoder, GzEncoder, ZlibEncoder},
    };
    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut e = GzEncoder::new(Vec::new(), Compression::fast());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }
    #[test]
    fn high_ratio_output_is_stopped_inside_each_decoder() {
        let input = vec![b'x'; 100_000];
        let mut z = ZlibEncoder::new(Vec::new(), Compression::fast());
        z.write_all(&input).unwrap();
        let mut raw = DeflateEncoder::new(Vec::new(), Compression::fast());
        raw.write_all(&input).unwrap();
        for (encoding, data) in [
            ("gzip", gzip(&input)),
            ("deflate", z.finish().unwrap()),
            ("deflate", raw.finish().unwrap()),
        ] {
            let mut d = BodyDecoder::new([encoding]);
            d.set_limit(32);
            assert_eq!(d.decode(&data).unwrap_err().kind, ErrorKind::LimitExceeded);
        }
    }
    #[test]
    fn intermediate_stage_is_bounded_even_when_final_output_is_small() {
        let data = gzip(&gzip(b"x"));
        let mut d = BodyDecoder::new(["gzip", "gzip"]);
        d.set_limit(16);
        assert_eq!(d.decode(&data).unwrap_err().kind, ErrorKind::LimitExceeded);
    }
    #[test]
    fn split_chunks_keep_one_cumulative_allowance() {
        let data = gzip(b"synthetic normal output");
        let mut d = BodyDecoder::new(["gzip"]);
        d.set_limit(23);
        let mut result = Vec::new();
        for chunk in data.chunks(3) {
            result.extend(d.decode(chunk).unwrap());
        }
        assert_eq!(result, b"synthetic normal output");
        let mut d = BodyDecoder::new(["identity"]);
        d.set_limit(4);
        assert_eq!(d.decode(b"ab").unwrap(), b"ab");
        assert_eq!(d.decode(b"cd").unwrap(), b"cd");
        assert_eq!(d.decode(b"e").unwrap_err().kind, ErrorKind::LimitExceeded);
    }
}
