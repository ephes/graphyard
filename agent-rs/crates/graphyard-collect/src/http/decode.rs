//! `Content-Encoding` decoding as httpx 0.28.1 does it (`httpx/_decoders.py`,
//! `Response._get_content_decoder`): `gzip`, `deflate` (zlib, or raw
//! deflate when the first chunk is not zlib) and `identity`; anything else
//! (`br`, `zstd`: not installed with the Python agent) passes through. The
//! decoded bytes are what `iter_bytes()` yields and what `max_body_bytes`
//! counts. When a limit is installed, every intermediate stage also shares
//! that output allowance, before its vector can grow. Raw input is independently
//! capped before decoding, even when it emits no output.
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

/// Maximum recognized Content-Encoding layers, including identity.
const MAX_DECODER_STAGES: usize = 4;
/// Raw response ceiling, including otherwise unbounded response.read() paths.
const MAX_ENCODED_BYTES: u64 = 64 * 1024 * 1024;
/// Encoding metadata/overhead allowance above an installed decoded byte cap.
const ENCODING_OVERHEAD_BYTES: u64 = 64 * 1024;

/// The decoder of one response body.
pub(crate) struct BodyDecoder {
    /// In decoding order (the reverse of the header order).
    stages: Vec<Stage>,
    identity: Output,
    encoded_remaining: u64,
}

impl BodyDecoder {
    /// `_get_content_decoder` for the comma-split `Content-Encoding` values.
    pub(crate) fn new<'a>(encodings: impl IntoIterator<Item = &'a str>) -> Result<Self, HttpError> {
        // Admit names before allocating any inflater state. Unsupported tokens
        // retain httpx's pass-through behavior and do not create stages.
        let mut admitted = Vec::with_capacity(MAX_DECODER_STAGES);
        for value in encodings {
            let value = value.trim();
            if ["identity", "gzip", "deflate"]
                .iter()
                .any(|name| value.eq_ignore_ascii_case(name))
            {
                if admitted.len() == MAX_DECODER_STAGES {
                    return Err(HttpError::new(
                        ErrorKind::LimitExceeded,
                        "collector content encoding stage limit exceeded",
                    ));
                }
                admitted.push(value);
            }
        }
        let stages = admitted
            .into_iter()
            .rev()
            .map(|value| {
                if value.eq_ignore_ascii_case("gzip") {
                    Stage::Gzip(Box::new(GzDecoder::new(Output::default())))
                } else if value.eq_ignore_ascii_case("deflate") {
                    Stage::Zlib(Box::new(ZlibDecoder::new(Output::default())), true)
                } else {
                    Stage::Identity(Output::default())
                }
            })
            .collect();
        Ok(Self {
            stages,
            identity: Output::default(),
            encoded_remaining: MAX_ENCODED_BYTES,
        })
    }

    pub(crate) fn set_limit(&mut self, limit: u64) {
        self.encoded_remaining =
            MAX_ENCODED_BYTES.min(limit.saturating_add(ENCODING_OVERHEAD_BYTES));
        self.identity.set_limit(limit);
        for stage in &mut self.stages {
            stage.set_limit(limit);
        }
    }

    /// Decode one raw chunk.
    ///
    /// # Errors
    /// `DecodingError` for corrupt compressed data, or `LimitExceeded` when
    /// raw input or any stage exceeds its cumulative allowance.
    pub(crate) fn decode(&mut self, data: &[u8]) -> Result<Vec<u8>, HttpError> {
        // Charge the complete transport chunk before any inflater sees it,
        // including metadata, nonproductive input and bytes after stream end.
        self.encoded_remaining = self
            .encoded_remaining
            .checked_sub(data.len() as u64)
            .ok_or_else(|| {
                HttpError::new(
                    ErrorKind::LimitExceeded,
                    "collector encoded response byte limit exceeded",
                )
            })?;
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
            let mut d = BodyDecoder::new([encoding]).unwrap();
            d.set_limit(32);
            assert_eq!(d.decode(&data).unwrap_err().kind, ErrorKind::LimitExceeded);
        }
    }
    #[test]
    fn intermediate_stage_is_bounded_even_when_final_output_is_small() {
        let data = gzip(&gzip(b"x"));
        let mut d = BodyDecoder::new(["gzip", "gzip"]).unwrap();
        d.set_limit(16);
        assert_eq!(d.decode(&data).unwrap_err().kind, ErrorKind::LimitExceeded);
    }
    #[test]
    fn split_chunks_keep_one_cumulative_allowance() {
        let data = gzip(b"synthetic normal output");
        let mut d = BodyDecoder::new(["gzip"]).unwrap();
        d.set_limit(23);
        let mut result = Vec::new();
        for chunk in data.chunks(3) {
            result.extend(d.decode(chunk).unwrap());
        }
        assert_eq!(result, b"synthetic normal output");
        let mut d = BodyDecoder::new(["identity"]).unwrap();
        d.set_limit(4);
        assert_eq!(d.decode(b"ab").unwrap(), b"ab");
        assert_eq!(d.decode(b"cd").unwrap(), b"cd");
        assert_eq!(d.decode(b"e").unwrap_err().kind, ErrorKind::LimitExceeded);
    }

    #[test]
    fn exact_stage_limit_and_unsupported_tokens_keep_decoding_semantics() {
        let mut decoder = BodyDecoder::new(["identity"; MAX_DECODER_STAGES]).unwrap();
        decoder.set_limit(2);
        assert_eq!(decoder.decode(b"ok").unwrap(), b"ok");
        assert!(BodyDecoder::new(["identity"; MAX_DECODER_STAGES + 1]).is_err());
        let mut decoder = BodyDecoder::new(["unsupported", " GZip "]).unwrap();
        decoder.set_limit(100);
        let data = gzip(&gzip(b"ok"));
        let mut stacked = BodyDecoder::new(["gzip", "gzip"]).unwrap();
        stacked.set_limit(100);
        assert_eq!(stacked.decode(&data).unwrap(), b"ok");
        assert_eq!(decoder.decode(&gzip(b"ok")).unwrap(), b"ok");
    }
    #[test]
    fn encoded_bytes_have_one_cumulative_allowance_before_decoding() {
        let mut decoder = BodyDecoder::new(["gzip"]).unwrap();
        decoder.set_limit(1);
        let mut header = vec![0x1f, 0x8b, 8, 8, 0, 0, 0, 0, 0, 0xff];
        header.resize(65537, b'f');
        for chunk in header.chunks(1024) {
            assert!(decoder.decode(chunk).unwrap().is_empty());
        }
        assert_eq!(
            decoder.decode(b"f").unwrap_err().message,
            "collector encoded response byte limit exceeded"
        );
    }
    #[test]
    fn default_raw_ceiling_also_charges_ignored_trailing_bytes() {
        let mut decoder = BodyDecoder::new(["gzip"]).unwrap();
        let encoded = gzip(b"");
        assert!(decoder.decode(&encoded).unwrap().is_empty());
        let padding = vec![0; 1024 * 1024];
        for _ in 0..63 {
            assert!(decoder.decode(&padding).unwrap().is_empty());
        }
        assert!(
            decoder
                .decode(&padding[..padding.len() - encoded.len()])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            decoder.decode(b"x").unwrap_err().message,
            "collector encoded response byte limit exceeded"
        );
    }
}
