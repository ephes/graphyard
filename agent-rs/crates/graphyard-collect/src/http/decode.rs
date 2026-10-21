//! `Content-Encoding` decoding as httpx 0.28.1 does it (`httpx/_decoders.py`,
//! `Response._get_content_decoder`): `gzip`, `deflate` (zlib, or raw
//! deflate when the first chunk is not zlib) and `identity`; anything else
//! (`br`, `zstd`: not installed with the Python agent) passes through. The
//! decoded bytes are what `iter_bytes()` yields and what `max_body_bytes`
//! counts.
//!
//! Like `zlib.decompressobj`, a truncated stream is not an error and bytes
//! after the end of a stream are ignored. Corrupt data is a
//! `DecodingError` (an `httpx.HTTPError`, P2 text); the gzip trailer's CRC
//! is not checked (flate2 checks it only when finishing, which would also
//! reject truncated streams; zlib checks it inline, P2).

use std::io::{self, Write};

use flate2::write::{DeflateDecoder, GzDecoder, ZlibDecoder};

use super::error::{ErrorKind, HttpError};

fn decoding_error(err: &io::Error) -> HttpError {
    HttpError::new(ErrorKind::DecodingError, err.to_string())
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
    Gzip(Box<GzDecoder<Vec<u8>>>),
    /// `DeflateDecoder.first_attempt`: zlib until the first chunk fails.
    Zlib(Box<ZlibDecoder<Vec<u8>>>, bool),
    RawDeflate(Box<DeflateDecoder<Vec<u8>>>),
    Identity,
}

impl Stage {
    fn decode(&mut self, data: &[u8]) -> Result<Vec<u8>, HttpError> {
        match self {
            Self::Gzip(decoder) => {
                feed(decoder.as_mut(), data).map_err(|err| decoding_error(&err))?;
                Ok(std::mem::take(decoder.get_mut()))
            }
            Self::Zlib(decoder, first_attempt) => {
                let first = std::mem::replace(first_attempt, false);
                match feed(decoder.as_mut(), data) {
                    Ok(()) => Ok(std::mem::take(decoder.get_mut())),
                    Err(_) if first => {
                        *self = Self::RawDeflate(Box::new(DeflateDecoder::new(Vec::new())));
                        self.decode(data)
                    }
                    Err(err) => Err(decoding_error(&err)),
                }
            }
            Self::RawDeflate(decoder) => {
                feed(decoder.as_mut(), data).map_err(|err| decoding_error(&err))?;
                Ok(std::mem::take(decoder.get_mut()))
            }
            Self::Identity => Ok(data.to_vec()),
        }
    }
}

/// The decoder of one response body.
pub(crate) struct BodyDecoder {
    /// In decoding order (the reverse of the header order).
    stages: Vec<Stage>,
}

impl BodyDecoder {
    /// `_get_content_decoder` for the comma-split `Content-Encoding` values.
    pub(crate) fn new<'a>(encodings: impl IntoIterator<Item = &'a str>) -> Self {
        let mut stages: Vec<Stage> = encodings
            .into_iter()
            .filter_map(|value| match value.trim().to_ascii_lowercase().as_str() {
                "identity" => Some(Stage::Identity),
                "gzip" => Some(Stage::Gzip(Box::new(GzDecoder::new(Vec::new())))),
                "deflate" => Some(Stage::Zlib(Box::new(ZlibDecoder::new(Vec::new())), true)),
                _ => None,
            })
            .collect();
        stages.reverse();
        Self { stages }
    }

    /// Decode one raw chunk.
    ///
    /// # Errors
    /// `DecodingError` for corrupt compressed data.
    pub(crate) fn decode(&mut self, data: &[u8]) -> Result<Vec<u8>, HttpError> {
        let mut data = data.to_vec();
        for stage in &mut self.stages {
            data = stage.decode(&data)?;
        }
        Ok(data)
    }
}
