mod compression;
mod encoding;

use crate::config::{Compression, EncodingRef};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::fs::File;
use std::io::{self, Read, Seek};
use tempfile::tempfile;

pub use self::compression::{detect_compression, CompressionError};
pub use self::encoding::{detect_encoding, TranscodingReader};

/// Type of content input provided to parsers.
pub enum Input {
    File(File),
    Stream(Box<dyn io::Read>),
    //BufferedStream(Box<dyn io::BufRead>),
}

impl Input {
    pub fn into_file(self) -> io::Result<File> {
        match self {
            Input::File(f) => Ok(f),
            Input::Stream(mut s) => {
                // This file will be automatically deleted by the OS as soon as the last handle to
                // it is closed.
                let mut file = tempfile()?;
                io::copy(&mut s, &mut file)?;
                // Ensure that the caller starts reading from the beginning of the file.
                // I'm not actually positive whether this is needed.
                file.seek(io::SeekFrom::Start(0))?;
                Ok(file)
            }
        }
    }
    pub fn into_buffered_stream(self, buffer_size: usize) -> Box<dyn io::BufRead> {
        match self {
            Input::File(f) => Box::new(io::BufReader::with_capacity(buffer_size, f)),
            //Input::BufferedStream(bs) => bs,
            Input::Stream(s) => Box::new(io::BufReader::with_capacity(buffer_size, s)),
        }
    }

    pub fn into_stream(self) -> Box<dyn io::Read> {
        match self {
            Input::File(f) => Box::new(f),
            //Input::BufferedStream(bs) => bs as Box<dyn io::Read>,
            Input::Stream(s) => s,
        }
    }

    /// Reads at most `max_bytes` into memory, and returns an `Input` that is still positioned at
    /// the beginning of the stream. The returned `Input` can be read as normal without missing any
    /// bytes. The returned `Bytes` will contain a duplicate of the first `max_bytes` (at most)
    /// from `self`.
    pub fn peek(self, max_bytes: usize) -> io::Result<(Bytes, Input)> {
        match self {
            Input::File(mut f) => {
                let result = read_at_most(&mut f, max_bytes)?;
                // Try to reset to the start of the file. This is desirable because it ensures that
                // the Input is still a File, which is helpful for formats like .xls that require
                // random access. But it's ok if it doesn't work.
                if f.seek(io::SeekFrom::Start(0)).is_ok() {
                    Ok((result, Input::File(f)))
                } else {
                    // We were unable to seek to the beginning of the file, which means that the
                    // file is likely stdin or a pipe. That's ok, because we'll just treat it as a
                    // stream from here on out.
                    let reader = result.clone().reader().chain(f);
                    Ok((result, Input::Stream(Box::new(reader))))
                }
            }
            Input::Stream(mut s) => {
                let result = read_at_most(&mut s, max_bytes)?;
                let reader = result.clone().reader().chain(s);
                Ok((result, Input::Stream(Box::new(reader))))
            }
        }
    }

    // Converts `self` into UTF-8. If the `source_encoding` is specified, then it is assumed to be
    // correct. Otherwise, this will attempt to detect the source encoding based on the first
    // `max_peek` bytes. If the source encoding is already UTF-8, then no transcoding will be
    // performed.
    pub fn transcode_non_utf8(
        self,
        source_encoding: Option<EncodingRef>,
        max_peek: usize,
    ) -> io::Result<Self> {
        let (resolved_encoding, input) = if let Some(e) = source_encoding {
            (e, self)
        } else {
            // There's no encoding specified, so we'll try to detect it.
            let (first_bytes, new_input) = self.peek(max_peek)?;
            let detected = detect_encoding(&first_bytes);
            (detected, new_input)
        };

        // If the source encoding is already utf-8, then we'll just pass it through. This does mean
        // that we _can't_ guarantee that the output bytes are valid utf-8. This is considered a fine
        // tradeoff because:
        // - Invalid utf-8 will be caught anyway by parser implementations, for example by reading into
        //   a String.
        // - It would be nice to avoid duplicating the work of utf-8 validation, especially since this
        //   is likely to be by far the most common input encoding.
        if resolved_encoding.is_utf8() {
            Ok(input)
        } else {
            tracing::debug!(
                "transcoding from '{}' into utf-8",
                resolved_encoding.encoding().name()
            );
            let reader = self::encoding::TranscodingReader::with_buffer_size(
                input.into_stream(),
                resolved_encoding,
                8192,
            );
            Ok(Input::Stream(Box::new(reader)))
        }
    }

    pub fn decompressed(self, compression: Compression) -> Result<Self, CompressionError> {
        let decompressed = self::compression::decompress_input(self, compression)?;
        Ok(Input::Stream(decompressed))
    }
}

fn read_at_most(input: &mut impl io::Read, max_bytes: usize) -> io::Result<Bytes> {
    let mut dest = BytesMut::with_capacity(max_bytes).writer();
    let mut src = input.take(max_bytes as u64);
    let total = io::copy(&mut src, &mut dest)?;
    tracing::debug!(nBytes = total, "successfully peeked at input");
    Ok(dest.into_inner().into())
}
