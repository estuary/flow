use super::Input;
use crate::config::Compression;
use flate2::read::GzDecoder;
use std::boxed::Box;
use std::io::{self, Read};

/// Checks for a "magic number" at the start of the content, and returns a corresponding
/// compression format if one is detected.
pub fn detect_compression(prefix: &[u8]) -> Option<Compression> {
    if prefix.starts_with(&[0x1f, 0x8b]) {
        Some(Compression::Gzip)
    } else if prefix.starts_with(&[0x50, 0x4B, 0x03, 0x04]) {
        Some(Compression::ZipArchive)
    } else if prefix.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]) {
        Some(Compression::Zstd)
    } else {
        None
    }
}

pub fn decompress_input(input: Input, compression: Compression) -> Result<Input, CompressionError> {
    match compression {
        Compression::Gzip => decompress_gzip(input.into_stream()).map(Input::Stream),
        Compression::Zstd => decompress_zstd(input.into_stream()).map(Input::Stream),
        Compression::None => Ok(input),
        Compression::ZipArchive => {
            unreachable!("zip archives are decompressed when extracting files to parse")
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("failed to read compressed input: {0}")]
    Io(#[from] io::Error),
    #[error("failed to decompress content using compression format: {0}")]
    InvalidCompression(Compression),
}

fn decompress_zstd(stream: Box<dyn Read>) -> Result<Box<dyn Read>, CompressionError> {
    let reader = zstd::stream::read::Decoder::new(stream)?;
    Ok(Box::new(reader))
}

fn decompress_gzip(stream: Box<dyn Read>) -> Result<Box<dyn Read>, CompressionError> {
    let decoder = GzDecoder::new(stream);
    if decoder.header().is_some() {
        Ok(Box::new(decoder))
    } else {
        Err(CompressionError::InvalidCompression(Compression::Gzip))
    }
}
