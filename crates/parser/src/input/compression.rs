use super::Input;
use crate::config::Compression;
use flate2::read::GzDecoder;
use std::boxed::Box;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::pin::Pin;
use zip::read::{ZipArchive, ZipFile};
use zip::result::ZipError;

/// Checks for a "magic number" at the start of the content, and returns a corresponding
/// compression format if one is detected.
pub fn detect_compression(prefix: &[u8]) -> Option<Compression> {
    if prefix.starts_with(&[0x1f, 0x8b]) {
        Some(Compression::Gzip)
    } else if prefix.starts_with(&[0x50, 0x4B, 0x03, 0x04]) {
        Some(Compression::ZipArchive)
    } else {
        None
    }
}

pub fn decompress_input(
    input: Input,
    compression: Compression,
) -> Result<Box<dyn Read>, CompressionError> {
    match compression {
        Compression::ZipArchive => decompress_zip_archive(input.into_file()?),
        Compression::Gzip => decompress_gzip(input.into_stream()),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("failed to read compressed input: {0}")]
    Io(#[from] io::Error),
    #[error(transparent)]
    ZipArchive(#[from] ZipError),
    #[error("failed to decompress content using compression format: {0}")]
    InvalidCompression(Compression),
}

fn decompress_gzip(stream: Box<dyn Read>) -> Result<Box<dyn Read>, CompressionError> {
    let decoder = GzDecoder::new(stream);
    if decoder.header().is_some() {
        Ok(Box::new(decoder))
    } else {
        Err(CompressionError::InvalidCompression(Compression::Gzip))
    }
}

fn decompress_zip_archive(zip_file: File) -> Result<Box<dyn Read>, CompressionError> {
    let archive = ZipArchive::new(zip_file)?;
    tracing::debug!(total_entries = archive.len(), "decompressing zip archive");
    Ok(Box::new(ArchiveReader::new(archive)))
}

/// Reads all the file contents of a zip archive, in the order they appear in the zip file.
struct ArchiveReader {
    /// We manually coerce these into 'static lifetimes to satisfy the borrow checker. This is safe
    /// as long as:
    /// - no references to `current` ever get returned outside of it's functions, and
    /// - the `ZipFile` is dropped before the `ZipArchive` (rust drops struct fields in the order
    ///   they were declared).
    current: Option<ZipFile<'static>>,
    /// ArchiveReader is a self-referential struct, where each `current` `ZipFile` contains
    /// references to fields within `archive`. In order for this to be safe, we pin `archive`
    /// to a single unmovable location within memory, and ensure that it's dropped after `current`.
    archive: Pin<Box<ZipArchive<File>>>,
    next_index: usize,
    file_count: usize,
}

impl ArchiveReader {
    fn new(archive: ZipArchive<File>) -> Self {
        ArchiveReader {
            archive: Box::pin(archive),
            next_index: 0,
            file_count: 0,
            current: None,
        }
    }
}

impl io::Read for ArchiveReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            // Find the next entry in the archive, if we're not in the middle of one.
            while self.current.is_none() && self.next_index < self.archive.len() {
                let entry = self
                    .archive
                    .by_index(self.next_index)
                    .map_err(zip_into_io_err)?;
                if should_include_archive_member(&entry) {
                    self.file_count += 1;
                    tracing::trace!(
                        file_num = self.file_count,
                        "reading zip file: {:?}",
                        entry.name()
                    );

                    // If the zip archive contains multiple files, we concatenate the binary
                    // content of each. This matches the behavior of `unzip -p archive.zip`.
                    // There's a pretty significant chance that the parser is going to barf, since
                    // many formats won't be able to handle this. So this warning will hopefully
                    // make it easy to find the root cause of those errors.
                    if self.file_count == 2 {
                        tracing::warn!("concatenating multiple files from zip archive");
                    }
                    // This transmute is safe as long as we don't ever return any references to it,
                    // since we're guaranteed to drop the entry before we drop the ZipArchive.
                    self.current =
                        Some(unsafe { std::mem::transmute::<ZipFile<'_>, ZipFile<'static>>(entry) })
                }
                self.next_index += 1;
            }

            // At this point, if there's no current entry, then we've finished the archive.
            if let Some(mut reader) = self.current.take() {
                match reader.read(buf) {
                    Ok(0) => { /* Don't put the reader back. Loop around and try again. */ }
                    other => {
                        // put the reader back, even if the result is an error. This ensures that
                        // ErrorKind::Interrupted errors can be retried, though in all honestly
                        // those are probably already retried by the ZipFile.
                        self.current = Some(reader);
                        return other;
                    }
                }
            } else {
                return Ok(0);
            }
        }
    }
}

fn zip_into_io_err(zip_err: ZipError) -> io::Error {
    match zip_err {
        ZipError::Io(ioe) => ioe,
        other => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid zip archive: {}", other),
        ),
    }
}

fn should_include_archive_member(entry: &ZipFile) -> bool {
    // OSX users will often end up with extra hidden files in their archives. An example is the
    // `.DS_Store` files that apple puts everywhere, but we've also seen `__MACOSX/.*`. So we
    // filter out any hidden files (those whose name begins with a '.').
    entry.is_file()
        && Path::new(entry.name())
            .file_name()
            .and_then(|n| n.to_str())
            .map(|name| !name.starts_with("."))
            .unwrap_or_else(|| {
                // If we got here, it's because the zip entry has a path that ends with '..' or
                // something like that, which seems unusual enough to be worth logging.
                tracing::warn!(
                    "skipping zip entry: {:?} since the filename does not appear to be valid",
                    entry.name()
                );
                false
            })
}
