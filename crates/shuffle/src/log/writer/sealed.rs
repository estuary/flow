use crate::log;
use anyhow::Context;
use std::io::Write;
use std::time::Duration;

/// A sealed (rolled) segment file, returned from `Writer::append_block()` when
/// the writer advances to a new segment. Unlinks the file on drop unless disarmed.
pub struct SealedSegment {
    pub path: std::path::PathBuf,
    pub size: u64,
    armed: bool,
}

impl SealedSegment {
    pub fn new(path: std::path::PathBuf, size: u64) -> Self {
        Self {
            path,
            size,
            armed: true,
        }
    }

    /// Disarm so Drop doesn't unlink (file already gone or atomically replaced).
    pub fn disarm(&mut self) {
        self.armed = false;
    }

    /// Returns an async `Stream` that services a sealed segment over its lifetime.
    ///
    /// The stream yields `anyhow::Result<u64>` values representing bytes of disk
    /// space reclaimed:
    ///
    /// 1. If the reader unlinks the file before compression, yields the original
    ///    size and terminates.
    /// 2. If the file survives past `COMPRESS_AFTER`, yields bytes saved by
    ///    compression (`old_size - new_size`), then continues polling.
    /// 3. When the (now compressed) file is eventually unlinked by the reader,
    ///    yields the compressed size and terminates.
    ///
    /// Any IO error other than NotFound (the expected unlink race) is yielded as
    /// `Err` and terminates the stream, which should tear down the owning LogActor.
    ///
    /// The `SealedSegment` is held for the stream's lifetime: its `Drop` impl
    /// guarantees the file is unlinked on teardown if the reader hasn't already.
    pub fn serve(self) -> impl futures::Stream<Item = anyhow::Result<u64>> + Send + 'static {
        futures::stream::unfold(State::Pending(self), |state| async move {
            match state {
                State::Pending(mut sealed) => {
                    // We perform frequent initial checks for existence because a
                    // tailing Reader will quickly unlink the file, which lets us
                    // propagate the reclaim to reduce our back-pressure measure.
                    let deadline = tokio::time::Instant::now() + COMPRESS_AFTER;
                    let mut backoff = Duration::from_millis(100);

                    loop {
                        tokio::time::sleep(backoff).await;

                        if !sealed.path.exists() {
                            sealed.disarm();
                            return Some((Ok(sealed.size), State::Done));
                        }

                        let now = tokio::time::Instant::now();
                        if now >= deadline {
                            break;
                        }
                        backoff =
                            std::cmp::min(backoff * 2, deadline.saturating_duration_since(now));
                    }

                    let path = sealed.path.clone();
                    let result = tokio::task::spawn_blocking(move || try_compress(&path))
                        .await
                        .expect("compress task must not panic");

                    match result {
                        Ok(new_size) => {
                            let reclaimed = sealed.size.saturating_sub(new_size);
                            sealed.size = new_size;
                            Some((Ok(reclaimed), State::Compressed(sealed)))
                        }
                        Err(err) if is_not_found(&err) => {
                            let size = sealed.size;
                            sealed.disarm();
                            Some((Ok(size), State::Done))
                        }
                        Err(err) => Some((
                            Err(err
                                .context(format!("compressing sealed segment {:?}", sealed.path))),
                            State::Done,
                        )),
                    }
                }

                State::Compressed(mut sealed) => {
                    // Poll with increasing backoff until the reader unlinks the file.
                    let mut backoff = Duration::from_secs(1);

                    loop {
                        tokio::time::sleep(backoff).await;

                        if !sealed.path.exists() {
                            sealed.disarm();
                            return Some((Ok(sealed.size), State::Done));
                        }
                        backoff = (backoff * 2).min(MAX_EXISTS_BACKOFF);
                    }
                }

                State::Done => None,
            }
        })
    }
}

impl Drop for SealedSegment {
    fn drop(&mut self) {
        if self.armed {
            match std::fs::remove_file(&self.path) {
                Ok(()) => tracing::debug!(path=?self.path, "unlinked sealed segment"),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!(path=?self.path, "lost race to unlink sealed segment");
                }
                Err(err) => {
                    tracing::warn!(%err, path=?self.path, "failed to unlink sealed segment")
                }
            }
        }
    }
}

/// Internal state machine for the sealed-segment stream.
enum State {
    /// Segment is uncompressed; waiting to check existence or compress.
    Pending(SealedSegment),
    /// Segment has been compressed; polling for reader to unlink it.
    Compressed(SealedSegment),
    /// Terminal; the file has been unlinked (or was never present).
    Done,
}

fn is_not_found(err: &anyhow::Error) -> bool {
    err.downcast_ref::<std::io::Error>()
        .is_some_and(|e| e.kind() == std::io::ErrorKind::NotFound)
}

/// Compress an uncompressed segment by rewriting it with LZ4-compressed block
/// payloads, then atomically replace the original file.
/// Returns the new (compressed) file size.
fn try_compress(path: &std::path::Path) -> anyhow::Result<u64> {
    let mut segment = log::reader::Segment::open(path)?;

    let dir = path
        .parent()
        .context("segment path must have a parent directory")?;
    let filename = path
        .file_name()
        .context("segment path must have a filename")?;
    let tmp_path = dir.join(format!(".compress-{}", filename.to_string_lossy()));

    let mut tmp_file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .with_context(|| format!("creating temp file {tmp_path:?}"))?;

    let mut offset: u32 = 0;
    let mut new_size: u64 = 0;

    while let Some((raw_len, lz4_len)) = segment.read_block_header(offset)? {
        let payload_offset = offset
            .checked_add(log::BLOCK_HEADER_LEN as u32)
            .context("segment file overflows 4GB after header read")?;
        let payload_len = if lz4_len > 0 { lz4_len } else { raw_len };

        // read_block_payload decompresses if needed, giving us raw bytes
        // regardless of whether the block was already compressed.
        let raw = segment.read_block_payload(payload_offset, raw_len, lz4_len)?;
        let compressed = log::lz4_compress(&raw)?;

        let lz4_len: u32 = compressed
            .len()
            .try_into()
            .context("compressed block exceeds u32::MAX bytes")?;

        let raw_len = raw_len.to_be_bytes();
        let lz4_len = lz4_len.to_be_bytes();

        tmp_file.write_all(&[
            raw_len[0], raw_len[1], raw_len[2], raw_len[3], // Raw length u32, big-endian.
            lz4_len[0], lz4_len[1], lz4_len[2], lz4_len[3], // LZ4 length u32, big-endian.
        ])?;
        tmp_file.write_all(&compressed)?;
        new_size += (log::BLOCK_HEADER_LEN + compressed.len()) as u64;

        offset = payload_offset
            .checked_add(payload_len)
            .context("segment file overflows 4GB after payload read")?;
    }
    drop(tmp_file);

    // Disarm so Segment::drop doesn't unlink the file we're about to replace.
    segment.disarm();
    drop(segment);

    // Atomically replace the original file with the compressed version.
    rename_exchange(&tmp_path, path)
        .with_context(|| format!("atomic rename of compressed segment {tmp_path:?} -> {path:?}"))?;

    Ok(new_size)
}

/// Atomically swap `from` (compressed temp file) into `to` (the segment path),
/// then unlink the old content. On Linux, uses `renameat2(RENAME_EXCHANGE)` for
/// atomicity. On other platforms, falls back to a racy exists-check + rename.
#[cfg(target_os = "linux")]
fn rename_exchange(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let from_c = CString::new(from.as_os_str().as_bytes()).map_err(std::io::Error::other)?;
    let to_c = CString::new(to.as_os_str().as_bytes()).map_err(std::io::Error::other)?;

    // RENAME_EXCHANGE atomically swaps the two directory entries.
    let ret = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            from_c.as_ptr(),
            libc::AT_FDCWD,
            to_c.as_ptr(),
            libc::RENAME_EXCHANGE,
        )
    };
    if ret != 0 {
        let err = std::io::Error::last_os_error();
        // Clean up temp file on failure.
        let _ = std::fs::remove_file(from);
        return Err(err);
    }

    // `from` now points to the old (uncompressed) content; unlink it.
    // Readers who already opened the old inode keep their handle.
    std::fs::remove_file(from)
}

#[cfg(not(target_os = "linux"))]
fn rename_exchange(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    // Non-atomic fallback for macOS/dev environments.
    if !to.exists() {
        let _ = std::fs::remove_file(from);
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "reader unlinked segment before compression could replace it",
        ));
    }
    std::fs::rename(from, to)
}

/// Delay before attempting compression of a sealed segment (2.5 seconds).
const COMPRESS_AFTER: std::time::Duration = std::time::Duration::from_millis(2_500);
/// Maximum backoff between sealed-segment existence checks.
const MAX_EXISTS_BACKOFF: std::time::Duration = std::time::Duration::from_secs(60);

#[cfg(test)]
mod test {
    use super::*;
    use crate::log::{self, BLOCK_HEADER_LEN, block, reader::Segment};

    #[test]
    fn test_try_compress_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = log::Writer::with_thresholds(dir.path(), 0, usize::MAX, u64::MAX).unwrap();

        // Write two blocks to segment 1.
        let (lsn1, _) = {
            use crate::log::block::BlockMeta;
            use proto_gazette::uuid;
            use std::collections::HashMap;

            let journals: HashMap<String, u16> = [("j/one".to_string(), 0)].into();
            let producers: HashMap<uuid::Producer, u16> =
                [(uuid::Producer([0x01, 0, 0, 0, 0, 0x01]), 0)].into();

            let alloc = doc::HeapNode::new_allocator();
            let node = doc::HeapNode::from_serde(&serde_json::json!({"k": "v"}), &alloc).unwrap();
            let doc_bytes = bytes::Bytes::from(node.to_archive().to_vec());

            let entries = vec![(
                BlockMeta {
                    binding: 0,
                    journal_bid: 0,
                    producer_bid: 0,
                    flags: 0x0001,
                    clock: 100,
                },
                0i64,
                bytes::Bytes::from_static(b"packed_key_______"),
                doc_bytes,
            )];
            writer.append_block(journals, producers, entries).unwrap()
        };

        let (lsn2, _) = {
            use crate::log::block::BlockMeta;
            use proto_gazette::uuid;
            use std::collections::HashMap;

            let journals: HashMap<String, u16> = [("j/two".to_string(), 0)].into();
            let producers: HashMap<uuid::Producer, u16> =
                [(uuid::Producer([0x02, 0, 0, 0, 0, 0x02]), 0)].into();

            let alloc = doc::HeapNode::new_allocator();
            let node = doc::HeapNode::from_serde(&serde_json::json!({"k2": "v2"}), &alloc).unwrap();
            let doc_bytes = bytes::Bytes::from(node.to_archive().to_vec());

            let entries = vec![(
                BlockMeta {
                    binding: 1,
                    journal_bid: 0,
                    producer_bid: 0,
                    flags: 0x0001,
                    clock: 200,
                },
                10i64,
                bytes::Bytes::from_static(b"packed_key2______"),
                doc_bytes,
            )];
            writer.append_block(journals, producers, entries).unwrap()
        };

        assert_eq!(lsn1, log::Lsn::new(1, 0));
        assert_eq!(lsn2, log::Lsn::new(1, 1));

        let seg_path = log::segment_path(dir.path(), 0, 1);
        let original_size = std::fs::metadata(&seg_path).unwrap().len();

        let new_size = try_compress(&seg_path).unwrap();

        assert!(new_size <= original_size);

        // Verify the compressed file is readable by Segment.
        let seg = Segment::open(&log::segment_path(dir.path(), 0, 1)).unwrap();

        // First block: should now be LZ4-compressed.
        let (raw_len, lz4_len) = seg.read_block_header(0).unwrap().unwrap();
        assert!(lz4_len > 0, "block should be compressed after try_compress");
        assert!(raw_len > 0);

        let buf = seg
            .read_block_payload(BLOCK_HEADER_LEN as u32, raw_len, lz4_len)
            .unwrap();
        let archived = rkyv::access::<block::ArchivedBlock, rkyv::rancor::Error>(&buf).unwrap();
        assert_eq!(archived.meta[0].clock.to_native(), 100);

        // Second block.
        let offset2 = (BLOCK_HEADER_LEN + lz4_len as usize) as u32;
        let (raw_len2, lz4_len2) = seg.read_block_header(offset2).unwrap().unwrap();
        assert!(lz4_len2 > 0);

        let buf2 = seg
            .read_block_payload(offset2 + BLOCK_HEADER_LEN as u32, raw_len2, lz4_len2)
            .unwrap();
        let archived2 = rkyv::access::<block::ArchivedBlock, rkyv::rancor::Error>(&buf2).unwrap();
        assert_eq!(archived2.meta[0].clock.to_native(), 200);
    }

    #[test]
    fn test_try_compress_race_lost() {
        let dir = tempfile::tempdir().unwrap();
        let seg_path = dir.path().join("mem-000-seg-000000000001.flog");

        // File doesn't exist — try_compress should fail with NotFound.
        let result = try_compress(&seg_path);
        assert!(result.is_err());
        assert!(is_not_found(&result.unwrap_err()));
    }

    #[test]
    fn test_rename_exchange_target_removed() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("from");
        let to = dir.path().join("to");

        std::fs::write(&from, b"new content").unwrap();
        // `to` doesn't exist — should report NotFound.

        let err = rename_exchange(&from, &to).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn test_rename_exchange_success() {
        let dir = tempfile::tempdir().unwrap();
        let from = dir.path().join("from");
        let to = dir.path().join("to");

        std::fs::write(&from, b"compressed").unwrap();
        std::fs::write(&to, b"original").unwrap();

        rename_exchange(&from, &to).unwrap();

        // `to` should now have compressed content.
        assert_eq!(std::fs::read(&to).unwrap(), b"compressed");
        // `from` should be gone (unlinked after exchange on Linux, or replaced on other platforms).
        assert!(!from.exists());
    }
}
