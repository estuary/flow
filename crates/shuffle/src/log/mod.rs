//! Log RPC: receives documents from Slices, merges them by priority/clock,
//! and writes to on-disk storage. The Log protocol handles append and flush
//! only. Consumer dequeue is out-of-band.
//!
//! Dequeue contract: the coordinator reads documents from log files up to
//! the `last_commit` clock reported in the NextCheckpoint delta.
//!
//! Documents from rolled-back transactions (where the producer's ACK never
//! advances past their clock) remain in log files but are never dequeued.
//! They idle harmlessly until the session ends and log files are cleaned up.

use futures::stream::BoxStream;
use proto_flow::shuffle;
use tokio::sync::mpsc;

/// Log Sequence Number identifies a specific block within a segmented log.
///
/// The high 6 bytes identify a segment file number and the low 2 bytes
/// identify a block number within that file. This gives up to 2^48 segment
/// files with up to 65,536 blocks per segment.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct Lsn(u64);

impl Lsn {
    /// Build an LSN from a segment file number and block number.
    pub const fn new(segment: u64, block: u16) -> Self {
        Self(segment << Self::BLOCK_BITS | block as u64)
    }

    /// Segment file number (high 6 bytes).
    pub fn segment(self) -> u64 {
        self.0 >> Self::BLOCK_BITS
    }

    /// Block number within the segment file (low 2 bytes).
    pub fn block(self) -> u16 {
        self.0 as u16
    }

    /// Return the LSN of the next block in the same segment.
    pub fn next_block(self) -> Self {
        debug_assert!(
            self.block() < u16::MAX,
            "block number overflow; segment should have rolled"
        );
        Self::new(self.segment(), self.block() + 1)
    }

    /// Return the LSN of the first block in the next segment.
    pub fn next_segment(self) -> Self {
        Self::new(self.segment() + 1, 0)
    }

    /// Map to native u64.
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Map from native u64.
    pub fn from_u64(raw: u64) -> Self {
        Self(raw)
    }

    /// Number of low bits which represent the block number.
    /// Remaining 48 high bits are the segment number.
    pub const BLOCK_BITS: u32 = 16;

    /// The zero-valued LSN, which comes before any actual log block.
    pub const ZERO: Self = Self::new(0, 0);
}

impl std::fmt::Debug for Lsn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.segment(), self.block())
    }
}

mod actor;
mod block;
mod handler;
mod heap;
pub mod reader;
mod state;
pub mod writer;

pub use reader::{FrontierScan, Reader, Remainder};
pub use writer::Writer;

pub(crate) use handler::serve_log;

/// Build the path of a segment file from its directory, shard index, and segment number.
pub(crate) fn segment_path(
    directory: &std::path::Path,
    shard_index: u32,
    segment: u64,
) -> std::path::PathBuf {
    let filename = format!("mem-{shard_index:03}-seg-{segment:012x}.flog");
    directory.join(filename)
}

/// Remove all on-disk log segment files for `shard_index` within `directory`.
///
/// Segments are matched by the `mem-{shard_index:03}-seg-*.flog` naming of
/// `segment_path`, scoped to this shard; files of sibling shards and transient
/// `.compress-*` files are left untouched. A never-created or already-removed
/// directory is treated as success, and per-file `NotFound` is tolerated — the
/// owning Log's `SealedSegment` / `Writer` `Drop`s may race these unlinks.
///
/// This is the escape hatch for tearing down a back-pressured Session. A Log
/// engages disk back-pressure once its sealed-segment backlog exceeds the
/// configured threshold, and releases it only as those segments are reclaimed —
/// normally by a shard worker consuming its local log and unlinking what it has
/// read. A coordinator shutting down stops consuming, so that back-pressure (and
/// the Slice EOF propagation it blocks) would never release on its own. Removing
/// a shard's segment files makes the Log's reclaim observe the unlinks and drop
/// its backlog below the threshold, so it resumes draining and reaches EOF. See
/// the crate README "Shutdown" notes.
pub fn remove_shard_segments(directory: &std::path::Path, shard_index: u32) -> anyhow::Result<()> {
    use anyhow::Context;

    // Matches the `mem-{shard_index:03}-seg-{segment:012x}.flog` naming of
    // `segment_path`, scoped to this shard.
    let prefix = format!("mem-{shard_index:03}-seg-");

    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => {
            return Err(anyhow::Error::new(err))
                .with_context(|| format!("reading shuffle log directory {directory:?}"));
        }
    };

    for entry in entries {
        let entry =
            entry.with_context(|| format!("listing shuffle log directory {directory:?}"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };

        if !(name.starts_with(&prefix) && name.ends_with(".flog")) {
            continue;
        }
        let path = entry.path();

        match std::fs::remove_file(&path) {
            Ok(()) => tracing::debug!(?path, "removed shuffle log segment on Stop"),
            // Raced the Log RPC's own SealedSegment/Writer unlink — benign.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(anyhow::Error::new(err))
                    .with_context(|| format!("removing shuffle log segment {path:?}"));
            }
        }
    }

    Ok(())
}

/// LZ4-compress a raw block payload, returning the compressed bytes.
pub(crate) fn lz4_compress(raw: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(lz4::block::compress_bound(raw.len())?);
    // Safety: compress_to_buffer treats the buffer as output-only.
    unsafe { buf.set_len(buf.capacity()) };
    let n = lz4::block::compress_to_buffer(
        raw,
        Some(lz4::block::CompressionMode::DEFAULT),
        false,
        &mut buf,
    )?;
    // Safety: compress_to_buffer initialized exactly n bytes.
    unsafe { buf.set_len(n) };
    Ok(buf)
}

/// Block header: two u32 big-endian values (raw_len, lz4_len).
pub const BLOCK_HEADER_LEN: usize = 8;

/// LogJoin coordinates multiple Slice streams connecting to the same Log.
/// Each Log shard receives connections from all Slices (M connections total).
pub(crate) struct LogJoin {
    shards: Vec<
        Option<(
            BoxStream<'static, tonic::Result<shuffle::LogRequest>>,
            mpsc::Sender<tonic::Result<shuffle::LogResponse>>,
        )>,
    >,
}

#[derive(Clone)]
pub(crate) struct Metrics {
    /// Total Append messages drained from the heap into the current block.
    appends: metrics::Counter,
    /// Total source bytes of those Appends (sum of `source_byte_length`),
    /// approximating the bytes of input that resulted in log entries.
    bytes_appended: metrics::Counter,
    /// Total block flushes started (encoded and written, partial or sealed).
    flushes: metrics::Counter,
    /// Total log segments sealed and rolled over.
    segments_sealed: metrics::Counter,
    /// Current on-disk backlog across all living sealed segments. Set on each
    /// segment seal and on each reclaim from compression / unlink.
    disk_backlog_bytes: metrics::Gauge,
}

impl Metrics {
    fn new(shard_id: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "shuffle_log_appends",
                metrics::Unit::Count,
                "Append messages drained from the heap into the current block",
            );
            metrics::describe_counter!(
                "shuffle_log_bytes_appended",
                metrics::Unit::Bytes,
                "source bytes of drained Appends (sum of source_byte_length)",
            );
            metrics::describe_counter!(
                "shuffle_log_flushes",
                metrics::Unit::Count,
                "block flushes started",
            );
            metrics::describe_counter!(
                "shuffle_log_segments_sealed",
                metrics::Unit::Count,
                "log segments sealed and rolled over",
            );
            metrics::describe_gauge!(
                "shuffle_log_disk_backlog_bytes",
                metrics::Unit::Bytes,
                "current on-disk backlog across all living sealed segments",
            );
        });

        Self {
            appends: metrics::counter!("shuffle_log_appends", "shard_id" => shard_id.to_string()),
            bytes_appended: metrics::counter!("shuffle_log_bytes_appended", "shard_id" => shard_id.to_string()),
            flushes: metrics::counter!("shuffle_log_flushes", "shard_id" => shard_id.to_string()),
            segments_sealed: metrics::counter!("shuffle_log_segments_sealed", "shard_id" => shard_id.to_string()),
            disk_backlog_bytes: metrics::gauge!("shuffle_log_disk_backlog_bytes", "shard_id" => shard_id.to_string()),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_remove_shard_segments_is_scoped() {
        let dir = tempfile::tempdir().unwrap();

        // This shard's segments, a sibling shard's segment, a compression temp
        // file, and an unrelated file.
        let ours = [
            super::segment_path(dir.path(), 0, 1),
            super::segment_path(dir.path(), 0, 2),
        ];
        let sibling = super::segment_path(dir.path(), 1, 1);
        let compress_tmp = dir
            .path()
            .join(".compress-mem-000-seg-000000000003.flogAB12");
        let unrelated = dir.path().join("CURRENT");

        for path in ours.iter().chain([&sibling, &compress_tmp, &unrelated]) {
            std::fs::write(path, b"x").unwrap();
        }

        super::remove_shard_segments(dir.path(), 0).unwrap();

        for path in &ours {
            assert!(!path.exists(), "expected {path:?} removed");
        }
        assert!(sibling.exists(), "sibling shard segment must be retained");
        assert!(compress_tmp.exists(), "compression temp must be retained");
        assert!(unrelated.exists(), "unrelated file must be retained");

        // Idempotent: a second call (segments now gone) still succeeds.
        super::remove_shard_segments(dir.path(), 0).unwrap();
    }

    #[test]
    fn test_remove_shard_segments_missing_dir_ok() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("never-created");
        super::remove_shard_segments(&missing, 0).unwrap();
    }
}
