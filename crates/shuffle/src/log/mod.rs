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
        debug_assert!(self.block() < u16::MAX, "block number overflow; segment should have rolled");
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
mod state;
mod writer;

pub(crate) use handler::serve_log;

/// LogJoin coordinates multiple Slice streams connecting to the same Log.
/// Each Log member receives connections from all Slices (M connections total).
pub(crate) struct LogJoin {
    members: Vec<
        Option<(
            BoxStream<'static, tonic::Result<shuffle::LogRequest>>,
            mpsc::Sender<tonic::Result<shuffle::LogResponse>>,
        )>,
    >,
}
