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

/// Log Sequence Number addressing a position within a segmented log.
///
/// The high 6 bytes identify a segment file number and the low 2 bytes
/// identify a block offset within that file. This gives up to 2^48
/// segment files with up to 65,536 blocks per segment.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Lsn(u64);

impl Lsn {
    const BLOCK_BITS: u32 = 16;

    /// Build an LSN from a segment file number and block offset.
    pub fn new(segment: u64, block: u16) -> Self {
        Self(segment << Self::BLOCK_BITS | block as u64)
    }

    /// Segment file number (high 6 bytes).
    pub fn segment(self) -> u64 {
        self.0 >> Self::BLOCK_BITS
    }

    /// Block offset within the segment file (low 2 bytes).
    pub fn block_offset(self) -> u16 {
        self.0 as u16
    }

    pub fn as_u64(self) -> u64 {
        self.0
    }

    pub fn from_u64(raw: u64) -> Self {
        Self(raw)
    }
}

mod actor;
#[allow(dead_code)] // Types are under active development.
mod block;
mod handler;
mod heap;
mod state;
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
