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

mod actor;
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
