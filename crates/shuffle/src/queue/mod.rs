//! Queue RPC: receives documents from Slices, merges them by priority/clock,
//! and writes to on-disk storage. The Queue protocol handles enqueue and flush
//! only. Consumer Dequeue is out-of-band.
//!
//! Dequeue contract: the coordinator reads documents from queue files up to
//! the `last_commit` clock reported in the NextCheckpoint delta.
//!
//! Documents from rolled-back transactions (where the producer's ACK never
//! advances past their clock) remain in queue files but are never dequeued.
//! They idle harmlessly until the session ends and queue files are cleaned up.

use futures::stream::BoxStream;
use proto_flow::shuffle;
use tokio::sync::mpsc;

mod actor;
mod handler;
mod heap;
mod state;
pub(crate) use handler::serve_queue;

/// QueueJoin coordinates multiple Slice streams connecting to the same Queue.
/// Each Queue member receives connections from all Slices (M connections total).
pub(crate) struct QueueJoin {
    members: Vec<
        Option<(
            BoxStream<'static, tonic::Result<shuffle::QueueRequest>>,
            mpsc::Sender<tonic::Result<shuffle::QueueResponse>>,
        )>,
    >,
}
