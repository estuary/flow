use futures::stream::BoxStream;
use proto_flow::shuffle;
use tokio::sync::mpsc;

mod actor;
mod handler;
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
