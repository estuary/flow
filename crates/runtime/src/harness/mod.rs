use proto_flow::flow;
use proto_gazette::consumer;

mod capture;
mod derive;
pub mod fixture;
mod materialize;

// Routines for building test harness of captures, derivations,
// and materializations. All test harnesses have the same basic
// shape:
// * `sessions` is the number of times the underlying connector should be re-opened,
//    exercising state & checkpoint recovery and resumption, and the target number
//    of transactions for each session.
// * `delay` is artificial delay added between transactions, simulating back-pressure.
// * `timeout` is how long the task may produce no data before its current session ends,
//    though a next may then start.
pub use capture::run_capture;
pub use derive::run_derive;
pub use materialize::run_materialize;

pub enum Read {
    Document { binding: u32, doc: bytes::Bytes },
    Checkpoint(consumer::Checkpoint),
}

/// Reader is used for derivation and materialization test harnesses.
/// It builds a stream of read collection documents, which may come
/// from a data fixture or represent live journal data.
pub trait Reader: Clone + Send + Sync + 'static {
    type Stream: futures::Stream<Item = anyhow::Result<Read>> + Send + 'static;

    fn start_for_derivation(
        self,
        derivation: &flow::CollectionSpec,
        resume: consumer::Checkpoint,
    ) -> Self::Stream;

    fn start_for_materialization(
        self,
        materialization: &flow::MaterializationSpec,
        resume: consumer::Checkpoint,
    ) -> Self::Stream;
}
