use proto_gazette::broker;

mod actor;
mod handler;
mod heap;
mod listing;
mod producer;
mod read;
mod routing;
mod state;

use actor::SliceActor;
pub(crate) use handler::serve_slice;

/// LazyJournalClient uses a LazyCell to defer initialization of the Client.
///
/// An instantiated Client requires a background task to perform token refreshes,
/// but at scale not every Slice will interact with every binding and collection,
/// so avoid building a Client until we know it's needed.
pub type LazyJournalClient = std::cell::LazyCell<
    gazette::journal::Client,
    Box<dyn FnOnce() -> gazette::journal::Client + Send>,
>;

/// ReadLines using a type-erased inner Stream. Pin-boxed so that `StreamFuture` works
/// (`StreamFuture` requires `Unpin`, which `Pin<Box<T>>` always satisfies).
pub type ReadLines = std::pin::Pin<
    Box<
        gazette::journal::read::ReadLines<
            1_000_000,
            64,
            futures::stream::BoxStream<'static, gazette::RetryResult<broker::ReadResponse>>,
        >,
    >,
>;
