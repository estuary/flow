pub mod appender;
pub mod binding;
pub mod intents;
pub mod mapping;
pub mod publisher;
pub mod watch;

pub use appender::{Appender, AppenderGroup};
pub use binding::Binding;
pub use publisher::Publisher;

/// Factory that builds a Gazette journal Client for appends to a Collection
/// on behalf of a task Name. Used by `Binding::from_capture_spec` to lazily
/// create per-binding clients: the factory is called at most once per binding,
/// only when the binding is first written to.
pub type JournalClientFactory = std::sync::Arc<
    dyn Fn(models::Collection, models::Name) -> gazette::journal::Client + Send + Sync,
>;

/// Boxed closure for lazy initialization of a partitions watch and journal Client.
/// Callers of `Binding::from_collection_spec` provide this to control how the
/// journal Client and partitions watch are created.
pub type PartitionsClientInit = Box<
    dyn FnOnce() -> (
            gazette::journal::Client,
            tokens::PendingWatch<Vec<watch::PartitionSplit>>,
        ) + Send,
>;

/// LazyPartitionsClient uses a LazyCell to defer initialization of a partitions
/// watch and a paired journal Client for List, Apply, and Append RPCs.
///
/// An instantiated client and watch each consume background resources:
/// periodic token refreshes for the client, and a long-lived list RPC for the watch.
/// However, many (most?) bindings and collections are infrequently written and
/// a Publisher instance may never interact with the binding during its lifetime,
/// so avoid paying this cost until we know it's needed.
type LazyPartitionsClient = std::sync::LazyLock<
    (
        gazette::journal::Client,
        tokens::PendingWatch<Vec<watch::PartitionSplit>>,
    ),
    PartitionsClientInit,
>;

