pub mod appender;
pub mod binding;
pub mod intents;
pub mod mapping;
pub mod publisher;
pub mod watch;

pub use appender::{Appender, AppenderGroup};
pub use binding::Binding;
pub use publisher::Publisher;

/// Boxed closure for lazy initialization of a partitions watch and journal Client.
/// Callers of `Binding::from_collection_spec` provide this to control how the
/// journal Client and partitions watch are created.
type PartitionsClientInit = Box<
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

/// Sanity-check that `intents` is non-empty NDJSON: terminated by a newline,
/// with every line a syntactically-valid JSON document.
pub(crate) fn validate_ndjson(journal: &str, intents: &bytes::Bytes) -> tonic::Result<()> {
    if !matches!(intents.last(), Some(b'\n')) {
        return Err(tonic::Status::internal(format!(
            "invalid ACK intents for {journal}: doesn't end in newline"
        )));
    }
    // Split on '\n' and deserialize each line as an `IgnoredAny` to validate
    // the JSON syntax. The trailing newline produces an empty final line,
    // which we skip to avoid a spurious EOF error.
    for line in intents[..intents.len() - 1].split(|b| *b == b'\n') {
        serde_json::from_slice::<serde::de::IgnoredAny>(line).map_err(|e| {
            tonic::Status::internal(format!(
                "invalid ACK intent for {journal} line {line:?}: {e}"
            ))
        })?;
    }
    Ok(())
}
