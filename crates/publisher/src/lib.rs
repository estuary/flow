pub mod appender;
pub mod binding;
pub mod intents;
pub mod mapping;
pub mod publisher;
pub mod watch;

pub use appender::{Appender, AppenderGroup};
pub use binding::{Binding, FixedBinding, MappedBinding};
pub use publisher::Publisher;

/// Boxed closure for lazy initialization of a Mapped binding's partitions
/// watch and journal Client.
type MappedClientInit = Box<
    dyn FnOnce() -> (
            gazette::journal::Client,
            tokens::PendingWatch<Vec<watch::PartitionSplit>>,
        ) + Send,
>;

/// Boxed closure for lazy initialization of a Fixed binding's journal Client.
type FixedClientInit = Box<dyn FnOnce() -> gazette::journal::Client + Send>;

/// LazyBindingClient defers initialization of per-binding journal resources
/// until first use.
///
/// Mapped bindings need both a journal Client and a long-lived list-watch
/// stream of partitions. Fixed bindings only need a Client (the journal is
/// known by name; no listing is required).
///
/// An instantiated client and watch each consume background resources:
/// periodic token refreshes for the client, and a long-lived list RPC for the
/// watch. However, many (most?) bindings and collections are infrequently
/// written and a Publisher instance may never interact with the binding during
/// its lifetime, so avoid paying this cost until we know it's needed.
pub(crate) enum LazyBindingClient {
    Mapped(
        std::sync::LazyLock<
            (
                gazette::journal::Client,
                tokens::PendingWatch<Vec<watch::PartitionSplit>>,
            ),
            MappedClientInit,
        >,
    ),
    Fixed(std::sync::LazyLock<gazette::journal::Client, FixedClientInit>),
}

impl LazyBindingClient {
    /// Force initialization and return the underlying journal Client.
    pub(crate) fn client(&self) -> &gazette::journal::Client {
        match self {
            Self::Mapped(lazy) => &lazy.0,
            Self::Fixed(lazy) => &**lazy,
        }
    }
}

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
