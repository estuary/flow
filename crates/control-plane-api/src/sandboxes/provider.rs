//! The interface to a service which hosts sandbox machines.
//!
//! It's deliberately small: some hosts offer only starting and stopping a
//! machine plus one network endpoint. Commands run within a machine through
//! `flowctl raw sandbox-exec` (see `crates/sandbox-exec`), whose protocol is
//! carried by [`Exec`] as opaque bytes.

/// Path of flowctl on each machine.
pub const FLOWCTL_PATH: &str = "/opt/estuary/bin/flowctl";

/// Argv which starts the command wrapper of `crates/sandbox-exec` on a machine.
pub const WRAPPER_ARGV: [&str; 3] = [FLOWCTL_PATH, "raw", "sandbox-exec"];

/// A service which creates, destroys, and runs processes on sandbox machines.
///
/// A machine is named by its handle. The handle is ours: it derives from the
/// sandbox record's id, and the provider uses it to name the machine (Docker
/// uses it as the container name). Identifiers the provider itself assigns
/// belong in [`State`], which is persisted on the sandbox record and passed
/// back on each call.
///
/// [`Error::Gone`] means the machine no longer exists and never will again.
/// It's final: no later call can succeed for that handle and state.
#[async_trait::async_trait]
pub trait Provider: Send + Sync {
    /// Persisted on each sandbox record, selecting the provider that serves it.
    fn kind(&self) -> &'static str;

    /// Creates machine `handle` at its baseline. Returns state to persist.
    async fn provision(&self, handle: &str) -> Result<State, Error>;

    /// Returns false if the machine was already gone.
    async fn destroy(&self, handle: &str, state: &State) -> Result<bool, Error>;

    /// Starts `argv` and connects its stdin, stdout, and stderr.
    /// `argv` never carries user content, so providers may log it.
    ///
    /// Closing the returned [`Exec`]'s stdin is the only teardown signal.
    /// A provider closes the process's stdin when the sink is dropped.
    /// When this process dies, the provider closes it too, or terminates the
    /// process. The process decides how to exit: the command wrapper cancels its
    /// command on stdin EOF or SIGTERM.
    ///
    /// Sends of output are best effort. Dropping the output stream doesn't end
    /// the process: the provider reads and discards the rest of its output, so
    /// it never blocks writing. The provider's resources for the process are
    /// released once it exits.
    async fn exec(&self, handle: &str, state: &State, argv: &[&str]) -> Result<Exec, Error>;

    /// Handles of every machine this provider holds, for reaping orphans.
    // TODO: Add a reaper which destroys each listed machine that no
    // `internal.sandboxes` record has. A `create` which fails can leave such a
    // machine behind.
    async fn list(&self) -> Result<Vec<String>, Error>;
}

/// Provider-assigned identifiers of a machine, persisted on its sandbox record.
pub type State = serde_json::Value;

/// A process started by [`Provider::exec`].
pub struct Exec {
    /// The process's stdin. Dropping it closes the process's stdin:
    /// `SinkExt::close` flushes but doesn't.
    pub stdin: std::pin::Pin<Box<dyn futures::Sink<bytes::Bytes, Error = Error> + Send>>,
    /// The process's output, until it's dropped.
    /// `Output::Exit` or an error is the last item.
    pub output: futures::stream::BoxStream<'static, Result<Output, Error>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    Stdout(bytes::Bytes),
    Stderr(bytes::Bytes),
    /// The process exited with this status. Last item of the stream.
    Exit(i32),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sandbox machine no longer exists")]
    Gone,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
