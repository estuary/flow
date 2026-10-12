mod actor;
mod ledger;

pub(super) use actor::WriterActor;

/// Handle to a tenure's journal writer.
///
/// A writer which fails answers the request which met that failure with it, and
/// refuses every request after that. A tenure ends at its first failure, so the
/// cause reaches a client exactly once.
pub struct Writer {
    commands: tokio::sync::mpsc::Sender<Command>,
    ended: tokio_util::sync::CancellationToken,
    #[cfg(test)]
    author: proto_gazette::uuid::Producer,
}

impl Writer {
    /// Confirm the delta and return its unappended acknowledgement, or `None`
    /// for an empty delta. The caller must first close device admission so
    /// draining the recording channel defines a cut.
    ///
    /// Commands run in order. A prepare queued after an acknowledgement waits
    /// for its confirmation; a second prepare before acknowledgement fails.
    pub async fn prepare(&self) -> anyhow::Result<Option<bytes::Bytes>> {
        self.call(Command::Prepare).await
    }

    /// Append the acknowledgement returned by [`Writer::prepare`], and await the
    /// broker's confirmation of it.
    ///
    /// A delta which completes a recovery horizon also moves the recovery floor
    /// stored on this journal.
    pub async fn acknowledge(&self, ack: bytes::Bytes) -> anyhow::Result<()> {
        self.call(|reply| Command::Acknowledge(ack, reply)).await
    }

    /// Cancel in-flight broker work and discard subsequent mutations. Continue
    /// draining them so teardown can unmount; see `Ledger::taking`.
    pub fn abandon(&self) {
        self.ended.cancel();
    }

    /// Author this tenure installs in the journal's `author` register.
    #[cfg(test)]
    pub fn author(&self) -> proto_gazette::uuid::Producer {
        self.author
    }

    async fn call<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> anyhow::Result<T> {
        let (reply, response) = tokio::sync::oneshot::channel();

        self.commands
            .send(command(reply))
            .await
            .map_err(|_| anyhow::anyhow!("the journal writer has stopped"))?;

        response
            .await
            .map_err(|_| anyhow::anyhow!("the journal writer has stopped"))?
    }
}

enum Command {
    Prepare(Reply<Option<bytes::Bytes>>),
    Acknowledge(bytes::Bytes, Reply<()>),
}

/// How one request's outcome reaches the client which made it.
///
/// A failure is moved rather than shared, and keeps its own causes: a tenure
/// stream derives its error code by downcasting through them, so a writer which
/// a replacement tenure fenced is reported as fenced.
type Reply<T> = tokio::sync::oneshot::Sender<anyhow::Result<T>>;
