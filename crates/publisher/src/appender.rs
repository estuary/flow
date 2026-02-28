use futures::StreamExt;
use proto_gazette::broker;
use std::sync::Arc;

/// An Appender manages a pipeline of sequential appends directed to a single journal.
pub struct Appender {
    /// A buffer of data which is to be appended. Clients directly mutate `buffer`,
    /// encoding data as they see fit, and call `checkpoint()` at message boundaries.
    pub buffer: bytes::BytesMut,
    /// Journal to which this Appender will append.
    journal: Box<str>,
    /// Client used for append RPCs.
    client: gazette::journal::Client,
    /// A barrier is a monotonic counter that increments with each call to barrier(),
    /// and is used to understand when all appends through the barrier() call are complete.
    barrier: usize,
    /// A watch of the most-recent AppendResponse and completed barrier.
    watch: Arc<dyn tokens::Watch<(broker::AppendResponse, usize)>>,
    /// State of the append pipeline.
    state: AppendState,
    /// Optional registers which must be present for the Append to succeed.
    /// This may be used with cooperative fencing of journals.
    check_registers: Option<Box<broker::LabelSelector>>,
}

type UpdateFn = Box<
    dyn FnMut(
            tonic::Result<(broker::AppendResponse, usize)>,
        ) -> Option<tokens::WaitForCancellationFutureOwned>
        + Send,
>;

enum AppendState {
    /// No append is in flight. The UpdateFn is ready for the next spawned task.
    Idle(UpdateFn),
    /// An append RPC is running in the background.
    InFlight(tokio::task::JoinHandle<UpdateFn>),
    /// Transiently empty while `update` is held on the stack during `start_flush`.
    /// Must not be observed outside of that scope.
    Empty,
}

impl Appender {
    pub fn new(client: gazette::journal::Client, journal: String) -> Self {
        let (watch, update) = tokens::manual::<(broker::AppendResponse, usize)>();
        let mut update: UpdateFn = Box::new(update);

        // Initialize the watch with a zero-valued AppendResponse so it's no longer pending.
        update(Ok((broker::AppendResponse::default(), 0)));
        let (watch, _signal) = watch.into_parts();

        Self {
            journal: journal.into_boxed_str(),
            client,
            barrier: 0,
            buffer: bytes::BytesMut::new(),
            watch,
            state: AppendState::Idle(update),
            check_registers: None,
        }
    }

    /// Checkpoint is called after one or more complete messages have been encoded
    /// into `buffer`. It may start a background append, or it may block until a
    /// currently-running append completes if `buffer` is large.
    ///
    /// Appender guarantees that the entire contents of `buffer` will land
    /// atomically in the journal (all or nothing).
    pub async fn checkpoint(&mut self) -> tonic::Result<()> {
        if let AppendState::InFlight(handle) = &self.state
            && !handle.is_finished()
            && self.buffer.len() < Self::BUFFER_FLUSH_THRESHOLD
        {
            return Ok(()); // Continue to buffer.
        }

        self.start_flush().await
    }

    /// Barrier returns a future that resolves when the contents of `buffer`
    /// and all prior appends have completed, returning success or failure.
    ///
    /// Note that barrier() does not itself spawn a background RPC and
    /// provides no guarantee that the returned Future will ever resolve.
    /// The caller must continue to drive the Appender via checkpoint(),
    /// start_flush(), or flush() to ensure progress.
    ///
    /// Barriers are useful as a share-able synchronization point that
    /// gates a task that cannot begin until the barrier has resolved
    /// (e.x. writing ACKs only after a durable write-ahead log commit).
    pub fn barrier(
        &mut self,
    ) -> impl std::future::Future<Output = tonic::Result<()>> + Send + Sync + 'static {
        self.barrier += 1;

        let target = self.barrier;
        let watch = self.watch.clone();

        async move {
            loop {
                let token = watch.token();
                let (_response, barrier) = token.result()?;

                if *barrier >= target {
                    return Ok(());
                }
                () = token.expired().await;
            }
        }
    }

    /// Begin to flush by first awaiting the completion of an ongoing Append RPC.
    /// Then, if `buffer` is non-empty, start a new background Append RPC for it.
    /// On return `self.buffer` is empty.
    pub async fn start_flush(&mut self) -> tonic::Result<()> {
        let mut update = match std::mem::replace(&mut self.state, AppendState::Empty) {
            AppendState::InFlight(handle) => handle.await.expect("append task panicked"),
            AppendState::Idle(update) => update,
            AppendState::Empty => panic!("AppendState::Empty outside of start_flush"),
        };

        // Verify that all prior appends were successful.
        // An error is terminal: the Appender cannot make further progress.
        let token = self.watch.token();
        let header = match token.result() {
            Ok((last_response, _last_barrier)) => {
                // Use the header of the last response to route our next one.
                last_response.header.clone()
            }
            Err(err) => {
                self.state = AppendState::Idle(update);
                return Err(err);
            }
        };

        // If there's nothing further to append, bail out now.
        if self.buffer.is_empty() {
            self.state = AppendState::Idle(update);
            return Ok(());
        }

        let barrier = self.barrier;
        let buffer = self.buffer.split().freeze();
        let client = self.client.clone();

        let request = proto_gazette::broker::AppendRequest {
            header,
            journal: self.journal.to_string(),
            check_registers: self.check_registers.as_ref().map(Box::as_ref).cloned(),
            ..Default::default()
        };

        let chunk_stream =
            move || {
                let buffer = buffer.clone();
                futures::stream::iter((0..buffer.len()).step_by(Self::CHUNK_SIZE).map(
                    move |offset| {
                        let end = (offset + Self::CHUNK_SIZE).min(buffer.len());
                        Ok(buffer.slice(offset..end))
                    },
                ))
            };

        let handle = tokio::spawn(async move {
            let stream = client.append(request, chunk_stream);

            futures::pin_mut!(stream);
            loop {
                match stream.next().await {
                    Some(Ok(response)) => {
                        update(Ok((response, barrier)));
                        return update;
                    }
                    Some(Err(gazette::RetryError {
                        attempt,
                        inner: err,
                    })) => {
                        let is_transient = match &err {
                            gazette::Error::BrokerStatus(
                                // Transient because a broker is in the process of being assigned
                                // (or would be, if the broker cluster had capacity to assign one).
                                broker::Status::NoJournalPrimaryBroker
                                // Transient because replicas are in the process of being assigned.
                                | broker::Status::InsufficientJournalBrokers
                                // Transient because an operator must resolve this condition.
                                | broker::Status::IndexHasGreaterOffset
                            ) => true,
                            err => err.is_transient(),
                        };

                        if is_transient {
                            tracing::warn!(?attempt, %err, "append failed (will retry)");
                        } else {
                            let err = match err {
                                gazette::Error::Grpc(status) => status,
                                other => tonic::Status::internal(other.to_string()),
                            };
                            update(Err(err));
                            return update;
                        }
                    }
                    None => unreachable!("append stream does not EOF without Ok response"),
                }
            }
        });
        self.state = AppendState::InFlight(handle);

        Ok(())
    }

    /// Start and await the completion of all required Append RPCs to fully drain the Appender.
    /// On return, all background appends have completed and `self.buffer` is empty.
    pub async fn flush(&mut self) -> tonic::Result<()> {
        // If there's an un-started remainder, start a flush for it.
        if !self.buffer.is_empty() {
            () = self.start_flush().await?;
        }
        assert!(self.buffer.is_empty());

        // Await the completion of an in-flight append, if there is one.
        self.start_flush().await
    }

    /// Buffer size after which checkpoint() will block to await the completion
    /// of an in-flight append in order to start a subsequent append.
    pub const BUFFER_FLUSH_THRESHOLD: usize = 1 << 23; // 8 MB

    /// Chunk size for streaming data to the broker during an append RPC.
    const CHUNK_SIZE: usize = 32 << 10; // 32 KB
}

/// Manages a pool of Appenders, separating those that have been recently active
/// from those that are idle. Idle appenders are retained for reuse across cycles.
pub struct AppenderGroup {
    /// Appenders that have been activated via `activate()`.
    active: std::collections::HashMap<Box<str>, Appender>,
    /// Appenders retained from prior cycles for reuse.
    idle: std::collections::HashMap<Box<str>, Appender>,
}

impl AppenderGroup {
    pub fn new() -> Self {
        Self {
            active: std::collections::HashMap::new(),
            idle: std::collections::HashMap::new(),
        }
    }

    /// Get or create an Appender for the named journal, marking it active.
    /// Reuses an idle Appender if one exists, otherwise creates a new one.
    pub fn activate(&mut self, journal: &str, client: &gazette::journal::Client) -> &mut Appender {
        if !self.active.contains_key(journal) {
            let appender = self
                .idle
                .remove(journal)
                .unwrap_or_else(|| Appender::new(client.clone(), journal.to_string()));
            self.active.insert(Box::from(journal), appender);
        }
        self.active.get_mut(journal).unwrap()
    }

    /// Iterate over active Appenders as (journal name, Appender) pairs.
    pub fn active_set(&mut self) -> impl Iterator<Item = (&str, &mut Appender)> {
        self.active.iter_mut().map(|(k, v)| (k.as_ref(), v))
    }

    /// Iterate over idle Appenders as (journal name, Appender) pairs.
    pub fn idle_set(&mut self) -> impl Iterator<Item = (&str, &mut Appender)> {
        self.idle.iter_mut().map(|(k, v)| (k.as_ref(), v))
    }

    /// Flush all active appenders.
    pub async fn flush(&mut self) -> tonic::Result<()> {
        let results = futures::future::join_all(
            self.active
                .values_mut()
                .map(|appender| async move { appender.flush().await }),
        )
        .await;

        for result in results {
            result?;
        }
        Ok(())
    }

    /// Move all active appenders to idle.
    pub fn sweep(&mut self) {
        self.idle.extend(self.active.drain());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Build an Appender whose watch can be driven externally via the returned UpdateFn.
    /// The Appender's state is Empty (tests must never call start_flush()).
    fn test_appender(journal: &str) -> (Appender, UpdateFn) {
        let mut appender = Appender::new(mock_journal_client(), journal.to_string());

        let AppendState::Idle(update) = std::mem::replace(&mut appender.state, AppendState::Empty)
        else {
            unreachable!()
        };
        (appender, update)
    }

    // --- AppenderGroup pool mechanics ---

    #[test]
    fn test_group_activate_and_sweep() {
        let client = mock_journal_client();
        let mut group = AppenderGroup::new();

        assert_eq!(group.active_set().count(), 0);
        assert_eq!(group.idle_set().count(), 0);

        group.activate("journal/a", &client);
        group.activate("journal/b", &client);
        assert_eq!(group.active_set().count(), 2);
        assert_eq!(group.idle_set().count(), 0);

        // Sweep moves active → idle.
        group.sweep();
        assert_eq!(group.active_set().count(), 0);
        assert_eq!(group.idle_set().count(), 2);

        // Re-activate reuses from idle.
        group.activate("journal/a", &client);
        assert_eq!(group.active_set().count(), 1);
        assert_eq!(group.idle_set().count(), 1);

        // Activate a new journal.
        group.activate("journal/c", &client);
        assert_eq!(group.active_set().count(), 2);
        assert_eq!(group.idle_set().count(), 1);
    }

    #[test]
    fn test_group_activate_is_idempotent() {
        let client = mock_journal_client();
        let mut group = AppenderGroup::new();

        group
            .activate("journal/a", &client)
            .buffer
            .extend_from_slice(b"hello");

        // Second activation returns the same appender with buffer intact.
        assert_eq!(
            group.activate("journal/a", &client).buffer.as_ref(),
            b"hello"
        );
        assert_eq!(group.active_set().count(), 1);
    }

    // --- Barrier watch mechanics ---

    #[tokio::test]
    async fn test_barrier_resolves_in_order() {
        let (mut appender, mut update) = test_appender("test/journal");

        let b1 = appender.barrier(); // target = 1
        let b2 = tokio::spawn(appender.barrier()); // target = 2
        let b3 = tokio::spawn(appender.barrier()); // target = 3

        // Advance watch to barrier 1.
        update(Ok((broker::AppendResponse::default(), 1)));
        b1.await.unwrap();

        // Yield to let b2/b3 poll — they should still be pending.
        tokio::task::yield_now().await;
        assert!(!b2.is_finished(), "b2 resolved before barrier 2");
        assert!(!b3.is_finished(), "b3 resolved before barrier 3");

        // Advance watch to barrier 3, skipping 2.
        update(Ok((broker::AppendResponse::default(), 3)));
        b2.await.unwrap().unwrap();
        b3.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_barrier_propagates_error() {
        let (mut appender, mut update) = test_appender("test/journal");

        let b1 = appender.barrier();
        let b2 = appender.barrier();

        update(Err(tonic::Status::internal("simulated failure")));

        let err = b1.await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
        assert!(err.message().contains("simulated failure"));

        // Subsequent barriers see the same error.
        let err = b2.await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    fn mock_journal_client() -> gazette::journal::Client {
        gazette::journal::Client::new(
            "http://localhost:0".to_string(),
            gazette::journal::Client::new_fragment_client(),
            proto_grpc::Metadata::new(),
            gazette::Router::new("local"),
        )
    }
}
