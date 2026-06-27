//! Checkpoint sourcing for leader sessions.
//!
//! A leader obtains a sequence of checkpoint [`shuffle::Frontier`]s and, for
//! each, reads documents from already-written shuffle log segments up to that
//! Frontier (`shard/*/scan.rs`). Where those Frontiers come from is encapsulated
//! by [`ShuffleSession`]: production and live `flowctl preview` read source
//! journals via an in-process shuffle [`shuffle::SessionClient`].
//!
//! ## Durability contract
//!
//! A Frontier yielded by [`ShuffleSession::recv_checkpoint`] must only reference
//! log content already durably written to the shard directories that the
//! runtime's shard scanners consume. The journal-reading session upholds this
//! (it completes segment flush IO before reporting progress); a fixture must
//! write its segment before feeding the matching Frontier.

/// A source of checkpoint [`shuffle::Frontier`]s for one leader session.
pub trait ShuffleSession: Send + 'static {
    /// Request the next checkpoint without awaiting it. At most one request is
    /// outstanding at a time; pair with [`Self::recv_checkpoint`].
    fn request_checkpoint(&self);

    /// Await the Frontier responding to a prior [`Self::request_checkpoint`].
    ///
    /// Cancel-safe: dropping the returned future before it resolves loses no
    /// checkpoint, so it may be re-awaited across `select!` iterations.
    fn recv_checkpoint(
        &mut self,
    ) -> impl std::future::Future<Output = anyhow::Result<shuffle::Frontier>> + Send;

    /// Cleanly close the session, draining any underlying topology to EOF.
    fn close(self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send;
}

/// Opens a [`ShuffleSession`] for each leader session.
pub trait ShuffleSessionFactory: Send + Sync + 'static {
    /// Concrete per-session shuffle session this factory produces.
    type Session: ShuffleSession;

    fn open(
        &self,
        task: shuffle::proto::Task,
        shards: Vec<shuffle::proto::Shard>,
        resume: shuffle::Frontier,
    ) -> impl std::future::Future<Output = anyhow::Result<Self::Session>> + Send;
}

/// The standard [`ShuffleSessionFactory`]: opens in-process journal-reading
/// shuffle sessions from a [`shuffle::Service`].
pub struct ShuffleServiceFactory {
    service: shuffle::Service,
}

impl ShuffleServiceFactory {
    pub fn new(service: shuffle::Service) -> Self {
        Self { service }
    }
}

impl ShuffleSessionFactory for ShuffleServiceFactory {
    type Session = shuffle::SessionClient;

    async fn open(
        &self,
        task: shuffle::proto::Task,
        shards: Vec<shuffle::proto::Shard>,
        resume: shuffle::Frontier,
    ) -> anyhow::Result<shuffle::SessionClient> {
        shuffle::SessionClient::open(&self.service, task, shards, resume).await
    }
}

// A journal-reading Session is the canonical ShuffleSession.
impl ShuffleSession for shuffle::SessionClient {
    fn request_checkpoint(&self) {
        shuffle::SessionClient::request_checkpoint(self)
    }
    fn recv_checkpoint(
        &mut self,
    ) -> impl std::future::Future<Output = anyhow::Result<shuffle::Frontier>> + Send {
        shuffle::SessionClient::recv_checkpoint(self)
    }
    fn close(self) -> impl std::future::Future<Output = anyhow::Result<()>> + Send {
        shuffle::SessionClient::close(self)
    }
}
