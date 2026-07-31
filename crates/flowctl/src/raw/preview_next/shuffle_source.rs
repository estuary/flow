//! Where a preview's source documents come from.
//!
//! `runtime-local` is agnostic about this: it takes whatever
//! [`ShuffleSessionFactory`] the caller hands it. Preview offers two sources, and
//! the choice is a preview concern — a live source needs a logged-in flowctl
//! token to authorize journal reads, which is exactly the kind of dependency the
//! generic layer must not acquire.

use runtime_next::{ShuffleSession, ShuffleSessionFactory};

/// Shuffle-session factory for a preview leader. The [`ShuffleSessionFactory`]
/// seam is monomorphized (`open` / `recv_checkpoint` are `-> impl Future` and
/// `close` takes `self`, so it is not object-safe); this enum lets one leader
/// `Service` host either source — a fixture replay (`--fixture`) or a live
/// in-process journal-reading shuffle Session — chosen per run.
pub enum PreviewShuffleFactory {
    Fixture(runtime_local::segments::FixtureOpener),
    Live(runtime_next::ShuffleServiceFactory),
}

impl ShuffleSessionFactory for PreviewShuffleFactory {
    type Session = PreviewShuffleSession;

    async fn open(
        &self,
        task: shuffle::proto::Task,
        shards: Vec<shuffle::proto::Shard>,
        resume: shuffle::Frontier,
    ) -> anyhow::Result<PreviewShuffleSession> {
        Ok(match self {
            Self::Fixture(f) => PreviewShuffleSession::Fixture(f.open(task, shards, resume).await?),
            Self::Live(f) => PreviewShuffleSession::Live(f.open(task, shards, resume).await?),
        })
    }
}

/// Per-session shuffle source opened by [`PreviewShuffleFactory`].
pub enum PreviewShuffleSession {
    Fixture(runtime_local::segments::FixtureCheckpoints),
    Live(shuffle::SessionClient),
}

impl ShuffleSession for PreviewShuffleSession {
    fn request_checkpoint(&self) {
        match self {
            Self::Fixture(s) => s.request_checkpoint(),
            Self::Live(s) => s.request_checkpoint(),
        }
    }

    async fn recv_checkpoint(&mut self) -> anyhow::Result<shuffle::Frontier> {
        match self {
            Self::Fixture(s) => s.recv_checkpoint().await,
            Self::Live(s) => s.recv_checkpoint().await,
        }
    }

    async fn close(self) -> anyhow::Result<()> {
        match self {
            Self::Fixture(s) => s.close().await,
            Self::Live(s) => s.close().await,
        }
    }
}

/// Build the shuffle source for a preview run, as the `build_shuffle` callback
/// [`runtime_local::services::Run::start_with_shuffle_leader`] expects.
///
/// A fixture preview reads no journals — flowctl writes the segments itself and
/// feeds synthetic frontiers — so it constructs no `shuffle::Service` and needs
/// neither a logged-in token nor a journal client factory. A live preview
/// authenticates and reads source collections from real journals via a loopback
/// `shuffle::Service`.
///
/// Returns the frontier sender for a fixture run (`None` when live), which the
/// caller uses to relay one checkpoint Frontier per fixture transaction.
pub fn build(
    ctx: &mut crate::CliContext,
    fixture: bool,
    registry: &service_kit::Registry,
    peer_endpoint: &str,
) -> anyhow::Result<(
    PreviewShuffleFactory,
    Option<shuffle::Service>,
    Option<tokio::sync::mpsc::UnboundedSender<runtime_local::segments::FixtureItem>>,
)> {
    if fixture {
        let (opener, tx) = runtime_local::segments::fixture_opener();
        return Ok((PreviewShuffleFactory::Fixture(opener), None, Some(tx)));
    }

    anyhow::ensure!(
        ctx.access_token().is_some(),
        "you must be logged in to preview. Try `flowctl auth login`"
    );

    // Share the live, auto-refreshing user-token watch so a long-lived preview
    // re-mints collection authorizations with a currently-valid access token and
    // survives rotation of both token layers.
    let factory = flow_client_next::workflows::user_collection_auth::new_journal_client_factory(
        ctx.rest.clone(),
        models::Capability::Read,
        ctx.router.clone(),
        ctx.user_tokens.clone(),
    );
    let svc = shuffle::Service::new_loopback(peer_endpoint.to_string(), factory, registry.clone());

    Ok((
        PreviewShuffleFactory::Live(runtime_next::ShuffleServiceFactory::new(svc.clone())),
        Some(svc),
        None,
    ))
}
