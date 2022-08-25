mod connector_tags;
mod directives;
mod discovers;
mod jobs;
pub mod logs;
mod publications;

pub use agent_sql::Id;
pub use connector_tags::TagHandler;
pub use directives::DirectiveHandler;
pub use discovers::DiscoverHandler;
pub use publications::PublishHandler;

/// Handler is the principal trait implemented by the various task-specific
/// event handlers that the agent runs.
#[async_trait::async_trait]
pub trait Handler {
    async fn handle(&mut self, pg_pool: &sqlx::PgPool) -> anyhow::Result<std::time::Duration>;

    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

// serve one or more Handlers until signaled by a ready |exit| future.
#[tracing::instrument(ret, skip_all)]
pub async fn serve<E>(
    handlers: Vec<Box<dyn Handler>>,
    pg_pool: sqlx::PgPool,
    exit: E,
) -> anyhow::Result<()>
where
    E: std::future::Future<Output = ()> + Send,
{
    let mut now = tokio::time::Instant::now();
    let mut handlers = handlers.into_iter().map(|h| (h, now)).collect::<Vec<_>>();

    tokio::pin!(exit);
    loop {
        // Pick handler with the next deadline.
        let (handler, deadline) = handlers
            .iter_mut()
            .min_by_key(|i| i.1)
            .expect("handlers is not empty");

        // Sleep until its deadline has elapsed.
        let sleep = tokio::time::sleep_until(*deadline);
        tokio::select! {
            _ = &mut exit => {
                tracing::debug!("caught signal; exiting...");
                return Ok(()) // All done.
            }
            _ = sleep => (),
        };

        now = tokio::time::Instant::now();
        let next_interval = handler.handle(&pg_pool).await?;
        tracing::trace!(delay=?now.checked_duration_since(*deadline), ?next_interval, handler = %handler.name(), "invoked handler");

        // Update the handler deadline to reflect its current execution time.
        *deadline = now + next_interval;
    }
}
