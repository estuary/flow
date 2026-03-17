pub mod mock_connectors;
mod server;
mod snapshot;

pub use self::server::TestServer;
pub use self::snapshot::new_snapshot;

pub fn init() -> tracing::subscriber::DefaultGuard {
    // Enable tracing for the test server.
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::level_filters::LevelFilter::DEBUG.into())
                .from_env_lossy(),
        )
        .finish();

    tracing::subscriber::set_default(subscriber)
}
