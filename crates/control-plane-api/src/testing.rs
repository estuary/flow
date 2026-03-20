pub mod mock_connectors;
mod server;
mod snapshot;

use std::sync::Arc;

use crate::{App, Snapshot};
use crate::proxy_connectors::{DiscoverConnectors, MakeConnectors};

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

pub struct TestApp {
    pg_pool: sqlx::PgPool,
    app: Arc<App>,
    tempdir: tempfile::TempDir,
}

pub fn new_test_app(
    pg_pool: sqlx::PgPool,
    snapshot: Arc<dyn tokens::Watch<Snapshot>>,
    connectors: impl MakeConnectors + DiscoverConnectors,
) -> TestApp {
    let jwt_secret = vec![0u8; 32]; // Test JWT secret
    let id_gen = ;

    // Used to store build databases from publications
    let tempdir = tempdir().expect("Failed to create tempdir");
    let builder = crate::publications::builds::new_builder(connectors);
    let publisher = Publisher::new(
        "/not/a/real/flowctl-go".into(),
        &url::Url::from_directory_path(tempdir.path()).unwrap(),
        "some-connector-network",
        &logs_tx,
        pg_pool.clone(),
        models::IdGenerator::new(1),
        builder,
    )
    .with_skip_all_tests();

    let app = Arc::new(control_plane_api::App::new(
        models::IdGenerator::new(2),
        &jwt_secret,
        pg_pool,
        publisher,
        snapshot,
    ));
    TestApp {
        pg_pool,
        app,
        tempdir,
    }
}
