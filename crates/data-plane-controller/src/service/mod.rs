pub mod handlers;
pub mod worker;

use crate::shared::logs;
use anyhow::Context;
use axum::{routing::{get, post}, Router};

#[derive(clap::Parser, Debug, serde::Serialize)]
pub struct ServiceArgs {
    /// URL of the postgres database for logging.
    #[clap(
        long = "database",
        env = "DPC_DATABASE_URL",
        default_value = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )]
    database_url: url::Url,
    /// Path to CA certificate of the database.
    #[clap(long = "database-ca", env = "DPC_DATABASE_CA")]
    database_ca: Option<String>,
    /// Port to listen on for HTTP requests.
    #[clap(long = "port", env = "DPC_PORT", default_value = "8080")]
    port: u16,
}

/// Run the data-plane-controller service (worker).
/// This is the HTTP server that receives execute requests and does the actual work.
pub async fn run_service(args: ServiceArgs) -> anyhow::Result<()> {
    let hostname = std::env::var("HOSTNAME").ok();
    let app_name = if let Some(hostname) = &hostname {
        hostname.as_str()
    } else {
        "data-plane-controller-service"
    };
    tracing::info!(args=?ops::DebugJson(&args), app_name, "service started!");

    let mut pg_options = args
        .database_url
        .as_str()
        .parse::<sqlx::postgres::PgConnectOptions>()
        .context("parsing database URL")?
        .application_name(app_name);

    // If a database CA was provided, require that we use TLS with full cert verification.
    if let Some(ca) = &args.database_ca {
        pg_options = pg_options
            .ssl_mode(sqlx::postgres::PgSslMode::VerifyFull)
            .ssl_root_cert(ca);
    } else {
        // Otherwise, prefer TLS but don't require it.
        pg_options = pg_options.ssl_mode(sqlx::postgres::PgSslMode::Prefer);
    }

    let pg_pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_secs(30))
        .connect_with(pg_options)
        .await
        .context("connecting to database")?;

    // Set up logs sink.
    let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(120);
    let logs_sink_task = tokio::spawn(async move {
        logs::serve_sink(pg_pool, logs_rx)
            .await
            .context("logs sink failed")
    });

    // Build the axum router.
    let app = Router::new()
        .route("/execute", post(handlers::handle_execute))
        .route("/health", get(handlers::handle_health))
        .with_state(logs_tx.clone());

    // Set up graceful shutdown signal.
    let shutdown_signal = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("caught shutdown signal, stopping service...");
            }
            Err(err) => {
                tracing::error!(?err, "error subscribing to shutdown signal");
            }
        }
    };

    // Start the HTTP server.
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", args.port))
        .await
        .context("failed to bind to port")?;

    tracing::info!(port = args.port, "service listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .context("HTTP server failed")?;

    // Drop logs_tx to allow logs_sink to finish.
    std::mem::drop(logs_tx);

    // Wait for logs sink to complete.
    logs_sink_task
        .await
        .context("logs sink task panicked")??;

    tracing::info!("service shut down cleanly");
    Ok(())
}
