use anyhow::Context;
use futures::{FutureExt, TryFutureExt};

mod controller;
mod logs;
mod repo;

pub use controller::Controller;

/// Agent is a daemon which runs server-side tasks of the Flow control-plane.
#[derive(clap::Parser, Debug, serde::Serialize)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// URL of the postgres database.
    #[clap(
        long = "database",
        env = "DATABASE_URL",
        default_value = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )]
    #[serde(skip_serializing)]
    database_url: url::Url,
    /// Path to CA certificate of the database.
    #[clap(long = "database-ca", env = "DATABASE_CA")]
    database_ca: Option<String>,
    /// Number of tasks which may be polled concurrently.
    #[clap(long = "concurrency", env = "CONCURRENCY", default_value = "1")]
    concurrency: u32,
    /// Interval between polls for dequeue-able tasks when otherwise idle.
    #[clap(
        long = "dequeue-interval",
        env = "DEQUEUE_INTERVAL",
        default_value = "5s"
    )]
    #[serde(with = "humantime_serde")]
    #[arg(value_parser = humantime::parse_duration)]
    dequeue_interval: std::time::Duration,
    /// Interval before a running task poll is presumed to have failed.
    /// Tasks updated their heartbeats every half of this interval.
    #[clap(
        long = "heartbeat-timeout",
        env = "HEARTBEAT_TIMEOUT",
        default_value = "30s"
    )]
    #[serde(with = "humantime_serde")]
    #[arg(value_parser = humantime::parse_duration)]
    heartbeat_timeout: std::time::Duration,
    /// Repository to clone for Pulumi and Ansible infrastructure.
    #[clap(
        long = "git-repo",
        env = "GIT_REPO",
        default_value = "git@github.com:estuary/est-dry-dock.git"
    )]
    git_repo: String,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let hostname = std::env::var("HOSTNAME").ok();
    let app_name = if let Some(hostname) = &hostname {
        hostname.as_str()
    } else {
        "data-plane-controller"
    };
    tracing::info!(args=?ops::DebugJson(&args), app_name, "started!");

    let repo = repo::Repo::new(&args.git_repo);

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
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect_with(pg_options)
        .await
        .context("connecting to database")?;

    let shutdown = async {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("caught shutdown signal, stopping...");
            }
            Err(err) => {
                tracing::error!(?err, "error subscribing to shutdown signal");
            }
        }
    };

    let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(120);
    let logs_sink = logs::serve_sink(pg_pool.clone(), logs_rx).map_err(|err| anyhow::anyhow!(err));

    let server = automations::Server::new()
        .register(controller::Controller { logs_tx, repo })
        .serve(
            args.concurrency,
            pg_pool,
            args.dequeue_interval,
            args.heartbeat_timeout,
            shutdown,
        )
        .map(|()| anyhow::Result::<()>::Ok(()));

    let ((), ()) = futures::try_join!(logs_sink, server)?;

    Ok(())
}
