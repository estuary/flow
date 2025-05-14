use anyhow::Context;
use futures::{FutureExt, TryFutureExt};

pub mod commands;
pub mod controller;
pub mod logs;
pub mod stack;

pub use controller::Controller;
use sqlx::types::uuid;

#[derive(clap::Parser, Debug, serde::Serialize)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// URL of the postgres database.
    #[clap(
        long = "database",
        env = "DPC_DATABASE_URL",
        default_value = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )]
    database_url: url::Url,
    /// Path to CA certificate of the database.
    #[clap(long = "database-ca", env = "DPC_DATABASE_CA")]
    database_ca: Option<String>,
    /// Number of tasks which may be polled concurrently.
    #[clap(long = "concurrency", env = "DPC_CONCURRENCY", default_value = "1")]
    concurrency: u32,
    /// Interval between polls for dequeue-able tasks when otherwise idle.
    #[clap(
        long = "dequeue-interval",
        env = "DPC_DEQUEUE_INTERVAL",
        default_value = "10s"
    )]
    #[serde(with = "humantime_serde")]
    #[arg(value_parser = humantime::parse_duration)]
    dequeue_interval: std::time::Duration,
    /// Interval before a running task poll is presumed to have failed.
    /// Tasks updated their heartbeats every half of this interval.
    #[clap(
        long = "heartbeat-timeout",
        env = "DPC_HEARTBEAT_TIMEOUT",
        default_value = "60s"
    )]
    #[serde(with = "humantime_serde")]
    #[arg(value_parser = humantime::parse_duration)]
    heartbeat_timeout: std::time::Duration,
    /// Repository to clone for Pulumi and Ansible dry_dockstructure.
    #[clap(
        long = "git-repo",
        env = "DPC_GIT_REPO",
        default_value = "git@github.com:estuary/est-dry-dock.git"
    )]
    git_repo: String,
    /// Repository to clone for ops validation
    #[clap(
        long = "ops-git-repo",
        env = "DPC_OPS_GIT_REPO",
        default_value = "git@github.com:estuary/ops.git"
    )]
    ops_git_repo: String,
    /// Pulumi secrets provider for encryption of stack secrets.
    #[clap(
        long = "secrets-provider",
        env = "DPC_SECRETS_PROVIDER",
        default_value = "gcpkms://projects/estuary-control/locations/us-central1/keyRings/pulumi/cryptoKeys/state-secrets"
    )]
    secrets_provider: String,
    /// Pulumi backend for storage of stack states.
    #[clap(
        long = "state-backend",
        env = "DPC_STATE_BACKEND",
        default_value = "gs://estuary-pulumi"
    )]
    state_backend: url::Url,
    /// When running in dry-run mode, the controller performs git checkouts but
    /// merely simulates Pulumi and Ansible commands without actually running them.
    /// It's not required that the Pulumi stacks of data planes actually exist.
    #[clap(long = "dry-run")]
    dry_run: bool,
}

pub async fn run(args: Args) -> anyhow::Result<()> {
    let hostname = std::env::var("HOSTNAME").ok();
    let app_name = if let Some(hostname) = &hostname {
        hostname.as_str()
    } else {
        "data-plane-controller"
    };
    tracing::info!(args=?ops::DebugJson(&args), app_name, "started!");

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

    let dns_ttl = if args.dry_run {
        DNS_TTL_DRY_RUN
    } else {
        DNS_TTL_ACTUAL
    };

    // Build a type-erased EmitLogFn which forwards to the logs sink.
    let emit_log_fn: controller::EmitLogFn = {
        let logs_tx = logs_tx.clone();

        Box::new(
            move |token: uuid::Uuid, stream: &'static str, line: String| {
                let logs_tx = logs_tx.clone();
                async move {
                    logs_tx
                        .send(logs::Line {
                            token,
                            stream: stream.to_string(),
                            line,
                        })
                        .await
                        .context("failed to send to logs sink")
                }
                .boxed()
            },
        )
    };

    // Build a type-erased RunCmdFn which dispatches to commands::dry_run()
    // when running in dry-run mode, or commands::run() otherwise, and forwards
    // to the logs sink.
    let run_cmd_fn: controller::RunCmdFn = if args.dry_run {
        let logs_tx = logs_tx.clone();
        Box::new(move |cmd, capture_stdout, stream, logs_token| {
            commands::dry_run(cmd, capture_stdout, stream, logs_tx.clone(), logs_token).boxed()
        })
    } else {
        let logs_tx = logs_tx.clone();
        Box::new(move |cmd, capture_stdout, stream, logs_token| {
            commands::run(cmd, capture_stdout, stream, logs_tx.clone(), logs_token).boxed()
        })
    };

    let controller = controller::Controller {
        dns_ttl,
        dry_dock_remote: args.git_repo,
        ops_remote: args.ops_git_repo,
        secrets_provider: args.secrets_provider,
        state_backend: args.state_backend,
        emit_log_fn,
        run_cmd_fn,
    };

    let server = automations::Server::new()
        .register(controller::Executor::new(controller))
        .serve(
            args.concurrency,
            pg_pool,
            args.dequeue_interval,
            args.heartbeat_timeout,
            shutdown,
        )
        .map(|()| anyhow::Result::<()>::Ok(()));

    // When `server` finishes it drops `controller`, which in turn drops all
    // outstanding references to `logs_tx`, which allows `logs_sink` to finish.
    std::mem::drop(logs_tx);

    let ((), ()) = futures::try_join!(logs_sink, server)?;

    Ok(())
}

const DNS_TTL_ACTUAL: std::time::Duration = std::time::Duration::from_secs(5 * 60);
const DNS_TTL_DRY_RUN: std::time::Duration = std::time::Duration::from_secs(10);
