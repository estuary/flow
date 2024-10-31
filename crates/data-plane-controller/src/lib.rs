use anyhow::Context;
use futures::{FutureExt, TryFutureExt};

mod controller;
mod logs;
mod repo;
mod stack;

pub use controller::Controller;

#[derive(clap::Parser, Debug, serde::Serialize)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// URL of the postgres database.
    #[clap(
        long = "database",
        env = "DPC_DATABASE_URL",
        default_value = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )]
    #[serde(skip_serializing)]
    database_url: url::Url,
    /// Path to CA certificate of the database.
    #[clap(long = "database-ca", env = "DPC_DATABASE_CA")]
    database_ca: Option<String>,
    /// Number of tasks which may be polled concurrently.
    #[clap(long = "concurrency", env = "DPC_CONCURRENCY", default_value = "2")]
    concurrency: u32,
    /// Interval between polls for dequeue-able tasks when otherwise idle.
    #[clap(
        long = "dequeue-interval",
        env = "DPC_DEQUEUE_INTERVAL",
        default_value = "5s"
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
    /// Repository to clone for Pulumi and Ansible infrastructure.
    #[clap(
        long = "git-repo",
        env = "DPC_GIT_REPO",
        default_value = "git@github.com:estuary/est-dry-dock.git"
    )]
    git_repo: String,
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
        .register(controller::Controller {
            logs_tx,
            repo,
            secrets_provider: args.secrets_provider,
            state_backend: args.state_backend,
        })
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

#[derive(Debug)]
struct NonZeroExit {
    status: std::process::ExitStatus,
    cmd: String,
    logs_token: sqlx::types::Uuid,
}

impl std::fmt::Display for NonZeroExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "command {} exited with status {:?} (logs token {})",
            self.cmd, self.status, self.logs_token
        )
    }
}

async fn run_cmd(
    cmd: &mut async_process::Command,
    stream: &str,
    logs_tx: &logs::Tx,
    logs_token: sqlx::types::Uuid,
) -> anyhow::Result<()> {
    cmd.stdin(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());

    logs_tx
        .send(logs::Line {
            token: logs_token,
            stream: "controller".to_string(),
            line: format!("Starting {stream}: {cmd:?}"),
        })
        .await
        .context("failed to send to logs sink")?;

    tracing::info!(?cmd, "starting command");

    let mut child: async_process::Child = cmd.spawn()?.into();

    let stdout = logs::capture_lines(
        logs_tx,
        format!("{stream}:0"),
        logs_token,
        child.stdout.take().unwrap(),
    );
    let stderr = logs::capture_lines(
        logs_tx,
        format!("{stream}:1"),
        logs_token,
        child.stderr.take().unwrap(),
    );

    let ((), (), status) = futures::try_join!(stdout, stderr, child.wait())?;

    tracing::info!(?cmd, ?status, "command completed");

    if !status.success() {
        let err = NonZeroExit {
            cmd: format!("{cmd:?}"),
            logs_token,
            status,
        };
        Err(anyhow::anyhow!(err))
    } else {
        Ok(())
    }
}
