use anyhow::Context;
use futures::TryFutureExt;
use mockall_double::double;

pub mod ansible;
mod controller;
mod logs;
pub mod pulumi;
pub mod repo;
pub mod stack;

pub use controller::Controller;

#[double]
use ansible::Ansible;
#[double]
use pulumi::Pulumi;
#[double]
use repo::Repo;

#[cfg(test)]
mod integration_tests;

#[derive(clap::Parser, Debug, serde::Serialize)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// URL of the postgres database.
    #[clap(
        long = "database",
        env = "DPC_DATABASE_URL",
        default_value = "postgres://postgres:postgres@127.0.0.1:5432/postgres"
    )]
    pub database_url: url::Url,
    /// Path to CA certificate of the database.
    #[clap(long = "database-ca", env = "DPC_DATABASE_CA")]
    pub database_ca: Option<String>,
    /// Number of tasks which may be polled concurrently.
    #[clap(long = "concurrency", env = "DPC_CONCURRENCY", default_value = "1")]
    pub concurrency: u32,
    /// Interval between polls for dequeue-able tasks when otherwise idle.
    #[clap(
        long = "dequeue-interval",
        env = "DPC_DEQUEUE_INTERVAL",
        default_value = "10s"
    )]
    #[serde(with = "humantime_serde")]
    #[arg(value_parser = humantime::parse_duration)]
    pub dequeue_interval: std::time::Duration,
    /// Interval before a running task poll is presumed to have failed.
    /// Tasks updated their heartbeats every half of this interval.
    #[clap(
        long = "heartbeat-timeout",
        env = "DPC_HEARTBEAT_TIMEOUT",
        default_value = "60s"
    )]
    #[serde(with = "humantime_serde")]
    #[arg(value_parser = humantime::parse_duration)]
    pub heartbeat_timeout: std::time::Duration,
    /// Repository to clone for Pulumi and Ansible infrastructure.
    #[clap(
        long = "git-repo",
        env = "DPC_GIT_REPO",
        default_value = "git@github.com:estuary/est-dry-dock.git"
    )]
    pub git_repo: String,
    /// Pulumi secrets provider for encryption of stack secrets.
    #[clap(
        long = "secrets-provider",
        env = "DPC_SECRETS_PROVIDER",
        default_value = "gcpkms://projects/estuary-control/locations/us-central1/keyRings/pulumi/cryptoKeys/state-secrets"
    )]
    pub secrets_provider: String,
    /// Pulumi backend for storage of stack states.
    #[clap(
        long = "state-backend",
        env = "DPC_STATE_BACKEND",
        default_value = "gs://estuary-pulumi"
    )]
    pub state_backend: url::Url,
    /// When running in dry-run mode, the controller performs git checkouts but
    /// merely simulates Pulumi and Ansible commands without actually running them.
    /// It's not required that the Pulumi stacks of data planes actually exist.
    #[clap(long = "dry-run")]
    pub dry_run: bool,
}

async fn run_internal(
    args: Args,
    shutdown: impl std::future::Future<Output = ()>,
) -> anyhow::Result<()> {
    let hostname = std::env::var("HOSTNAME").ok();
    let app_name = if let Some(hostname) = &hostname {
        hostname.as_str()
    } else {
        "data-plane-controller"
    };
    tracing::info!(args=?ops::DebugJson(&args), app_name, "started!");

    let repo = Repo::new(&args.git_repo);

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

    let (logs_tx, logs_rx) = tokio::sync::mpsc::channel(120);
    let logs_sink = logs::serve_sink(pg_pool.clone(), logs_rx).map_err(|err| anyhow::anyhow!(err));

    let server = automations::Server::new()
        .register(controller::Controller {
            dry_run: args.dry_run,
            logs_tx,
            repo,
            secrets_provider: args.secrets_provider,
            state_backend: args.state_backend,
            pulumi: Pulumi::new(),
            ansible: Ansible::new(),
        })
        .serve(
            args.concurrency,
            pg_pool,
            args.dequeue_interval,
            args.heartbeat_timeout,
            shutdown,
            if cfg!(test) { true } else { false },
        );

    let ((), ()) = futures::try_join!(logs_sink, server)?;

    Ok(())
}

pub async fn run(args: Args) -> anyhow::Result<()> {
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

    run_internal(args, shutdown).await
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
    dry_run: bool,
    stream: &str,
    logs_tx: &logs::Tx,
    logs_token: sqlx::types::Uuid,
) -> anyhow::Result<()> {
    cmd.stdin(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());

    let args: Vec<_> = std::iter::once(cmd.get_program())
        .chain(cmd.get_args())
        .map(|s| s.to_os_string())
        .collect();

    logs_tx
        .send(logs::Line {
            token: logs_token,
            stream: "controller".to_string(),
            line: format!("Starting {stream}: {args:?}"),
        })
        .await
        .context("failed to send to logs sink")?;

    tracing::info!(?args, "starting command");

    let status = if dry_run {
        std::process::ExitStatus::default()
    } else {
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
        status
    };
    tracing::info!(?args, %status, "command completed");

    logs_tx
        .send(logs::Line {
            token: logs_token,
            stream: "controller".to_string(),
            line: format!("Completed {stream} ({status}): {args:?}"),
        })
        .await
        .context("failed to send to logs sink")?;

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
