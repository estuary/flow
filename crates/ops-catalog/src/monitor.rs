use std::{
    io::{stderr, stdout, Write},
    path,
    time::{Duration, Instant},
};

use anyhow::Context;
use derivative::Derivative;
use tracing::{debug, info};

use crate::{render::Renderer, TenantInfo};

#[derive(Derivative, clap::Args)]
#[derivative(Debug)]
pub struct MonitorArgs {
    /// URL of the postgres database.
    #[derivative(Debug = "ignore")]
    #[clap(
        long = "database",
        env = "DATABASE_URL",
        default_value = "postgres://flow:flow@127.0.0.1:5432/control_development"
    )]
    database_url: url::Url,
    /// Path to CA certificate of the database.
    #[clap(long = "database-ca", env = "DATABASE_CA")]
    database_ca: Option<String>,
    /// Directory that will be used for rendering templates. Will be created if it does not exist.
    ///
    /// It is important that this directory remain stable to allow the flowctl machinery to avoid
    /// re-publishing unchanged specs. If it is changed, the bundled specs will change as the
    /// inlined schema $id values depend on the actual system path in which the specs are bundled.
    /// Changing this value will most likely result in a re-publication of all ops catalog specs.
    #[clap(long = "working-dir", env = "WORKING_DIR")]
    working_dir: String,
    /// Path to directory containing `flowctl`.
    #[clap(long = "bin-dir", env = "BIN_DIR")]
    bin_dir: String,
    /// Access token used by flowctl to authenticate as the ops/ tenant.
    #[clap(long = "flowctl-access-token", env = "FLOWCTL_ACCESS_TOKEN")]
    flowctl_access_token: String,
    /// Profile used by flowctl.
    #[clap(long = "flowctl-profile", env = "FLOWCTL_PROFILE")]
    flowctl_profile: String,
    /// Use the local materialization credentials. Used when running a local development stack.
    #[clap(long = "local", env = "LOCAL", default_value = "true")]
    local: bool,
}

impl MonitorArgs {
    pub async fn run(&self) -> anyhow::Result<()> {
        let bin_dir = std::fs::canonicalize(&self.bin_dir)
            .context("canonicalize --bin-dir")?
            .into_os_string()
            .into_string()
            .expect("os path must be utf8");

        let working_dir = path::Path::new(&self.working_dir);

        let mut pg_options = self
            .database_url
            .as_str()
            .parse::<sqlx::postgres::PgConnectOptions>()
            .context("parsing database URL")?
            .application_name("agent");

        // If a database CA was provided, require that we use TLS with full cert verification.
        if let Some(ca) = &self.database_ca {
            pg_options = pg_options
                .ssl_mode(sqlx::postgres::PgSslMode::VerifyFull)
                .ssl_root_cert(ca);
        } else {
            // Otherwise, prefer TLS but don't require it.
            pg_options = pg_options.ssl_mode(sqlx::postgres::PgSslMode::Prefer);
        }

        let pg_pool = sqlx::postgres::PgPool::connect_with(pg_options)
            .await
            .context("connecting to database")?;

        tracing::info!(
            bin_dir,
            working_dir = &working_dir.to_str().expect("working dir must be utf8"),
            flowctl_profile = &self.flowctl_profile,
            local = &self.local,
            "starting ops-catalog"
        );

        monitor(
            pg_pool,
            bin_dir,
            working_dir,
            self.local,
            &self.flowctl_profile,
            &self.flowctl_access_token,
        )
        .await
    }
}

// Frequency to re-process the ops catalog from the list of tenants in the system. In the future it
// may be useful to listen for changes to the tenants table to trigger this instead of / as well.
const MONITOR_FREQUENCY_SECS: u64 = 60;

async fn monitor(
    pg_pool: sqlx::PgPool,
    bin_dir: String,
    working_dir: &path::Path,
    local: bool,
    profile: &str,
    access_token: &str,
) -> anyhow::Result<()> {
    let renderer = Renderer::new(local, false)?;
    flowctl_auth(&bin_dir, profile, access_token)?;

    loop {
        let now = Instant::now();
        let tenants = get_tenants(pg_pool.clone()).await?;
        if !tenants.is_empty() {
            renderer
                .render(tenants, working_dir)
                .context("rendering templates")?;

            flowctl_publish(&bin_dir, profile, working_dir)
                .context("publishing updated ops specs")?;
        }

        info!(took = ?now.elapsed(), "updated ops catalog");

        // In reality we may end up waiting longer than MONITOR_FREQUENCY_SECS between processing
        // invocations because of the time it takes to actually do the processing. Most of the time
        // process will basically be a no-op so we aren't accounting for that here for the sake of
        // simplicity.
        tokio::time::sleep(Duration::from_secs(MONITOR_FREQUENCY_SECS)).await;
    }
}

async fn get_tenants(pg_pool: sqlx::PgPool) -> anyhow::Result<Vec<TenantInfo>> {
    let res = sqlx::query_as::<_, TenantInfo>(
        r#"
        select tenant, l1_stat_rollup
        from tenants;
        "#,
    )
    .fetch_all(&pg_pool)
    .await
    .context("fetching tenants")?;

    Ok(res)
}

fn flowctl_auth(bin_dir: &str, profile: &str, access_token: &str) -> anyhow::Result<()> {
    info!(bin_dir, profile, "authenticating flowctl with access token");

    run_flowctl_cmd(
        bin_dir,
        profile,
        None,
        &["auth", "token", "--token", access_token],
        false,
    )
}

fn flowctl_publish(bin_dir: &str, profile: &str, working_dir: &path::Path) -> anyhow::Result<()> {
    debug!("publishing ops catalog specs");

    run_flowctl_cmd(
        bin_dir,
        profile,
        Some(working_dir),
        &[
            "catalog",
            "publish",
            "--auto-approve",
            "--source",
            "flow.yaml",
        ],
        true,
    )
}

fn run_flowctl_cmd(
    bin_dir: &str,
    profile: &str,
    current_dir: Option<&path::Path>,
    args: &[&str],
    log_args: bool,
) -> anyhow::Result<()> {
    let mut cmd = std::process::Command::new(format!("{bin_dir}/flowctl"));

    let args = [&["--profile", profile], args].concat();
    for &arg in args.iter() {
        cmd.arg(arg);
    }

    if let Some(dir) = current_dir {
        cmd.current_dir(dir);
    }

    let logged_args = if log_args { args } else { vec!["hidden"] };
    tracing::debug!(?current_dir, ?logged_args, "running flowctl");

    let output = cmd.output().context("failed to run flowctl")?;

    if !output.status.success() {
        stdout()
            .write(output.stdout.as_slice())
            .context("failed to write flowctl output to stdout")?;
        stderr()
            .write(output.stderr.as_slice())
            .context("failed to write flowctl output to stderr")?;
        anyhow::bail!("flowctl failed output logged")
    }

    Ok(())
}
