//! `flowctl raw sandbox-exec`: run one command inside a sandbox, speaking the
//! framed protocol of `crates/sandbox-exec` over stdin and stdout.
//!
//! `Cli::run` dispatches it before loading a profile config or refreshing
//! tokens: it needs neither, and stdout carries only frames.

use anyhow::Context;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct SandboxExec {
    /// Directory under which workspace directories are created.
    #[clap(long, default_value = "/work")]
    workspace_root: std::path::PathBuf,
    /// Directory whose contents are copied into each new workspace.
    /// If it's missing, new workspaces are empty.
    #[clap(long, default_value = "/opt/estuary/skel")]
    workspace_skeleton: std::path::PathBuf,
    /// How long a cancelled command has to exit after SIGTERM, before SIGKILL.
    #[clap(long, default_value = "5s", value_parser = humantime::parse_duration)]
    kill_grace: std::time::Duration,
    /// How long to read remaining output after the command exits.
    #[clap(long, default_value = "1s", value_parser = humantime::parse_duration)]
    drain_timeout: std::time::Duration,
}

impl SandboxExec {
    pub async fn run(&self) -> anyhow::Result<()> {
        let config = sandbox_exec::Config {
            workspace_root: self.workspace_root.clone(),
            workspace_skeleton: self.workspace_skeleton.clone(),
            kill_grace: self.kill_grace,
            drain_timeout: self.drain_timeout,
        };

        let install =
            |kind| tokio::signal::unix::signal(kind).context("failed to install signal handler");
        let (mut term, mut hup, mut int) = (
            install(tokio::signal::unix::SignalKind::terminate())?,
            install(tokio::signal::unix::SignalKind::hangup())?,
            install(tokio::signal::unix::SignalKind::interrupt())?,
        );
        let cancel = async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = hup.recv() => {}
                _ = int.recv() => {}
            }
        };

        sandbox_exec::run(&config, tokio::io::stdin(), tokio::io::stdout(), cancel).await
    }
}
