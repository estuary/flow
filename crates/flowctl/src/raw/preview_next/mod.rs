//! `flowctl preview` on top of the runtime-next + shuffle stack.
//!
//! Spawns one in-process tonic server hosting both `runtime_next::Service`
//! and `shuffle::Service` on a single ephemeral 127.0.0.1 port, then runs
//! N synthetic shards as tokio tasks each driving one long-lived SessionLoop.
//! Source documents come from real Gazette journal reads (authed via the
//! user's flowctl token); endpoint mutations go to the connector container
//! as in production.
use crate::local_specs;
use anyhow::Context;

mod driver;
mod services;
mod shards;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Preview {
    /// Path or URL to a Flow specification file.
    #[clap(long)]
    source: String,
    /// Name of the task to preview within the Flow specification file.
    /// Required if there are multiple tasks in --source specifications.
    #[clap(long)]
    name: Option<String>,
    /// Number of synthetic shards to drive in parallel. Default 1.
    #[clap(long, default_value = "1")]
    shards: u32,
    /// How long should preview run before gracefully stopping?
    /// The default is no timeout. Note that the task may finish active
    /// transaction activity even after this timeout is reached.
    #[clap(long)]
    timeout: Option<humantime::Duration>,
    /// How many connector sessions should be run, and what is the target number
    /// of transactions for each session?
    ///
    /// Sessions are specified as a comma-separated list of the number of
    /// transactions for the ordered session. For a given session, a value less
    /// than zero means "unlimited transactions", though the session will still
    /// end upon a connector exit / EOF (when a capture) or timeout.
    ///
    /// For example, to run three sessions consisting of two transactions,
    /// then one transaction, and then unlimited transactions,
    /// use argument `--sessions 2,1,-1`.
    ///
    /// A session is stopped and the next started upon reaching the target number
    /// of transactions, or upon a timeout, or if the connector exits.
    ///
    /// The default is a single session with an unbounded number of transactions.
    #[clap(long, value_parser, value_delimiter = ',')]
    sessions: Option<Vec<isize>>,
    /// Docker network to run connector images.
    #[clap(long, default_value = "bridge")]
    network: String,
    /// Output task logs in JSON format to stderr.
    #[clap(long, action)]
    log_json: bool,
}

impl Preview {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let Self {
            source,
            name,
            shards,
            timeout,
            sessions,
            network,
            log_json,
        } = self;

        let source_url = build::arg_source_to_url(source, false)?;

        let log_handler: fn(&::ops::Log) = if *log_json {
            ::ops::stderr_log_handler
        } else {
            ::ops::tracing_log_handler
        };

        let (_sources, _live, validations) = local_specs::load_and_validate_full(
            &ctx.client,
            source_url.as_str(),
            network,
            log_handler,
        )
        .await?;

        let spec = resolve_materialization(&validations, name.as_deref())?;

        let timeout = timeout.map(|i| i.into());

        let session_targets: Vec<u32> = if let Some(s) = sessions {
            s.iter()
                .map(|i| {
                    if *i < 0 {
                        Ok(0)
                    } else {
                        u32::try_from(*i).context("--sessions values must fit in uint32")
                    }
                })
                .collect::<anyhow::Result<_>>()?
        } else {
            vec![0]
        };

        let run = services::Run::start(ctx, network.clone(), *log_json, *shards).await?;
        let stop_token = tokio_util::sync::CancellationToken::new();

        let session_loop = driver::run_sessions(&run, &spec, session_targets, stop_token.clone());
        tokio::pin!(session_loop);

        if let Some(timeout) = timeout {
            tokio::select! {
                result = &mut session_loop => result?,
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Ctrl-C received; aborting in-flight session");
                }
                _ = tokio::time::sleep(timeout) => {
                    tracing::info!(?timeout, "preview --timeout reached; stopping active session");
                    stop_token.cancel();
                    session_loop.await?;
                }
            }
        } else {
            tokio::select! {
                result = &mut session_loop => result?,
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("Ctrl-C received; aborting in-flight session");
                }
            }
        }

        // `run` drops here, aborting the tonic server and removing the
        // RocksDB / shuffle-log tempdirs.
        Ok(())
    }
}

fn resolve_materialization(
    validations: &tables::Validations,
    name: Option<&str>,
) -> anyhow::Result<proto_flow::flow::MaterializationSpec> {
    let num_tasks = validations.built_captures.len()
        + validations.built_materializations.len()
        + validations
            .built_collections
            .iter()
            .filter(|c| {
                c.spec
                    .as_ref()
                    .map(|s| s.derivation.is_some())
                    .unwrap_or_default()
            })
            .count();

    if num_tasks == 0 {
        anyhow::bail!(
            "sourced specification files do not contain any tasks (captures, derivations, or materializations)",
        );
    }
    if num_tasks > 1 && name.is_none() {
        anyhow::bail!(
            "sourced specification files contain multiple tasks; use --name to identify a materialization",
        );
    }

    // Fail fast if the named target is a capture or a derivation.
    if let Some(target) = name {
        if validations
            .built_captures
            .iter()
            .any(|c| c.capture.as_str() == target)
        {
            anyhow::bail!(
                "runtime-next preview supports materializations only; capture and derivation will be re-added before upstream merge",
            );
        }
        if validations.built_collections.iter().any(|c| {
            c.collection.as_str() == target
                && c.spec
                    .as_ref()
                    .map(|s| s.derivation.is_some())
                    .unwrap_or(false)
        }) {
            anyhow::bail!(
                "runtime-next preview supports materializations only; capture and derivation will be re-added before upstream merge",
            );
        }
    }

    for row in validations.built_materializations.iter() {
        if let Some(target) = name {
            if row.materialization.as_str() != target {
                continue;
            }
        }
        let Some(spec) = &row.spec else { continue };
        return Ok(spec.clone());
    }

    if let Some(target) = name {
        anyhow::bail!("could not find materialization {target}");
    }
    anyhow::bail!(
        "no materialization in source; runtime-next preview supports materializations only",
    );
}
