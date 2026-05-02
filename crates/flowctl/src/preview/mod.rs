//! `flowctl preview` on top of the runtime-next + shuffle stack.
//!
//! Spawns one in-process tonic server hosting both `runtime_next::Service`
//! and `shuffle::Service` on a single ephemeral 127.0.0.1 port, then runs
//! N synthetic shards as tokio tasks driving
//! `runtime_next::shard::materialize::handler::serve` over mpsc channels.
//! Source
//! documents come from real Gazette journal reads (authed via the user's
//! flowctl token); endpoint mutations go to the connector container as in
//! production.
//!
//! Materializations only — capture and derivation paths are out of scope
//! for the first runtime-next preview cut.

use crate::local_specs;

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
    /// How long can the task run before this command stops?
    /// The default is no timeout.
    #[clap(long)]
    timeout: Option<humantime::Duration>,
    /// How many connector sessions should be run, and what is the target
    /// number of transactions for each session?
    ///
    /// Sessions are specified as a comma-separated list of the number of
    /// transactions for each ordered session. A value less than zero means
    /// "unlimited transactions". A "transaction" is one
    /// `L:Flush → L:Acknowledge` cycle (not one `next_checkpoint()` —
    /// materialize transactions may span multiple checkpoints).
    ///
    /// For example, `--sessions 2,1,-1` runs three sessions of 2, then 1,
    /// then unlimited transactions.
    ///
    /// Default: a single session with an unbounded number of transactions.
    #[clap(long, value_parser, value_delimiter = ',')]
    sessions: Option<Vec<isize>>,
    /// Docker network to run connector images.
    #[clap(long, default_value = "bridge")]
    network: String,
    /// Output state updates as `["connectorState",{...}]` lines.
    #[clap(long, action)]
    output_state: bool,
    /// Output the resolved connector image as `["applied",{...}]`.
    #[clap(long, action)]
    output_apply: bool,
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
            output_state,
            output_apply,
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

        let timeout = timeout
            .map(|i| i.into())
            .unwrap_or(std::time::Duration::MAX);

        let session_targets: Vec<usize> = if let Some(s) = sessions {
            s.iter()
                .map(|i| usize::try_from(*i).unwrap_or(usize::MAX))
                .collect()
        } else {
            vec![usize::MAX]
        };

        let run = services::Run::start(ctx, network.clone(), *log_json, *shards).await?;
        let stdout = std::sync::Arc::new(std::sync::Mutex::new(std::io::stdout()));

        let session_loop = async {
            for (idx, target_txns) in session_targets.iter().enumerate() {
                tracing::info!(
                    session = idx + 1,
                    of = session_targets.len(),
                    target_txns,
                    "starting preview session",
                );
                driver::run_session(
                    &run,
                    &spec,
                    idx + 1,
                    *target_txns,
                    *output_state,
                    *output_apply,
                    stdout.clone(),
                )
                .await?;
            }
            anyhow::Ok(())
        };

        tokio::select! {
            result = session_loop => result?,
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Ctrl-C received; aborting in-flight session");
            }
            _ = tokio::time::sleep(timeout) => {
                tracing::info!(?timeout, "preview --timeout reached; aborting");
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
