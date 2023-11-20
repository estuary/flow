use crate::local_specs;
use anyhow::Context;
use futures::TryStreamExt;
use proto_flow::{capture, derive, flow, materialize};

mod journal_reader;

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
    /// Optional, artificial delay between transactions to simulate back-pressure
    /// and encourage reductions. The default is no delay.
    #[clap(long)]
    delay: Option<humantime::Duration>,
    /// How long can the task produce no data before this command stops?
    /// The default is that there is no timeout.
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
    /// Path to a transactions fixture to use, instead of reading live collections.
    /// Fixtures are used only for derivations and materializations.
    /// They are a JSON array, one item per transaction, of arrays of tuples specifying
    /// a "read" source collection and its document. For example:
    /// [
    ///     [
    ///         ["collection/one", {"foo": 1}],
    ///         ["collection/two", {"bar": 2}]
    ///     ],
    ///     [
    ///         ["collection/one", {"foo": 2}]
    ///     ]
    /// ]
    #[clap(long)]
    fixture: Option<String>,
    /// Docker network to run connector images.
    #[clap(long, default_value = "bridge")]
    network: String,
}

impl Preview {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let Self {
            source,
            name,
            delay,
            timeout,
            sessions,
            fixture,
            network,
        } = self;

        let source = build::arg_source_to_url(source, false)?;
        let client = ctx.controlplane_client().await?;

        // TODO(johnny): validate only `name`, if presented.
        let (_sources, validations) =
            local_specs::load_and_validate_full(client, source.as_str(), &network).await?;

        let runtime = runtime::Runtime::new(
            true, // Allow local.
            network.clone(),
            ops::tracing_log_handler,
            None,
            "preview".to_string(),
        );

        // Default to no delay.
        let delay = delay
            .map(|i| i.clone().into())
            .unwrap_or(std::time::Duration::ZERO);

        // Default to no timeout.
        let timeout = timeout
            .map(|i| i.clone().into())
            .unwrap_or(std::time::Duration::MAX);

        // Negative sessions mean "unlimited transactions", and default to a single unlimited session.
        let sessions = if let Some(sessions) = sessions {
            sessions
                .iter()
                .map(|i| usize::try_from(*i).unwrap_or(usize::MAX))
                .collect()
        } else {
            vec![usize::MAX]
        };

        // Parse a provided data fixture.
        let fixture_reader = if let Some(fixture) = fixture {
            let fixture = std::fs::read(fixture).context("couldn't open fixture file")?;
            let fixture: runtime::harness::fixture::Fixture =
                serde_json::from_slice(&fixture).context("couldn't parse fixture")?;

            Some(runtime::harness::fixture::Reader(fixture))
        } else {
            None
        };
        let journal_reader = journal_reader::Reader::new(ctx.controlplane_client().await?, delay);

        let state = models::RawValue::default();
        let state_dir = tempfile::tempdir().unwrap();

        let num_tasks = validations.built_captures.len()
            + validations.built_materializations.len()
            + validations
                .built_collections
                .iter()
                .filter(|c| c.spec.derivation.is_some())
                .count();

        if num_tasks == 0 {
            anyhow::bail!("sourced specification files do not contain any tasks (captures, derivations, or materializations)");
        } else if num_tasks > 1 && name.is_none() {
            anyhow::bail!("sourced specification files contain multiple tasks (captures, derivations, or materializations). Use --name to identify a specific task");
        }

        for capture in validations.built_captures.iter() {
            if !matches!(name, Some(n) if n == &capture.capture) && name.is_some() {
                continue;
            }
            let mut spec = capture.spec.clone();

            // Disable UUID placeholders.
            for binding in spec.bindings.iter_mut() {
                binding.collection.as_mut().unwrap().uuid_ptr = String::new();
            }

            return preview_capture(
                delay,
                runtime,
                sessions,
                spec,
                state,
                state_dir.path(),
                timeout,
            )
            .await;
        }

        for collection in validations.built_collections.iter() {
            if !matches!(name, Some(n) if n == collection.collection.as_str()) && name.is_some() {
                continue;
            } else if collection.spec.derivation.is_none() && name.is_some() {
                anyhow::bail!("{} is not a derivation", name.as_ref().unwrap());
            } else if collection.spec.derivation.is_none() {
                continue;
            }
            let mut spec = collection.spec.clone();

            // Disable UUID placeholders.
            spec.uuid_ptr = String::new();

            if let Some(reader) = fixture_reader {
                return preview_derivation(
                    reader,
                    runtime,
                    sessions,
                    spec,
                    state,
                    state_dir.path(),
                    timeout,
                )
                .await;
            } else {
                return preview_derivation(
                    journal_reader,
                    runtime,
                    sessions,
                    spec,
                    state,
                    state_dir.path(),
                    timeout,
                )
                .await;
            }
        }

        for materialization in validations.built_materializations.iter() {
            if !matches!(name, Some(n) if n == materialization.materialization.as_str())
                && name.is_some()
            {
                continue;
            }
            let spec = materialization.spec.clone();

            if let Some(reader) = fixture_reader {
                return preview_materialization(
                    reader,
                    runtime,
                    sessions,
                    spec,
                    state,
                    state_dir.path(),
                    timeout,
                )
                .await;
            } else {
                return preview_materialization(
                    journal_reader,
                    runtime,
                    sessions,
                    spec,
                    state,
                    state_dir.path(),
                    timeout,
                )
                .await;
            }
        }

        anyhow::bail!("could not find task {}", name.as_ref().unwrap());
    }
}

async fn preview_capture<L: runtime::LogHandler>(
    delay: std::time::Duration,
    runtime: runtime::Runtime<L>,
    sessions: Vec<usize>,
    spec: flow::CaptureSpec,
    state: models::RawValue,
    state_dir: &std::path::Path,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let responses_rx =
        runtime::harness::run_capture(delay, runtime, sessions, &spec, state, state_dir, timeout);
    tokio::pin!(responses_rx);

    while let Some(response) = responses_rx.try_next().await? {
        let internal = response
            .get_internal()
            .context("failed to decode internal runtime.CaptureResponseExt")?;

        if let Some(capture::response::Applied { action_description }) = response.applied {
            tracing::info!(action_description, "capture was applied");
        } else if let Some(capture::response::Captured { binding, doc_json }) = response.captured {
            let proto_flow::runtime::capture_response_ext::Captured {
                key_packed,
                partitions_packed,
            } = internal.captured.unwrap_or_default();

            tracing::trace!(?key_packed, ?partitions_packed, "captured");

            let collection = &spec.bindings[binding as usize]
                .collection
                .as_ref()
                .unwrap()
                .name;

            print!("[{collection:?},{doc_json}]\n");
        } else if let Some(capture::response::Checkpoint { state }) = response.checkpoint {
            let proto_flow::runtime::capture_response_ext::Checkpoint { stats, .. } =
                internal.checkpoint.unwrap_or_default();
            tracing::debug!(stats=?ops::DebugJson(stats), state=?ops::DebugJson(state), "checkpoint");
        }
    }

    Ok(())
}

async fn preview_derivation<L: runtime::LogHandler>(
    reader: impl runtime::harness::Reader,
    runtime: runtime::Runtime<L>,
    sessions: Vec<usize>,
    spec: flow::CollectionSpec,
    state: models::RawValue,
    state_dir: &std::path::Path,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let responses_rx =
        runtime::harness::run_derive(reader, runtime, sessions, &spec, state, state_dir, timeout);
    tokio::pin!(responses_rx);

    while let Some(response) = responses_rx.try_next().await? {
        let internal = response
            .get_internal()
            .context("failed to decode internal runtime.DeriveResponseExt")?;

        if let Some(derive::response::Published { doc_json }) = response.published {
            let proto_flow::runtime::derive_response_ext::Published {
                max_clock,
                key_packed,
                partitions_packed,
            } = internal.published.unwrap_or_default();

            tracing::trace!(?max_clock, ?key_packed, ?partitions_packed, "published");

            print!("{doc_json}\n");
        } else if let Some(derive::response::Flushed {}) = response.flushed {
            let proto_flow::runtime::derive_response_ext::Flushed { stats } =
                internal.flushed.unwrap_or_default();
            tracing::debug!(stats=?ops::DebugJson(stats), "flushed");
        } else if let Some(derive::response::StartedCommit { state }) = response.started_commit {
            tracing::debug!(state=?ops::DebugJson(state), "started commit");
        }
    }

    Ok(())
}

async fn preview_materialization<L: runtime::LogHandler>(
    reader: impl runtime::harness::Reader,
    runtime: runtime::Runtime<L>,
    sessions: Vec<usize>,
    spec: flow::MaterializationSpec,
    state: models::RawValue,
    state_dir: &std::path::Path,
    timeout: std::time::Duration,
) -> anyhow::Result<()> {
    let responses_rx = runtime::harness::run_materialize(
        reader, runtime, sessions, &spec, state, state_dir, timeout,
    );
    tokio::pin!(responses_rx);

    while let Some(response) = responses_rx.try_next().await? {
        let internal = response
            .get_internal()
            .context("failed to decode internal runtime.MaterializeResponseExt")?;

        if let Some(materialize::response::Applied { action_description }) = response.applied {
            tracing::info!(action_description, "materialization was applied");
        } else if let Some(materialize::response::Flushed {}) = response.flushed {
            let proto_flow::runtime::materialize_response_ext::Flushed { stats } =
                internal.flushed.unwrap_or_default();
            tracing::debug!(stats=?ops::DebugJson(stats), "flushed");
        } else if let Some(materialize::response::StartedCommit { state }) = response.started_commit
        {
            tracing::debug!(state=?ops::DebugJson(state), "started commit");
        }
    }

    Ok(())
}
