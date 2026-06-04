use crate::{collection::CollectionJournalSelector, output::OutputType};
use anyhow::Context;
use time::OffsetDateTime;

#[derive(clap::Args, Default, Debug, Clone)]
pub struct ReadArgs {
    #[clap(flatten)]
    pub selector: CollectionJournalSelector,
    #[clap(flatten)]
    pub bounds: ReadBounds,
}

/// Common definition for arguments specifying the begin and and bounds of a read command.
#[derive(clap::Args, Debug, Default, Clone)]
pub struct ReadBounds {
    /// Continue reading indefinitely, tailing the collection's journals for live
    /// content until interrupted. Without this flag the read is non-blocking: it
    /// outputs all currently-available content and then exits.
    #[clap(long)]
    pub follow: bool,
    /// Start reading from this far in the past, as a humantime duration (e.g. "1d2h3m").
    /// Mutually exclusive with --not-before.
    #[clap(long)]
    pub since: Option<humantime::Duration>,
    /// Start reading from this absolute point in time, as an RFC-3339 timestamp
    /// (e.g. "2024-01-02T15:04:05Z"). Mutually exclusive with --since.
    #[clap(long, conflicts_with = "since", value_parser = crate::parse_rfc3339)]
    pub not_before: Option<OffsetDateTime>,
}

impl ReadBounds {
    /// Resolve `--since` or `--not-before` into a `not_before` timestamp bound,
    /// or None for "no lower bound". The two flags are mutually exclusive
    /// (enforced by clap).
    pub fn not_before(&self) -> Option<pbjson_types::Timestamp> {
        let start_time = crate::resolve_not_before(self.since, self.not_before)?;
        tracing::debug!(begin = %start_time, "resolved read lower bound to not_before");

        // Times before the Unix epoch are treated as no bound: the downstream
        // Timestamp -> uuid::Clock conversion assumes a post-epoch seconds count.
        let seconds = start_time.unix_timestamp();
        if seconds < 0 {
            return None;
        }
        Some(pbjson_types::Timestamp {
            seconds,
            nanos: start_time.nanosecond() as i32,
        })
    }
}

/// Reads collection data through the shuffle crate and prints each committed
/// document as a JSON line to stdout.
pub async fn read_collection(
    ctx: &mut crate::CliContext,
    ReadArgs { selector, bounds }: &ReadArgs,
) -> anyhow::Result<()> {
    if let Some(output) = ctx.output.output.filter(|ot| *ot != OutputType::Json) {
        let output = clap::ValueEnum::to_possible_value(&output).unwrap();
        let name = output.get_name();
        anyhow::bail!(
            "cannot use --output {name} when reading collection data (only json is supported)",
        );
    }
    let collection_spec = fetch_built_collection_spec(ctx, &selector.collection).await?;

    // The partition selector filters which of the collection's journals are read
    // (built from the real spec's partition template plus the user's --partitions).
    let partition_selector =
        assemble::journal_selector(&collection_spec, selector.partitions.as_ref());

    let task = shuffle::proto::Task {
        task: Some(shuffle::proto::task::Task::CollectionPartitions(
            shuffle::proto::CollectionPartitions {
                collection: Some(collection_spec),
                partition_selector: Some(partition_selector),
                not_before: bounds.not_before(),
                not_after: None,
            },
        )),
    };

    let factory = flow_client_next::workflows::user_collection_auth::new_journal_client_factory(
        ctx.rest.clone(),
        models::Capability::Read,
        ctx.router.clone(),
        ctx.user_tokens.clone(),
    );

    crate::shuffle_read::read_to_stdout(ctx.registry.clone(), task, factory, bounds.follow).await
}

/// Fetch the built `CollectionSpec` of a published collection from the control
/// plane (`live_specs_ext`). The shuffle `Task::CollectionPartitions` needs the
/// full spec: its key, schema, and partition template.
async fn fetch_built_collection_spec(
    ctx: &crate::CliContext,
    collection: &str,
) -> anyhow::Result<proto_flow::flow::CollectionSpec> {
    #[derive(serde::Deserialize)]
    struct LiveSpecResult {
        built_spec: Option<models::RawValue>,
    }

    let results: Vec<LiveSpecResult> = flow_client_next::postgrest::exec(
        ctx.pg
            .from("live_specs_ext")
            .select("built_spec")
            .eq("catalog_name", collection)
            .eq("spec_type", "collection")
            .limit(1),
        ctx.access_token().as_deref(),
    )
    .await
    .context("fetching collection's built spec")?;

    match results.into_iter().next() {
        Some(LiveSpecResult {
            built_spec: Some(spec),
        }) => serde_json::from_str(spec.get()).context("parsing built CollectionSpec"),
        Some(LiveSpecResult { built_spec: None }) => {
            anyhow::bail!("collection '{collection}' exists but has no built spec")
        }
        None => anyhow::bail!(
            "collection '{collection}' was not found (it may not exist, or you may not have access)"
        ),
    }
}
