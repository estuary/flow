use crate::dataplane::{self};
use crate::{collection::CollectionJournalSelector, output::OutputType};
use anyhow::Context;
use futures::StreamExt;
use gazette::journal::ReadJsonLine;
use proto_gazette::broker;
use time::OffsetDateTime;
use tokio::io::AsyncWriteExt;

#[derive(clap::Args, Default, Debug, Clone)]
pub struct ReadArgs {
    #[clap(flatten)]
    pub selector: CollectionJournalSelector,
    #[clap(flatten)]
    pub bounds: ReadBounds,
    /// Read all journal data, including messages from transactions which were
    /// rolled back or never committed. Due to the current limitations of the Rust
    /// Gazette client library, this is the only mode that's currently supported,
    /// and this flag must be provided. In the future, committed reads will become
    /// the default.
    #[clap(long)]
    pub uncommitted: bool,
    #[clap(skip)]
    pub auth_prefixes: Vec<String>,
}

/// Common definition for arguments specifying the begin and and bounds of a read command.
#[derive(clap::Args, Debug, Default, Clone)]
pub struct ReadBounds {
    /// Continue reading indefinitely until interrupted or ending due to an error.
    #[clap(long)]
    pub follow: bool,

    /// Start reading from approximately this far in the past. For example `--since 10m` will output all data that was added within the last 10 minutes.
    /// The actual start of the read will always be at a fragment boundary, and thus may include data from significantly before the requested time period.
    #[clap(long)]
    pub since: Option<humantime::Duration>,
}

/// Reads collection data and prints it to stdout. This function has a number of limitations at present:
/// - The provided `CollectionJournalSelector` must select a single journal.
/// - Only uncommitted reads are supported
/// - Any acknowledgements (documents with `/_meta/ack` value `true`) are also printed
/// These limitations should all be addressed in the future when we add support for committed reads.
pub async fn read_collection(ctx: &mut crate::CliContext, args: &ReadArgs) -> anyhow::Result<()> {
    if !args.uncommitted {
        anyhow::bail!("missing the `--uncommitted` flag. This flag is currently required, though a future release will add support for committed reads, which will be the default.");
    }
    // output can be either None or Some(OutputType::Json), but cannot be explicitly set to
    // anything else. _Eventually_, we may want to support outputting collection data as yaml
    // or a table, but certainly not right now.
    if let Some(naughty_output_type) = ctx
        .output_args()
        .output
        .filter(|ot| *ot != OutputType::Json)
    {
        let clap_enum = clap::ValueEnum::to_possible_value(&naughty_output_type)
            .expect("possible value cannot be None");
        let name = clap_enum.get_name();
        anyhow::bail!(
            "cannot use --output {name} when reading collection data (only json is supported)"
        );
    }

    let auth_prefixes = if args.auth_prefixes.is_empty() {
        vec![args.selector.collection.clone()]
    } else {
        args.auth_prefixes.clone()
    };
    let cp_client = ctx.controlplane_client().await?;
    let client = dataplane::journal_client_for(cp_client, auth_prefixes).await?;

    let list_resp = client
        .list(broker::ListRequest {
            selector: Some(args.selector.build_label_selector()),
            ..Default::default()
        })
        .await
        .context("listing journals for collection read")?;

    let mut journals = list_resp
        .journals
        .into_iter()
        .map(|j| j.spec.unwrap())
        .collect::<Vec<_>>();

    tracing::debug!(journal_count = journals.len(), collection = %args.selector.collection, "listed journals");
    let maybe_journal = journals.pop();
    if !journals.is_empty() {
        // TODO: implement a sequencer and allow reading from multiple journals
        anyhow::bail!("flowctl is not yet able to read from partitioned collections (coming soon)");
    }

    let journal = maybe_journal.ok_or_else(|| {
        anyhow::anyhow!(
            "collection '{}' does not exist or has never been written to (it has no journals)",
            args.selector.collection
        )
    })?;

    let begin_mod_time = if let Some(since) = args.bounds.since {
        let start_time = OffsetDateTime::now_utc() - *since;
        tracing::debug!(%since, begin_mod_time = %start_time, "resolved --since to begin_mod_time");
        (start_time - OffsetDateTime::UNIX_EPOCH).as_seconds_f64() as i64
    } else {
        0
    };

    let mut lines = client.read_json_lines(
        broker::ReadRequest {
            journal: journal.name.clone(),
            offset: 0,
            block: args.bounds.follow,
            begin_mod_time,
            ..Default::default()
        },
        1,
    );
    tracing::debug!(journal = %journal.name, "starting read of journal");

    let policy = doc::SerPolicy::noop();

    while let Some(line) = lines.next().await {
        match line {
            Err(err) if err.is_transient() => {
                tracing::warn!(%err, "error reading collection (will retry)");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
            Err(err) => anyhow::bail!(err),
            Ok(ReadJsonLine::Meta(_)) => (),
            Ok(ReadJsonLine::Doc {
                root,
                next_offset: _,
            }) => {
                let mut v = serde_json::to_vec(&policy.on(root.get())).unwrap();
                v.push(b'\n');
                tokio::io::stdout().write_all(&v).await?;
            }
        }
    }

    Ok(())
}
