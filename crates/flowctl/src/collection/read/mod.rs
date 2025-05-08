use crate::{collection::CollectionJournalSelector, output::OutputType};
use anyhow::Context;
use futures::StreamExt;
use gazette::journal::ReadJsonLine;
use proto_gazette::broker;
use std::io::Write;
use time::OffsetDateTime;

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
pub async fn read_collection(
    ctx: &mut crate::CliContext,
    ReadArgs {
        selector,
        bounds,
        uncommitted,
    }: &ReadArgs,
) -> anyhow::Result<()> {
    if !uncommitted {
        anyhow::bail!("missing the `--uncommitted` flag. This flag is currently required, though a future release will add support for committed reads, which will be the default.");
    }
    // output can be either None or Some(OutputType::Json), but cannot be explicitly set to
    // anything else. _Eventually_, we may want to support outputting collection data as yaml
    // or a table, but certainly not right now.
    if let Some(naughty_output_type) = ctx.output.output.filter(|ot| *ot != OutputType::Json) {
        let clap_enum = clap::ValueEnum::to_possible_value(&naughty_output_type)
            .expect("possible value cannot be None");
        let name = clap_enum.get_name();
        anyhow::bail!(
            "cannot use --output {name} when reading collection data (only json is supported)"
        );
    }

    let (journal_client, journal_name_prefix) = read_client(ctx, &selector.collection).await?;

    let list_resp = journal_client
        .list(broker::ListRequest {
            selector: Some(selector.build_label_selector(journal_name_prefix)),
            ..Default::default()
        })
        .await
        .context("listing journals for collection read")?;

    let mut journals = list_resp
        .journals
        .into_iter()
        .map(|j| j.spec.unwrap())
        .collect::<Vec<_>>();

    tracing::debug!(journal_count = journals.len(), collection = %selector.collection, "listed journals");
    let maybe_journal = journals.pop();
    if !journals.is_empty() {
        // TODO: implement a sequencer and allow reading from multiple journals
        anyhow::bail!("flowctl is not yet able to read from partitioned collections (coming soon)");
    }

    let journal = maybe_journal.ok_or_else(|| {
        anyhow::anyhow!(
            "collection '{}' does not exist or has never been written to (it has no journals)",
            selector.collection
        )
    })?;

    read_collection_journal(journal_client, &journal.name, bounds).await
}

pub async fn read_collection_journal(
    journal_client: gazette::journal::Client,
    journal_name: &str,
    bounds: &ReadBounds,
) -> anyhow::Result<()> {
    let begin_mod_time = if let Some(since) = bounds.since {
        let start_time = OffsetDateTime::now_utc() - *since;
        tracing::debug!(%since, begin_mod_time = %start_time, "resolved --since to begin_mod_time");
        (start_time - OffsetDateTime::UNIX_EPOCH).as_seconds_f64() as i64
    } else {
        0
    };

    let mut lines = journal_client.read_json_lines(
        broker::ReadRequest {
            journal: journal_name.to_string(),
            offset: 0,
            block: bounds.follow,
            begin_mod_time,
            // TODO(johnny): Set `do_not_proxy: true` once cronut is migrated.
            ..Default::default()
        },
        1,
    );
    tracing::debug!(%journal_name, "starting read of journal");

    let policy = doc::SerPolicy::noop();
    let mut stdout = std::io::stdout();

    while let Some(line) = lines.next().await {
        match line {
            Ok(ReadJsonLine::Meta(broker::ReadResponse {
                fragment,
                write_head,
                ..
            })) => {
                tracing::debug!(?fragment, %write_head, "journal metadata");
            }
            Ok(ReadJsonLine::Doc {
                root,
                next_offset: _,
            }) => {
                let mut v = serde_json::to_vec(&policy.on(root.get())).unwrap();
                v.push(b'\n');
                () = stdout.write_all(&v)?;
            }
            Err(gazette::RetryError {
                inner: err,
                attempt,
            }) => match err {
                err if err.is_transient() => {
                    tracing::warn!(?err, %attempt, "error reading collection (will retry)");
                }
                gazette::Error::BrokerStatus(broker::Status::Suspended) if bounds.follow => {
                    tracing::debug!(?err, %attempt, "journal is suspended (will retry)");
                }
                gazette::Error::BrokerStatus(
                    status @ broker::Status::OffsetNotYetAvailable
                    | status @ broker::Status::Suspended,
                ) => {
                    tracing::debug!(?status, "stopping read at end of journal content");
                    break; // Graceful EOF of non-blocking read.
                }
                err => anyhow::bail!(err),
            },
        }
    }

    Ok(())
}
