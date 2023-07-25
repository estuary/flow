use crate::dataplane::{self, fetch_data_plane_access_token};
use crate::{collection::CollectionJournalSelector, output::OutputType};
use anyhow::Context;
use journal_client::{
    broker,
    fragments::FragmentIter,
    list::list_journals,
    read::uncommitted::{ExponentialBackoff, JournalRead, ReadStart, ReadUntil, Reader},
    Client,
};
use reqwest::StatusCode;
use time::OffsetDateTime;
use tokio_util::compat::FuturesAsyncReadCompatExt;

#[derive(clap::Args, Debug, Clone)]
pub struct SchemaInferenceArgs {
    #[clap(flatten)]
    pub selector: CollectionJournalSelector,
}

pub async fn get_collection_inferred_schema(
    ctx: &mut crate::CliContext,
    args: &SchemaInferenceArgs,
) -> anyhow::Result<()> {
    if args.selector.exclude_partitions.len() > 0 || args.selector.include_partitions.len() > 0 {
        anyhow::bail!("flowctl is not yet able to read from partitioned collections (coming soon)");
    }

    let cp_client = ctx.controlplane_client().await?;
    let token = fetch_data_plane_access_token(cp_client, vec![args.selector.collection.clone()])
        .await
        .context("fetching data plane access token")?;

    let client = reqwest::Client::new();

    let inference_response = client
        .get(format!("{}/infer_schema", token.gateway_url))
        .query(&[("collection", args.selector.collection.clone())])
        .bearer_auth(token.auth_token.clone())
        .send()
        .await
        .context("schema inference request")?;

    match inference_response.status() {
        StatusCode::OK => {
            let response: schema_inference::server::InferenceResponse =
                inference_response.json().await?;
            let schema_json = serde_json::to_string(&response.schema)?;

            println!("{}", schema_json);
        }
        err => {
            anyhow::bail!("[{}]: {}", err, inference_response.text().await?);
        }
    }

    Ok(())
}

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

pub async fn journal_reader(
    ctx: &mut crate::CliContext,
    args: &ReadArgs,
) -> anyhow::Result<Reader<ExponentialBackoff>> {
    let cp_client = ctx.controlplane_client().await?;
    let mut data_plane_client =
        dataplane::journal_client_for(cp_client, vec![args.selector.collection.clone()]).await?;

    let selector = args.selector.build_label_selector();
    tracing::debug!(?selector, "build label selector");

    let mut journals = list_journals(&mut data_plane_client, &selector)
        .await
        .context("listing journals for collection read")?;
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

    let start = if let Some(since) = args.bounds.since {
        let start_time = OffsetDateTime::now_utc() - *since;
        tracing::debug!(%since, begin_mod_time = %start_time, "resolved --since to begin_mod_time");
        find_start_offset(data_plane_client.clone(), journal.name.clone(), start_time).await?
    } else {
        ReadStart::Offset(0)
    };
    let end = if args.bounds.follow {
        ReadUntil::Forever
    } else {
        ReadUntil::WriteHead
    };
    let read = JournalRead::new(journal.name.clone())
        .starting_at(start)
        .read_until(end);

    tracing::debug!(journal = %journal.name, "starting read of journal");

    // It would seem unusual for a CLI to retry indefinitely, so limit the number of retries.
    let backoff = ExponentialBackoff::new(5);
    let reader = Reader::start_read(data_plane_client.clone(), read, backoff);

    Ok(reader)
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
        let name = clap::ValueEnum::to_possible_value(&naughty_output_type)
            .expect("possible value cannot be None")
            .get_name();
        anyhow::bail!(
            "cannot use --output {name} when reading collection data (only json is supported)"
        );
    }

    let reader = journal_reader(ctx, args).await?;

    tokio::io::copy(&mut reader.compat(), &mut tokio::io::stdout()).await?;
    Ok(())
}

async fn find_start_offset(
    client: Client,
    journal: String,
    start_time: OffsetDateTime,
) -> anyhow::Result<ReadStart> {
    let frag_req = broker::FragmentsRequest {
        journal,
        header: None,
        begin_mod_time: start_time.unix_timestamp(),
        end_mod_time: 0,
        next_page_token: 0,
        page_limit: 1,
        signature_ttl: None,
        do_not_proxy: false,
    };
    let mut iter = FragmentIter::new(client, frag_req);
    match iter.next().await {
        None => {
            tracing::debug!(requested_start_time = %start_time, "no fragment found covering start time");
            Ok(ReadStart::WriteHead)
        }
        Some(result) => {
            let frag = result?
                .spec
                .ok_or_else(|| anyhow::anyhow!("response is missing fragment spec"))?;
            tracing::debug!(requested_start_time = %start_time, resolved_fragment = ?frag, "resolved start time to fragment");
            Ok(ReadStart::Offset(frag.begin as u64))
        }
    }
}
