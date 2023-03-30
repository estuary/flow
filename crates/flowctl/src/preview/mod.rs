use crate::{dataplane, local_specs};
use anyhow::Context;
use futures::{SinkExt, StreamExt, TryStreamExt};
use prost::Message;
use proto_flow::{derive, flow, flow::collection_spec::derivation::Transform};
use proto_gazette::broker;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Preview {
    /// Path or URL to a Flow specification file to generate development files for.
    #[clap(long)]
    source: String,
    /// Name of the derived collection to preview within the Flow specification file.
    #[clap(long)]
    collection: String,
}

impl Preview {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let Self { source, collection } = self;

        let client = ctx.controlplane_client().await?;
        let (_sources, validations) = local_specs::load_and_validate(client, source).await?;

        let built_collection = match validations
            .built_collections
            .binary_search_by_key(&collection.as_str(), |b| b.collection.as_str())
        {
            Ok(index) => &validations.built_collections[index],
            Err(_) => anyhow::bail!("could not find the collection {collection}"),
        };

        let derivation = &built_collection
            .spec
            .derivation
            .as_ref()
            .context("collection is not a derivation")?;

        let access_prefixes = derivation
            .transforms
            .iter()
            .map(|Transform { collection, .. }| collection.as_ref().unwrap().name.clone())
            .collect();

        let data_plane_client =
            dataplane::journal_client_for(ctx.controlplane_client().await?, access_prefixes)
                .await?;

        // Perform a listing of all collection journals to read.
        let listings = futures::future::try_join_all(derivation.transforms.iter().enumerate().map(
            |(
                index,
                Transform {
                    name,
                    partition_selector,
                    ..
                },
            )| {
                let mut data_plane_client = data_plane_client.clone();
                async move {
                    Result::<_, anyhow::Error>::Ok((
                        index,
                        journal_client::list::list_journals(
                            &mut data_plane_client,
                            partition_selector.as_ref().unwrap(),
                        )
                        .await
                        .with_context(|| format!("failed to list journal for transform {name}"))?,
                    ))
                }
            },
        ))
        .await?;

        // Start derivation connector.
        let (mut request_tx, request_rx) = futures::channel::mpsc::channel(64);

        request_tx
            .send(Ok(derive::Request {
                open: Some(derive::request::Open {
                    collection: Some(built_collection.spec.clone()),
                    range: Some(flow::RangeSpec {
                        key_begin: 0,
                        key_end: u32::MAX,
                        r_clock_begin: 0,
                        r_clock_end: u32::MAX,
                    }),
                    version: "local".to_string(),
                    state_json: "{}".to_string(),
                }),
                ..Default::default()
            }))
            .await?;

        let mut responses_rx = runtime::derive::Middleware::new(ops::tracing_log_handler, None)
            .serve(request_rx)
            .await
            .map_err(|status| anyhow::anyhow!("{}", status.message()))?;

        let _opened = responses_rx
            .next()
            .await
            .context("expected Opened, not EOF")?
            .map_err(status_to_anyhow)?
            .opened
            .context("expected Opened")?;

        // Start reads of all journals.
        let reads = listings
            .into_iter()
            .flat_map(|(transform, journals)| {
                journals
                    .into_iter()
                    .map(move |journal| (transform, journal))
            })
            .map(|(transform, journal)| {
                read_journal(
                    journal,
                    transform.clone(),
                    request_tx.clone(),
                    data_plane_client.clone(),
                )
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect();

        // Send in a periodic Flush request.
        let flushes = tick_flushes(request_tx.clone());

        // Write published documents to stdout.
        let output = async move {
            while let Some(response) = responses_rx.next().await {
                let response = response.map_err(status_to_anyhow)?;

                let internal: proto_flow::runtime::DeriveResponseExt =
                    Message::decode(response.internal.map(|i| i.value).unwrap_or_default())
                        .context("failed to decode internal runtime.DeriveResponseExt")?;

                if let Some(derive::response::Published { doc_json }) = response.published {
                    let proto_flow::runtime::derive_response_ext::Published {
                        max_clock,
                        key_packed,
                        partitions_packed,
                    } = internal.published.unwrap_or_default();

                    tracing::debug!(?max_clock, ?key_packed, ?partitions_packed, "published");
                    print!("{doc_json}\n");
                } else if let Some(derive::response::Flushed {}) = response.flushed {
                    let proto_flow::runtime::derive_response_ext::Flushed { stats } =
                        internal.flushed.unwrap_or_default();
                    let stats = serde_json::to_string(&stats).unwrap();

                    tracing::info!(%stats, "flushed");
                } else if let Some(derive::response::StartedCommit { state }) =
                    response.started_commit
                {
                    tracing::info!(?state, "started commit");
                }
            }
            Ok(())
        };

        let ((), (), ()) = futures::try_join!(reads, flushes, output)?;
        Ok(())
    }
}

async fn read_journal(
    journal: broker::JournalSpec,
    transform: usize,
    mut request_tx: futures::channel::mpsc::Sender<tonic::Result<derive::Request>>,
    client: journal_client::Client,
) -> anyhow::Result<()> {
    use futures::AsyncBufReadExt;
    use journal_client::read::uncommitted::{
        ExponentialBackoff, JournalRead, ReadStart, ReadUntil, Reader,
    };

    tracing::info!(journal = %journal.name, "starting read of journal");
    let read = JournalRead::new(journal.name)
        .starting_at(ReadStart::Offset(0))
        .read_until(ReadUntil::Forever);

    let backoff = ExponentialBackoff::new(2);
    let reader = Reader::start_read(client, read, backoff);

    // TODO(johnny): Reader should directly implement futures::io::AsyncBufRead.
    let mut lines = futures::io::BufReader::new(reader).lines();

    while let Some(doc_json) = lines.try_next().await? {
        // TODO(johnny): This is pretty janky.
        if doc_json.starts_with("{\"_meta\":{\"ack\":true,") {
            continue;
        }
        request_tx
            .send(Ok(derive::Request {
                read: Some(derive::request::Read {
                    transform: transform as u32,
                    doc_json,
                    // TODO: attach real `shuffle` and `uuid` messages.
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await?;
    }

    Ok(())
}

async fn tick_flushes(
    mut request_tx: futures::channel::mpsc::Sender<tonic::Result<derive::Request>>,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    loop {
        _ = ticker.tick().await;

        if let Err(_) = request_tx
            .send_all(&mut futures::stream::iter([
                Ok(Ok(derive::Request {
                    flush: Some(derive::request::Flush {}),
                    ..Default::default()
                })),
                Ok(Ok(derive::Request {
                    start_commit: Some(derive::request::StartCommit {
                        runtime_checkpoint: Some(proto_gazette::consumer::Checkpoint {
                            sources: [(
                                "a/journal".to_string(),
                                proto_gazette::consumer::checkpoint::Source {
                                    read_through: 1,
                                    ..Default::default()
                                },
                            )]
                            .into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
            ]))
            .await
        {
            // We can error only because there's no receiver,
            // but that's not an error from this routine's perspective.
            return Ok(());
        }
    }
}

fn status_to_anyhow(status: tonic::Status) -> anyhow::Error {
    anyhow::anyhow!(status.message().to_string())
}
