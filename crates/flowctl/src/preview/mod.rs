use crate::{dataplane, local_specs};
use anyhow::Context;
use doc::shape::schema::to_schema;
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt, TryStreamExt};
use prost::Message;
use proto_flow::runtime::{derive_request_ext, DeriveRequestExt};
use proto_flow::{derive, flow, flow::collection_spec::derivation::Transform};
use proto_gazette::broker;
use tokio::sync::broadcast;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Preview {
    /// Path or URL to a Flow specification file to generate development files for.
    #[clap(long)]
    source: String,
    /// Name of the derived collection to preview within the Flow specification file.
    /// Collection is required if there are multiple derivations in --source specifications.
    #[clap(long)]
    collection: Option<String>,
    /// When exiting (for example, upon Ctrl-D), should we update the derivation schema
    /// based on observed output documents?
    #[clap(long)]
    infer_schema: bool,
    /// When previewing a SQLite derivation, the path URI of the database to use.
    /// This can be useful for debugging the internal state of a database.
    /// If not specified, an in-memory-only database is used.
    #[clap(long, default_value = ":memory:")]
    sqlite_uri: String,
    /// How frequently should we close transactions and emit combined documents?
    /// If not specified, the default is one second.
    #[clap(long)]
    interval: Option<humantime::Duration>,
}

impl Preview {
    pub async fn run(&self, ctx: &mut crate::CliContext) -> anyhow::Result<()> {
        let Self {
            source,
            collection,
            infer_schema,
            sqlite_uri: sqlite_path,
            interval: flush_interval,
        } = self;
        let source = local_specs::arg_source_to_url(source, false)?;

        if self.infer_schema && source.scheme() != "file" {
            anyhow::bail!("schema inference can only be used with a local file --source");
        }

        let client = ctx.controlplane_client().await?;
        let (sources, validations) =
            local_specs::load_and_validate(client, source.as_str()).await?;

        // Identify the derivation to preview.
        let needle = if let Some(needle) = collection {
            needle.as_str()
        } else if sources.collections.len() == 1 {
            sources.collections.first().unwrap().collection.as_str()
        } else if sources.collections.is_empty() {
            anyhow::bail!("sourced specification files do not contain any derivations");
        } else {
            anyhow::bail!("sourced specification files contain multiple derivations. Use --collection to identify the specific one to preview");
        };

        // Resolve the built collection and its contained derivation.
        let built_collection = match validations
            .built_collections
            .binary_search_by_key(&needle, |b| b.collection.as_str())
        {
            Ok(index) => &validations.built_collections[index],
            Err(_) => anyhow::bail!("could not find the collection {needle}"),
        };
        let derivation = built_collection
            .spec
            .derivation
            .as_ref()
            .context("collection is not a derivation")?;

        // We must be able to access all of its sourced collections.
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
                    let listing = journal_client::list::list_journals(
                        &mut data_plane_client,
                        partition_selector.as_ref().unwrap(),
                    )
                    .await
                    .with_context(|| format!("failed to list journal for transform {name}"))?;

                    if listing.is_empty() {
                        anyhow::bail!(
                            "no journals were returned by the selector: {}",
                            serde_json::to_string_pretty(partition_selector).unwrap()
                        );
                    }
                    Result::<_, anyhow::Error>::Ok((index, listing))
                }
            },
        ))
        .await?;

        // Start derivation connector.
        let (mut request_tx, request_rx) = mpsc::channel(64);

        // Remove `uuid_ptr` so that UUID placeholders aren't included in preview output.
        let mut spec = built_collection.spec.clone();
        spec.uuid_ptr = String::new();

        request_tx
            .send(Ok(derive::Request {
                open: Some(derive::request::Open {
                    collection: Some(spec),
                    range: Some(flow::RangeSpec {
                        key_begin: 0,
                        key_end: u32::MAX,
                        r_clock_begin: 0,
                        r_clock_end: u32::MAX,
                    }),
                    version: "local".to_string(),
                    state_json: "{}".to_string(),
                }),
                internal: Some(proto_flow::Any {
                    type_url: "flow://runtime.DeriveResponseExt".to_string(),
                    value: DeriveRequestExt {
                        open: Some(derive_request_ext::Open {
                            sqlite_vfs_uri: sqlite_path.clone(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }
                    .encode_to_vec()
                    .into(),
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

        let (cancel_tx, cancel_rx) = broadcast::channel(1);

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
                    cancel_rx.resubscribe(),
                    journal,
                    transform.clone(),
                    request_tx.clone(),
                    data_plane_client.clone(),
                )
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect();

        // Future that sends a periodic Flush request.
        let flushes = tick_flushes(cancel_rx.resubscribe(), request_tx, flush_interval.as_ref());

        // Future that emits previewed documents and gathers inference.
        let output = output(responses_rx, *infer_schema);

        let cancel = async move {
            let _cancel_tx = cancel_tx; // Owned and dropped by this future.

            // Blocking read until stdin is closed.
            tokio::io::copy(&mut tokio::io::stdin(), &mut tokio::io::sink()).await?;
            Ok::<(), anyhow::Error>(())
        };

        let ((), (), (), schema) = futures::try_join!(cancel, reads, flushes, output)?;

        // Update with an inferred schema and write out the updated Flow spec.
        if let Some(schema) = schema {
            // Reload `sources`, this time without inlining them.
            let mut sources = local_specs::surface_errors(local_specs::load(&source).await)
                .expect("sources must load a second time");

            // Find the derivation we just previewed.
            let index = sources
                .collections
                .binary_search_by_key(&needle, |b| b.collection.as_str())
                .unwrap();

            // Update (just) its schema, making no other changes to loaded `sources`.
            // We don't attempt to inline it.
            let collection = &mut sources.collections[index].spec;
            collection.read_schema = None;
            collection.write_schema = None;
            collection.schema = Some(models::Schema::new(models::RawValue::from_value(&schema)));

            _ = local_specs::write_resources(sources)?;
        }
        tracing::info!("all done");

        Ok(())
    }
}

async fn read_journal(
    mut cancel_rx: broadcast::Receiver<()>,
    journal: broker::JournalSpec,
    transform: usize,
    mut request_tx: mpsc::Sender<tonic::Result<derive::Request>>,
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

    loop {
        let doc_json = tokio::select! {
            doc_json = lines.try_next() => match doc_json? {
                Some(doc_json) => doc_json,
                None => {
                    return Ok(()) // All done.
                }
            },
            _ = cancel_rx.recv() => {
                return Ok(()) // Cancelled.
            }
        };

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
}

async fn tick_flushes(
    mut cancel_rx: broadcast::Receiver<()>,
    mut request_tx: mpsc::Sender<tonic::Result<derive::Request>>,
    flush_interval: Option<&humantime::Duration>,
) -> anyhow::Result<()> {
    let period = flush_interval
        .map(|i| i.clone().into())
        .unwrap_or(std::time::Duration::from_secs(1));

    let mut ticker = tokio::time::interval(period);
    loop {
        tokio::select! {
            _ = ticker.tick() => (), // Fall through.
            _ = cancel_rx.recv() => {
                return Ok(()) // Cancelled.
            }
        }

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

async fn output<R>(
    mut responses_rx: R,
    infer_schema: bool,
) -> anyhow::Result<Option<serde_json::Value>>
where
    R: futures::Stream<Item = tonic::Result<derive::Response>> + Unpin,
{
    let mut inferred_shape = doc::Shape::nothing();

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

            if infer_schema {
                let doc: serde_json::Value =
                    serde_json::from_str(&doc_json).context("failed to parse derived document")?;

                inferred_shape.widen(&doc);
            }

            print!("{doc_json}\n");
        } else if let Some(derive::response::Flushed {}) = response.flushed {
            let proto_flow::runtime::derive_response_ext::Flushed { stats } =
                internal.flushed.unwrap_or_default();
            let stats = serde_json::to_string(&stats).unwrap();

            tracing::debug!(%stats, "flushed");
        } else if let Some(derive::response::StartedCommit { state }) = response.started_commit {
            tracing::debug!(?state, "started commit");
        }
    }

    Ok(if infer_schema {
        Some(serde_json::to_value(to_schema(inferred_shape)).unwrap())
    } else {
        None
    })
}

fn status_to_anyhow(status: tonic::Status) -> anyhow::Error {
    anyhow::anyhow!(status.message().to_string())
}
