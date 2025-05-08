use anyhow::Context;
use futures::{channel::mpsc, SinkExt, StreamExt, TryFutureExt};
use proto_flow::flow;
use proto_gazette::{broker, consumer};

/// Reader is a runtime::harness::Reader which performs active reads of live
/// collection journals.
#[derive(Clone)]
pub struct Reader {
    client: crate::Client,
    delay: std::time::Duration,
}

/// Source is a common read description across a derivation and materialization.
struct Source {
    collection: String,
    not_before: Option<pbjson_types::Timestamp>,
    partition_selector: broker::LabelSelector,
    read_suffix: String,
}

impl Reader {
    /// Return a new Reader which uses the `control_plane` to identify and read journals from
    /// their respective collection data planes.
    ///
    /// `delay` is an artificial, injected delay between a read and a subsequent checkpoint.
    /// It emulates back-pressure and encourages amortized transactions and reductions.
    pub fn new(client: &crate::Client, delay: std::time::Duration) -> Self {
        Self {
            client: client.clone(),
            delay,
        }
    }

    fn start(
        self,
        sources: Vec<Source>,
        mut resume: proto_gazette::consumer::Checkpoint,
    ) -> mpsc::Receiver<anyhow::Result<runtime::harness::Read>> {
        let reader = coroutines::try_coroutine(move |mut co| async move {
            // Concurrently fetch authorizations for all sourced collections.
            let sources = futures::future::try_join_all(sources.iter().map(|source| {
                flow_client::fetch_user_collection_authorization(
                    &self.client,
                    &source.collection,
                    models::Capability::Read,
                )
                .map_ok(move |(_journal_name_prefix, client)| (source, client))
            }))
            .await?;

            // Concurrently list the journals of every Source.
            let journals: Vec<(&Source, Vec<broker::JournalSpec>, &gazette::journal::Client)> =
                futures::future::try_join_all(sources.iter().map(|(source, client)| {
                    Self::list_journals(*source, client).map_ok(move |l| (*source, l, client))
                }))
                .await?;

            // Flatten into (binding, source, journal, client).
            let journals: Vec<(u32, &Source, String, &gazette::journal::Client)> = journals
                .iter()
                .enumerate()
                .flat_map(|(binding, (source, journals, client))| {
                    journals.into_iter().map(move |journal| {
                        (
                            binding as u32,
                            *source,
                            format!("{};{}", journal.name, source.read_suffix),
                            *client,
                        )
                    })
                })
                .collect();

            // Map into a stream that yields lines from across all journals, as they're ready.
            let mut journals = futures::stream::select_all(journals.iter().map(
                |(binding, source, journal, client)| {
                    Self::read_journal_lines(*binding, client, journal, &resume, source).boxed()
                },
            ));

            // Reset-able timer for delivery of delayed checkpoints.
            let deadline = tokio::time::sleep(std::time::Duration::MAX);
            tokio::pin!(deadline);

            let mut in_txn = false; // Have we emitted a document that awaits a checkpoint?

            loop {
                let step = tokio::select! {
                    Some(read) = journals.next() => Ok(read?),
                    () = deadline.as_mut(), if in_txn => Err(())
                };

                match step {
                    Ok((binding, doc_json, journal, read_through)) => {
                        let resume = match resume.sources.get_mut(journal) {
                            Some(entry) => entry,
                            None => resume.sources.entry(journal.clone()).or_default(),
                        };
                        resume.read_through = read_through;

                        () = co
                            .yield_(runtime::harness::Read::Document {
                                binding: binding as u32,
                                doc: doc_json,
                            })
                            .await;

                        // If this is the first Read of this transaction,
                        // schedule when it will Checkpoint.
                        if !in_txn {
                            in_txn = true;
                            deadline
                                .as_mut()
                                .reset(tokio::time::Instant::now() + self.delay);
                        }
                    }
                    Err(()) => {
                        () = co
                            .yield_(runtime::harness::Read::Checkpoint(resume.clone()))
                            .await;
                        in_txn = false;
                    }
                }
            }
        });

        // Dispatch through an mpsc for a modest parallelism improvement.
        let (mut tx, rx) = mpsc::channel(runtime::CHANNEL_BUFFER);

        tokio::spawn(async move {
            tokio::pin!(reader);

            while let Some(read) = reader.next().await {
                if let Err(_) = tx.feed(read).await {
                    break; // Receiver was dropped.
                }
            }
        });

        rx
    }

    async fn list_journals(
        source: &Source,
        client: &gazette::journal::Client,
    ) -> anyhow::Result<Vec<broker::JournalSpec>> {
        let resp = client
            .list(broker::ListRequest {
                selector: Some(source.partition_selector.clone()),
                ..Default::default()
            })
            .await
            .with_context(|| {
                format!(
                    "failed to list journals for collection {}",
                    &source.collection
                )
            })?;

        let listing = resp
            .journals
            .into_iter()
            .map(|j| j.spec.unwrap())
            .collect::<Vec<_>>();

        if listing.is_empty() {
            anyhow::bail!(
                "the collection '{}' has not had any data written to it",
                &source.collection,
            );
        }
        Ok(listing)
    }

    fn read_journal_lines<'s>(
        binding: u32,
        client: &gazette::journal::Client,
        journal: &'s String,
        resume: &consumer::Checkpoint,
        source: &Source,
    ) -> impl futures::Stream<Item = gazette::Result<(u32, String, &'s String, i64)>> {
        use gazette::journal::ReadJsonLine;

        let mut offset = resume
            .sources
            .get(journal)
            .map(|s| s.read_through)
            .unwrap_or_default();

        let begin_mod_time = source
            .not_before
            .as_ref()
            .map(|b| b.seconds)
            .unwrap_or_default();

        let mut lines = client.clone().read_json_lines(
            broker::ReadRequest {
                journal: journal.clone(),
                offset,
                block: true,
                begin_mod_time,
                // TODO(johnny): Set `do_not_proxy: true` once cronut is migrated.
                ..Default::default()
            },
            1,
        );

        let ser_policy = doc::SerPolicy::noop();

        coroutines::try_coroutine(move |mut co| async move {
            while let Some(line) = lines.next().await {
                let (root, next_offset) = match line {
                    Ok(ReadJsonLine::Doc { root, next_offset }) => (root, next_offset),
                    Ok(ReadJsonLine::Meta(meta)) => {
                        offset = meta.offset;
                        continue;
                    }
                    Err(gazette::RetryError {
                        attempt,
                        inner: err,
                    }) if err.is_transient() => {
                        tracing::warn!(?err, %attempt, %journal, %binding, "transient error reading journal (will retry)");
                        continue;
                    }
                    Err(gazette::RetryError { inner, .. }) => return Err(inner),
                };

                // TODO(johnny): plumb through OwnedArchivedNode end-to-end.
                let doc_json = serde_json::to_string(&ser_policy.on(root.get())).unwrap();

                // TODO(johnny): This is pretty janky.
                if doc_json.starts_with("{\"_meta\":{\"ack\":true,") {
                    continue;
                }

                () = co.yield_((binding, doc_json, journal, offset)).await;
                offset = next_offset;
            }
            Ok(())
        })
    }
}

impl runtime::harness::Reader for Reader {
    type Stream = mpsc::Receiver<anyhow::Result<runtime::harness::Read>>;

    fn start_for_derivation(
        self,
        derivation: &flow::CollectionSpec,
        resume: proto_gazette::consumer::Checkpoint,
    ) -> Self::Stream {
        let transforms = &derivation.derivation.as_ref().unwrap().transforms;

        let sources = transforms
            .iter()
            .map(|t| {
                let collection = t.collection.as_ref().unwrap();

                Source {
                    collection: collection.name.clone(),
                    not_before: t.not_before.clone(),
                    partition_selector: t.partition_selector.clone().unwrap(),
                    read_suffix: t.journal_read_suffix.clone(),
                }
            })
            .collect();

        self.start(sources, resume)
    }

    fn start_for_materialization(
        self,
        materialization: &flow::MaterializationSpec,
        resume: proto_gazette::consumer::Checkpoint,
    ) -> Self::Stream {
        let sources = materialization
            .bindings
            .iter()
            .map(|b| {
                let collection = b.collection.as_ref().unwrap();
                Source {
                    collection: collection.name.clone(),
                    not_before: b.not_before.clone(),
                    partition_selector: b.partition_selector.clone().unwrap(),
                    read_suffix: b.journal_read_suffix.clone(),
                }
            })
            .collect();

        self.start(sources, resume)
    }
}
