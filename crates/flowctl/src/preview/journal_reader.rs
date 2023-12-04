use anyhow::Context;
use futures::{channel::mpsc, SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use proto_flow::flow;
use proto_gazette::{broker, consumer};

/// Reader is a runtime::harness::Reader which performs active reads of live
/// collection journals.
#[derive(Clone)]
pub struct Reader {
    control_plane: crate::controlplane::Client,
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
    pub fn new(control_plane: crate::controlplane::Client, delay: std::time::Duration) -> Self {
        Self {
            control_plane,
            delay,
        }
    }

    fn start(
        self,
        sources: Vec<Source>,
        mut resume: proto_gazette::consumer::Checkpoint,
    ) -> mpsc::Receiver<anyhow::Result<runtime::harness::Read>> {
        let reader = coroutines::try_coroutine(move |mut co| async move {
            // We must be able to access all sourced collections.
            let access_prefixes = sources
                .iter()
                .map(|source| source.collection.clone())
                .collect();

            let data_plane_client =
                crate::dataplane::journal_client_for(self.control_plane, access_prefixes).await?;

            // Concurrently list the journals of every Source.
            let journals: Vec<(&Source, Vec<broker::JournalSpec>)> =
                futures::future::try_join_all(sources.iter().map(|source| {
                    Self::list_journals(source, data_plane_client.clone())
                        .map_ok(move |l| (source, l))
                }))
                .await?;

            // Flatten into (binding, source, journal).
            let journals: Vec<(u32, &Source, String)> = journals
                .into_iter()
                .enumerate()
                .flat_map(|(binding, (source, journals))| {
                    journals.into_iter().map(move |journal| {
                        (
                            binding as u32,
                            source,
                            format!("{};{}", journal.name, source.read_suffix),
                        )
                    })
                })
                .collect();

            // Map into a stream that yields lines from across all journals, as they're ready.
            let mut journals =
                futures::stream::select_all(journals.iter().map(|(binding, source, journal)| {
                    Self::read_journal_lines(
                        *binding,
                        data_plane_client.clone(),
                        journal,
                        &resume,
                        source,
                    )
                    .boxed()
                }));

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
        mut client: journal_client::Client,
    ) -> anyhow::Result<Vec<broker::JournalSpec>> {
        let listing = journal_client::list::list_journals(&mut client, &source.partition_selector)
            .await
            .with_context(|| {
                format!(
                    "failed to list journals for collection {}",
                    &source.collection
                )
            })?;

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
        client: journal_client::Client,
        journal: &'s String,
        resume: &consumer::Checkpoint,
        source: &Source,
    ) -> impl futures::Stream<Item = anyhow::Result<(u32, String, &'s String, i64)>> {
        use futures::AsyncBufReadExt;
        use journal_client::read::uncommitted::{
            ExponentialBackoff, JournalRead, ReadStart, ReadUntil, Reader,
        };

        let offset = resume
            .sources
            .get(journal)
            .map(|s| s.read_through)
            .unwrap_or_default();

        let read = JournalRead::new(journal.clone())
            .starting_at(ReadStart::Offset(offset as u64))
            .begin_mod_time(
                source
                    .not_before
                    .as_ref()
                    .map(|b| b.seconds)
                    .unwrap_or_default(),
            )
            .read_until(ReadUntil::Forever);

        coroutines::try_coroutine(move |mut co| async move {
            let backoff = ExponentialBackoff::new(2);
            let reader = Reader::start_read(client, read, backoff);
            let mut reader = futures::io::BufReader::new(reader);

            // Fill the buffer and establish the first read byte offset.
            let buf_len = reader.fill_buf().await?.len();
            let mut offset = reader.get_ref().current_offset() - buf_len as i64;

            let mut lines = reader.lines();

            loop {
                let Some(doc_json) = lines.try_next().await? else {
                    break;
                };
                // Attempt to keep the offset up to date.
                // TODO(johnny): This is subtly broken because it doesn't handle offset jumps.
                // Fixing requires a deeper refactor of journal_client::Reader.
                offset += doc_json.len() as i64 + 1;

                // TODO(johnny): This is pretty janky.
                if doc_json.starts_with("{\"_meta\":{\"ack\":true,") {
                    continue;
                }

                () = co.yield_((binding, doc_json, journal, offset)).await;
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
