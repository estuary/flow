use proto_gazette::broker;

/// Partition describes a collection partition.
/// It's similar to activate::JournalSplit, but focuses on the efficient mapping
/// of documents to physical collection partitions based on the document's
/// key hash and partitioning values.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct PartitionSplit {
    pub name: Box<str>,
    pub key_begin: u32,
    pub key_end: u32,
    pub mod_revision: i64,
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(std::mem::size_of::<PartitionSplit>() == 32);

/// Create a live partition watch backed by a gazette `list_watch`.
///
/// Returns a `PendingWatch` that resolves once the first listing snapshot
/// arrives. The watch continuously updates as partitions change.
pub fn watch_partitions(
    journal_client: gazette::journal::Client,
    collection_name: &str,
) -> tokens::PendingWatch<Vec<PartitionSplit>> {
    use futures::StreamExt;

    let mut request = activate::list_partitions_request(collection_name);
    request.watch = true;

    let list_watch = journal_client.list_watch_with(request, PartitionSplitFold::default());

    // Adapt the RetryResult stream into a tonic::Result stream for StreamSource.
    let adapted = list_watch.filter_map(|item| async {
        match item {
            Ok(splits) => Some(Ok(splits)),
            Err(gazette::RetryError {
                attempt,
                inner: err,
                ..
            }) => {
                if err.is_transient() {
                    tracing::warn!(attempt, %err, "partition listing watch failed (will retry)");
                    None // Don't surface transient errors (aside from logging).
                } else {
                    Some(Err(match err {
                        gazette::Error::Grpc(status) => status,
                        other => tonic::Status::internal(other.to_string()),
                    }))
                }
            }
        }
    });

    tokens::watch(tokens::StreamSource::new(adapted))
}

/// A `gazette::journal::list::Fold` that extracts `PartitionSplit` from listing chunks.
#[derive(Default)]
struct PartitionSplitFold(Vec<PartitionSplit>);

impl gazette::journal::list::Fold for PartitionSplitFold {
    type Output = Vec<PartitionSplit>;

    async fn begin(&mut self) {
        self.0.clear();
    }

    async fn chunk(&mut self, resp: broker::ListResponse) -> gazette::Result<()> {
        for journal in resp.journals {
            let broker::JournalSpec { name, labels, .. } = journal
                .spec
                .ok_or(gazette::Error::Protocol("listing response is missing spec"))?;

            let (key_begin, key_end) = labels::partition::decode_key_range_labels(
                &labels.unwrap_or_default(),
            )
            .map_err(|err| {
                gazette::Error::Grpc(tonic::Status::internal(format!(
                    "invalid partition labels: {err}"
                )))
            })?;

            // We re-allocate `name` in a tight loop to maximize cache locality.
            let name = name.as_str().into();

            self.0.push(PartitionSplit {
                name,
                key_begin,
                key_end,
                mod_revision: journal.mod_revision,
            });
        }
        Ok(())
    }

    async fn finish(&mut self) -> gazette::Result<Self::Output> {
        Ok(std::mem::take(&mut self.0))
    }
}
