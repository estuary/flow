use futures::StreamExt;
use proto_flow::shuffle;
use proto_gazette::broker;
use tokio::sync::mpsc;

/// Gazette list::Subscriber that sends listing events to an mpsc channel.
pub struct Subscriber {
    binding: u32,
    tx: mpsc::Sender<tonic::Result<shuffle::SliceResponse>>,
}

impl gazette::journal::list::Subscriber for Subscriber {
    async fn add_journal(
        &mut self,
        create_revision: i64,
        journal_spec: broker::JournalSpec,
        mod_revision: i64,
        route: broker::Route,
    ) -> gazette::Result<()> {
        let added = shuffle::slice_response::ListingAdded {
            binding: self.binding,
            create_revision,
            mod_revision,
            spec: Some(journal_spec),
            route: Some(route),
        };
        tracing::debug!(added=?ops::DebugJson(&added), "journal added");

        // Blocking safety: we may stuff this channel, but we're doing so from a
        // dedicated task. The session may reflect these events back to this same
        // Slice RPC, but those reflected requests are read by SliceActor::rx_loop
        // which (crucially) does not itself send any SliceResponse's.
        let _ignored = self
            .tx
            .send(Ok(shuffle::SliceResponse {
                listing_added: Some(added),
                ..Default::default()
            }))
            .await;

        Ok(())
    }

    async fn remove_journal(&mut self, journal: String) -> gazette::Result<()> {
        tracing::debug!(binding = self.binding, journal, "journal removed");
        Ok(())
    }
}

pub fn spawn_listing(
    binding: &crate::Binding,
    client: gazette::journal::Client,
    tx: mpsc::Sender<tonic::Result<shuffle::SliceResponse>>,
    cancel: tokens::CancellationToken,
) -> tokio::task::JoinHandle<Option<anyhow::Error>> {
    let request = broker::ListRequest {
        selector: Some(binding.partition_selector.clone()),
        watch: true,
        watch_resume: None,
    };
    let subscriber = Subscriber {
        binding: binding.index as u32,
        tx: tx.clone(),
    };
    let list_watch = client.list_watch_with(
        request,
        gazette::journal::list::SubscriberFold::new(subscriber),
    );

    let collection = binding.collection.clone();
    let binding = binding.index;

    let list_watch = async move {
        tokio::pin!(list_watch);

        loop {
            match list_watch.next().await {
                Some(Ok((added, removed))) => {
                    tracing::info!(
                        binding,
                        %collection,
                        added,
                        removed,
                        "collection listing updated",
                    );
                }
                Some(Err(gazette::RetryError {
                    attempt,
                    inner: err,
                })) => {
                    if err.is_transient() {
                        tracing::warn!(
                            binding,
                            %collection,
                            attempt,
                            %err,
                            "collection journal listing watch failed (will retry)",
                        );
                    } else {
                        return match err {
                            gazette::Error::Grpc(status) => crate::status_to_anyhow(status),
                            err => anyhow::anyhow!(err),
                        }
                        .context(format!(
                            "listing of collection {collection} (binding {binding}) failed"
                        ));
                    }
                }
                None => {
                    return anyhow::anyhow!(
                        "list_watch Stream of collection {collection} (binding {binding}) closed unexpectedly"
                    );
                }
            }
        }
    };

    tokio::spawn(cancel.run_until_cancelled_owned(list_watch))
}
