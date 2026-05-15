use futures::StreamExt;
use proto_flow::shuffle;
use proto_gazette::broker;
use tokio::sync::mpsc;
use tracing::Instrument;

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
        let journal = journal_spec.name.clone();
        let added = shuffle::slice_response::ListingAdded {
            binding: self.binding,
            create_revision,
            mod_revision,
            spec: Some(journal_spec),
            route: Some(route),
        };

        service_kit::event!(
            tracing::Level::DEBUG,
            "watch",
            binding = self.binding,
            journal,
            "journal added to listing",
        );

        // Blocking safety: we may stuff this channel, but we're doing so from a
        // dedicated task. We compete with SliceActor::serve(), which obtains
        // permits to send on this channel.
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
        // We don't forward removed journals, because readers dynamically detect
        // them via the JOURNAL_NOT_FOUND broker status.
        service_kit::event!(
            tracing::Level::DEBUG,
            "watch",
            binding = self.binding,
            journal,
            "journal removed from listing",
        );
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
        gazette::journal::list::SubscriberFold::new_filtering_suspended(subscriber),
    );

    let collection = binding.collection.clone();
    let binding = binding.index;

    let list_watch = async move {
        tokio::pin!(list_watch);

        loop {
            match list_watch.next().await {
                Some(Ok((added, removed))) => {
                    service_kit::event!(
                        tracing::Level::DEBUG,
                        "watch",
                        binding,
                        collection = collection.to_string(),
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
                        service_kit::event!(
                            tracing::Level::WARN,
                            "watch",
                            binding,
                            collection = collection.to_string(),
                            attempt,
                            err = service_kit::event::debug(err),
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

    // Carry the caller's span (the Slice handler's `service_kit::HandlerGuard`
    // span) into the spawned task: tokio::spawn does not inherit the current
    // span, so without this the task's tracing events would not be associated
    // with the handler's event tracks.
    tokio::spawn(
        cancel
            .run_until_cancelled_owned(list_watch)
            .instrument(tracing::Span::current()),
    )
}
