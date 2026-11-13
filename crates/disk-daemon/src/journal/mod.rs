mod floor;
mod reader;
mod writer;

pub use reader::{Read, open};
pub use writer::Writer;

use proto_gazette::broker;

/// Confirm the head of `journal` with a zero-byte append, which changes its
/// suspension as `suspend` says. Returns the head and the journal's registers,
/// or None if `Keep` finds the journal fully suspended.
async fn confirm_head(
    client: &gazette::journal::Client,
    journal: &str,
    suspend: broker::append_request::Suspend,
) -> anyhow::Result<Option<(i64, broker::LabelSet)>> {
    let request = broker::AppendRequest {
        journal: journal.to_string(),
        suspend: suspend as i32,
        ..Default::default()
    };
    let mut stream = std::pin::pin!(client.append(request, futures::stream::empty));

    loop {
        match futures::StreamExt::next(&mut stream).await {
            Some(Ok(response)) => {
                let commit = response
                    .commit
                    .expect("an append which succeeded reports its commit");
                return Ok(Some((commit.end, response.registers.unwrap_or_default())));
            }
            Some(Err(gazette::RetryError {
                inner: gazette::Error::BrokerStatus(broker::Status::Suspended),
                ..
            })) => return Ok(None),
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                service_kit::event!(
                    tracing::Level::WARN,
                    "broker",
                    attempt,
                    inner = inner.to_string(),
                    "confirming the head failed (will retry)"
                );
            }
            Some(Err(gazette::RetryError { inner, .. })) => {
                return Err(
                    anyhow::Error::new(inner).context(format!("confirming the head of {journal}"))
                );
            }
            None => unreachable!("an append stream does not end without a response"),
        }
    }
}

struct Metrics {
    replayed_bytes: metrics::Counter,
    deltas: metrics::Counter,
    delta_bytes: metrics::Counter,
    last_delta_bytes: metrics::Gauge,
    horizons_opened: metrics::Counter,
    floors_stored: metrics::Counter,
}

impl Metrics {
    fn new(journal: &str) -> Self {
        static DESCRIBE: std::sync::Once = std::sync::Once::new();
        DESCRIBE.call_once(|| {
            metrics::describe_counter!(
                "disk_daemon_journal_replayed_bytes",
                metrics::Unit::Bytes,
                "journal bytes read for replay",
            );
            metrics::describe_counter!(
                "disk_daemon_journal_deltas",
                metrics::Unit::Count,
                "deltas prepared, including empty ones",
            );
            metrics::describe_counter!(
                "disk_daemon_journal_delta_bytes",
                metrics::Unit::Bytes,
                "encoded bytes of the deltas prepared, including horizon copies",
            );
            metrics::describe_gauge!(
                "disk_daemon_journal_last_delta_bytes",
                metrics::Unit::Bytes,
                "encoded bytes of the last delta prepared",
            );
            metrics::describe_counter!(
                "disk_daemon_journal_horizons_opened",
                metrics::Unit::Count,
                "recovery horizons whose opener the writer appended",
            );
            metrics::describe_counter!(
                "disk_daemon_journal_floors_stored",
                metrics::Unit::Count,
                "recovery floors the writer stored, each once its horizon completed and that delta committed",
            );
        });

        Self {
            replayed_bytes: metrics::counter!("disk_daemon_journal_replayed_bytes", "journal" => journal.to_string()),
            deltas: metrics::counter!("disk_daemon_journal_deltas", "journal" => journal.to_string()),
            delta_bytes: metrics::counter!("disk_daemon_journal_delta_bytes", "journal" => journal.to_string()),
            last_delta_bytes: metrics::gauge!("disk_daemon_journal_last_delta_bytes", "journal" => journal.to_string()),
            horizons_opened: metrics::counter!("disk_daemon_journal_horizons_opened", "journal" => journal.to_string()),
            floors_stored: metrics::counter!("disk_daemon_journal_floors_stored", "journal" => journal.to_string()),
        }
    }
}
