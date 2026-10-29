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
                tracing::warn!(journal, attempt, %inner, "confirming the head failed (will retry)");
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
