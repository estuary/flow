//! Test helpers for journal creation, updates, and head/register probes.
//!
//! Free functions accept a cloned client so fixtures need not borrow their
//! `DataPlane` across awaits. General listing remains `Client::get_journal`.

use anyhow::Context;
use proto_gazette::broker;

/// Probe registers and write head with a zero-byte append, leaving content and
/// registers unchanged. `suspend` controls whether to resume or suspend the journal.
/// Return `None` for SUSPENDED, including when the requested suspension takes effect
/// or `NoResume` encounters an already suspended journal.
pub async fn probe(
    client: &gazette::journal::Client,
    journal: &str,
    suspend: broker::append_request::Suspend,
) -> anyhow::Result<Option<broker::AppendResponse>> {
    let request = broker::AppendRequest {
        journal: journal.to_string(),
        suspend: suspend as i32,
        ..Default::default()
    };
    let source = || futures::stream::empty::<std::io::Result<bytes::Bytes>>();

    let stream = client.append(request, source);
    futures::pin_mut!(stream);

    loop {
        match futures::StreamExt::next(&mut stream).await {
            Some(Ok(response)) => return Ok(Some(response)),
            Some(Err(gazette::RetryError {
                inner: gazette::Error::BrokerStatus(broker::Status::Suspended),
                ..
            })) => return Ok(None),
            // Polling again pays the stream's backoff and restarts route discovery.
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "probe append failed (will retry)");
            }
            Some(Err(gazette::RetryError { inner, .. })) => {
                return Err(anyhow::Error::new(inner).context(format!("probing {journal}")));
            }
            None => unreachable!("an append stream does not end without a response"),
        }
    }
}

/// Broker-confirmed write head of `journal`. A journal which was created and never
/// appended to holds zero.
pub async fn head(client: &gazette::journal::Client, journal: &str) -> anyhow::Result<i64> {
    let probed = probe(client, journal, broker::append_request::Suspend::Resume)
        .await?
        .context("a resuming probe is never refused as suspended")?;

    Ok(probed.commit.map(|fragment| fragment.end).unwrap_or(0))
}

/// Return a register's single nonempty value, or `None` if absent or ambiguous.
pub async fn register(
    client: &gazette::journal::Client,
    journal: &str,
    name: &str,
) -> anyhow::Result<Option<String>> {
    let probed = probe(client, journal, broker::append_request::Suspend::Resume)
        .await?
        .context("a resuming probe is never refused as suspended")?;

    let Some(registers) = probed.registers else {
        return Ok(None);
    };
    let mut values = registers
        .labels
        .iter()
        .filter(|label| label.name == name && !label.value.is_empty());

    match (values.next(), values.next()) {
        (Some(one), None) => Ok(Some(one.value.clone())),
        _ => Ok(None),
    }
}

/// Create the journal if absent. Repeated setup and races with another creator
/// succeed without replacing the existing spec.
pub async fn create(
    client: &gazette::journal::Client,
    spec: broker::JournalSpec,
) -> anyhow::Result<()> {
    let journal = spec.name.clone();

    let request = broker::ApplyRequest {
        changes: vec![broker::apply_request::Change {
            // Zero requires that no journal of this name exists.
            expect_mod_revision: 0,
            upsert: Some(spec),
            delete: String::new(),
        }],
    };

    match client.apply(request).await {
        Ok(_response) => Ok(()),
        Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => Ok(()),
        Err(err) => Err(anyhow::Error::new(err).context(format!("creating {journal}"))),
    }
}

/// Modify the live spec with a compare-and-swap against its listed revision.
/// Return a conflict instead of retrying or overwriting a concurrent change.
pub async fn update(
    client: &gazette::journal::Client,
    journal: &str,
    modify: impl FnOnce(&mut broker::JournalSpec),
) -> anyhow::Result<()> {
    let listed = client
        .get_journal(journal)
        .await
        .with_context(|| format!("listing {journal}"))?;

    let Some(listed) = listed else {
        anyhow::bail!("journal {journal} does not exist");
    };
    let mut spec = listed.spec.expect("a listed journal has a spec");
    modify(&mut spec);

    _ = client
        .apply(broker::ApplyRequest {
            changes: vec![broker::apply_request::Change {
                expect_mod_revision: listed.mod_revision,
                upsert: Some(spec),
                delete: String::new(),
            }],
        })
        .await
        .with_context(|| format!("updating {journal}"))?;

    Ok(())
}
