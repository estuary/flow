//! Gazette journal operations which are particular to a test, and which
//! [`gazette::journal::Client`] therefore does not carry. Listing one journal is that
//! client's own `get_journal`, and creating or updating one is its raw `apply`, which
//! [`create`] and [`update`] here condition as a test wants.
//!
//! These are free functions over a [`gazette::journal::Client`], as
//! [`crate::reset_journals`] is, so a fixture which holds a pre-cloned client can use
//! them without borrowing its [`crate::DataPlane`] across an await point.

use anyhow::Context;
use proto_gazette::broker;

/// Read `journal`'s registers and write head with a zero-byte append, or `None` where
/// the broker refused the append because the journal is suspended.
///
/// Only an append which carries content may modify registers, so this changes nothing
/// of what the journal holds. `suspend` decides what it does to suspension: `Resume`
/// wakes a suspended journal as any append does, and `IfFlushed` or `Now` ask the
/// broker to suspend it exactly as its own idle pulse eventually would.
///
/// `None` is how a suspending mode reports that it took effect: the broker re-resolves
/// the journal it just suspended, and answers SUSPENDED. `NoResume` is refused the
/// same way, having declined to wake a journal which is already suspended.
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

    match client.append_once(request, source).await {
        Ok(response) => Ok(Some(response)),
        Err(gazette::Error::BrokerStatus(broker::Status::Suspended)) => Ok(None),
        Err(err) => Err(anyhow::Error::new(err).context(format!("probing {journal}"))),
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

/// Value of one of `journal`'s registers, and `None` where the journal carries none
/// of that name.
///
/// A register set with more than one value of a name is read as none at all, because
/// nothing which writes one register writes it twice.
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

/// Create `spec`'s journal unless it exists, and report whether this call created it.
///
/// This is what a deployer does, and it is the request an activation and the
/// publisher's partition mapping each build inline. It is here because a test is the
/// only caller which wants the outcome as a bool: a fixture which opens the same
/// journal twice creates it once, and a lost race to another creator is the rare
/// instance of that same outcome.
pub async fn create(
    client: &gazette::journal::Client,
    spec: broker::JournalSpec,
) -> anyhow::Result<bool> {
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
        Ok(_response) => Ok(true),
        Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => Ok(false),
        Err(err) => Err(anyhow::Error::new(err).context(format!("creating {journal}"))),
    }
}

/// Apply `modify` to the live spec of `journal`, and apply what it leaves. Report
/// whether `modify` changed anything.
///
/// The change is conditioned on the `mod_revision` which was listed, so a spec which
/// something else changed in between is refused rather than overwritten. That refusal
/// is returned as an error, because a test plants the value it wants and does not
/// retry.
pub async fn update(
    client: &gazette::journal::Client,
    journal: &str,
    modify: impl FnOnce(&mut broker::JournalSpec) -> bool,
) -> anyhow::Result<bool> {
    let listed = client
        .get_journal(journal)
        .await
        .with_context(|| format!("listing {journal}"))?;

    let Some(listed) = listed else {
        anyhow::bail!("journal {journal} does not exist");
    };
    let mut spec = listed.spec.expect("a listed journal has a spec");

    if !modify(&mut spec) {
        return Ok(false);
    }

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

    Ok(true)
}
