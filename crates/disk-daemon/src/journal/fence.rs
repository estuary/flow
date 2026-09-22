//! Claiming sole authority to append to a disk journal.
//!
//! Every disk journal carries an `author` register. It names the epoch which may
//! append to that journal. A tenure reads the value as `R` at the moment it claims
//! the journal, and appends a fence record which swaps `R` for its own epoch `E`.
//!
//! `R` is read then and not earlier. A standby opens once and claims much later, and
//! every fence in between was another writer's — so a claim against the author its
//! open read would fail although that standby is the rightful writer, and two
//! standbies which both held that stale value could neither of them claim at all.
//! The swap arbitrates between claims which race, and for that it only has to
//! compare against the value each claimant just read: two which read the same value
//! both try to replace it, and one installs itself over the other.
//!
//! What keeps a displaced tenure from taking its journal back is not the staleness
//! of `R`. It is that a tenure claims once, at [`super::Opening::claim_journal`], so
//! a tenure whose appends start failing its author check has no path back to a
//! claim. Its client opens a new tenure, which is a new claimant.
//!
//! Gazette orders the fence with every other append. An append issued before a
//! fence, but ordered after it, therefore fails its author check.
//!
//! The register is not commit authority. Etcd can lose register state
//! independently of journal contents, and an empty register set matches any
//! selector. A journal whose registers were lost is therefore writable again,
//! while its committed records stay authoritative.

use proto_gazette::{broker, uuid};

/// Register naming the epoch permitted to append.
const AUTHOR: &str = "author";

/// What a journal held when a tenure opened it.
pub struct Probe {
    /// Value of the `author` register, absent while no writer holds the journal.
    pub author: Option<String>,
    /// Write head confirmed by the broker.
    pub head: i64,
}

/// Read a journal's registers and write head with a zero-byte append. Only an
/// append which carries content may modify registers, so this changes nothing.
///
/// The append is still an append, and it resumes a journal Gazette suspended as
/// any append does. That is what a recovery of the journal needs, and of a disk
/// nobody writes it costs a journal which idles back to sleep. A journal which is
/// absent here was deleted after the tenure listed it, which is a failure.
pub async fn probe(client: &gazette::journal::Client, journal: &str) -> anyhow::Result<Probe> {
    let request = broker::AppendRequest {
        journal: journal.to_string(),
        suspend: broker::append_request::Suspend::Resume as i32,
        ..Default::default()
    };
    let source = || futures::stream::empty::<std::io::Result<bytes::Bytes>>();

    let stream = client.append(request, source);
    futures::pin_mut!(stream);

    let response = loop {
        match futures::StreamExt::next(&mut stream).await {
            Some(Ok(response)) => break response,
            // Polling again pays the stream's backoff and restarts route discovery.
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "probe append failed (will retry)");
            }
            Some(Err(gazette::RetryError { inner, .. })) => {
                return Err(anyhow::Error::new(inner).context(format!("probing {journal}")));
            }
            None => unreachable!("an append stream does not end without a response"),
        }
    };

    Ok(Probe {
        author: author_of(&response),
        head: response.commit.map(|fragment| fragment.end).unwrap_or(0),
    })
}

/// Append `record` to claim `journal` for `epoch`, replacing the `prior` author.
///
/// An append whose RPC failed may still have landed, and a landed fence has
/// already excluded the previous writer. Such an attempt is therefore resolved by
/// probing again for `epoch`, and not by choosing a new one. A new epoch would
/// leave two epochs which each believe they hold the journal.
pub async fn claim(
    client: &gazette::journal::Client,
    journal: &str,
    prior: Option<&str>,
    epoch: uuid::Producer,
    record: bytes::Bytes,
) -> anyhow::Result<()> {
    let held = value(epoch);

    let request = broker::AppendRequest {
        journal: journal.to_string(),
        check_registers: Some(selector(prior)),
        union_registers: Some(labels::build_set([(AUTHOR, held.as_str())])),
        subtract_registers: prior.map(|prior| labels::build_set([(AUTHOR, prior)])),
        ..Default::default()
    };
    let source = || futures::stream::once(futures::future::ready(Ok(record.clone())));

    let stream = client.append(request, source);
    futures::pin_mut!(stream);

    // A transient failure is retried rather than probed: `check_registers` refuses a
    // retry whose earlier attempt had landed, and the probe below settles that outcome
    // either way. Giving up here instead would fail a claim which no writer holds.
    let err = loop {
        match futures::StreamExt::next(&mut stream).await {
            Some(Ok(response)) => {
                let author = author_of(&response);

                anyhow::ensure!(
                    author.as_deref() == Some(held.as_str()),
                    "claimed {journal} but its author is {author:?} rather than {held}",
                );
                return Ok(());
            }
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "claim append failed (will retry)");
            }
            Some(Err(gazette::RetryError { inner, .. })) => break inner,
            None => unreachable!("an append stream does not end without a response"),
        }
    };

    let probe = probe(client, journal).await?;

    if probe.author.as_deref() == Some(held.as_str()) {
        tracing::info!(journal, %held, "fence append failed but had landed");
        return Ok(());
    }
    Err(anyhow::Error::new(err).context(format!(
        "failed to claim {journal} for {held}, whose author is now {:?}",
        probe.author,
    )))
}

/// Build the fence record which installs `epoch` as a journal's author.
///
/// Its own producer differs from `epoch`. Were it `epoch`, this record's wall-clock
/// stamp would become that producer's last commit, and a clock which stepped back
/// before the tenure's first record would have replay drop that record as a
/// duplicate. A transient retry within one append re-sends these same bytes, and
/// Gazette de-duplicates them by UUID.
pub fn record(epoch: uuid::Producer) -> bytes::Bytes {
    let record = crate::proto::DiskRecord {
        uuid: super::uuid_bytes(
            super::random_producer(),
            uuid::Clock::from_time(std::time::SystemTime::now()),
            uuid::Flags::OUTSIDE_TXN,
        ),
        chunks: Vec::new(),
        opens_horizon: false,
        installs_epoch: bytes::Bytes::copy_from_slice(epoch.as_bytes()),
    };

    let mut buf = bytes::BytesMut::new();
    proto_gazette::fixed_framing::encode(&record, &mut buf);
    buf.freeze()
}

/// Selector which every append of a claimed journal carries. An append which
/// races a replacement tenure then fails rather than advancing the disk.
pub fn held_by(epoch: uuid::Producer) -> broker::LabelSelector {
    selector(Some(&value(epoch)))
}

/// Register value of an epoch, which is its producer in hex.
pub fn value(epoch: uuid::Producer) -> String {
    epoch
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Selector matching a journal whose author is `expect`, or which has no author
/// at all when `expect` is None.
fn selector(expect: Option<&str>) -> broker::LabelSelector {
    match expect {
        Some(author) => broker::LabelSelector {
            include: Some(labels::build_set([(AUTHOR, author)])),
            exclude: None,
        },
        // An exclude of any value rejects a journal which some other tenure
        // claimed between this tenure's probe and its claim.
        None => broker::LabelSelector {
            include: None,
            exclude: Some(labels::build_set([(AUTHOR, "")])),
        },
    }
}

fn author_of(response: &broker::AppendResponse) -> Option<String> {
    // A claim installs exactly one author: it subtracts the prior value and unions
    // its own. More than one, which `maybe_one` refuses, is therefore not something
    // this daemon can produce, and is read the same way as no author at all.
    match labels::maybe_one(response.registers.as_ref()?, AUTHOR) {
        Ok("") | Err(_) => None,
        Ok(author) => Some(author.to_string()),
    }
}

#[cfg(test)]
mod test {
    use super::{author_of, held_by, selector, value};
    use proto_gazette::{broker, uuid};

    const EPOCH: uuid::Producer = uuid::Producer([0x01, 0x23, 0x45, 0x67, 0x89, 0xab]);

    #[test]
    fn test_an_epoch_is_a_stable_register_value() {
        assert_eq!(value(EPOCH), "0123456789ab");

        let include = held_by(EPOCH).include.unwrap();
        assert_eq!(include.labels[0].name, "author");
        assert_eq!(include.labels[0].value, "0123456789ab");
    }

    #[test]
    fn test_an_unclaimed_journal_is_selected_by_excluding_any_author() {
        let unclaimed = selector(None);

        assert_eq!(unclaimed.include, None);
        assert_eq!(unclaimed.exclude.unwrap().labels[0].value, "");
    }

    #[test]
    fn test_the_author_register_is_read_from_a_response() {
        assert_eq!(author_of(&broker::AppendResponse::default()), None);

        let response = broker::AppendResponse {
            registers: Some(broker::LabelSet {
                labels: vec![
                    broker::Label {
                        name: "author".to_string(),
                        value: "0123456789ab".to_string(),
                        prefix: false,
                    },
                    broker::Label {
                        name: "other".to_string(),
                        value: "ignored".to_string(),
                        prefix: false,
                    },
                ],
            }),
            ..Default::default()
        };
        assert_eq!(author_of(&response).as_deref(), Some("0123456789ab"));
    }
}

#[cfg(test)]
mod broker_test {
    //! Claiming a journal against a real broker.

    use super::{claim, record, value};
    use crate::test_support::broker::Fixture;

    /// A claim whose append landed, even though its RPC reported no success, is
    /// resolved by finding the epoch already installed. It does not choose a new epoch,
    /// because two epochs which each believe they hold the journal is the one outcome
    /// fencing exists to prevent.
    #[tokio::test]
    async fn test_an_ambiguous_claim_finds_its_own_epoch() {
        let fixture = Fixture::start().await;
        let journal = "acmeCo/disk/ambiguous";

        () = fixture.create_journal(fixture.spec(journal)).await.unwrap();

        // A tenure's epoch, which this claims with directly rather than through an
        // `Opening`. Nothing else appends here.
        let epoch = crate::journal::random_producer();
        let fence_record = record(epoch);

        () = claim(&fixture.client, journal, None, epoch, fence_record.clone())
            .await
            .unwrap();

        // A retry of an ambiguous append repeats the claim. It re-appends identical
        // content under a check which no longer matches.
        () = claim(&fixture.client, journal, None, epoch, fence_record)
            .await
            .unwrap();

        assert_eq!(
            fixture.author(journal).await.as_deref(),
            Some(value(epoch).as_str()),
        );

        fixture.stop().await;
    }
}
