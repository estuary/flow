//! Claiming sole authority to append to a disk journal.
//!
//! Every disk journal carries an `author` register. It names the epoch which may
//! append to that journal. A session reads the value once as `R` when it opens. It
//! then appends a fence record to swap `R` for its own epoch `E`. Nothing ever
//! re-reads `R`. A session may only replace the value it observed at startup, so a
//! session which was itself displaced cannot take the journal back.
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

/// What a journal held when a session opened it.
pub struct Probe {
    /// Value of the `author` register, absent while no writer holds the journal.
    pub author: Option<String>,
    /// Write head confirmed by the broker.
    pub head: i64,
}

/// Read a journal's registers and write head with a zero-byte append. Only an
/// append which carries content may modify registers, so this changes nothing.
///
/// The append is still an append, and it resumes a suspended journal as any
/// append does. A caller uses it once it has decided the journal must be awake,
/// because the journal holds content a recovery reads, or because this session
/// has already appended. A journal which is absent here was deleted after the
/// session listed it, which is a failure and not a fresh disk.
pub async fn probe(client: &gazette::journal::Client, journal: &str) -> anyhow::Result<Probe> {
    let probe = probe_with(client, journal, broker::append_request::Suspend::Resume).await?;

    Ok(probe.expect("a resuming probe is never refused as suspended"))
}

/// [`probe`], refusing to resume a suspended journal. `None` reports one found
/// suspended and left exactly as it was.
///
/// A session probes this way when its listing said the journal was active. A
/// suspension landing between the two must not be undone by what is only a read.
/// The caller re-reads the listing, which now carries the suspension record,
/// and decides deliberately.
pub async fn probe_unless_suspended(
    client: &gazette::journal::Client,
    journal: &str,
) -> anyhow::Result<Option<Probe>> {
    probe_with(client, journal, broker::append_request::Suspend::NoResume).await
}

async fn probe_with(
    client: &gazette::journal::Client,
    journal: &str,
    suspend: broker::append_request::Suspend,
) -> anyhow::Result<Option<Probe>> {
    let request = broker::AppendRequest {
        journal: journal.to_string(),
        suspend: suspend as i32,
        ..Default::default()
    };

    let source = || futures::stream::empty::<std::io::Result<bytes::Bytes>>();

    match super::append(client, request, source).await {
        Ok(response) => Ok(Some(Probe {
            author: author_of(&response),
            head: response.commit.map(|fragment| fragment.end).unwrap_or(0),
        })),
        Err(gazette::Error::BrokerStatus(broker::Status::Suspended)) => Ok(None),
        Err(err) => Err(anyhow::Error::new(err).context(format!("probing {journal}"))),
    }
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
        union_registers: Some(label_set(&held)),
        subtract_registers: prior.map(label_set),
        ..Default::default()
    };
    let source = || futures::stream::once(futures::future::ready(Ok(record.clone())));

    let err = match super::append(client, request, source).await {
        Ok(response) => {
            let author = author_of(&response);

            anyhow::ensure!(
                author.as_deref() == Some(held.as_str()),
                "claimed {journal} but its author is {author:?} rather than {held}",
            );
            return Ok(());
        }
        Err(err) => err,
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
/// Its own producer differs from `epoch`. A reader therefore attributes the fence
/// to the session which appended it, and not to the epoch it installs. A transient
/// retry within one append re-sends these same bytes, and Gazette de-duplicates
/// them by UUID.
pub fn record(epoch: uuid::Producer) -> bytes::Bytes {
    let record = crate::proto::DiskRecord {
        uuid: super::uuid_bytes(
            super::fresh_producer(),
            uuid::Clock::from_time(std::time::SystemTime::now()),
            uuid::Flags::OUTSIDE_TXN,
        ),
        chunks: Vec::new(),
        opens_horizon: false,
        installs_epoch: bytes::Bytes::copy_from_slice(epoch.as_bytes()),
    };

    let mut buf = bytes::BytesMut::new();
    gazette::journal::framing::encode(&record, &mut buf);
    buf.freeze()
}

/// Selector which every append of a claimed journal carries. An append which
/// races a replacement session then fails rather than advancing the disk.
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
            include: Some(label_set(author)),
            exclude: None,
        },
        // An exclude of any value rejects a journal which some other session
        // claimed between this session's probe and its claim.
        None => broker::LabelSelector {
            include: None,
            exclude: Some(label_set("")),
        },
    }
}

fn label_set(author: &str) -> broker::LabelSet {
    broker::LabelSet {
        labels: vec![broker::Label {
            name: AUTHOR.to_string(),
            value: author.to_string(),
            prefix: false,
        }],
    }
}

fn author_of(response: &broker::AppendResponse) -> Option<String> {
    response
        .registers
        .iter()
        .flat_map(|set| set.labels.iter())
        .find(|label| label.name == AUTHOR)
        .map(|label| label.value.clone())
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
