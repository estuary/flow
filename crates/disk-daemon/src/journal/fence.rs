//! Claiming sole authority to append to a disk journal.
//!
//! Every disk journal carries an `author` register naming the epoch which may
//! append to it. A session reads that value once as `R` when it opens, and
//! swaps it for its own epoch `E` by appending a fence record. `R` is never
//! re-read: a session may only replace the value it observed at startup, so a
//! session which was itself displaced cannot take the journal back.
//!
//! Gazette orders the fence with every other append, so an append issued before
//! a fence but ordered after it fails its author check.
//!
//! The register is not commit authority. Etcd can lose register state
//! independently of journal contents, and an empty register set matches any
//! selector, so a journal whose registers were lost is writable again while its
//! committed records remain authoritative.

use proto_gazette::{broker, uuid};

/// Register naming the epoch permitted to append.
const AUTHOR: &str = "author";

/// What a journal held when a session opened it.
pub struct Probe {
    /// Value of the `author` register, absent while no writer holds the journal.
    pub author: Option<String>,
    /// Write head confirmed by the broker.
    pub head: i64,
    /// False when the journal does not exist, which is the ordinary state of a
    /// disk that has never published.
    pub exists: bool,
}

/// Read a journal's registers and write head with a zero-byte append. Only an
/// append carrying content may modify registers, so this changes nothing.
pub async fn probe(client: &gazette::journal::Client, journal: &str) -> anyhow::Result<Probe> {
    let request = broker::AppendRequest {
        journal: journal.to_string(),
        ..Default::default()
    };

    let source = || futures::stream::empty::<std::io::Result<bytes::Bytes>>();

    let response = match super::append(client, request, source).await {
        Ok(response) => response,
        Err(gazette::Error::BrokerStatus(broker::Status::JournalNotFound)) => {
            return Ok(Probe {
                author: None,
                head: 0,
                exists: false,
            });
        }
        Err(err) => return Err(anyhow::Error::new(err).context(format!("probing {journal}"))),
    };

    Ok(Probe {
        author: author_of(&response),
        head: response.commit.map(|fragment| fragment.end).unwrap_or(0),
        exists: true,
    })
}

/// Append `record` to claim `journal` for `epoch`, replacing the `prior` author.
///
/// An append whose RPC failed may still have landed, and a landed fence has
/// already excluded the previous writer. Such an attempt is therefore resolved
/// by re-probing for `epoch` rather than by choosing a new one, which would
/// leave two epochs believing they hold the journal.
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
/// Its own producer is distinct from `epoch`, so that a reader attributes the
/// fence to the session which appended it rather than to the epoch it installs.
/// A transient retry within one append re-sends these same bytes, which Gazette
/// de-duplicates by UUID.
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

/// Selector which every append of a claimed journal carries, so that an append
/// racing a replacement session fails rather than advancing the disk.
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
        // An exclude of any value rejects a journal some other session claimed
        // between this session's probe and its claim.
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
