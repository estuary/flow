use crate::proto;
use proto_gazette::{broker, fixed_framing, uuid};

mod ledger;
mod lifecycle;
mod replay;
mod sequencer;
mod uncommitted;
mod writer;

pub use lifecycle::{Promoted, Recovered, Standby};
pub use uncommitted::UncommittedDelta;
pub use writer::Writer;

/// Append state carried from [`Standby::new`] through promotion into the writer.
struct Journal {
    /// Name of the disk's journal.
    name: String,
    /// Lists the journal and updates its recovery-floor label.
    client: gazette::journal::Client,
    /// Cancelled when the attachment ends, abandoning the lifecycle phase in progress.
    ended: tokio_util::sync::CancellationToken,
    /// Installed in the journal's `author` register and checked by subsequent
    /// appends.
    author: uuid::Producer,
    /// Write head the broker last confirmed.
    head: i64,
    /// Recovery floor resolved at open, and stored again as the writer completes a
    /// horizon.
    floor: i64,
}

impl Journal {
    async fn append_record(
        &mut self,
        appender: &mut publisher::Appender,
        record: &proto::DiskRecord,
    ) -> anyhow::Result<()> {
        fixed_framing::encode(record, &mut appender.buffer);

        appender
            .checkpoint()
            .await
            .map_err(proto_grpc::status_to_anyhow)
    }

    /// Append the exact prepared acknowledgement and wait for confirmation.
    async fn append_ack(
        &mut self,
        appender: &mut publisher::Appender,
        ack: &bytes::Bytes,
    ) -> anyhow::Result<()> {
        appender.buffer.extend_from_slice(ack);

        self.flush(appender).await
    }

    /// Flush and learn the broker-confirmed head.
    async fn flush(&mut self, appender: &mut publisher::Appender) -> anyhow::Result<()> {
        let response = appender
            .flush()
            .await
            .map_err(proto_grpc::status_to_anyhow)?;

        if let Some(commit) = &response.commit {
            self.head = commit.end;
        }
        Ok(())
    }
}

/// The write head of `journal`, confirmed by a zero-byte append which also resumes a
/// suspended journal.
pub(crate) async fn confirm_head(
    client: &gazette::journal::Client,
    journal: &str,
) -> anyhow::Result<i64> {
    let request = broker::AppendRequest {
        journal: journal.to_string(),
        suspend: broker::append_request::Suspend::Resume as i32,
        ..Default::default()
    };
    let mut stream = std::pin::pin!(client.append(request, futures::stream::empty));

    loop {
        match futures::StreamExt::next(&mut stream).await {
            Some(Ok(response)) => {
                let commit = response
                    .commit
                    .expect("an append which succeeded reports its commit");
                return Ok(commit.end);
            }
            // Polling again pays the stream's backoff and restarts route discovery.
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "write barrier failed (will retry)");
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

/// The recovery floor in `listing`'s label, or zero if it has none.
fn listed_floor(
    journal: &str,
    listing: broker::list_response::Journal,
    head: i64,
) -> anyhow::Result<i64> {
    let spec = listing.spec.expect("a listed journal has a spec");
    let set = spec.labels.unwrap_or_default();

    let value = labels::maybe_one(&set, labels::DISK_RECOVERY_FLOOR).map_err(|err| {
        crate::invalid_argument(format!(
            "journal {journal}'s recovery-floor label is invalid: {err}"
        ))
    })?;
    if value.is_empty() {
        return Ok(0);
    }
    // Replay reads from the floor to the head. From a floor at or past the head it
    // reads nothing, and Gazette reads from -1 as from the head, so replay would
    // find no committed data and format a fresh disk over it.
    i64::from_str_radix(value, 16)
        .ok()
        .filter(|floor| (0..head).contains(floor))
        .ok_or_else(|| {
            crate::invalid_argument(format!(
                "journal {journal}'s recovery-floor label {value:?} is not an offset \
                 below its head {head}"
            ))
        })
}

/// The spec change which sets `listing`'s recovery-floor label to `floor`, or `None`
/// if the label already holds it.
fn floor_change(
    listing: broker::list_response::Journal,
    floor: i64,
) -> Option<broker::apply_request::Change> {
    let mut spec = listing.spec.expect("a listed journal has a spec");
    // Zero-padding keeps the label's string order the same as its numeric order.
    let value = format!("{floor:016x}");

    let held = spec
        .labels
        .as_ref()
        .and_then(|set| labels::maybe_one(set, labels::DISK_RECOVERY_FLOOR).ok());

    if held == Some(value.as_str()) {
        return None;
    }
    spec.labels = Some(labels::set_value(
        spec.labels.take().unwrap_or_default(),
        labels::DISK_RECOVERY_FLOOR,
        &value,
    ));

    Some(broker::apply_request::Change {
        expect_mod_revision: listing.mod_revision,
        upsert: Some(spec),
        delete: String::new(),
    })
}

fn uuid_bytes(producer: uuid::Producer, clock: uuid::Clock, flags: uuid::Flags) -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(uuid::build(producer, clock, flags).as_bytes())
}

#[cfg(test)]
mod test {
    use super::{floor_change, listed_floor};
    use proto_gazette::broker;

    /// A disk journal's listing at revision 42, carrying `floor` as its
    /// recovery-floor label if given.
    fn listing(floor: Option<&str>) -> broker::list_response::Journal {
        let mut set = labels::build_set([(labels::CONTENT_TYPE, crate::CONTENT_TYPE_DISK)]);
        if let Some(floor) = floor {
            set = labels::set_value(set, labels::DISK_RECOVERY_FLOOR, floor);
        }
        broker::list_response::Journal {
            spec: Some(broker::JournalSpec {
                labels: Some(set),
                ..Default::default()
            }),
            mod_revision: 42,
            ..Default::default()
        }
    }

    /// A journal without a floor label replays from zero. A label must be a hex
    /// offset below the head (here 1000000); any other is refused.
    #[test]
    fn test_a_floor_label_is_an_offset_below_the_head() {
        let mut out = Vec::new();

        for value in [
            None,
            Some("00000000000186a0"),
            Some("00000000000f4240"), // Exactly the head.
            Some("ffffffffffffffff"), // Beyond i64::MAX.
            Some("-1"),
            Some("not-hex"),
        ] {
            let outcome = match listed_floor("acmeCo/disk/one", listing(value), 1_000_000) {
                Ok(floor) => format!("floor {floor}"),
                Err(err) => format!("refused: {err:#}"),
            };
            out.push(format!("{:<18}{outcome}", value.unwrap_or("no label")));
        }
        insta::assert_snapshot!(out.join("\n"), @r#"
        no label          floor 0
        00000000000186a0  floor 100000
        00000000000f4240  refused: journal acmeCo/disk/one's recovery-floor label "00000000000f4240" is not an offset below its head 1000000 (InvalidArgument)
        ffffffffffffffff  refused: journal acmeCo/disk/one's recovery-floor label "ffffffffffffffff" is not an offset below its head 1000000 (InvalidArgument)
        -1                refused: journal acmeCo/disk/one's recovery-floor label "-1" is not an offset below its head 1000000 (InvalidArgument)
        not-hex           refused: journal acmeCo/disk/one's recovery-floor label "not-hex" is not an offset below its head 1000000 (InvalidArgument)
        "#);
    }

    /// A change sets the floor label at the listing's revision, so a spec something
    /// else changed meanwhile refuses it, and keeps the other labels. It lowers a
    /// floor as readily as it raises one, and leaves a label which already holds it.
    #[test]
    fn test_a_floor_change_sets_the_label_at_the_listed_revision() {
        let mut out = Vec::new();

        for held in [None, Some(99_999), Some(100_000), Some(100_001)] {
            let label = held.map(|held| format!("{held:016x}"));

            let outcome = match floor_change(listing(label.as_deref()), 100_000) {
                None => "leaves it".to_string(),
                Some(change) => {
                    let set = change.upsert.unwrap().labels.unwrap();
                    format!(
                        "sets {} at revision {}, content type {}",
                        labels::maybe_one(&set, labels::DISK_RECOVERY_FLOOR).unwrap(),
                        change.expect_mod_revision,
                        labels::maybe_one(&set, labels::CONTENT_TYPE).unwrap(),
                    )
                }
            };
            out.push(format!("held {:<16}{outcome}", format!("{held:?}")));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        held None            sets 00000000000186a0 at revision 42, content type application/x-journal-backed-disk
        held Some(99999)     sets 00000000000186a0 at revision 42, content type application/x-journal-backed-disk
        held Some(100000)    leaves it
        held Some(100001)    sets 00000000000186a0 at revision 42, content type application/x-journal-backed-disk
        ");
    }
}
