//! The recovery floor: where a replay of a disk journal may begin.
//!
//! The daemon derives the floor from journal contents, so it also persists it,
//! as a label on the journal's own spec under a name each deployment states.
//! It is a seek position and never a filter, because filtering by clock could
//! drop a record from the middle of a delta, and a floor which is absent or
//! behind costs replay work and changes nothing about the result. A floor
//! *ahead* of the derived one is the one thing which must never be written: it
//! would invite the deletion of fragments a live disk still needs.
//!
//! The value encoding is fixed rather than configurable: sixteen lowercase hex
//! characters of the u64 message clock. Fixed-width hex orders lexicographically
//! as it does numerically, and it matches the `estuary.dev/truncated-at` value
//! Flow already writes, so tooling which reads that label name generically
//! cannot misparse a disk journal.

use anyhow::Context;
use proto_gazette::{broker, uuid};

/// Modification time at which a replay of `journal` begins, in seconds.
///
/// A fragment is persisted no earlier than the records it holds, so a fragment
/// modified before the floor's clock holds only records below the floor and the
/// broker may skip it. Zero, which is an unnamed or absent label, begins at the
/// first fragment available.
pub async fn seek(
    client: &gazette::journal::Client,
    journal: &str,
    label: &str,
) -> anyhow::Result<i64> {
    if label.is_empty() {
        return Ok(0);
    }
    let response = client
        .list(list_request(journal, false))
        .await
        .with_context(|| format!("listing {journal} for its {label} label"))?;

    let Some(value) = response
        .journals
        .iter()
        .filter_map(|listed| listed.spec.as_ref())
        .flat_map(|spec| spec.labels.iter())
        .flat_map(|set| set.labels.iter())
        .find(|candidate| candidate.name == label)
    else {
        return Ok(0);
    };
    let clock = u64::from_str_radix(&value.value, 16)
        .with_context(|| format!("{journal} has a malformed {label} of {:?}", value.value))?;

    Ok(uuid::Clock::from_u64(clock).to_unix().0 as i64)
}

/// Advance `journal`'s floor label to `clock` in a task of its own.
///
/// The write is off the commit path and best effort: a floor which does not
/// reach the label costs a later recovery replay work and nothing else, so a
/// disk is never held up by one.
pub fn advance(
    client: gazette::journal::Client,
    journal: String,
    label: String,
    clock: uuid::Clock,
) {
    if label.is_empty() {
        return;
    }
    tokio::spawn(async move {
        if let Err(err) = write(&client, &journal, &label, clock).await {
            tracing::error!(journal, label, ?err, "failed to advance the recovery floor");
        }
    });
}

/// Set `label` on `journal`'s spec to `clock`, unless it already holds that
/// floor or a later one.
///
/// The write is a compare-and-swap against the spec's revision, because the
/// daemon is not its only writer. A lost race is another writer having changed
/// the spec, which the watch reports as its next snapshot and this then applies
/// over.
async fn write(
    client: &gazette::journal::Client,
    journal: &str,
    label: &str,
    clock: uuid::Clock,
) -> anyhow::Result<()> {
    let value = value(clock);

    let listings = client.clone().list_watch(list_request(journal, true));
    futures::pin_mut!(listings);

    loop {
        let listing = match futures::StreamExt::next(&mut listings).await {
            Some(Ok(listing)) => listing,
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "listing failed (will retry)");
                continue;
            }
            Some(Err(gazette::RetryError { inner, .. })) => {
                return Err(anyhow::Error::new(inner).context(format!("watching {journal}")));
            }
            None => anyhow::bail!("the listing of {journal} ended"),
        };
        let Some(change) = change(listing, journal, label, &value) else {
            return Ok(());
        };

        match client
            .apply(broker::ApplyRequest {
                changes: vec![change],
            })
            .await
        {
            Ok(_response) => return Ok(()),
            Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => {
                tracing::debug!(journal, "lost a race to write the recovery floor");
            }
            Err(err) => return Err(err.into()),
        }
    }
}

/// The change which sets `label` to `value` on the listed `journal`, or `None`
/// if it already holds that value or a later one, which includes a journal
/// which has since been deleted.
///
/// The listed spec is mutated and applied back rather than rebuilt from the
/// daemon's own inputs, because it carries fields the daemon does not model,
/// notably the `suspend` Gazette sets when it idles a journal.
fn change(
    listing: broker::ListResponse,
    journal: &str,
    label: &str,
    value: &str,
) -> Option<broker::apply_request::Change> {
    let (mut spec, mod_revision) = listing.journals.into_iter().find_map(|listed| {
        let spec = listed.spec?;
        (spec.name == journal).then_some((spec, listed.mod_revision))
    })?;

    let labels = &mut spec.labels.get_or_insert_default().labels;

    // Fixed-width hex orders lexicographically as the clock does numerically,
    // so this one comparison covers an equal floor and a later one alike.
    if labels
        .iter()
        .any(|held| held.name == label && held.value.as_str() >= value)
    {
        return None;
    }
    labels.retain(|held| held.name != label);

    labels.push(broker::Label {
        name: label.to_string(),
        value: value.to_string(),
        prefix: false,
    });
    labels.sort_by(|l, r| (&l.name, &l.value).cmp(&(&r.name, &r.value)));

    Some(broker::apply_request::Change {
        expect_mod_revision: mod_revision,
        upsert: Some(spec),
        delete: String::new(),
    })
}

/// Label value of `clock`, which every reader of this label parses the same way.
fn value(clock: uuid::Clock) -> String {
    format!("{:016x}", clock.as_u64())
}

fn list_request(journal: &str, watch: bool) -> broker::ListRequest {
    broker::ListRequest {
        selector: Some(broker::LabelSelector {
            include: Some(broker::LabelSet {
                labels: vec![broker::Label {
                    name: "name".to_string(),
                    value: journal.to_string(),
                    prefix: false,
                }],
            }),
            exclude: None,
        }),
        watch,
        ..Default::default()
    }
}

#[cfg(test)]
mod test {
    use super::{change, value};
    use proto_gazette::{broker, uuid};

    const LABEL: &str = "acmeCo/truncated-at";

    fn listing(labels: &[(&str, &str)]) -> broker::ListResponse {
        broker::ListResponse {
            journals: vec![broker::list_response::Journal {
                spec: Some(broker::JournalSpec {
                    name: "acmeCo/disk/one".to_string(),
                    labels: Some(broker::LabelSet {
                        labels: labels
                            .iter()
                            .map(|(name, value)| broker::Label {
                                name: name.to_string(),
                                value: value.to_string(),
                                prefix: false,
                            })
                            .collect(),
                    }),
                    // A field the daemon does not model, which the spec it
                    // applies back must still carry.
                    suspend: Some(broker::journal_spec::Suspend {
                        level: 1,
                        offset: 12345,
                    }),
                    ..Default::default()
                }),
                mod_revision: 42,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn advanced(labels: &[(&str, &str)], to: u64) -> Option<broker::JournalSpec> {
        let change = change(
            listing(labels),
            "acmeCo/disk/one",
            LABEL,
            &value(uuid::Clock::from_u64(to)),
        )?;

        assert_eq!(change.expect_mod_revision, 42);
        change.upsert
    }

    #[test]
    fn test_the_floor_label_only_advances() {
        // Absent, and behind, are both written.
        for held in [Vec::new(), vec![(LABEL, "0000000000000010")]] {
            let spec = advanced(&held, 0x20).expect("the floor advances");
            let labels = spec.labels.unwrap().labels;

            assert_eq!(labels.len(), 1);
            assert_eq!(labels[0].name, LABEL);
            assert_eq!(labels[0].value, "0000000000000020");

            // The listed spec is what is applied back, so what the daemon does
            // not model survives the write.
            assert_eq!(spec.suspend.unwrap().offset, 12345);
        }

        // Equal and ahead are both left alone.
        assert!(advanced(&[(LABEL, "0000000000000020")], 0x20).is_none());
        assert!(advanced(&[(LABEL, "0000000000000030")], 0x20).is_none());
    }

    /// The label joins the spec's own labels, which stay sorted as Gazette
    /// requires and are otherwise untouched.
    #[test]
    fn test_other_labels_are_preserved() {
        let spec = advanced(&[("acmeCo/tenant", "acmeCo"), ("zzz", "last")], 0xdeadbeef)
            .expect("the floor advances");

        let labels = spec.labels.unwrap().labels;
        let rendered: Vec<(&str, &str)> = labels
            .iter()
            .map(|label| (label.name.as_str(), label.value.as_str()))
            .collect();

        assert_eq!(
            rendered,
            vec![
                ("acmeCo/tenant", "acmeCo"),
                (LABEL, "00000000deadbeef"),
                ("zzz", "last"),
            ],
        );
    }

    /// A journal which is not in the listing is one which has been deleted, and
    /// there is nothing to label.
    #[test]
    fn test_a_missing_journal_is_not_written() {
        assert!(
            change(
                broker::ListResponse::default(),
                "acmeCo/disk/one",
                LABEL,
                "0000000000000020",
            )
            .is_none()
        );
    }
}
