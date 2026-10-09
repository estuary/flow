//! The journal specification a disk is served from, and the recovery floor which
//! rides on it.
//!
//! The daemon creates no journal and converges none. It validates the live spec of
//! the journal an `Open` names, because a spec a disk could not be recovered from is
//! a disk which can never prepare a delta. The recovery floor is the one field it
//! writes: it is the daemon's own state about a disk rather than the spec owner's.

use super::until_ended;
use crate::failure;
use anyhow::Context;
use proto_gazette::broker;

/// What a journal holds as a tenure opens it.
#[derive(Debug)]
pub(super) struct Resolved {
    /// Write head. Zero is a journal with no content at all.
    pub(super) head: i64,
    /// Recovery floor a replay of this journal seeks from.
    pub(super) floor: i64,
    /// See [`Listed::marked`].
    pub(super) marked: bool,
}

/// What a journal's listing says of it.
#[derive(Debug)]
pub(super) struct Listed {
    /// Recovery floor a replay of this journal seeks from.
    pub(super) floor: i64,
    /// Whether the journal carries a recovery-floor label. The writer stores one once
    /// a delta of the disk first commits, so a journal which carries one has committed
    /// state, whatever its fragments still hold.
    pub(super) marked: bool,
}

/// Resolve what `journal` holds as a tenure opens it: [`list`] it, then confirm its
/// head with a [`barrier`], and [`check_floor`] the one against the other.
pub(super) async fn resolve(
    client: &gazette::journal::Client,
    journal: &str,
    ended: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<Resolved> {
    let listed = list(client, journal, ended).await?;
    let head = until_ended(ended, "confirming the head", barrier(client, journal)).await?;
    () = check_floor(journal, &listed, head)?;

    Ok(Resolved {
        head,
        floor: listed.floor,
        marked: listed.marked,
    })
}

/// Refuse a journal whose recovery-floor label is at or above its `head`, which
/// must be read after `listed` was.
///
/// A label proves that records at or above its floor committed. The first commit
/// stores a floor of zero after it lands, and a horizon's floor is the offset of the
/// record which opened it, which the acknowledgement completing it follows. So an
/// intact journal always holds a head past its floor, and the read order makes that
/// hold of a head read later too, because a floor stored in between is below it. A
/// head at or below the floor is committed state lost, as a recovery log's player
/// refuses hints which name offsets past its write head as possible data loss.
///
/// The daemon cannot tell why. Either the journal's head regressed, as a reset after
/// its brokers lost a tail they never persisted leaves it, or the label is not this
/// journal's. It cannot repair either: a lower floor would serve a disk behind its
/// client's checkpoint as though it were whole.
pub(super) fn check_floor(journal: &str, listed: &Listed, head: i64) -> anyhow::Result<()> {
    failure::ensure_valid!(
        !listed.marked || listed.floor < head,
        "journal {journal}'s recovery-floor label is {} but its head is {head}, so \
         records it committed at or above its floor are gone: its head regressed, or \
         the label is not this journal's",
        listed.floor,
    );
    Ok(())
}

/// List `journal`, refusing a journal which does not exist and a live spec a disk
/// could not be recovered from.
///
/// A journal nothing has created is what the tenure asked for rather than a fresh
/// disk: the daemon creates none, so no retry of this `Open` could ever find one.
///
/// A tenure lists at `Open` and again at its claim, and nothing of the first is
/// carried to the second. A standby holds a disk across other tenures' deltas, and
/// one which parked over an empty journal read none of them: only the claim's
/// listing sees the floor label another tenure's first commit stored.
pub(super) async fn list(
    client: &gazette::journal::Client,
    journal: &str,
    ended: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<Listed> {
    let listing = until_ended(ended, "listing", async {
        client
            .get_journal(journal)
            .await
            .with_context(|| format!("listing {journal}"))
    })
    .await?;

    // Indistinguishable here: the daemon's token selects the disk content type, so a
    // journal of some other type lists as absent rather than reaching the content-type
    // check below.
    let Some(listing) = listing else {
        return Err(anyhow::Error::new(failure::Failure::Invalid(format!(
            "journal {journal} does not exist or is not a disk journal, and the daemon \
             creates none: whoever deploys this disk owns its journal's specification",
        ))));
    };
    let floor = listed_floor(listing)?;

    Ok(Listed {
        floor: floor.unwrap_or_default(),
        marked: floor.is_some(),
    })
}

/// Confirm `journal`'s write head with a zero-byte append, which Gazette calls a write
/// barrier. Only an append which carries content may change a journal's content or
/// registers, so this changes neither.
///
/// It is how a recovery log's player learns the head it backfills to, standby or
/// not. The head is transactional: it is ordered after every append committed before
/// it. And the append resumes a journal
/// Gazette suspended, as any append does. Of a disk nobody writes, that costs a
/// journal which idles back to sleep.
///
/// A journal which is absent here was deleted after the tenure listed it, which is a
/// failure.
async fn barrier(client: &gazette::journal::Client, journal: &str) -> anyhow::Result<i64> {
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

/// Recovery floor `journal` carries now.
///
/// A playback whose range was deleted reads this to learn where to start again. It
/// cannot reuse the floor its tenure opened with, because the floor moved: that is
/// why the range it was reading is gone.
///
/// A journal which is absent here is loss. Only a read which found a gap asks this,
/// and a gap is content which existed.
pub(super) async fn current_floor(
    client: &gazette::journal::Client,
    journal: &str,
) -> anyhow::Result<i64> {
    let listing = client
        .get_journal(journal)
        .await
        .with_context(|| format!("listing {journal}"))?;

    let Some(listing) = listing else {
        anyhow::bail!("journal {journal} no longer exists");
    };
    Ok(listed_floor(listing)?.unwrap_or_default())
}

/// Advance `journal`'s recovery-floor label to `floor`.
///
/// This is the one part of a journal's specification the daemon writes. The floor
/// is the daemon's own state about a disk rather than the caller's, so the daemon
/// keeps it here, attached to the journal it describes, where a later tenure finds
/// it without a client having to carry it. Whatever converges the journal's
/// specification must carry the label over: one which rebuilt it away would cost a
/// later replay the work of reading below it, and nothing else.
///
/// The value is fixed-width hex, so the label is advanced by comparing strings and
/// only ever moves forward: a tenure which finds a newer floor leaves it alone.
///
/// A lost compare-and-swap is a specification something else changed. Its listing
/// is read again, because this floor must advance over whatever that change left
/// behind, and the attempts are bounded because the caller can carry on without a
/// stored floor.
pub(super) async fn advance_floor(
    client: &gazette::journal::Client,
    journal: &str,
    floor: i64,
) -> anyhow::Result<()> {
    let value = crate::recovery_floor_value(floor as u64);

    for _ in 0..3 {
        let listing = client
            .get_journal(journal)
            .await
            .with_context(|| format!("listing {journal}"))?;

        let Some(listing) = listing else {
            anyhow::bail!("journal {journal} no longer exists");
        };
        let Some(change) = floor_change(listing, &value) else {
            return Ok(()); // A floor at or ahead of this one is another tenure's.
        };

        match client
            .apply(broker::ApplyRequest {
                changes: vec![change],
            })
            .await
        {
            Ok(_response) => return Ok(()),
            Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => continue,
            Err(err) => {
                return Err(anyhow::Error::new(err)
                    .context(format!("applying the recovery floor of {journal}")));
            }
        }
    }
    anyhow::bail!("gave up advancing the recovery floor of {journal} after three attempts")
}

/// Report the recovery floor of `journal`'s listing, refusing a live spec a disk
/// could not be recovered from.
///
/// The spec is validated rather than converged. It belongs to whoever created the
/// journal, and a daemon which quietly fixed a field would be deciding a durable
/// property of a disk it merely serves. The recovery floor is the exception, and it
/// is the daemon's own state rather than that owner's: see [`advance_floor`].
///
/// The floor is the [`crate::DISK_RECOVERY_FLOOR`] label's, and `None` when the
/// journal carries none. A label which does not parse is treated as absent, because
/// a floor is only ever a seek.
fn listed_floor(listing: broker::list_response::Journal) -> anyhow::Result<Option<i64>> {
    let spec = listing.spec.expect("a listed journal has a spec");
    () = validate_recoverable(&spec)?;

    Ok(spec
        .labels
        .as_ref()
        .and_then(|set| labels::maybe_one(set, crate::DISK_RECOVERY_FLOOR).ok())
        .and_then(|value| crate::parse_recovery_floor(value).ok())
        .map(|floor| floor as i64))
}

/// Refuse a spec a disk could not be recovered from.
///
/// This is applied to the live spec of the journal an `Open` names, whoever wrote
/// it. A spec which fails any of these is a disk which can never prepare a delta.
fn validate_recoverable(spec: &broker::JournalSpec) -> anyhow::Result<()> {
    let journal = &spec.name;
    let fragment = spec.fragment.clone().unwrap_or_default();

    let content_type = spec
        .labels
        .as_ref()
        .and_then(|set| labels::maybe_one(set, labels::CONTENT_TYPE).ok())
        .unwrap_or_default();

    // Gazette requires this of a journal which serves as a shard recovery log, and
    // it is the same rule for the same reason. Without it, an `Open` of a collection
    // partition or of somebody's recovery log would fence that journal and append
    // disk records over content this daemon cannot read.
    failure::ensure_valid!(
        content_type == crate::CONTENT_TYPE_DISK,
        "journal {journal} has content type {content_type:?}, and a disk's journal must \
         declare {:?}",
        crate::CONTENT_TYPE_DISK,
    );

    // The journal client decodes every codec Gazette names, so a value which names
    // none of them is the one thing left to refuse: fragments this daemon cannot
    // read back are records it cannot recover the disk from.
    failure::ensure_valid!(
        matches!(
            broker::CompressionCodec::try_from(fragment.compression_codec),
            Ok(codec) if codec != broker::CompressionCodec::Invalid,
        ),
        "journal {journal} declares compression codec {}, which is not one Gazette \
         defines, and this daemon must read the journal back to recover the disk",
        fragment.compression_codec,
    );

    // Gazette deletes fragments by age, and age cannot see the recovery floor.
    // Any retention therefore risks deleting records a live disk needs.
    failure::ensure_valid!(
        fragment
            .retention
            .is_none_or(|retention| retention.seconds == 0 && retention.nanos == 0),
        "journal {journal} sets a fragment retention, which deletes fragments by age and \
         cannot see the disk's recovery floor",
    );
    // A bucket lifecycle rule keys on date-prefixed paths, which is age-based
    // deletion by another route.
    failure::ensure_valid!(
        fragment.path_postfix_template.is_empty(),
        "journal {journal} sets a fragment path postfix template, which date-prefixes \
         fragment paths so a bucket lifecycle rule can delete them by age",
    );
    // The daemon both appends to this journal and replays it. NOT_SPECIFIED is
    // Gazette's own default of read-write.
    failure::ensure_valid!(
        spec.flags == broker::journal_spec::Flag::NotSpecified as u32
            || spec.flags == broker::journal_spec::Flag::ORdwr as u32,
        "journal {journal} has flags {:#x}, and a disk's journal must be read-write",
        spec.flags,
    );

    Ok(())
}

/// Build the change which sets `listing`'s recovery-floor label to `value`, or `None`
/// where the journal already holds a floor at or ahead of it.
///
/// The held floor is compared as a string, which is why the value is fixed-width hex:
/// an equal floor is this tenure's own work applied twice, and a greater one is a
/// newer tenure's.
fn floor_change(
    listing: broker::list_response::Journal,
    value: &str,
) -> Option<broker::apply_request::Change> {
    let mut spec = listing.spec.expect("a listed journal has a spec");

    let held = spec
        .labels
        .as_ref()
        .and_then(|set| labels::maybe_one(set, crate::DISK_RECOVERY_FLOOR).ok())
        .unwrap_or_default();

    if held >= value {
        return None;
    }
    spec.labels = Some(labels::set_value(
        spec.labels.take().unwrap_or_default(),
        crate::DISK_RECOVERY_FLOOR,
        value,
    ));

    Some(broker::apply_request::Change {
        expect_mod_revision: listing.mod_revision,
        upsert: Some(spec),
        delete: String::new(),
    })
}

#[cfg(test)]
mod test {
    use super::{Listed, check_floor, floor_change, listed_floor};
    use proto_gazette::broker;

    const JOURNAL: &str = "acmeCo/disk/one";

    /// The listing of one journal whose spec is what its creator applied.
    fn listing(spec: broker::JournalSpec) -> broker::list_response::Journal {
        broker::list_response::Journal {
            spec: Some(spec),
            mod_revision: 42,
            ..Default::default()
        }
    }

    /// A spec a disk can be recovered from, for a case to break one field of.
    fn recoverable() -> broker::JournalSpec {
        broker::JournalSpec {
            name: JOURNAL.to_string(),
            replication: 1,
            fragment: Some(broker::journal_spec::Fragment {
                length: 1 << 26,
                compression_codec: broker::CompressionCodec::Snappy as i32,
                stores: vec!["file:///".to_string()],
                refresh_interval: Some(std::time::Duration::from_secs(300).into()),
                flush_interval: Some(std::time::Duration::from_secs(3600).into()),
                retention: None,
                path_postfix_template: String::new(),
            }),
            flags: broker::journal_spec::Flag::ORdwr as u32,
            labels: Some(labels::build_set([(
                labels::CONTENT_TYPE,
                crate::CONTENT_TYPE_DISK,
            )])),
            ..Default::default()
        }
    }

    /// NOT_SPECIFIED is Gazette's own read-write default, so a journal whose
    /// creator set no flag at all is accepted.
    #[test]
    fn test_a_recoverable_spec_is_accepted() {
        for flags in [
            broker::journal_spec::Flag::NotSpecified as u32,
            broker::journal_spec::Flag::ORdwr as u32,
        ] {
            let spec = broker::JournalSpec {
                flags,
                ..recoverable()
            };
            assert_eq!(listed_floor(listing(spec)).unwrap(), None);
        }
    }

    /// Each of these would let a disk lose records it still needs, or leave the
    /// daemon unable to read the journal back at all.
    #[test]
    fn test_a_spec_a_disk_cannot_recover_from_is_refused() {
        let fragment = || recoverable().fragment.unwrap();

        // This daemon decodes every codec Gazette names, so INVALID and a number
        // from some future Gazette are what a codec check can refuse.
        let cases: [(broker::JournalSpec, &str); 7] = [
            // A journal which declares nothing, and one which declares that it
            // holds somebody else's content.
            (
                broker::JournalSpec {
                    labels: None,
                    ..recoverable()
                },
                "must declare",
            ),
            (
                broker::JournalSpec {
                    labels: Some(labels::build_set([(
                        labels::CONTENT_TYPE,
                        labels::CONTENT_TYPE_RECOVERY_LOG,
                    )])),
                    ..recoverable()
                },
                "must declare",
            ),
            (
                broker::JournalSpec {
                    fragment: Some(broker::journal_spec::Fragment {
                        compression_codec: broker::CompressionCodec::Invalid as i32,
                        ..fragment()
                    }),
                    ..recoverable()
                },
                "not one Gazette defines",
            ),
            (
                broker::JournalSpec {
                    fragment: Some(broker::journal_spec::Fragment {
                        compression_codec: 99,
                        ..fragment()
                    }),
                    ..recoverable()
                },
                "not one Gazette defines",
            ),
            (
                broker::JournalSpec {
                    fragment: Some(broker::journal_spec::Fragment {
                        retention: Some(std::time::Duration::from_secs(86400).into()),
                        ..fragment()
                    }),
                    ..recoverable()
                },
                "fragment retention",
            ),
            (
                broker::JournalSpec {
                    fragment: Some(broker::journal_spec::Fragment {
                        path_postfix_template: "{{.Spool.FirstAppendTime.Format \"2006\"}}"
                            .to_string(),
                        ..fragment()
                    }),
                    ..recoverable()
                },
                "path postfix template",
            ),
            (
                broker::JournalSpec {
                    flags: broker::journal_spec::Flag::ORdonly as u32,
                    ..recoverable()
                },
                "must be read-write",
            ),
        ];

        for (spec, expect) in cases {
            let err = listed_floor(listing(spec)).unwrap_err();

            assert!(format!("{err}").contains(expect), "{expect}: {err}");
            assert!(err.chain().any(|cause| matches!(
                cause.downcast_ref::<crate::failure::Failure>(),
                Some(crate::failure::Failure::Invalid(_)),
            )));
        }
    }

    /// The recovery floor rides on a label of the spec, and a value which does not
    /// parse is treated as no floor at all: a floor is only ever a seek.
    #[test]
    fn test_the_recovery_floor_is_read_from_a_label() {
        for (value, expect) in [
            (None, None),
            (Some("0000000000000000"), Some(0)),
            (Some("00000000000186a0"), Some(100_000)),
            (Some("ffffffffffffffff"), Some(u64::MAX as i64)),
            (Some("not-hex"), None),
        ] {
            let mut spec = recoverable();

            if let Some(value) = value {
                spec.labels = Some(labels::set_value(
                    spec.labels.take().unwrap_or_default(),
                    crate::DISK_RECOVERY_FLOOR,
                    value,
                ));
            }
            assert_eq!(listed_floor(listing(spec)).unwrap(), expect, "{value:?}");
        }
    }

    /// A marked journal's head is always past its floor, so one at or below it is
    /// refused. An unmarked journal's floor is no claim at all, even at an empty head.
    #[test]
    fn test_a_floor_at_or_above_the_head_is_refused() {
        let mut out = Vec::new();

        for (marked, floor, head) in [
            (false, 0, 0),
            (true, 0, 0),
            (true, 100, 100),
            (true, 100, 101),
        ] {
            let outcome = match check_floor("acmeCo/disk/one", &Listed { floor, marked }, head) {
                Ok(()) => "accepted",
                Err(_) => "refused",
            };
            out.push(format!(
                "marked {marked:<5} floor {floor:<3} head {head:<3} {outcome}"
            ));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        marked false floor 0   head 0   accepted
        marked true  floor 0   head 0   refused
        marked true  floor 100 head 100 refused
        marked true  floor 100 head 101 accepted
        ");
    }

    /// The floor label only ever advances: a change sets it where the journal holds no
    /// floor or an older one, and leaves a floor at or ahead of it to whichever tenure
    /// wrote that. The change carries the listing's revision, so a spec something else
    /// changed meanwhile refuses it, and it keeps every other label.
    #[test]
    fn test_the_recovery_floor_only_advances() {
        let value = crate::recovery_floor_value(100_000);
        let mut out = Vec::new();

        for held in [None, Some(99_999), Some(100_000), Some(100_001)] {
            let mut spec = recoverable();

            if let Some(held) = held {
                spec.labels = Some(labels::set_value(
                    spec.labels.take().unwrap_or_default(),
                    crate::DISK_RECOVERY_FLOOR,
                    &crate::recovery_floor_value(held),
                ));
            }
            let outcome = match floor_change(listing(spec), &value) {
                None => "leaves it".to_string(),
                Some(change) => {
                    let spec = change.upsert.expect("a change upserts the spec");
                    let labels = spec.labels.expect("the spec keeps its labels");

                    format!(
                        "sets {} at revision {}, content type {}",
                        labels::maybe_one(&labels, crate::DISK_RECOVERY_FLOOR).unwrap(),
                        change.expect_mod_revision,
                        labels::maybe_one(&labels, labels::CONTENT_TYPE).unwrap(),
                    )
                }
            };
            let held = format!("{held:?}");
            out.push(format!("held {held:<16}{outcome}"));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        held None            sets 00000000000186a0 at revision 42, content type application/x-journal-backed-disk
        held Some(99999)     sets 00000000000186a0 at revision 42, content type application/x-journal-backed-disk
        held Some(100000)    leaves it
        held Some(100001)    leaves it
        ");
    }
}
