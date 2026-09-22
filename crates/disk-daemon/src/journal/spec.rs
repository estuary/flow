//! The journal specification a disk is served from, and the recovery floor which
//! rides on it.
//!
//! The daemon creates no journal and converges none. It validates the live spec of
//! the journal an `Open` names, because a spec a disk could not be recovered from is
//! a disk which can never prepare a delta. The recovery floor is the one field it
//! writes: it is the daemon's own state about a disk rather than the spec owner's.

use super::{fence, until_ended};
use anyhow::Context;
use proto_gazette::broker;

/// What a journal holds at the moment it is asked.
#[derive(Debug)]
pub(super) struct Resolved {
    /// Value of the `author` register, which a claim must replace. Absent while no
    /// writer holds the journal.
    pub(super) prior: Option<String>,
    /// Write head. Zero is a journal with no content at all.
    pub(super) head: i64,
    /// Recovery floor a replay of this journal seeks from.
    pub(super) floor: i64,
}

/// Resolve what `journal` holds now, refusing a journal which does not exist and a
/// live spec a disk could not be recovered from.
///
/// The journal is listed before it is probed, because the listing is where the
/// daemon checks both of those. A journal nothing has created is what the tenure
/// asked for rather than a fresh disk: the daemon creates none, so no retry of this
/// `Open` could ever find one.
///
/// The probe resumes a journal Gazette suspended, and answers with the authority on
/// that journal's own head and author, however stale the listing already is. A
/// resumption is what a recovery needs anyway, and of a disk nobody writes it costs
/// a journal which idles back to sleep.
///
/// This runs twice for one tenure: at `Open`, and again at the claim. Nothing it
/// reports may be carried from the first to the second, because a standby holds a
/// disk across other tenures' fences and other tenures' deltas.
pub(super) async fn resolve(
    client: &gazette::journal::Client,
    journal: &str,
    ended: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<Resolved> {
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
        return Err(anyhow::Error::new(crate::Failure::Invalid(format!(
            "journal {journal} does not exist or is not a disk journal, and the daemon \
             creates none: whoever deploys this disk owns its journal's specification",
        ))));
    };
    let floor = listed_floor(listing)?;

    let probe = until_ended(ended, "probing", fence::probe(client, journal)).await?;

    Ok(Resolved {
        prior: probe.author,
        head: probe.head,
        floor,
    })
}

/// Report the recovery floor of `journal`'s listing, refusing a live spec a disk
/// could not be recovered from.
///
/// The spec is validated rather than converged. It belongs to whoever created the
/// journal, and a daemon which quietly fixed a field would be deciding a durable
/// property of a disk it merely serves. The recovery floor is the exception, and it
/// is the daemon's own state rather than that owner's: see [`advance_floor`].
///
/// The floor is the [`crate::DISK_RECOVERY_FLOOR`] label's, and zero when the
/// journal carries none. A label which does not parse is treated as absent, because
/// a floor is only ever a seek.
fn listed_floor(listing: broker::list_response::Journal) -> anyhow::Result<i64> {
    let spec = listing.spec.expect("a listed journal has a spec");
    () = validate_recoverable(&spec)?;

    Ok(spec
        .labels
        .as_ref()
        .and_then(|set| labels::maybe_one(set, crate::DISK_RECOVERY_FLOOR).ok())
        .and_then(|value| crate::parse_recovery_floor(value).ok())
        .unwrap_or_default() as i64)
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
    crate::ensure_valid!(
        content_type == crate::CONTENT_TYPE_DISK,
        "journal {journal} has content type {content_type:?}, and a disk's journal must \
         declare {:?}",
        crate::CONTENT_TYPE_DISK,
    );

    // The journal client decodes every codec Gazette names, so a value which names
    // none of them is the one thing left to refuse: fragments this daemon cannot
    // read back are records it cannot recover the disk from.
    crate::ensure_valid!(
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
    crate::ensure_valid!(
        fragment
            .retention
            .is_none_or(|retention| retention.seconds == 0 && retention.nanos == 0),
        "journal {journal} sets a fragment retention, which deletes fragments by age and \
         cannot see the disk's recovery floor",
    );
    // A bucket lifecycle rule keys on date-prefixed paths, which is age-based
    // deletion by another route.
    crate::ensure_valid!(
        fragment.path_postfix_template.is_empty(),
        "journal {journal} sets a fragment path postfix template, which date-prefixes \
         fragment paths so a bucket lifecycle rule can delete them by age",
    );
    // The daemon both appends to this journal and replays it. NOT_SPECIFIED is
    // Gazette's own default of read-write.
    crate::ensure_valid!(
        spec.flags == broker::journal_spec::Flag::NotSpecified as u32
            || spec.flags == broker::journal_spec::Flag::ORdwr as u32,
        "journal {journal} has flags {:#x}, and a disk's journal must be read-write",
        spec.flags,
    );

    Ok(())
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
    listed_floor(listing)
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
mod test;
