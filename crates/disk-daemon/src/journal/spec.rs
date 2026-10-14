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
    /// The floor label also proves that the disk committed state, even if its
    /// fragments have since been lost.
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
    let head = super::until_ended(ended, "confirming the head", barrier(client, journal)).await?;
    () = check_floor(journal, &listed, head)?;

    Ok(Resolved {
        head,
        floor: listed.floor,
        marked: listed.marked,
    })
}

/// Validate the live journal, refusing an absent or unrecoverable one.
/// Repeat at claim time: an empty standby may have observed none of another
/// tenure's writes, including the first commit that stored the floor label.
pub(super) async fn list(
    client: &gazette::journal::Client,
    journal: &str,
    ended: &tokio_util::sync::CancellationToken,
) -> anyhow::Result<Listed> {
    let listing = super::until_ended(ended, "listing", async {
        client
            .get_journal(journal)
            .await
            .map_err(super::broker_failure)
            .with_context(|| format!("listing {journal}"))
    })
    .await?;

    // Indistinguishable here: the daemon's token selects the disk content type, so a
    // journal of some other type lists as absent rather than reaching the content-type
    // check below.
    let Some(listing) = listing else {
        return Err(crate::invalid_argument(format!(
            "journal {journal} does not exist or is not a disk journal, and the daemon \
             creates none: whoever deploys this disk owns its journal's specification",
        )));
    };
    let floor = listed_floor(listing)?;

    Ok(Listed {
        floor: floor.unwrap_or_default(),
        marked: floor.is_some(),
    })
}

/// Require a marked journal's head to be past its floor. Read `head` after
/// `listed`: a stored floor always follows the commit it describes.
///
/// Failure means the head regressed or the label belongs to another journal.
/// Lowering the floor could serve a disk behind the client's checkpoint.
pub(super) fn check_floor(journal: &str, listed: &Listed, head: i64) -> anyhow::Result<()> {
    if listed.marked && listed.floor >= head {
        return Err(crate::invalid_argument(format!(
            "journal {journal}'s recovery-floor label is {} but its head is {head}, so \
             records it committed at or above its floor are gone: its head regressed, or \
             the label is not this journal's",
            listed.floor,
        )));
    }
    Ok(())
}

/// Advance the recovery-floor label while preserving the rest of the spec.
/// Fixed-width hex makes lexical comparison monotonic across tenures.
///
/// Spec convergence must preserve this label: it bounds replay and records
/// that the disk has committed state. Retry lost compare-and-swaps from a fresh
/// listing, with bounded attempts because the caller can proceed without the
/// update.
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

/// Validate the live spec and parse its recovery-floor label. An absent or
/// malformed label yields `None`.
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

/// Reject settings that prevent recovery before serving the disk.
fn validate_recoverable(spec: &broker::JournalSpec) -> anyhow::Result<()> {
    let journal = &spec.name;
    let fragment = spec.fragment.clone().unwrap_or_default();

    let content_type = spec
        .labels
        .as_ref()
        .and_then(|set| labels::maybe_one(set, labels::CONTENT_TYPE).ok())
        .unwrap_or_default();

    // Do not fence or append disk records to a collection or recovery log.
    if content_type != crate::CONTENT_TYPE_DISK {
        return Err(crate::invalid_argument(format!(
            "journal {journal} has content type {content_type:?}, and a disk's journal must \
             declare {:?}",
            crate::CONTENT_TYPE_DISK,
        )));
    }

    // Every named Gazette codec is readable; reject unknown encodings.
    if !matches!(
        broker::CompressionCodec::try_from(fragment.compression_codec),
        Ok(codec) if codec != broker::CompressionCodec::Invalid,
    ) {
        return Err(crate::invalid_argument(format!(
            "journal {journal} declares compression codec {}, which is not one Gazette \
             defines, and this daemon must read the journal back to recover the disk",
            fragment.compression_codec,
        )));
    }

    // Gazette deletes fragments by age, and age cannot see the recovery floor.
    // Any retention therefore risks deleting records a live disk needs.
    if fragment
        .retention
        .is_some_and(|retention| retention.seconds != 0 || retention.nanos != 0)
    {
        return Err(crate::invalid_argument(format!(
            "journal {journal} sets a fragment retention, which deletes fragments by age and \
             cannot see the disk's recovery floor",
        )));
    }
    // A bucket lifecycle rule keys on date-prefixed paths, which is age-based
    // deletion by another route.
    if !fragment.path_postfix_template.is_empty() {
        return Err(crate::invalid_argument(format!(
            "journal {journal} sets a fragment path postfix template, which date-prefixes \
             fragment paths so a bucket lifecycle rule can delete them by age",
        )));
    }
    // The daemon both appends to this journal and replays it. NOT_SPECIFIED is
    // Gazette's own default of read-write.
    if spec.flags != broker::journal_spec::Flag::NotSpecified as u32
        && spec.flags != broker::journal_spec::Flag::ORdwr as u32
    {
        return Err(crate::invalid_argument(format!(
            "journal {journal} has flags {:#x}, and a disk's journal must be read-write",
            spec.flags,
        )));
    }

    Ok(())
}

/// Confirm the write head with a zero-byte append, ordered after earlier
/// commits. This changes neither content nor registers, but resumes a suspended
/// journal. A journal deleted since listing fails here.
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
                return Err(super::broker_failure(inner)
                    .context(format!("confirming the head of {journal}")));
            }
            None => unreachable!("an append stream does not end without a response"),
        }
    }
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

    fn outcome(floor: anyhow::Result<Option<i64>>) -> String {
        match floor {
            Ok(floor) => format!("accepted, floor {floor:?}"),
            Err(err) => format!("refused: {err:#}"),
        }
    }

    /// NOT_SPECIFIED is Gazette's own read-write default, so a journal whose
    /// creator set no flag at all is accepted.
    #[test]
    fn test_a_recoverable_spec_is_accepted() {
        let mut out = Vec::new();

        for flag in [
            broker::journal_spec::Flag::NotSpecified,
            broker::journal_spec::Flag::ORdwr,
        ] {
            let spec = broker::JournalSpec {
                flags: flag as u32,
                ..recoverable()
            };
            let flag = format!("{flag:?}");
            out.push(format!(
                "{flag:<14}{}",
                outcome(listed_floor(listing(spec)))
            ));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        NotSpecified  accepted, floor None
        ORdwr         accepted, floor None
        ");
    }

    /// Each of these would let a disk lose records it still needs, or leave the
    /// daemon unable to read the journal back at all.
    #[test]
    fn test_a_spec_a_disk_cannot_recover_from_is_refused() {
        let fragment = || recoverable().fragment.unwrap();

        // This daemon decodes every codec Gazette names, so INVALID and a number
        // from some future Gazette are what a codec check can refuse.
        let cases: [(&str, broker::JournalSpec); 7] = [
            // A journal which declares nothing, and one which declares that it
            // holds somebody else's content.
            (
                "no labels",
                broker::JournalSpec {
                    labels: None,
                    ..recoverable()
                },
            ),
            (
                "a recovery log's labels",
                broker::JournalSpec {
                    labels: Some(labels::build_set([(
                        labels::CONTENT_TYPE,
                        labels::CONTENT_TYPE_RECOVERY_LOG,
                    )])),
                    ..recoverable()
                },
            ),
            (
                "codec Invalid",
                broker::JournalSpec {
                    fragment: Some(broker::journal_spec::Fragment {
                        compression_codec: broker::CompressionCodec::Invalid as i32,
                        ..fragment()
                    }),
                    ..recoverable()
                },
            ),
            (
                "codec 99",
                broker::JournalSpec {
                    fragment: Some(broker::journal_spec::Fragment {
                        compression_codec: 99,
                        ..fragment()
                    }),
                    ..recoverable()
                },
            ),
            (
                "a retention",
                broker::JournalSpec {
                    fragment: Some(broker::journal_spec::Fragment {
                        retention: Some(std::time::Duration::from_secs(86400).into()),
                        ..fragment()
                    }),
                    ..recoverable()
                },
            ),
            (
                "a path postfix template",
                broker::JournalSpec {
                    fragment: Some(broker::journal_spec::Fragment {
                        path_postfix_template: "{{.Spool.FirstAppendTime.Format \"2006\"}}"
                            .to_string(),
                        ..fragment()
                    }),
                    ..recoverable()
                },
            ),
            (
                "flags ORdonly",
                broker::JournalSpec {
                    flags: broker::journal_spec::Flag::ORdonly as u32,
                    ..recoverable()
                },
            ),
        ];
        let mut out = Vec::new();

        for (label, spec) in cases {
            out.push(format!(
                "{label:<25}{}",
                outcome(listed_floor(listing(spec)))
            ));
        }
        insta::assert_snapshot!(out.join("\n"), @r#"
        no labels                refused: journal acmeCo/disk/one has content type "", and a disk's journal must declare "application/x-journal-backed-disk" (InvalidArgument)
        a recovery log's labels  refused: journal acmeCo/disk/one has content type "application/x-gazette-recoverylog", and a disk's journal must declare "application/x-journal-backed-disk" (InvalidArgument)
        codec Invalid            refused: journal acmeCo/disk/one declares compression codec 0, which is not one Gazette defines, and this daemon must read the journal back to recover the disk (InvalidArgument)
        codec 99                 refused: journal acmeCo/disk/one declares compression codec 99, which is not one Gazette defines, and this daemon must read the journal back to recover the disk (InvalidArgument)
        a retention              refused: journal acmeCo/disk/one sets a fragment retention, which deletes fragments by age and cannot see the disk's recovery floor (InvalidArgument)
        a path postfix template  refused: journal acmeCo/disk/one sets a fragment path postfix template, which date-prefixes fragment paths so a bucket lifecycle rule can delete them by age (InvalidArgument)
        flags ORdonly            refused: journal acmeCo/disk/one has flags 0x1, and a disk's journal must be read-write (InvalidArgument)
        "#);
    }

    /// The recovery floor rides on a label of the spec, and a value which does not
    /// parse is treated as no floor at all: a floor is only ever a seek.
    #[test]
    fn test_the_recovery_floor_is_read_from_a_label() {
        let mut out = Vec::new();

        for value in [
            None,
            Some("0000000000000000"),
            Some("00000000000186a0"),
            // u64::MAX, which casts to an i64 floor of -1.
            Some("ffffffffffffffff"),
            Some("not-hex"),
        ] {
            let mut spec = recoverable();

            if let Some(value) = value {
                spec.labels = Some(labels::set_value(
                    spec.labels.take().unwrap_or_default(),
                    crate::DISK_RECOVERY_FLOOR,
                    value,
                ));
            }
            let value = value.unwrap_or("no label");
            out.push(format!(
                "{value:<18}{}",
                outcome(listed_floor(listing(spec)))
            ));
        }
        insta::assert_snapshot!(out.join("\n"), @"
        no label          accepted, floor None
        0000000000000000  accepted, floor Some(0)
        00000000000186a0  accepted, floor Some(100000)
        ffffffffffffffff  accepted, floor Some(-1)
        not-hex           accepted, floor None
        ");
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
