use proto_gazette::broker;

/// The recovery floor in `listing`'s label, or zero if it has none.
pub(super) fn parse(
    journal: &str,
    listing: &broker::list_response::Journal,
) -> anyhow::Result<i64> {
    let spec = listing.spec.as_ref().expect("a listed journal has a spec");
    let Some(set) = &spec.labels else {
        return Ok(0);
    };

    let value = labels::maybe_one(set, labels::DISK_REPLAY_FROM).map_err(|err| {
        crate::invalid_argument(format!(
            "journal {journal}'s replay-from label is invalid: {err}"
        ))
    })?;
    if value.is_empty() {
        return Ok(0);
    }
    i64::from_str_radix(value, 16)
        .ok()
        .filter(|floor| *floor >= 0)
        .ok_or_else(|| {
            crate::invalid_argument(format!(
                "journal {journal}'s replay-from label {value:?} is not an offset"
            ))
        })
}

/// Store `floor` as `journal`'s replay-from label.
pub(super) async fn store(
    client: &gazette::journal::Client,
    journal: &str,
    floor: i64,
) -> anyhow::Result<()> {
    let request = broker::ListRequest {
        selector: Some(broker::LabelSelector {
            include: Some(labels::build_set([("name", journal)])),
            exclude: None,
        }),
        watch: true,
        ..Default::default()
    };
    let mut watch = std::pin::pin!(client.clone().list_watch(request));

    loop {
        let listing = match futures::StreamExt::next(&mut watch).await {
            Some(Ok(listing)) => listing,
            Some(Err(gazette::RetryError { attempt, inner })) if inner.is_transient() => {
                tracing::warn!(journal, attempt, %inner, "listing failed (will retry)");
                continue;
            }
            Some(Err(gazette::RetryError { inner, .. })) => {
                return Err(anyhow::Error::new(inner).context("listing"));
            }
            None => unreachable!("a listing watch does not end"),
        };
        let Some(listing) = listing.journals.into_iter().next() else {
            anyhow::bail!("the journal no longer exists");
        };
        let Some(change) = change(journal, listing, floor)? else {
            return Ok(());
        };

        // A transient failure may yet have applied the change. Its retry then
        // loses to it, and the listing which follows shows the floor stored.
        let mut attempt: u32 = 0;
        loop {
            let request = broker::ApplyRequest {
                changes: vec![change.clone()],
            };
            let err = match client.apply(request).await {
                Ok(_) => return Ok(()),
                Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => break,
                Err(err) => err,
            };
            attempt += 1;

            if !err.is_transient() || attempt == 8 {
                return Err(
                    anyhow::Error::new(err).context("applying the floor update spec change")
                );
            }
            // The publisher's backoff for its truncated-at labels: exponential from
            // 100ms, capped at 10 seconds.
            let backoff = std::time::Duration::from_millis(100)
                .saturating_mul(2u32.saturating_pow(attempt - 1))
                .min(std::time::Duration::from_secs(10));
            tracing::warn!(journal, attempt, %err, "applying the floor failed (will retry)");
            () = tokio::time::sleep(backoff).await;
        }
    }
}

/// The spec change which advances `listing`'s replay-from label to `floor`,
/// or none if the label is already at or past it.
fn change(
    journal: &str,
    listing: broker::list_response::Journal,
    floor: i64,
) -> anyhow::Result<Option<broker::apply_request::Change>> {
    if parse(journal, &listing)? >= floor {
        return Ok(None);
    }
    let mut spec = listing.spec.expect("a listed journal has a spec");
    // Zero-padding keeps the label's string order the same as its numeric order.
    let value = format!("{floor:016x}");

    spec.labels = Some(labels::set_value(
        spec.labels.take().unwrap_or_default(),
        labels::DISK_REPLAY_FROM,
        &value,
    ));

    Ok(Some(broker::apply_request::Change {
        expect_mod_revision: listing.mod_revision,
        upsert: Some(spec),
        delete: String::new(),
    }))
}

#[cfg(test)]
mod test {
    use proto_gazette::broker;

    /// A floor label is a non-negative hex offset, and a journal without one replays
    /// from zero. A change advances only a label below the floor.
    #[test]
    fn test_floor_label() {
        // (label, floor it parses as and whether a change to 100_000 advances it, or
        // None where it is refused)
        let cases = [
            (None, Some((0, true))),
            (Some("0000000000000000"), Some((0, true))),
            (Some("000000000001869f"), Some((99_999, true))),
            (Some("00000000000186a0"), Some((100_000, false))),
            (Some("00000000000186a1"), Some((100_001, false))),
            // Beyond i64::MAX.
            (Some("ffffffffffffffff"), None),
            (Some("-1"), None),
            (Some("not-hex"), None),
        ];

        for (label, expect) in cases {
            let listed = listing(label);
            let parsed = super::parse("acmeCo/disk/one", &listed);
            let change = super::change("acmeCo/disk/one", listed, 100_000);

            let Some((floor, advances)) = expect else {
                let refused = format!(
                    "journal acmeCo/disk/one's replay-from label {:?} is not an offset \
                     (InvalidArgument)",
                    label.unwrap()
                );
                assert_eq!(format!("{:#}", parsed.unwrap_err()), refused);
                assert_eq!(format!("{:#}", change.unwrap_err()), refused);
                continue;
            };
            assert_eq!(parsed.unwrap(), floor, "{label:?}");

            let change = change.unwrap();
            assert_eq!(change.is_some(), advances, "{label:?}");
            let Some(change) = change else {
                continue;
            };

            // The change applies at the listed revision, so a spec something else
            // changed meanwhile refuses it. It keeps the other labels, and sets one
            // which parses as the floor.
            let spec = change.upsert.unwrap();
            let set = spec.labels.as_ref().unwrap();
            assert_eq!(change.expect_mod_revision, 42);
            assert_eq!(
                labels::maybe_one(set, labels::CONTENT_TYPE).unwrap(),
                crate::CONTENT_TYPE_DISK
            );
            let listed = broker::list_response::Journal {
                spec: Some(spec),
                ..Default::default()
            };
            assert_eq!(super::parse("acmeCo/disk/one", &listed).unwrap(), 100_000);
        }
    }

    /// A disk journal's listing at revision 42, carrying `floor` as its replay-from
    /// label if given.
    fn listing(floor: Option<&str>) -> broker::list_response::Journal {
        let mut set = labels::build_set([(labels::CONTENT_TYPE, crate::CONTENT_TYPE_DISK)]);
        if let Some(floor) = floor {
            set = labels::set_value(set, labels::DISK_REPLAY_FROM, floor);
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
}
