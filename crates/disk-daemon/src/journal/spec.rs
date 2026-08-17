//! A disk journal's `JournalSpec`, built from the typed inputs a session supplies.
//!
//! The daemon builds the spec rather than accepting one. A disk's recoverability
//! rests on fields a client must not be able to get wrong. The journal must be
//! writable and readable. Its fragments must use a codec this daemon can
//! decompress, because it replays them to rebuild the disk.
//!
//! The daemon also holds no defaults. A journal's spec is created once and never
//! converged. A value the daemon invented would be one the disk is stuck with, and
//! a later change to the daemon would not reach it.

use crate::proto;
use proto_gazette::broker;

/// Build the spec of `config`, which must supply every field.
///
/// Most are rejected when zero. Gazette rejects them too, and a failure here names
/// the field. The two which Gazette accepts as zero are `optional` on the wire, so
/// their absence is refused instead. No append ceiling and no flush on time alone
/// stay reachable, but only by being asked for.
pub fn build(config: &proto::JournalConfig) -> anyhow::Result<broker::JournalSpec> {
    let proto::JournalConfig {
        journal,
        fragment_stores,
        replication,
        labels,
        fragment_length,
        flush_interval_seconds,
        refresh_interval_seconds,
        max_append_rate,
        compression_codec,
    } = config;

    crate::ensure_valid!(!journal.is_empty(), "no journal name was supplied");
    crate::ensure_valid!(
        !fragment_stores.is_empty(),
        "journal {journal} was given no fragment store",
    );
    crate::ensure_valid!(
        *replication != 0,
        "journal {journal} was given no replication",
    );
    crate::ensure_valid!(
        *fragment_length != 0,
        "journal {journal} was given no fragment length",
    );
    crate::ensure_valid!(
        *refresh_interval_seconds != 0,
        "journal {journal} was given no refresh interval",
    );

    crate::ensure_valid!(
        flush_interval_seconds.is_some(),
        "journal {journal} was given no flush interval",
    );
    crate::ensure_valid!(
        max_append_rate.is_some(),
        "journal {journal} was given no maximum append rate",
    );

    let codec = broker::CompressionCodec::try_from(*compression_codec)
        .unwrap_or(broker::CompressionCodec::Invalid);

    crate::ensure_valid!(
        gazette::journal::read::supports_codec(codec),
        "journal {journal} was given compression codec {}, which this daemon cannot \
         decompress, and it must read this journal back to recover the disk",
        codec.as_str_name(),
    );

    let mut labels: Vec<broker::Label> = labels
        .iter()
        .map(|label| broker::Label {
            name: label.name.clone(),
            value: label.value.clone(),
            prefix: false,
        })
        .collect();

    labels.sort_by(|l, r| (&l.name, &l.value).cmp(&(&r.name, &r.value)));
    labels.dedup_by(|l, r| (&l.name, &l.value) == (&r.name, &r.value));

    Ok(broker::JournalSpec {
        name: journal.clone(),
        replication: *replication as i32,
        labels: Some(broker::LabelSet { labels }),
        fragment: Some(broker::journal_spec::Fragment {
            length: *fragment_length,
            compression_codec: codec as i32,
            stores: fragment_stores.clone(),
            refresh_interval: Some(seconds(*refresh_interval_seconds)),
            flush_interval: Some(seconds(flush_interval_seconds.unwrap_or_default())),
            // Gazette deletes fragments by age, and age cannot see the recovery
            // floor. Any retention therefore risks deleting records a live disk
            // needs.
            retention: None,
            // A bucket lifecycle rule keys on date-prefixed paths, which is
            // age-based deletion by another route.
            path_postfix_template: String::new(),
        }),
        // The daemon both appends to this journal and replays it.
        flags: broker::journal_spec::Flag::ORdwr as u32,
        max_append_rate: max_append_rate.unwrap_or_default(),
        suspend: None,
    })
}

/// Create `spec` if the journal does not exist, and otherwise leave it alone.
///
/// The insert is conditioned on the journal's absence. A lost race means another
/// writer created the journal, which is the outcome this wanted. The client
/// converges an existing spec, not the daemon.
pub async fn create(
    client: &gazette::journal::Client,
    spec: broker::JournalSpec,
) -> anyhow::Result<()> {
    let request = broker::ApplyRequest {
        changes: vec![broker::apply_request::Change {
            expect_mod_revision: 0,
            upsert: Some(spec),
            delete: String::new(),
        }],
    };

    match client.apply(request).await {
        Ok(_response) => Ok(()),
        Err(gazette::Error::BrokerStatus(broker::Status::EtcdTransactionFailed)) => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn seconds(seconds: impl Into<u64>) -> pbjson_types::Duration {
    std::time::Duration::from_secs(seconds.into()).into()
}

#[cfg(test)]
mod test {
    use super::build;
    use crate::proto;
    use proto_gazette::broker;

    /// A config which supplies everything, for a case to take one field away.
    fn complete() -> proto::JournalConfig {
        proto::JournalConfig {
            journal: "acmeCo/disk/one".to_string(),
            fragment_stores: vec!["file:///".to_string()],
            replication: 1,
            labels: Vec::new(),
            fragment_length: 4096,
            flush_interval_seconds: Some(3600),
            refresh_interval_seconds: 300,
            max_append_rate: Some(1 << 20),
            compression_codec: broker::CompressionCodec::None as i32,
        }
    }

    #[test]
    fn test_every_field_reaches_the_spec() {
        let spec = build(&complete()).unwrap();
        let fragment = spec.fragment.clone().unwrap();

        assert_eq!(spec.name, "acmeCo/disk/one");
        assert_eq!(spec.replication, 1);
        assert_eq!(spec.max_append_rate, 1 << 20);
        assert_eq!(fragment.length, 4096);
        assert_eq!(fragment.stores, vec!["file:///"]);
        assert_eq!(fragment.refresh_interval.unwrap().seconds, 300);
        assert_eq!(fragment.flush_interval.unwrap().seconds, 3600);

        // The daemon fixes these, whatever the session asked for.
        assert_eq!(fragment.retention, None);
        assert_eq!(fragment.path_postfix_template, "");
        assert_eq!(spec.flags, broker::journal_spec::Flag::ORdwr as u32);
    }

    #[test]
    fn test_a_missing_field_names_itself() {
        for (take, expected) in [
            (
                (|c: &mut proto::JournalConfig| c.journal.clear()) as fn(&mut _),
                "no journal name",
            ),
            (|c| c.fragment_stores.clear(), "no fragment store"),
            (|c| c.replication = 0, "no replication"),
            (|c| c.fragment_length = 0, "no fragment length"),
            (|c| c.refresh_interval_seconds = 0, "no refresh interval"),
            (|c| c.flush_interval_seconds = None, "no flush interval"),
            (|c| c.max_append_rate = None, "no maximum append rate"),
            (|c| c.compression_codec = 0, "INVALID"),
        ] {
            let mut config = complete();
            () = take(&mut config);

            let err = build(&config).unwrap_err();
            assert!(format!("{err}").contains(expected), "{expected}: {err}");
        }
    }

    /// Gazette reads zero as a choice for these two, so the daemon carries it
    /// through.
    #[test]
    fn test_zero_is_a_value_for_the_two_optional_fields() {
        let spec = build(&proto::JournalConfig {
            max_append_rate: Some(0),
            flush_interval_seconds: Some(0),
            ..complete()
        })
        .unwrap();

        assert_eq!(spec.max_append_rate, 0);
        assert_eq!(spec.fragment.unwrap().flush_interval.unwrap().seconds, 0);
    }
}
