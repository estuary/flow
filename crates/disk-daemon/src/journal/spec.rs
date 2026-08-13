//! A disk journal's `JournalSpec`, built from the typed inputs a session
//! supplies over fallbacks the daemon configures.
//!
//! The daemon builds the spec rather than accepting one, because a disk's
//! recoverability rests on fields a client must not be able to get wrong: the
//! journal must be writable and readable, and its fragments must be in a codec
//! this daemon can decompress, since it replays them to rebuild the disk.

use crate::proto;
use proto_gazette::broker;

/// Build the spec of `config`, filling each field it leaves unset from
/// `defaults`.
///
/// Zero and empty are the unset markers throughout. That is unambiguous because
/// zero is invalid for every field: Gazette rejects a zero replication, fragment
/// length, or interval, and codec zero is `INVALID`.
pub fn build(
    config: &proto::JournalConfig,
    defaults: &proto::JournalConfig,
) -> anyhow::Result<broker::JournalSpec> {
    anyhow::ensure!(!config.journal.is_empty(), "no journal name was supplied");

    let stores = pick(&config.fragment_stores, &defaults.fragment_stores);
    anyhow::ensure!(
        !stores.is_empty(),
        "no fragment store for journal {}: neither the session nor the daemon supplies one",
        config.journal,
    );

    let codec = broker::CompressionCodec::try_from(or(
        config.compression_codec,
        defaults.compression_codec,
    ))
    .unwrap_or(broker::CompressionCodec::Invalid);

    anyhow::ensure!(
        gazette::journal::read::supports_codec(codec),
        "compression codec {} cannot be decompressed by this daemon, which must read \
         this journal back to recover the disk",
        codec.as_str_name(),
    );

    let mut labels: Vec<broker::Label> = pick(&config.labels, &defaults.labels)
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
        name: config.journal.clone(),
        replication: or(config.replication, defaults.replication) as i32,
        labels: Some(broker::LabelSet { labels }),
        fragment: Some(broker::journal_spec::Fragment {
            length: or(config.fragment_length, defaults.fragment_length),
            compression_codec: codec as i32,
            stores: stores.to_vec(),
            refresh_interval: Some(seconds(or(
                config.refresh_interval_seconds,
                defaults.refresh_interval_seconds,
            ))),
            flush_interval: Some(seconds(or(
                config.flush_interval_seconds,
                defaults.flush_interval_seconds,
            ))),
            // Gazette deletes fragments by age, which cannot see the recovery
            // floor, so any retention risks deleting records a live disk needs.
            retention: None,
            // Date-prefixed paths are what a bucket lifecycle rule keys on,
            // which is age-based deletion by another route.
            path_postfix_template: String::new(),
        }),
        // The daemon both appends to this journal and replays it.
        flags: broker::journal_spec::Flag::ORdwr as u32,
        max_append_rate: or(config.max_append_rate, defaults.max_append_rate),
        suspend: None,
    })
}

/// Create `spec` if the journal does not exist, and otherwise leave it alone.
///
/// The insert is conditioned on the journal's absence, so a lost race is
/// another writer having created the journal, which is the outcome intended.
/// Convergence of an existing spec belongs to the client.
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

/// The session's value, or the daemon's when the session set none.
fn pick<'a, T>(config: &'a [T], defaults: &'a [T]) -> &'a [T] {
    if config.is_empty() { defaults } else { config }
}

/// The session's value, or the daemon's when the session set none.
fn or<T: Default + PartialEq>(config: T, defaults: T) -> T {
    if config == T::default() {
        defaults
    } else {
        config
    }
}

fn seconds(seconds: impl Into<u64>) -> pbjson_types::Duration {
    std::time::Duration::from_secs(seconds.into()).into()
}

#[cfg(test)]
mod test {
    use super::{build, or};
    use crate::proto;
    use proto_gazette::broker;

    fn defaults() -> proto::JournalConfig {
        crate::journal::Config::default().journal_defaults
    }

    #[test]
    fn test_a_session_field_wins_over_the_daemon_default() {
        let spec = build(
            &proto::JournalConfig {
                journal: "acmeCo/disk/one".to_string(),
                fragment_stores: vec!["file:///".to_string()],
                replication: 1,
                fragment_length: 4096,
                ..Default::default()
            },
            &defaults(),
        )
        .unwrap();

        let fragment = spec.fragment.unwrap();
        assert_eq!(spec.name, "acmeCo/disk/one");
        assert_eq!(spec.replication, 1);
        assert_eq!(fragment.length, 4096);
        assert_eq!(fragment.stores, vec!["file:///"]);
        // Unset by the session, so the daemon's value stands.
        assert_eq!(spec.max_append_rate, defaults().max_append_rate);
        assert_eq!(fragment.retention, None);
        assert_eq!(spec.flags, broker::journal_spec::Flag::ORdwr as u32);
    }

    #[test]
    fn test_a_journal_needing_a_store_or_a_name_is_rejected() {
        let err = build(
            &proto::JournalConfig {
                journal: "acmeCo/disk/one".to_string(),
                ..Default::default()
            },
            &defaults(),
        )
        .unwrap_err();
        assert!(format!("{err}").contains("no fragment store"), "{err}");

        let err = build(&proto::JournalConfig::default(), &defaults()).unwrap_err();
        assert!(format!("{err}").contains("no journal name"), "{err}");
    }

    #[test]
    fn test_a_codec_which_cannot_be_read_back_is_rejected() {
        let err = build(
            &proto::JournalConfig {
                journal: "acmeCo/disk/one".to_string(),
                fragment_stores: vec!["file:///".to_string()],
                compression_codec: broker::CompressionCodec::Snappy as i32,
                ..Default::default()
            },
            &defaults(),
        )
        .unwrap_err();

        assert!(format!("{err}").contains("SNAPPY"), "{err}");
    }

    #[test]
    fn test_zero_selects_the_default() {
        assert_eq!(or(0, 7), 7);
        assert_eq!(or(3, 7), 3);
    }
}
