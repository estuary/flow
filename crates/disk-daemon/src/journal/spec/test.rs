use super::listed_floor;
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
        assert_eq!(listed_floor(listing(spec)).unwrap(), 0);
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
                    path_postfix_template: "{{.Spool.FirstAppendTime.Format \"2006\"}}".to_string(),
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
        (None, 0),
        (Some("0000000000000000"), 0),
        (Some("00000000000186a0"), 100_000),
        (Some("ffffffffffffffff"), u64::MAX as i64),
        (Some("not-hex"), 0),
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
