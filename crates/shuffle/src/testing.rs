use proto_flow::{flow, shuffle};
use proto_gazette::broker;
use proto_gazette::uuid::{Clock, Producer};

pub fn producer(id: u8) -> Producer {
    Producer::from_bytes([id | 0x01, 0, 0, 0, 0, 0])
}

/// Build a ProducerFrontier with clock values expressed as seconds.
/// Zero maps to Clock::zero() (the "no hint" / "no progress" sentinel).
pub fn pf(id: u8, last_commit: u64, hinted_commit: u64, offset: i64) -> crate::ProducerFrontier {
    fn clock_seconds(seconds: u64) -> Clock {
        if seconds == 0 {
            Clock::from_u64(0)
        } else {
            Clock::from_unix(seconds, 0)
        }
    }
    crate::ProducerFrontier {
        producer: producer(id),
        last_commit: clock_seconds(last_commit),
        hinted_commit: clock_seconds(hinted_commit),
        offset,
    }
}

pub fn jf(
    journal: &str,
    binding: u32,
    producers: Vec<crate::ProducerFrontier>,
) -> crate::JournalFrontier {
    crate::JournalFrontier {
        journal: journal.into(),
        binding,
        producers,
    }
}

/// Compact representation for assertion: (last_commit_seconds, hinted_commit_seconds, offset).
pub fn pf_tuple(pf: &crate::ProducerFrontier) -> (u64, u64, i64) {
    (
        pf.last_commit.to_unix().0,
        pf.hinted_commit.to_unix().0,
        pf.offset,
    )
}

/// Build a 3-member topology:
///   member 0: 0x00000000-0x55555554
///   member 1: 0x55555555-0xaaaaaaa9
///   member 2: 0xaaaaaaaa-0xffffffff
pub fn test_members_3() -> Vec<shuffle::Member> {
    vec![
        shuffle::Member {
            range: Some(flow::RangeSpec {
                key_begin: 0x00000000,
                key_end: 0x55555554,
                r_clock_begin: 0,
                r_clock_end: 0xffffffff,
            }),
            ..Default::default()
        },
        shuffle::Member {
            range: Some(flow::RangeSpec {
                key_begin: 0x55555555,
                key_end: 0xaaaaaaa9,
                r_clock_begin: 0,
                r_clock_end: 0xffffffff,
            }),
            ..Default::default()
        },
        shuffle::Member {
            range: Some(flow::RangeSpec {
                key_begin: 0xaaaaaaaa,
                key_end: 0xffffffff,
                r_clock_begin: 0,
                r_clock_end: 0xffffffff,
            }),
            ..Default::default()
        },
    ]
}

/// Build a minimal Binding with just the fields used by on_listing_added.
pub fn test_binding(
    index: u32,
    uses_source_key: bool,
    partition_fields: Option<Vec<String>>,
    journal_read_suffix: &str,
) -> crate::Binding {
    crate::Binding {
        index,
        collection: models::Collection::new("test/collection"),
        filter_r_clocks: false,
        journal_read_suffix: journal_read_suffix.to_string(),
        priority: 0,
        read_delay: Clock::from_u64(0),
        key_extractors: Vec::new(),
        shuffle_key_partition_fields: partition_fields,
        partition_selector: broker::LabelSelector::default(),
        source_uuid_ptr: json::Pointer::from_str("/_meta/uuid"),
        uses_lambda: false,
        uses_source_key,
        not_before: Clock::UNIX_EPOCH,
        not_after: Clock::from_u64(u64::MAX),
        cohort: 0,
        partition_template_name: "test/collection".into(),
        partition_fields: Vec::new(),
    }
}

/// Build a JournalSpec with the given name and key range labels.
/// Optional field_labels are added as "estuary.dev/field/<name>" = "<value>".
pub fn test_journal_spec(
    name: &str,
    key_begin: u32,
    key_end: u32,
    field_labels: &[(&str, &str)],
) -> broker::JournalSpec {
    let mut label_pairs: Vec<(&str, String)> = vec![
        (labels::KEY_BEGIN, format!("{key_begin:08x}")),
        (labels::KEY_END, format!("{key_end:08x}")),
    ];

    let field_label_pairs: Vec<(String, String)> = field_labels
        .iter()
        .map(|(field, value)| {
            (
                format!("{}{field}", labels::FIELD_PREFIX),
                value.to_string(),
            )
        })
        .collect();

    for (n, v) in &field_label_pairs {
        label_pairs.push((n.as_str(), v.clone()));
    }

    let labels = labels::build_set(label_pairs.iter().map(|(n, v)| (n.to_string(), v.clone())));

    broker::JournalSpec {
        name: name.to_string(),
        labels: Some(labels),
        ..Default::default()
    }
}

pub fn test_listing_added(
    binding_index: u32,
    spec: broker::JournalSpec,
) -> shuffle::slice_response::ListingAdded {
    shuffle::slice_response::ListingAdded {
        binding: binding_index,
        spec: Some(spec),
        create_revision: 1,
        mod_revision: 1,
        route: None,
    }
}
