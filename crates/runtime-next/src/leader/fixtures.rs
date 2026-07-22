//! Shared fixtures for leader tests.
use super::frontier_mapping;
use bytes::Bytes;
use proto_gazette::{consumer, uuid};
use std::collections::BTreeMap;

pub(crate) fn clk(secs: u64) -> uuid::Clock {
    uuid::Clock::from_unix(secs, 0)
}

pub(crate) fn prod(tag: u8) -> uuid::Producer {
    uuid::Producer::from_bytes([0x01, tag, 0, 0, 0, 0])
}

pub(crate) fn pf(
    tag: u8,
    last_commit: uuid::Clock,
    hinted_commit: uuid::Clock,
    offset: i64,
) -> shuffle::ProducerFrontier {
    shuffle::ProducerFrontier {
        producer: prod(tag),
        last_commit,
        hinted_commit,
        offset,
    }
}

/// A single `shuffle::JournalFrontier` with zero byte deltas.
pub(crate) fn journal_frontier(
    journal: &str,
    binding: u16,
    producers: Vec<shuffle::ProducerFrontier>,
) -> shuffle::JournalFrontier {
    shuffle::JournalFrontier {
        journal: journal.into(),
        binding,
        producers,
        bytes_read_delta: 0,
        bytes_behind_delta: 0,
    }
}

/// A single-journal, single-binding `shuffle::Frontier`.
pub(crate) fn frontier(
    journal: &str,
    binding: u16,
    producers: Vec<shuffle::ProducerFrontier>,
) -> shuffle::Frontier {
    shuffle::Frontier::new(vec![journal_frontier(journal, binding, producers)], vec![]).unwrap()
}

/// A checkpoint source `ProducerEntry` for producer `tag`, with the given
/// `last_ack` Clock (as a u64) and pending-transaction `begin` offset.
pub(crate) fn producer_entry(
    tag: u8,
    last_ack: u64,
    begin: i64,
) -> consumer::checkpoint::source::ProducerEntry {
    consumer::checkpoint::source::ProducerEntry {
        id: Bytes::copy_from_slice(prod(tag).as_bytes()),
        state: Some(consumer::checkpoint::ProducerState { last_ack, begin }),
    }
}

/// A checkpoint `Source` read through `read_through` with the given `producers`.
pub(crate) fn source(
    read_through: i64,
    producers: Vec<consumer::checkpoint::source::ProducerEntry>,
) -> consumer::checkpoint::Source {
    consumer::checkpoint::Source {
        read_through,
        producers,
    }
}

/// An authoritative checkpoint (no committed-close key) mapping
/// "{journal};{suffix}" to a single producer at committed end offset
/// `read_through` (begin=-1) and `last_ack`.
pub(crate) fn authoritative_checkpoint(
    journal: &str,
    suffix: &str,
    tag: u8,
    last_ack: uuid::Clock,
    read_through: i64,
) -> consumer::Checkpoint {
    let mut sources = BTreeMap::new();
    sources.insert(
        format!("{journal};{suffix}"),
        source(
            read_through,
            vec![producer_entry(tag, last_ack.as_u64(), -1)],
        ),
    );
    consumer::Checkpoint {
        sources,
        ack_intents: [("ack/j".to_string(), Bytes::from_static(b"ACK"))].into(),
    }
}

/// A checkpoint carrying only a committed-close Clock (no data sources).
pub(crate) fn close_only_checkpoint(close: uuid::Clock, ack_key: &str) -> consumer::Checkpoint {
    let (k, v) = frontier_mapping::encode_committed_close(close);
    consumer::Checkpoint {
        sources: [(k, v)].into(),
        ack_intents: [(ack_key.to_string(), Bytes::from_static(b"C"))].into(),
    }
}

pub(crate) fn producer_tags(f: &shuffle::Frontier) -> Vec<u8> {
    f.journals
        .iter()
        .flat_map(|jf| jf.producers.iter().map(|p| p.producer.as_bytes()[1]))
        .collect()
}
