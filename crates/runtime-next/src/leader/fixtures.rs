//! Shared fixtures for leader tests.
use bytes::Bytes;
use proto_gazette::{consumer, uuid};

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
