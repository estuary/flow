use proto_gazette::uuid;
use std::collections::HashMap;

mod encoding;

#[cfg(test)]
mod fuzz;

/// Block is a columnar block of document entries.
///
/// Fields are factored for efficient zero-copy access: fixed-size metadata
/// is separate from variable-size documents, and journal names are
/// deduplicated and sorted by name.
///
/// Outside of tests we never actually use `Block` directly. We DO use its
/// rkyv-derived ArchivedBlock for zero-copy access within encoded block buffers.
///
/// Instead of encoding a Block through rkyv, encode() produces a bit-for-bit
/// equivalent encoding using pre-serialized `doc::ArchivedEmbedded` bytes.
///
/// A Block (and all columns thereof) is constrained to having at most 65,536 entries.
#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
#[allow(dead_code)]
pub struct Block<'alloc> {
    /// Deduplicated, sorted journal names.
    /// Each entry carries a `journal_bid` (block-internal ID) that documents
    /// reference via `BlockMeta::journal_bid`.
    pub journals: Vec<BlockJournal>,
    /// Deduplicated producers sorted by their 6-byte value. Each entry
    /// carries a `producer_bid` that documents reference via
    /// `BlockMeta::producer_bid`.
    pub producers: Vec<BlockProducer>,
    /// Reverse mapping from `journal_bid` => offset in `journals`.
    pub journals_reverse: Vec<u16>,
    /// Reverse mapping from `producer_bid` => offset in `producers`.
    pub producers_reverse: Vec<u16>,
    /// Per-document metadata, 1:1 with `docs`.
    pub meta: Vec<BlockMeta>,
    /// Per-document content with offset and key prefix.
    pub docs: Vec<BlockDoc<'alloc>>,
}

/// A journal entry within a block.
///
/// Journals are sorted by name for efficient frontier matching.
/// The `journal_bid` is the block-internal ID that `BlockMeta` references,
/// assigned during block construction as journals are encountered.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize)]
#[rkyv(derive(Debug))]
pub struct BlockJournal {
    /// Block-internal ID for this journal, referenced by `BlockMeta::journal_bid`.
    pub journal_bid: u16,
    /// Full journal name.
    pub name: String,
}

/// A deduplicated producer entry within a block.
///
/// Producers are sorted by their 6-byte value for efficient lookup.
/// The `producer_bid` is the block-internal ID that `BlockMeta` references.
#[derive(Debug, Clone, Copy, rkyv::Archive, rkyv::Serialize)]
#[rkyv(derive(Debug))]
pub struct BlockProducer {
    /// Block-internal ID for this producer, referenced by `BlockMeta::producer_bid`.
    pub producer_bid: u16,
    /// 6-byte producer identity.
    pub producer: [u8; 6],
}

/// Fixed-size per-document metadata.
#[derive(Debug, Clone, Copy, rkyv::Archive, rkyv::Serialize)]
#[rkyv(derive(Debug))]
pub struct BlockMeta {
    /// Binding of this document.
    pub binding: u16,
    /// Journal BID of this document.
    pub journal_bid: u16,
    /// Producer BID of this document.
    pub producer_bid: u16,
    /// Bit-flags of this document. Defined flags:
    ///  - 0x0001: Document passed JSON Schema validation
    pub flags: u16,
    /// Producer Clock of this document.
    pub clock: u64,
}

/// Per-document content with its journal offset and packed key prefix.
#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
pub struct BlockDoc<'alloc> {
    /// Journal byte offset where this document begins.
    pub offset: i64,
    /// Leading prefix of the packed key, zero-padded if shorter than 16 bytes.
    pub packed_key_prefix: [u8; 16],
    /// Pre-serialized document content as an embedded ArchivedNode buffer.
    pub doc: doc::HeapEmbedded<'alloc>,
}

impl std::fmt::Debug for ArchivedBlock<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchivedBlock")
            .field("journals", &self.journals)
            .field("producers", &self.producers)
            .field("journals_reverse", &self.journals_reverse)
            .field("producers_reverse", &self.producers_reverse)
            .field("meta", &self.meta)
            .field("docs", &self.docs)
            .finish()
    }
}

impl std::fmt::Debug for ArchivedBlockDoc<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchivedBlockDoc")
            .field("offset", &self.offset)
            .field("packed_key_prefix", &self.packed_key_prefix)
            .field("doc", &self.doc)
            .finish()
    }
}

/// Serialize a block from pre-archived components into an rkyv archive buffer.
///
/// Journals are sorted by name and delta-encoded. Producers are sorted by
/// their 6-byte value. Entries are split into the `meta` and `docs` columns.
///
/// Panics if any of `journals`, `producers`, or `entries` exceeds 65,536 items.
pub fn encode(
    journals: HashMap<String, u16>,
    producers: HashMap<uuid::Producer, u16>,
    entries: Vec<(BlockMeta, i64, bytes::Bytes, bytes::Bytes)>,
) -> rkyv::util::AlignedVec {
    assert!(journals.len() <= 1 << 16);
    assert!(producers.len() <= 1 << 16);
    assert!(entries.len() <= 1 << 16);

    let (meta, docs) = encode_entries(entries);

    let journals = encode_journals(journals);
    let producers = encode_producers(producers);

    let journals_reverse = build_bid_reverse(&journals, |j| j.journal_bid);
    let producers_reverse = build_bid_reverse(&producers, |p| p.producer_bid);

    let bytes_block = encoding::BytesBlock {
        journals,
        producers,
        journals_reverse,
        producers_reverse,
        meta,
        docs,
    };
    let encoded_size = encoding::encoded_size(&bytes_block);

    let buf = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(
        &bytes_block,
        rkyv::util::AlignedVec::with_capacity(encoded_size),
    )
    .unwrap();

    debug_assert_eq!(buf.len(), encoded_size);
    buf
}

/// Build a reverse mapping from bid → sorted index.
fn build_bid_reverse<T>(items: &[T], bid: impl Fn(&T) -> u16) -> Vec<u16> {
    let mut reverse = vec![0u16; items.len()];
    for (sorted_idx, item) in items.iter().enumerate() {
        reverse[bid(item) as usize] = sorted_idx as u16;
    }
    reverse
}

fn encode_producers(producers: HashMap<uuid::Producer, u16>) -> Vec<BlockProducer> {
    let mut producers: Vec<_> = producers.into_iter().collect();
    producers.sort();

    producers
        .into_iter()
        .map(|(producer, bid)| BlockProducer {
            producer_bid: bid,
            producer: producer.0,
        })
        .collect()
}

fn encode_journals(journals: HashMap<String, u16>) -> Vec<BlockJournal> {
    let mut journals: Vec<_> = journals.into_iter().collect();
    journals.sort();

    journals
        .into_iter()
        .map(|(name, bid)| BlockJournal {
            journal_bid: bid,
            name: name.to_string(),
        })
        .collect()
}

fn encode_entries(
    entries: Vec<(BlockMeta, i64, bytes::Bytes, bytes::Bytes)>,
) -> (Vec<BlockMeta>, Vec<encoding::BytesDoc>) {
    // Split entries into `meta` and `docs` columns.
    let mut meta = Vec::with_capacity(entries.len());
    let mut docs = Vec::with_capacity(entries.len());

    for (block_meta, offset, packed_key, doc_bytes) in entries {
        meta.push(block_meta);

        let mut packed_key_prefix = [0u8; 16];
        let copy_len = packed_key.len().min(16);
        packed_key_prefix[..copy_len].copy_from_slice(&packed_key[..copy_len]);

        docs.push(encoding::BytesDoc {
            offset,
            packed_key_prefix,
            doc_bytes,
        });
    }
    (meta, docs)
}

#[cfg(test)]
mod test {
    use super::*;

    fn meta(binding: u16, journal_bid: u16, producer_bid: u16, clock: u64) -> BlockMeta {
        BlockMeta {
            binding,
            journal_bid,
            producer_bid,
            flags: 0,
            clock,
        }
    }

    #[test]
    fn test_encode_journals_sorting() {
        let input: HashMap<String, u16> = [
            ("acme/alpha/one", 0),
            ("acme/alpha/three", 1),
            ("acme/beta/seven", 3),
            ("acme/beta/two", 4),
            ("other/journal", 2),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        let expected: Vec<(&str, u16)> = vec![
            ("acme/alpha/one", 0),
            ("acme/alpha/three", 1),
            ("acme/beta/seven", 3),
            ("acme/beta/two", 4),
            ("other/journal", 2),
        ];

        let actual = encode_journals(input);
        assert_eq!(actual.len(), expected.len());

        for (got, (name, bid)) in actual.iter().zip(&expected) {
            assert_eq!(got.name, *name, "name mismatch");
            assert_eq!(got.journal_bid, *bid, "bid mismatch");
        }
    }

    #[test]
    fn test_encode_producers_sorting_and_bids() {
        let input: HashMap<uuid::Producer, u16> = [
            (uuid::Producer([0xff, 0, 0, 0, 0, 0]), 2),
            (uuid::Producer([0, 0, 0, 0, 0, 1]), 0),
            (uuid::Producer([0x80, 0, 0, 0, 0, 0]), 1),
        ]
        .into_iter()
        .collect();

        let expected = vec![
            ([0, 0, 0, 0, 0, 1], 0),
            ([0x80, 0, 0, 0, 0, 0], 1),
            ([0xff, 0, 0, 0, 0, 0], 2),
        ];

        let actual = encode_producers(input);
        assert_eq!(actual.len(), expected.len());

        for (got, (prod, bid)) in actual.iter().zip(&expected) {
            assert_eq!(got.producer, *prod);
            assert_eq!(got.producer_bid, *bid);
        }
    }

    #[test]
    fn test_encode_round_trip_access_archived() {
        let alloc = doc::HeapNode::new_allocator();

        let journals: HashMap<String, u16> = [
            ("acme/widgets".to_string(), 0u16),
            ("acme/gadgets".to_string(), 1),
        ]
        .into();
        let producers: HashMap<uuid::Producer, u16> = [
            (uuid::Producer([0, 0, 0, 0, 0, 1]), 0u16),
            (uuid::Producer([0, 0, 0, 0, 0, 2]), 1),
        ]
        .into();

        let doc1 = doc::HeapNode::from_serde(&serde_json::json!({"key": "val1"}), &alloc).unwrap();
        let doc2 = doc::HeapNode::from_serde(&serde_json::json!({"key": "val2"}), &alloc).unwrap();

        let entries = vec![
            (
                meta(0, 0, 0, 10),
                100i64,
                bytes::Bytes::from_static(b"packed_key_one__"),
                bytes::Bytes::from(doc1.to_archive().to_vec()),
            ),
            (
                meta(1, 1, 1, 20),
                200i64,
                bytes::Bytes::from_static(b"packed_key_two__extra_ignored"),
                bytes::Bytes::from(doc2.to_archive().to_vec()),
            ),
        ];

        let buf = encode(journals, producers, entries);
        let archived = rkyv::access::<ArchivedBlock, rkyv::rancor::Error>(&buf).unwrap();

        insta::assert_debug_snapshot!(archived);
    }

    #[test]
    fn test_encode_empty_block() {
        let buf = encode(HashMap::new(), HashMap::new(), Vec::new());
        let archived = rkyv::access::<ArchivedBlock, rkyv::rancor::Error>(&buf).unwrap();
        assert_eq!(archived.journals.len(), 0);
        assert_eq!(archived.producers.len(), 0);
        assert_eq!(archived.meta.len(), 0);
        assert_eq!(archived.docs.len(), 0);
    }
}
