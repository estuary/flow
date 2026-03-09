use proto_gazette::uuid;
use std::collections::HashMap;

mod encoding;

#[cfg(test)]
mod fuzz;

/// Block is a columnar block of document entries.
///
/// Fields are factored for efficient zero-copy access: fixed-size metadata
/// is separate from variable-size documents, and journal names are
/// deduplicated with delta encoding.
///
/// Outside of tests we never actually use `Block` directly. We DO use its
/// rkyv-derived ArchivedBlock for zero-copy access within encoded block buffers.
///
/// Instead of encoding a Block through rkyv, encode() produces a bit-for-bit
/// equivalent encoding using pre-serialized ArchivedNode bytes.
#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
#[allow(dead_code)]
pub struct Block<'a> {
    /// Deduplicated, delta-encoded journal names sorted by full journal name.
    /// Each entry carries a `journal_bid` (block-internal ID) that documents
    /// reference via `BlockMeta::journal_bid`.
    pub journals: Vec<BlockJournal>,
    /// Deduplicated producers sorted by their 6-byte value. Each entry
    /// carries a `producer_bid` that documents reference via
    /// `BlockMeta::producer_bid`.
    pub producers: Vec<BlockProducer>,
    /// Per-document metadata, 1:1 with `docs`.
    pub meta: Vec<BlockMeta>,
    /// Per-document content with offset and key prefix.
    pub docs: Vec<BlockDoc<'a>>,
}

/// A delta-encoded journal entry within a block.
///
/// Journals are sorted by full name for efficient frontier matching.
/// The `journal_bid` is the block-internal ID that `BlockMeta` references,
/// assigned during block construction as journals are encountered.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize)]
#[rkyv(derive(Debug))]
pub struct BlockJournal {
    /// Block-internal ID for this journal, referenced by `BlockMeta::journal_bid`.
    pub journal_bid: u16,
    /// Bytes to truncate from the preceding journal name before appending
    /// `suffix`. Zero for the first entry (which is the full name).
    pub truncate_delta: i32,
    /// Suffix to append to the truncated preceding name to
    /// reconstruct the full journal name.
    pub suffix: String,
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
pub struct BlockDoc<'a> {
    /// Journal byte offset where this document begins.
    pub offset: i64,
    /// Leading prefix of the packed key, zero-padded if shorter than 16 bytes.
    pub packed_key_prefix: [u8; 16],
    /// The document content.
    pub doc: doc::HeapNode<'a>,
}

impl std::fmt::Debug for ArchivedBlock<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchivedBlock")
            .field("journals", &self.journals)
            .field("producers", &self.producers)
            .field("meta", &self.meta)
            .field("docs", &self.docs)
            .finish()
    }
}

impl std::fmt::Debug for ArchivedBlockDoc<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Safety: ArchivedNode's lifetime parameter is meaningless for archived
        // data (see doc::archived module). Transmute to 'static to satisfy
        // SerPolicy::on's lifetime bounds.
        let doc: &doc::ArchivedNode = unsafe {
            std::mem::transmute::<&doc::heap::ArchivedNode<'_>, &doc::ArchivedNode>(&self.doc)
        };
        let doc = serde_json::to_string(&doc::SerPolicy::debug().on(doc)).unwrap();

        f.debug_struct("ArchivedBlockDoc")
            .field("offset", &self.offset)
            .field("packed_key_prefix", &self.packed_key_prefix)
            .field("doc", &doc)
            .finish()
    }
}

/// Serialize a block from pre-archived components into an rkyv archive buffer.
///
/// Journals are sorted by name and delta-encoded. Producers are sorted by
/// their 6-byte value. Entries are split into the `meta` and `docs` columns.
pub fn encode(
    journals: &HashMap<String, u16>,
    producers: &HashMap<uuid::Producer, u16>,
    entries: &[(BlockMeta, i64, bytes::Bytes, bytes::Bytes)],
) -> rkyv::util::AlignedVec {
    let (meta, docs) = encode_entries(entries);

    let bytes_block = encoding::BytesBlock {
        journals: encode_journals(journals),
        producers: encode_producers(producers),
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

fn encode_producers(producers: &HashMap<uuid::Producer, u16>) -> Vec<BlockProducer> {
    // Sort producers by their 6-byte value.
    let mut sorted_producers: Vec<_> = producers.iter().collect();
    sorted_producers.sort_by_key(|(p, _)| *p);

    sorted_producers
        .iter()
        .map(|(producer, bid)| BlockProducer {
            producer_bid: **bid,
            producer: producer.0,
        })
        .collect()
}

fn encode_journals(journals: &HashMap<String, u16>) -> Vec<BlockJournal> {
    // Sort journals by name and delta-encode.
    let mut sorted_journals: Vec<_> = journals.iter().collect();
    sorted_journals.sort_by_key(|(name, _)| name.as_str());

    let mut journal_entries = Vec::with_capacity(sorted_journals.len());
    let mut prev_name = String::new();
    for (name, bid) in &sorted_journals {
        let (truncate_delta, suffix) = gazette::delta::encode(&prev_name, name);

        journal_entries.push(BlockJournal {
            journal_bid: **bid,
            truncate_delta,
            suffix: suffix.to_string(),
        });
        gazette::delta::decode(&mut prev_name, truncate_delta, suffix);
    }

    journal_entries
}

fn encode_entries(
    entries: &[(BlockMeta, i64, bytes::Bytes, bytes::Bytes)],
) -> (Vec<BlockMeta>, Vec<encoding::BytesDoc>) {
    // Split entries into meta and docs columns.
    let mut meta = Vec::with_capacity(entries.len());
    let mut docs = Vec::with_capacity(entries.len());
    for (block_meta, offset, packed_key, doc_bytes) in entries {
        meta.push(*block_meta);

        let mut packed_key_prefix = [0u8; 16];
        let copy_len = packed_key.len().min(16);
        packed_key_prefix[..copy_len].copy_from_slice(&packed_key[..copy_len]);

        docs.push(encoding::BytesDoc {
            offset: *offset,
            packed_key_prefix,
            doc_bytes: doc_bytes.clone(),
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
    fn test_encode_journals_delta_encoding_and_sorting() {
        // Note `journal_map` will randomize ordering.
        let journal_map: HashMap<String, u16> = [
            ("acme/alpha/one", 0),
            ("acme/alpha/three", 1),
            ("acme/beta/seven", 3),
            ("acme/beta/two", 4),
            ("other/journal", 2),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();

        // Expected after sorting by name and delta-encoding against predecessor.
        // truncate = prev.len() - common_prefix_len:
        //   prev=""                 → "acme/alpha/one":   truncate 0,  suffix "acme/alpha/one"
        //   prev="acme/alpha/one"   → "acme/alpha/three": truncate 3,  suffix "three"   (common "acme/alpha/", drop "one")
        //   prev="acme/alpha/three" → "acme/beta/seven":  truncate 11, suffix "beta/seven" (common "acme/", drop "alpha/three")
        //   prev="acme/beta/seven"  → "acme/beta/two":    truncate 5,  suffix "two"      (common "acme/beta/", drop "seven")
        //   prev="acme/beta/two"    → "other/journal":    truncate 13, suffix "other/journal" (no common prefix)
        let expected: Vec<(&str, i32, u16)> = vec![
            ("acme/alpha/one", 0, 0),
            ("three", 3, 1),
            ("beta/seven", 11, 3),
            ("two", 5, 4),
            ("other/journal", 13, 2),
        ];

        let actual = encode_journals(&journal_map);
        assert_eq!(actual.len(), expected.len());

        for (got, (suffix, truncate, bid)) in actual.iter().zip(&expected) {
            assert_eq!(got.suffix, *suffix, "suffix mismatch");
            assert_eq!(got.truncate_delta, *truncate, "truncate mismatch");
            assert_eq!(got.journal_bid, *bid, "bid mismatch");
        }
    }

    #[test]
    fn test_encode_producers_sorting_and_bids() {
        let input = vec![
            (uuid::Producer([0xff, 0, 0, 0, 0, 0]), 2),
            (uuid::Producer([0, 0, 0, 0, 0, 1]), 0),
            (uuid::Producer([0x80, 0, 0, 0, 0, 0]), 1),
        ];
        let expected = vec![
            ([0, 0, 0, 0, 0, 1], 0),
            ([0x80, 0, 0, 0, 0, 0], 1),
            ([0xff, 0, 0, 0, 0, 0], 2),
        ];

        let actual = encode_producers(&input.into_iter().collect());
        assert_eq!(actual.len(), expected.len());

        for (got, (prod, bid)) in actual.iter().zip(&expected) {
            assert_eq!(got.producer, *prod);
            assert_eq!(got.producer_bid, *bid);
        }
    }

    #[test]
    fn test_encode_round_trip_access_archived() {
        let alloc = doc::HeapNode::new_allocator();

        let journal_map: HashMap<String, u16> = [
            ("acme/widgets".to_string(), 0u16),
            ("acme/gadgets".to_string(), 1),
        ]
        .into();
        let producer_map: HashMap<uuid::Producer, u16> = [
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

        let buf = encode(&journal_map, &producer_map, &entries);
        let archived = rkyv::access::<ArchivedBlock, rkyv::rancor::Error>(&buf).unwrap();

        insta::assert_debug_snapshot!(archived);
    }

    #[test]
    fn test_encode_empty_block() {
        let buf = encode(&HashMap::new(), &HashMap::new(), &[]);
        let archived = rkyv::access::<ArchivedBlock, rkyv::rancor::Error>(&buf).unwrap();
        assert_eq!(archived.journals.len(), 0);
        assert_eq!(archived.producers.len(), 0);
        assert_eq!(archived.meta.len(), 0);
        assert_eq!(archived.docs.len(), 0);
    }
}
