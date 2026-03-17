use super::*;
use quickcheck::Arbitrary;

#[derive(Clone, Debug)]
struct FuzzInput {
    /// Per-entry metadata variations.
    entries: Vec<FuzzEntry>,
}

#[derive(Clone, Debug)]
struct FuzzEntry {
    journal_idx: u8,
    producer_idx: u8,
    binding: u16,
    clock: u64,
    packed_key: Vec<u8>,
    value: String,
}

impl Arbitrary for FuzzInput {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        let num_entries = u8::arbitrary(g) % 8;
        let entries = (0..num_entries).map(|_| FuzzEntry::arbitrary(g)).collect();

        FuzzInput { entries }
    }
}

impl Arbitrary for FuzzEntry {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        let key_len = u8::arbitrary(g) % 20;
        let packed_key: Vec<u8> = (0..key_len).map(|_| u8::arbitrary(g)).collect();

        FuzzEntry {
            journal_idx: u8::arbitrary(g) % 4,
            producer_idx: u8::arbitrary(g) % 4,
            binding: u16::arbitrary(g),
            clock: u64::arbitrary(g),
            packed_key,
            value: String::arbitrary(g),
        }
    }
}

/// Fixed set of journal names indexed by FuzzEntry::journal_idx.
const JOURNALS: &[&str] = &[
    "acmeCo/alpha/one",
    "acmeCo/alpha/three",
    "acmeCo/beta/seven",
    "other/journal",
];

/// Fixed set of producer values indexed by FuzzEntry::producer_idx.
const PRODUCERS: &[[u8; 6]] = &[
    [0, 0, 0, 0, 0, 0],
    [0, 0, 0, 0, 0, 1],
    [0x80, 0, 0, 0, 0, 0],
    [0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
];

/// Build a Block and its BlockParts encoding from fuzz input, then verify
/// bit-for-bit equivalence.
fn round_trip(input: FuzzInput) -> bool {
    let alloc = doc::HeapNode::new_allocator();

    let mut journal_map: HashMap<String, u16> = HashMap::new();
    let mut producer_map: HashMap<uuid::Producer, u16> = HashMap::new();

    let mut heap_meta = Vec::new();
    let mut heap_docs = Vec::new();
    let mut encode_entries = Vec::new();

    for entry in &input.entries {
        let journal = JOURNALS[entry.journal_idx as usize];
        let producer = proto_gazette::uuid::Producer(PRODUCERS[entry.producer_idx as usize]);

        let next_bid = journal_map.len() as u16;
        let journal_bid = *journal_map.entry(journal.to_string()).or_insert(next_bid);

        let next_bid = producer_map.len() as u16;
        let producer_bid = *producer_map.entry(producer).or_insert(next_bid);

        let mut packed_key_prefix = [0u8; 16];
        let copy_len = entry.packed_key.len().min(16);
        packed_key_prefix[..copy_len].copy_from_slice(&entry.packed_key[..copy_len]);

        let meta = BlockMeta {
            binding: entry.binding,
            journal_bid,
            producer_bid,
            flags: 0,
            clock: entry.clock,
        };
        heap_meta.push(meta);

        let doc_json = serde_json::json!({"p": [42, entry.value]});
        let heap_doc = doc::HeapNode::from_serde(&doc_json, &alloc).unwrap();
        let doc_bytes = heap_doc.to_archive();

        encode_entries.push((
            meta,
            doc_bytes.len() as u32,
            bytes::Bytes::copy_from_slice(&entry.packed_key),
            bytes::Bytes::from(doc_bytes.to_vec()),
        ));

        // Allocate the archive bytes as u64-aligned in the bump allocator
        // and wrap in HeapEmbedded for the reference Block path.
        let embedded_doc = unsafe {
            let buffer = core::slice::from_raw_parts(
                doc_bytes.as_ptr() as *const doc::embedded::U64Le,
                doc_bytes.len() / 8,
            );
            // Copy to bump allocator, as we'll drop `doc_bytes`.
            let buffer = alloc.alloc_slice_copy(buffer);

            doc::HeapEmbedded::from_buffer(buffer)
        };

        heap_docs.push(BlockDoc {
            packed_key_prefix,
            source_byte_length: doc_bytes.len() as u32,
            doc: embedded_doc,
        });
    }

    let journals = super::encode_journals(journal_map.clone());
    let producers = super::encode_producers(producer_map.clone());

    let journals_reverse = super::build_bid_reverse(&journals, |j| j.journal_bid);
    let producers_reverse = super::build_bid_reverse(&producers, |p| p.producer_bid);

    let heap = Block {
        journals,
        producers,
        journals_reverse,
        producers_reverse,
        meta: heap_meta,
        docs: heap_docs,
    };

    // Encode via BytesBlock path.
    let custom_encoding = encode(journal_map, producer_map, encode_entries);
    // Encode via rkyv derive on Block.
    let rkyv_encoding = rkyv::to_bytes::<rkyv::rancor::Error>(&heap).unwrap();

    // They should be byte-for-byte identical.
    rkyv_encoding.as_slice() == custom_encoding.as_slice()
}

#[quickcheck_macros::quickcheck]
fn fuzz_block_parts_encoding(input: FuzzInput) -> bool {
    round_trip(input)
}
