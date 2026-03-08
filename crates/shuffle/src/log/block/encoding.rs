use super::{
    ArchivedBlock, ArchivedBlockDoc, ArchivedBlockJournal, ArchivedBlockMeta,
    ArchivedBlockProducer, BlockJournal, BlockMeta, BlockProducer,
};

/// BytesBlock is like Block, with `docs` as BytesDoc instead of BlockDoc.
#[derive(rkyv::Archive, rkyv::Serialize)]
pub struct BytesBlock {
    pub journals: Vec<BlockJournal>,
    pub producers: Vec<BlockProducer>,
    pub meta: Vec<BlockMeta>,
    pub docs: Vec<BytesDoc>,
}

/// BytesDoc is like BlockDoc, but holds a pre-serialized doc as Bytes.
///
/// It implements rkyv's Serialize and Archive traits to produce the same byte
/// layout as `BlockDoc`. During serialize, the `doc_bytes` sub-data (everything
/// before the trailing 16-byte root) is written verbatim. During resolve,
/// other fixed fields are written and the root bytes are copied with relative
/// offsets adjusted to account for the new distance between root and sub-data.
pub struct BytesDoc {
    pub offset: i64,
    pub packed_key_prefix: [u8; 16],
    pub doc_bytes: bytes::Bytes,
}

#[derive(Debug, thiserror::Error)]
#[error("doc_bytes are malformed")]
struct DocBytesMalformed;

impl<S> rkyv::Serialize<S> for BytesDoc
where
    S: rkyv::ser::Writer + rkyv::ser::Allocator + rkyv::rancor::Fallible + ?Sized,
    <S as rkyv::rancor::Fallible>::Error: rkyv::rancor::Source,
{
    fn serialize(&self, serializer: &mut S) -> Result<usize, <S as rkyv::rancor::Fallible>::Error> {
        const NODE_SIZE: usize = size_of::<doc::ArchivedNode>();
        let child_len = self.doc_bytes.len().saturating_sub(NODE_SIZE);

        // This is far from exhaustive, but will catch simple wiring errors.
        if self.doc_bytes.len() < NODE_SIZE || self.doc_bytes.len() % 8 != 0 {
            rkyv::rancor::fail!(DocBytesMalformed);
        }

        let pos = serializer.pos();
        serializer.write(&self.doc_bytes[..child_len])?;
        Ok(pos)
    }
}

impl rkyv::Archive for BytesDoc {
    type Archived = ArchivedBlockDoc<'static>;
    /// The serializer position where this doc's sub-data was written.
    type Resolver = usize;

    fn resolve(&self, sub_data_pos: usize, out: rkyv::Place<Self::Archived>) {
        const NODE_SIZE: usize = size_of::<doc::ArchivedNode>();
        let child_len = self.doc_bytes.len().saturating_sub(NODE_SIZE);

        unsafe {
            let ptr = out.ptr();

            // Resolve the fixed-size fields.
            rkyv::Archive::resolve(
                &self.offset,
                (),
                rkyv::Place::from_field_unchecked(out, core::ptr::addr_of_mut!((*ptr).offset)),
            );
            rkyv::Archive::resolve(
                &self.packed_key_prefix,
                [(); 16],
                rkyv::Place::from_field_unchecked(
                    out,
                    core::ptr::addr_of_mut!((*ptr).packed_key_prefix),
                ),
            );

            // Resolve the doc field: copy root bytes with adjusted relative offsets.
            let doc_out =
                rkyv::Place::from_field_unchecked(out, core::ptr::addr_of_mut!((*ptr).doc));
            let root_pos = doc_out.pos();
            let delta = (sub_data_pos + child_len) as i64 - root_pos as i64;

            let mut adjusted = [0u8; 16];
            adjusted[..NODE_SIZE].copy_from_slice(&self.doc_bytes[child_len..]);
            adjust_archived_node_root(&mut adjusted, delta as i32);
            core::ptr::copy_nonoverlapping(adjusted.as_ptr(), doc_out.ptr() as *mut u8, NODE_SIZE);
        }
    }
}

/// Compute the exact rkyv-encoded byte count for the given block parts.
///
/// Mirrors the serialization order in `BlockParts::serialize`:
/// for each vec, element sub-data is written first, then alignment padding,
/// then the fixed-size element array. The root struct is written last.
pub fn encoded_size(parts: &BytesBlock) -> usize {
    fn align_up(pos: usize, align: usize) -> usize {
        (pos + align - 1) & !(align - 1)
    }

    let mut pos = 0usize;

    // Journals: out-of-line suffix bytes, then aligned journal array.
    for j in &parts.journals {
        if j.suffix.len() > rkyv::string::repr::INLINE_CAPACITY {
            pos += j.suffix.len();
        }
    }
    pos = align_up(pos, align_of::<ArchivedBlockJournal>());
    pos += parts.journals.len() * size_of::<ArchivedBlockJournal>();

    // Producers: no sub-data, just the aligned array.
    pos = align_up(pos, align_of::<ArchivedBlockProducer>());
    pos += parts.producers.len() * size_of::<ArchivedBlockProducer>();

    // Meta: no sub-data, just the aligned array.
    pos = align_up(pos, align_of::<ArchivedBlockMeta>());
    pos += parts.meta.len() * size_of::<ArchivedBlockMeta>();

    // Docs: pre-serialized sub-data bytes, then aligned doc array.
    for d in &parts.docs {
        pos += d
            .doc_bytes
            .len()
            .saturating_sub(size_of::<doc::ArchivedNode>());
    }
    pos = align_up(pos, align_of::<ArchivedBlockDoc<'static>>());
    pos += parts.docs.len() * size_of::<ArchivedBlockDoc<'static>>();

    // Root struct.
    pos = align_up(pos, align_of::<ArchivedBlock<'static>>());
    pos += size_of::<ArchivedBlock<'static>>();

    pos
}

/// Adjust relative offsets within an ArchivedNode's 16-byte root representation.
///
/// When a root is relocated relative to its sub-data, every relative offset
/// must be adjusted by `delta = sub_data_end_pos - new_root_pos`.
///
/// Scalar variants (Bool, Float, NegInt, Null, PosInt) and inline strings
/// have no relative offsets and are unaffected.
fn adjust_archived_node_root(root: &mut [u8; 16], delta: i32) {
    if delta == 0 {
        return;
    }

    let discriminant = root[0];

    // Determine which 4-byte i32 field (if any) contains a relative offset.
    //   Array(i32, ArchivedVec):  vec RelPtr at bytes 8..12
    //   Bool(bool):               no offset
    //   Bytes(ArchivedVec):       vec RelPtr at bytes 4..8
    //   Float/NegInt/PosInt:      no offset
    //   Null:                     no offset
    //   Object(i32, ArchivedVec): vec RelPtr at bytes 8..12
    //   String(ArchivedString):   out-of-line offset at bytes 8..12 (if not inline)
    let offset_pos: Option<usize> = match discriminant {
        0 | 6 => Some(8), // Array, Object
        2 => Some(4),     // Bytes
        8 => {
            // String: ArchivedStringRepr at bytes 4..12.
            // Inline when first byte (bytes[4]) has bits [7:6] != 0b10.
            if root[4] & 0xc0 == 0x80 {
                Some(8) // Out-of-line
            } else {
                None // Inline
            }
        }
        1 | 3 | 4 | 5 | 7 => None,
        _ => panic!("invalid ArchivedNode discriminant: {discriminant}"),
    };

    if let Some(pos) = offset_pos {
        let bytes: [u8; 4] = root[pos..pos + 4].try_into().unwrap();
        let adjusted = i32::from_le_bytes(bytes)
            .checked_add(delta)
            .expect("relative offset overflow");
        root[pos..pos + 4].copy_from_slice(&adjusted.to_le_bytes());
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn serialize_bytes_block(block: &BytesBlock) -> rkyv::util::AlignedVec {
        rkyv::to_bytes::<rkyv::rancor::Error>(block).unwrap()
    }

    #[test]
    fn test_encoded_size_empty_block() {
        let block = BytesBlock {
            journals: vec![],
            producers: vec![],
            meta: vec![],
            docs: vec![],
        };
        assert_eq!(encoded_size(&block), serialize_bytes_block(&block).len());
    }

    #[test]
    fn test_encoded_size_matches_actual() {
        let alloc = doc::HeapNode::new_allocator();

        let doc_values = [
            serde_json::json!(null),
            serde_json::json!({"key": "value", "n": [1, 2]}),
            serde_json::json!("a string longer than eight bytes"),
        ];
        let docs: Vec<BytesDoc> = doc_values
            .iter()
            .map(|val| {
                let heap = doc::HeapNode::from_serde(val, &alloc).unwrap();
                BytesDoc {
                    offset: 0,
                    packed_key_prefix: [0; 16],
                    doc_bytes: bytes::Bytes::from(heap.to_archive().to_vec()),
                }
            })
            .collect();

        let block = BytesBlock {
            journals: vec![
                crate::log::block::BlockJournal {
                    journal_bid: 0,
                    truncate_delta: 0,
                    suffix: "short".to_string(), // inline
                },
                crate::log::block::BlockJournal {
                    journal_bid: 1,
                    truncate_delta: 5,
                    suffix: "a".repeat(rkyv::string::repr::INLINE_CAPACITY + 10), // out-of-line
                },
            ],
            producers: vec![
                crate::log::block::BlockProducer {
                    producer_bid: 0,
                    producer: [0; 6],
                },
                crate::log::block::BlockProducer {
                    producer_bid: 1,
                    producer: [0xff; 6],
                },
            ],
            meta: vec![
                crate::log::block::BlockMeta {
                    binding: 0,
                    journal_bid: 0,
                    producer_bid: 0,
                    flags: 0,
                    clock: 0,
                },
                crate::log::block::BlockMeta {
                    binding: 1,
                    journal_bid: 1,
                    producer_bid: 1,
                    flags: 0,
                    clock: 1,
                },
                crate::log::block::BlockMeta {
                    binding: 2,
                    journal_bid: 0,
                    producer_bid: 1,
                    flags: 0,
                    clock: 2,
                },
            ],
            docs,
        };

        assert_eq!(encoded_size(&block), serialize_bytes_block(&block).len());
    }

    #[test]
    fn test_bytes_doc_round_trip_various_docs() {
        let alloc = doc::HeapNode::new_allocator();

        let cases: Vec<serde_json::Value> = vec![
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(42),
            serde_json::json!(-99),
            serde_json::json!(3.14),
            serde_json::json!("short"),
            serde_json::json!("a string that is definitely longer than inline capacity"),
            serde_json::json!([1, "two", null]),
            serde_json::json!({"a": 1, "b": [2, 3]}),
        ];

        for val in &cases {
            let heap_doc = doc::HeapNode::from_serde(val, &alloc).unwrap();

            // Encode via BytesDoc (the custom path).
            let doc_bytes = bytes::Bytes::from(heap_doc.to_archive().to_vec());
            let bytes_doc = BytesDoc {
                offset: 42,
                packed_key_prefix: [0xAB; 16],
                doc_bytes,
            };
            let block = BytesBlock {
                journals: vec![],
                producers: vec![],
                meta: vec![],
                docs: vec![bytes_doc],
            };
            let custom = serialize_bytes_block(&block);

            // Encode via rkyv derive (the reference path).
            let block_doc = crate::log::block::BlockDoc {
                offset: 42,
                packed_key_prefix: [0xAB; 16],
                doc: heap_doc,
            };
            let ref_block = crate::log::block::Block {
                journals: vec![],
                producers: vec![],
                meta: vec![],
                docs: vec![block_doc],
            };
            let reference = rkyv::to_bytes::<rkyv::rancor::Error>(&ref_block).unwrap();

            assert_eq!(
                custom.as_slice(),
                reference.as_slice(),
                "byte mismatch for doc: {val}"
            );
        }
    }
}
