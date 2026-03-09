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
/// layout as `BlockDoc`. The complete `doc_bytes` buffer is written as opaque
/// sub-data behind an `ArchivedVec<u64>` (via `ArchivedEmbedded`).
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
    fn serialize(
        &self,
        serializer: &mut S,
    ) -> Result<rkyv::vec::VecResolver, <S as rkyv::rancor::Fallible>::Error> {
        use rkyv::ser::WriterExt;

        // This is far from exhaustive, but will catch simple wiring errors.
        if self.doc_bytes.len() < size_of::<doc::ArchivedNode>() || self.doc_bytes.len() % 8 != 0 {
            rkyv::rancor::fail!(DocBytesMalformed);
        }

        serializer.align_for::<u64>()?;
        let pos = serializer.pos();
        serializer.write(&self.doc_bytes)?;
        Ok(rkyv::vec::VecResolver::from_pos(pos))
    }
}

impl rkyv::Archive for BytesDoc {
    type Archived = ArchivedBlockDoc<'static>;
    type Resolver = rkyv::vec::VecResolver;

    fn resolve(&self, resolver: rkyv::vec::VecResolver, out: rkyv::Place<Self::Archived>) {
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

            // Resolve the doc field as ArchivedVec<U64Le> via ArchivedEmbedded.
            let doc_out =
                rkyv::Place::from_field_unchecked(out, core::ptr::addr_of_mut!((*ptr).doc));
            let vec_out = rkyv::Place::new_unchecked(
                doc_out.pos(),
                doc_out.ptr() as *mut rkyv::vec::ArchivedVec<doc::embedded::U64Le>,
            );
            rkyv::vec::ArchivedVec::resolve_from_len(self.doc_bytes.len() / 8, resolver, vec_out);
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

    // Journals: out-of-line name bytes, then aligned journal array.
    for j in &parts.journals {
        if j.name.len() > rkyv::string::repr::INLINE_CAPACITY {
            pos += j.name.len();
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

    // Docs: complete pre-serialized buffers (u64-aligned per doc), then aligned doc array.
    for d in &parts.docs {
        pos = align_up(pos, align_of::<u64>());
        pos += d.doc_bytes.len();
    }
    pos = align_up(pos, align_of::<ArchivedBlockDoc<'static>>());
    pos += parts.docs.len() * size_of::<ArchivedBlockDoc<'static>>();

    // Root struct.
    pos = align_up(pos, align_of::<ArchivedBlock<'static>>());
    pos += size_of::<ArchivedBlock<'static>>();

    pos
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
                    name: "short".to_string(), // inline
                },
                crate::log::block::BlockJournal {
                    journal_bid: 1,
                    name: "a".repeat(rkyv::string::repr::INLINE_CAPACITY + 10), // out-of-line
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
            let archive_buf = heap_doc.to_archive();
            let doc_bytes = bytes::Bytes::from(archive_buf.to_vec());
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
            let embedded_doc = unsafe {
                let buffer = core::slice::from_raw_parts(
                    archive_buf.as_ptr() as *const doc::embedded::U64Le,
                    archive_buf.len() / 8,
                );
                doc::HeapEmbedded::from_buffer(buffer)
            };
            let block_doc = crate::log::block::BlockDoc {
                offset: 42,
                packed_key_prefix: [0xAB; 16],
                doc: embedded_doc,
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
