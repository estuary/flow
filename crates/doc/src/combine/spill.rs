use super::{bump_mem_used, DrainedDoc, Error, HeapEntry, Spec, BUMP_THRESHOLD};
use crate::owned::OwnedArchivedNode;
use crate::{Extractor, HeapNode, LazyNode, OwnedHeapNode, OwnedNode};
use bumpalo::Bump;
use bytes::Buf;
use rkyv::ser::Serializer;
use std::collections::BinaryHeap;
use std::ops::Range;
use std::sync::Arc;
use std::{cmp, io};

/// SpillWriter writes segments of sorted documents to a spill file,
/// and tracks each of the written segment range offsets within the file.
pub struct SpillWriter<F: io::Read + io::Write + io::Seek> {
    ranges: Vec<Range<u64>>,
    spill: F,
}

impl<F: io::Read + io::Write + io::Seek> SpillWriter<F> {
    /// Build a SpillWriter around the given spill file.
    pub fn new(mut spill: F) -> Result<Self, std::io::Error> {
        assert_eq!(
            spill.seek(io::SeekFrom::Current(0))?,
            0,
            "expected file offset to be zero"
        );

        Ok(Self {
            ranges: Vec::new(),
            spill,
        })
    }

    /// Write a segment to the spill file. The segment array documents must
    /// already be in sorted key order. Documents will be grouped into chunks
    /// of the given size, and are then written in-order to the spill file.
    /// Each chunks is compressed using LZ4.
    /// The written size of the segment is returned.
    pub fn write_segment(
        &mut self,
        entries: &[HeapEntry<'_>],
        chunk_target_size: usize,
    ) -> Result<u64, io::Error> {
        if entries.is_empty() {
            return Ok(0);
        }

        let begin = self.spill.seek(io::SeekFrom::Current(0))?;

        let mut last_chunk_index = 0;
        let mut lz4_buf = Vec::new();
        let mut raw_buf = rkyv::AlignedVec::with_capacity(2 * chunk_target_size);
        let mut rkyv_scratch = Default::default();

        for (
            index,
            HeapEntry {
                binding,
                reduced,
                root,
            },
        ) in entries.iter().enumerate()
        {
            let offset = raw_buf.len();

            // Pack `reduced` into binding by setting its high bit.
            let binding = binding | if *reduced { 1 << 31 } else { 0 };
            // Write binding header.
            raw_buf.extend_from_slice(&u32::to_le_bytes(binding));
            // Reserve space for document size header.
            raw_buf.extend_from_slice(&[0; 4]);

            // Re-constitute an rkyv serializer around `raw_buf`.
            let mut wrapped_buf = rkyv::ser::serializers::AlignedSerializer::new(raw_buf);
            let mut rkyver = rkyv::ser::serializers::AllocSerializer::<8192>::new(
                wrapped_buf,
                rkyv_scratch,
                Default::default(), // We don't use shared smart pointers, so this is always empty.
            );

            _ = rkyver
                .serialize_value(root)
                .expect("serialize of HeapNode to memory always succeeds");

            // Disassemble `rkyver` to recover `raw_buf`.
            (wrapped_buf, rkyv_scratch, _) = rkyver.into_components();
            raw_buf = wrapped_buf.into_inner();

            // Update header with the final document length, excluding header.
            let doc_len = raw_buf.len() - offset - 8;
            raw_buf[offset + 4..offset + 8].copy_from_slice(&u32::to_le_bytes(doc_len as u32));

            // If this isn't the last element and our chunk is under threshold then continue accruing documents.
            if index != entries.len() - 1 && raw_buf.len() < chunk_target_size {
                continue;
            }
            // We have a complete chunk. Next we compress and write it to the spill file.

            // Prepare `lz4_buf` to hold the compressed result, reserving leading bytes for a chunk header.
            lz4_buf.reserve(8 + lz4::block::compress_bound(raw_buf.len())?);
            unsafe { lz4_buf.set_len(lz4_buf.capacity()) };

            // Compress the raw buffer, reserving the header.
            let n = lz4::block::compress_to_buffer(
                &raw_buf,
                Some(lz4::block::CompressionMode::DEFAULT),
                false,
                &mut lz4_buf[8..],
            )?;
            // Safety: lz4 will not write beyond our given slice.
            unsafe { lz4_buf.set_len(8 + n) };

            // Update the header with the raw and lz4'd chunk lengths, then send to writer.
            let lz4_len = u32::to_ne_bytes(lz4_buf.len() as u32 - 8);
            let raw_len = u32::to_ne_bytes(raw_buf.len() as u32);
            lz4_buf[0..4].copy_from_slice(&lz4_len);
            lz4_buf[4..8].copy_from_slice(&raw_len);

            self.spill.write_all(&lz4_buf)?;

            tracing::trace!(
                chunk_docs = %(1 + index - last_chunk_index),
                bytes_per_doc = (raw_buf.len() / (1 + index - last_chunk_index)),
                raw_len = %raw_buf.len(),
                lz4_len = %lz4_buf.len(),
                remaining = %(entries.len() - (1 + index)),
                "wrote chunk",
            );

            last_chunk_index = index;
            lz4_buf.clear();
            raw_buf.clear();
        }

        let end = self.spill.seek(io::SeekFrom::Current(0))?;
        self.ranges.push(begin..end);

        Ok(end - begin)
    }

    pub fn segment_ranges(&self) -> &[Range<u64>] {
        &self.ranges
    }

    /// Destructure the SpillWriter into its spill file and segment ranges.
    pub fn into_parts(self) -> (F, Vec<Range<u64>>) {
        let Self { spill, ranges } = self;
        (spill, ranges)
    }
}

// Entry is a parsed document entry of a spill file.
struct Entry {
    binding: u32,
    reduced: bool,
    root: OwnedArchivedNode,
}

impl Entry {
    // Parse the next Entry from a SpillWriter chunk.
    fn parse(mut chunk: bytes::Bytes) -> Result<(Self, bytes::Bytes), io::Error> {
        if chunk.len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "corrupt segment: chunk length is smaller than header: {}",
                    chunk.len()
                ),
            ));
        }

        // Parse entry header.
        let reduced_binding = u32::from_le_bytes(chunk[0..4].try_into().unwrap());
        let doc_len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
        chunk.advance(8); // Consume header.

        // Decompose `reduced_binding` into its parts.
        let reduced = reduced_binding & 1 << 31 != 0;
        let binding = reduced_binding & ((1 << 31) - 1);

        if chunk.len() < doc_len {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "corrupt segment: chunk is smaller than document length: {} vs {doc_len}",
                    chunk.len(),
                ),
            ));
        }

        let rest = chunk.split_off(doc_len);
        let root = unsafe { OwnedArchivedNode::new(chunk) };

        Ok((
            Self {
                binding,
                reduced,
                root,
            },
            rest,
        ))
    }
}

/// Segment is a segment region of a spill file which is being incrementally read.
/// Entries are written to the spill file in sorted order within a segment,
/// so this iterator-like object will yield entries in ascending order.
struct Segment {
    head: Entry,                   // Next Entry of Segment.
    keys: Arc<[Box<[Extractor]>]>, // Keys for comparing Entries across Segments.
    next: Range<u64>,              // Next chunk of this Segment.
    tail: bytes::Bytes,            // Remainder of the current chunk.
}

impl Segment {
    /// Build a new Segment covering the given range of the spill file.
    fn new<R: io::Read + io::Seek>(
        keys: Arc<[Box<[Extractor]>]>,
        r: &mut R,
        range: Range<u64>,
    ) -> Result<Self, io::Error> {
        assert_ne!(range.start, range.end);

        // Read chunk header.
        let mut header = [0, 0, 0, 0, 0, 0, 0, 0];
        r.seek(io::SeekFrom::Start(range.start))?;
        r.read_exact(&mut header)?;

        let lz4_len = u32::from_ne_bytes(header[0..4].try_into().unwrap()) as u64;
        let raw_len = u32::from_ne_bytes(header[4..8].try_into().unwrap()) as u64;

        // Compute implied next chunk range and ensure it remains valid.
        let next = range.start + 8 + lz4_len..range.end;
        if next.start > next.end {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("read header len {lz4_len} which is outside of region {next:?}"),
            ));
        }

        // Allocate and read compressed chunk into `lz4_buf`.
        // Safety: we're immediately reading into allocated memory, overwriting its uninitialized content.
        let mut lz4_buf = Vec::with_capacity(lz4_len as usize);
        unsafe { lz4_buf.set_len(lz4_len as usize) }
        r.read_exact(&mut lz4_buf)?;

        // Allocate and decompress into `raw_buf`.
        // Safety: we're immediately decompressing into allocated memory, overwriting its uninitialized content.
        let mut raw_buf = rkyv::AlignedVec::with_capacity(raw_len as usize);
        unsafe { raw_buf.set_len(raw_len as usize) }

        let decompressed_bytes =
            lz4::block::decompress_to_buffer(&lz4_buf, Some(raw_len as i32), &mut raw_buf)?;

        if decompressed_bytes != raw_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("corrupt segment: decompressed chunk bytes don't match the length encoded in the chunk header: {decompressed_bytes} vs {}", raw_buf.len()),
            ));
        }

        let chunk: bytes::Bytes = raw_buf.into_vec().into();
        let (head, tail) = Entry::parse(chunk)?;

        Ok(Self {
            head,
            keys,
            next,
            tail,
        })
    }

    // Pop the head Entry of the Segment, returning ownership to the caller,
    // as well as a Segment Option if any further Entries remain.
    fn pop_head<R: io::Read + io::Seek>(
        self,
        r: &mut R,
    ) -> Result<(Entry, Option<Self>), io::Error> {
        let Segment {
            head: popped,
            keys,
            next,
            tail,
        } = self;

        if !tail.is_empty() {
            let (head, tail) = Entry::parse(tail)?;

            Ok((
                popped,
                Some(Self {
                    head,
                    keys,
                    next,
                    tail,
                }),
            ))
        } else if !next.is_empty() {
            Ok((popped, Some(Self::new(keys, r, next)?)))
        } else {
            Ok((popped, None))
        }
    }
}

impl Ord for Segment {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        let (lhs, rhs) = (&self.head, &other.head);

        lhs.binding
            .cmp(&rhs.binding)
            .then_with(|| {
                Extractor::compare_key(
                    &self.keys[lhs.binding as usize],
                    lhs.root.get(),
                    rhs.root.get(),
                )
            })
            .then_with(||
                // When keys are equal than take the Segment which was produced into the spill file first.
                // This maintains the left-to-right associative ordering of reductions.
                self.next.start.cmp(&other.next.start))
    }
}
impl PartialOrd for Segment {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for Segment {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}
impl Eq for Segment {}

/// SpillDrainer drains documents across all segments of a spill file,
/// yielding drained entries (one per binding & key) in ascending order.
pub struct SpillDrainer<F: io::Read + io::Seek> {
    heap: BinaryHeap<cmp::Reverse<Segment>>,
    spec: Spec,
    spill: F,
    // Allocator for reductions, which is not referenced by other internal state of SpillDrainer.
    alloc: Arc<Bump>,
}

// Safety: SpillDrainer is safe to Send because its iterators never have lent references,
// and we're sending them and their backing Bump allocator together.
unsafe impl<F: io::Read + io::Seek> Send for SpillDrainer<F> {}

impl<F: io::Read + io::Seek> Iterator for SpillDrainer<F> {
    type Item = Result<DrainedDoc, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut inner = || {
            let Some(cmp::Reverse(cur_segment)) = self.heap.pop() else {
                return Ok(None);
            };

            let (
                Entry {
                    binding,
                    mut reduced,
                    root: owned_root,
                },
                cur_segment,
            ) = cur_segment.pop_head(&mut self.spill)?;

            let key = &self.spec.keys[binding as usize];
            let (validator, ref schema) = &mut self.spec.validators[binding as usize];

            // Reduced HeapNode which is updated as reductions occur.
            let mut root: Option<HeapNode<'_>> = None;

            // Poll the heap to find additional documents in other segments which share root's key.
            // Note that there can be at-most one instance of a key within a single segment,
            // so we don't need to re-heap `cur_segment` just yet.
            while matches!(self.heap.peek(), Some(cmp::Reverse(peek))
                if binding == peek.head.binding
                    && Extractor::compare_key(
                        key,
                        owned_root.get(),
                        peek.head.root.get()
                    ).is_eq())
            {
                let other_segment = self.heap.pop().unwrap().0;

                let (
                    Entry {
                        binding: _,
                        reduced: rhs_reduced,
                        root: rhs_root,
                    },
                    other_segment,
                ) = other_segment.pop_head(&mut self.spill)?;

                let smashed = super::smash(
                    &self.alloc,
                    match &root {
                        Some(root) => LazyNode::Heap(root),
                        None => LazyNode::Node(owned_root.get()),
                    },
                    reduced,
                    LazyNode::Node(rhs_root.get()),
                    rhs_reduced,
                    schema.as_ref(),
                    validator,
                )?;
                (root, reduced) = (Some(smashed.0), smashed.1);

                // Re-heap `other_segment`.
                if let Some(other) = other_segment {
                    self.heap.push(cmp::Reverse(other));
                }
            }

            // Re-heap `cur_segment`.
            if let Some(segment) = cur_segment {
                self.heap.push(cmp::Reverse(segment));
            }

            // Map `root` into an owned variant.
            let root = match root {
                None => {
                    // `owned_root` was spilled to disk and was not reduced again.
                    // We validate !reduced documents when spilling to disk and
                    // can skip doing so now (this is the common case).
                    if reduced {
                        validator
                            .validate(schema.as_ref(), owned_root.get())
                            .map_err(Error::SchemaError)?
                            .ok()
                            .map_err(Error::FailedValidation)?;
                    }

                    OwnedNode::Archived(owned_root)
                }
                Some(root) => {
                    // We built `root` via reduction and must re-validate it.
                    validator
                        .validate(schema.as_ref(), &root)
                        .map_err(Error::SchemaError)?
                        .ok()
                        .map_err(Error::FailedValidation)?;

                    // Safety: we allocated `root` out of `self.alloc`.
                    let root = unsafe { OwnedHeapNode::new(root, self.alloc.clone()) };

                    // Safety: we must hold `alloc` constant for all reductions of a yielded entry.
                    if bump_mem_used(&self.alloc) > BUMP_THRESHOLD {
                        self.alloc = Arc::new(Bump::new());
                    }

                    OwnedNode::Heap(root)
                }
            };

            Ok(Some(DrainedDoc {
                binding,
                reduced,
                root,
            }))
        };
        inner().transpose()
    }
}

impl<F: io::Read + io::Seek> SpillDrainer<F> {
    /// Build a new SpillDrainer which drains the given segment ranges previously
    /// written to the spill file.
    pub fn new(spec: Spec, mut spill: F, ranges: &[Range<u64>]) -> Result<Self, std::io::Error> {
        let mut heap = BinaryHeap::with_capacity(ranges.len());

        for range in ranges {
            let segment = Segment::new(spec.keys.clone(), &mut spill, range.clone())?;
            heap.push(cmp::Reverse(segment));
        }

        Ok(Self {
            alloc: Arc::new(Bump::new()),
            heap,
            spec,
            spill,
        })
    }

    pub fn into_parts(self) -> (Spec, F) {
        let Self {
            alloc: _,
            heap: _,
            spec,
            spill,
        } = self;
        (spec, spill)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        combine::CHUNK_TARGET_SIZE, validation::build_schema, HeapNode, SerPolicy, Validator,
    };
    use itertools::Itertools;
    use serde_json::{json, Value};

    #[test]
    fn test_spill_writes_to_segments() {
        let fixture = &[
            (0, json!({"key": "aaa", "v": "apple"}), false),
            (1, json!({"key": "bbb", "v": "banana"}), true),
            (2, json!({"key": "ccc", "v": "carrot"}), true),
        ];
        let alloc = Bump::new();
        let segment = segment_fixture(fixture, &alloc);
        // We're not using a SpillDrainer and are not comparing keys.
        let keys: Arc<[Box<[Extractor]>]> = Vec::new().into();

        // Write segment fixture into a SpillWriter.
        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();

        // 130 is calibrated to include two, but not three documents in a chunk.
        spill.write_segment(&segment, 130).unwrap();
        let (mut spill, ranges) = spill.into_parts();

        // Assert we wrote the expected range and regression fixture.
        assert_eq!(ranges, vec![0..186]);

        insta::assert_snapshot!(to_hex(&spill.get_ref()), @r###"
        |68000000 90000000 b0000000 00400000| h............@.. 00000000
        |006b6579 0b008103 08000000 6161610c| .key........aaa. 00000010
        |00000500 10760500 31000001 18007070| .....v..1.....pp 00000020
        |706c6500 00051100 90060000 00ccffff| ple............. 00000030
        |ff020d00 7c000000 01000080 48003062| ....|.......H.0b 00000040
        |62621c00 10030500 08480061 62616e61| bb.......H.abana 00000050
        |6e614300 01050003 48005000 00000000| naC.....H.P..... 00000060
        |42000000 48000000 f1080200 00804000| B...H.........@. 00000070
        |00006b65 79000000 00030800 00006363| ..key.........cc 00000080
        |630c0000 05001076 05003100 00011800| c......v..1..... 00000090
        |70617272 6f740006 11000005 00c0ccff| parrot.......... 000000a0
        |ffff0200 00000000 0000|              ..........       000000b0
                                                               000000ba
        "###);

        // Parse the region as a Segment.
        let mut segment = Segment::new(keys, &mut spill, ranges[0].clone()).unwrap();

        // First chunk has two documents.
        assert_eq!(segment.head.binding, 0);
        assert_eq!(segment.head.reduced, false);
        assert!(crate::compare(segment.head.root.get(), &fixture[0].1).is_eq());
        assert!(!segment.tail.is_empty());
        assert_eq!(segment.next, 112..186);

        let (_, next_segment) = segment.pop_head(&mut spill).unwrap();
        segment = next_segment.unwrap();

        assert_eq!(segment.head.binding, 1);
        assert_eq!(segment.head.reduced, true);
        assert!(crate::compare(segment.head.root.get(), &fixture[1].1).is_eq());
        assert!(segment.tail.is_empty()); // Chunk is empty.
        assert_eq!(segment.next, 112..186);

        // Next chunk is read and has one document.
        let (_, next_segment) = segment.pop_head(&mut spill).unwrap();
        segment = next_segment.unwrap();

        assert_eq!(segment.head.binding, 2);
        assert_eq!(segment.head.reduced, true);
        assert!(crate::compare(segment.head.root.get(), &fixture[2].1).is_eq());
        assert!(segment.tail.is_empty()); // Chunk is empty.
        assert_eq!(segment.next, 186..186);

        // Stepping the segment again consumes it, as no chunks remain.
        let (_, next_segment) = segment.pop_head(&mut spill).unwrap();
        assert!(next_segment.is_none());
    }

    #[test]
    fn test_heap_merge() {
        let spec = Spec::with_bindings(
            std::iter::repeat_with(|| {
                let schema = build_schema(
                    url::Url::parse("http://example/schema").unwrap(),
                    &json!({
                        "properties": {
                            "key": { "type": "string", "default": "def" },
                            "v": {
                                "type": "array",
                                "reduce": { "strategy": "append" }
                            }
                        },
                        "reduce": { "strategy": "merge" }
                    }),
                )
                .unwrap();

                (
                    vec![Extractor::with_default(
                        "/key",
                        &SerPolicy::default(),
                        json!("def"),
                    )],
                    None,
                    Validator::new(schema).unwrap(),
                )
            })
            .take(3),
        );

        let alloc = Bump::new();
        let fixtures = vec![
            segment_fixture(
                &[
                    (0, json!({"key": "aaa", "v": ["apple"]}), true),
                    (0, json!({"key": "bbb", "v": ["banana"]}), false),
                    (1, json!({"key": "ccc", "v": ["carrot"]}), false),
                ],
                &alloc,
            ),
            segment_fixture(
                &[
                    (0, json!({"key": "bbb", "v": ["avocado"]}), true),
                    (1, json!({"key": "bbb", "v": ["apricot"]}), true),
                    (1, json!({"key": "ccc", "v": ["raisin"]}), true),
                    (2, json!({"key": "ddd", "v": ["tomato"]}), true),
                ],
                &alloc,
            ),
            segment_fixture(
                &[
                    (1, json!({"key": "ccc", "v": ["dill"]}), false),
                    (2, json!({"key": "ddd", "v": ["pickle"]}), false),
                    (2, json!({"key": "eee", "v": ["squash"]}), false),
                ],
                &alloc,
            ),
        ];

        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
        for segment in fixtures {
            spill.write_segment(&segment, 2).unwrap();
        }

        // Map from SpillWriter => SpillDrainer.
        let (spill, ranges) = spill.into_parts();
        let drainer = SpillDrainer::new(spec, spill, &ranges).unwrap();

        let actual = drainer
            .map_ok(|doc| {
                (
                    doc.binding,
                    serde_json::to_value(SerPolicy::default().on_owned(&doc.root)).unwrap(),
                    doc.reduced,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        insta::assert_json_snapshot!(actual, @r###"
        [
          [
            0,
            {
              "key": "aaa",
              "v": [
                "apple"
              ]
            },
            true
          ],
          [
            0,
            {
              "key": "bbb",
              "v": [
                "avocado",
                "banana"
              ]
            },
            true
          ],
          [
            1,
            {
              "key": "bbb",
              "v": [
                "apricot"
              ]
            },
            true
          ],
          [
            1,
            {
              "key": "ccc",
              "v": [
                "raisin",
                "carrot",
                "dill"
              ]
            },
            true
          ],
          [
            2,
            {
              "key": "ddd",
              "v": [
                "tomato",
                "pickle"
              ]
            },
            true
          ],
          [
            2,
            {
              "key": "eee",
              "v": [
                "squash"
              ]
            },
            false
          ]
        ]
        "###);
    }

    #[test]
    fn test_drain_validation() {
        let spec = Spec::with_bindings(
            std::iter::repeat_with(|| {
                let schema = build_schema(
                    url::Url::parse("http://example/schema").unwrap(),
                    &json!({
                        "properties": {
                            "key": { "type": "string" },
                            "v": { "const": "good" },
                        }
                    }),
                )
                .unwrap();

                (
                    vec![Extractor::new("/key", &SerPolicy::default())],
                    None,
                    Validator::new(schema).unwrap(),
                )
            })
            .take(1),
        );

        let alloc = Bump::new();

        let fixtures = vec![
            segment_fixture(
                &[
                    (0, json!({"key": "aaa", "v": "good"}), true),
                    (0, json!({"key": "bbb", "v": "bad"}), false),
                    (0, json!({"key": "ccc", "v": "bad"}), true),
                    (0, json!({"key": "ddd", "v": "bad"}), true),
                    (0, json!({"key": "eee", "v": "good"}), true),
                ],
                &alloc,
            ),
            segment_fixture(
                &[
                    (0, json!({"key": "ddd", "v": "good"}), false),
                    (0, json!({"key": "eee", "v": "bad"}), false),
                ],
                &alloc,
            ),
        ];

        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
        for segment in fixtures {
            spill.write_segment(&segment, CHUNK_TARGET_SIZE).unwrap();
        }
        let (spill, ranges) = spill.into_parts();
        let mut drainer = SpillDrainer::new(spec, spill, &ranges).unwrap();

        // "aaa" is reduced & validated, and matches the schema.
        assert!(matches!(
            drainer.next().unwrap(),
            Ok(DrainedDoc { reduced: true, .. })
        ));
        // "bbb" doesn't match the schema but is marked as !reduced (we assume it was validated on spill).
        assert!(matches!(
            drainer.next().unwrap(),
            Ok(DrainedDoc { reduced: false, .. })
        ));
        // "ccc" doesn't match the schema, is marked reduced, and fails validation.
        assert!(matches!(
            drainer.next().unwrap(),
            Err(Error::FailedValidation(_))
        ));
        // "ddd" has an invalid reduced document, but is further reduced upon drain,
        // and the reduction output is itself valid.
        assert!(matches!(
            drainer.next().unwrap(),
            Ok(DrainedDoc { reduced: true, .. })
        ));
        // "eee" is reduced on drain, and its output doesn't match the schema.
        assert!(matches!(
            drainer.next().unwrap(),
            Err(Error::FailedValidation(_))
        ));

        assert!(drainer.next().is_none());
    }

    #[test]
    fn test_bumpalo_chunk_capacity() {
        let alloc = bumpalo::Bump::with_capacity(1 << 15);
        assert_eq!(alloc.chunk_capacity(), 36800);

        // Allocation which fits within the current chunk.
        let s = alloc.alloc_str("hello world");

        // Expect chunk capacity is lower than before, because of the allocation.
        assert_eq!(alloc.chunk_capacity(), 36800 - s.len());
    }

    fn to_hex(b: &[u8]) -> String {
        hexdump::hexdump_iter(b)
            .map(|line| format!("{line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn segment_fixture<'alloc>(
        fixture: &[(u32, Value, bool)],
        alloc: &'alloc bumpalo::Bump,
    ) -> Vec<HeapEntry<'alloc>> {
        fixture
            .into_iter()
            .map(|(binding, value, reduced)| HeapEntry {
                binding: *binding,
                reduced: *reduced,
                root: HeapNode::from_node(value, &alloc),
            })
            .collect()
    }
}
