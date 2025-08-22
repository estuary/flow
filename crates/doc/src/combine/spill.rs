use super::{bump_mem_used, reduce, DrainedDoc, Error, HeapEntry, Meta, Spec, BUMP_THRESHOLD};
use crate::owned::OwnedArchivedNode;
use crate::{Extractor, HeapNode, LazyNode, OwnedHeapNode, OwnedNode};
use bumpalo::Bump;
use bytes::Buf;
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
        let mut raw_buf = rkyv::util::AlignedVec::<8>::with_capacity(2 * chunk_target_size);
        let mut arena = rkyv::ser::allocator::Arena::new();

        for (index, HeapEntry { meta, root }) in entries.iter().enumerate() {
            let offset = raw_buf.len();

            // This is a hot loop. A key optimization is that we're directly
            // serializing into `raw_buf` and re-using its storage each
            // iteration to avoid extra allocation.

            // Write meta header.
            raw_buf.extend_from_slice(&meta.to_bytes());
            // Reserve space for document size header.
            raw_buf.extend_from_slice(&[0; 4]);

            raw_buf = rkyv::api::low::to_bytes_in_with_alloc::<_, _, rkyv::rancor::Error>(
                root,
                raw_buf,
                arena.acquire(),
            )
            .expect("serialize of HeapNode to memory always succeeds");

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
    meta: Meta,
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
        let meta = Meta::from_bytes(chunk[0..4].try_into().unwrap());
        let doc_len = u32::from_le_bytes(chunk[4..8].try_into().unwrap()) as usize;
        chunk.advance(8); // Consume header.

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

        Ok((Self { meta, root }, rest))
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
        let mut raw_buf = rkyv::util::AlignedVec::<8>::with_capacity(raw_len as usize);
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
        let (l, r) = (&self.head, &other.head);

        // Order entries on (binding, key, !front, spill-order):
        // For each (binding, key), we take front() entries first, and then
        // take the Segment which was produced into the spill file first.
        // This maintains the left-to-right associative ordering of reductions.
        let binding = l.meta.binding().cmp(&r.meta.binding());
        binding
            .then_with(|| {
                Extractor::compare_key(&self.keys[l.meta.binding()], l.root.get(), r.root.get())
            })
            .then_with(|| l.meta.front().cmp(&r.meta.front()).reverse())
            .then_with(|| self.next.start.cmp(&other.next.start))
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
    alloc: Arc<Bump>, // Used for individual key reductions.
    heap: BinaryHeap<cmp::Reverse<Segment>>,
    in_group: bool,
    spec: Spec,
    spill: F,
}

// Safety: SpillDrainer is safe to Send because it wraps Bump with Arc,
// and we emit OwnedNodes that own a reference count to the Bump.
unsafe impl<F: io::Read + io::Seek> Send for SpillDrainer<F> {}

impl<F: io::Read + io::Seek> SpillDrainer<F> {
    pub fn drain_next(&mut self) -> Result<Option<DrainedDoc>, Error> {
        let Some(cmp::Reverse(segment)) = self.heap.pop() else {
            return Ok(None);
        };

        // Pop `segment`'s next Entry, and then re-heap it.
        let (entry, segment) = segment.pop_head(&mut self.spill)?;
        if let Some(segment) = segment {
            self.heap.push(cmp::Reverse(segment));
        }

        let Entry { mut meta, root } = entry;
        let is_full = self.spec.is_full[meta.binding()];
        let key = self.spec.keys[meta.binding()].as_ref();
        let &mut (ref mut validator, ref schema) = &mut self.spec.validators[meta.binding()];

        // `reduced` root which is updated as reductions occur.
        let mut reduced: Option<HeapNode<'_>> = None;

        // Attempt to reduce additional entries.
        while let Some(cmp::Reverse(next)) = self.heap.peek() {
            if meta.binding() != next.head.meta.binding()
                || !Extractor::compare_key(key, root.get(), next.head.root.get()).is_eq()
            {
                self.in_group = false;
                break;
            } else if !is_full && (!self.in_group || meta.not_associative()) {
                // We're performing associative reductions and:
                // * This is the first document of a group, which we cannot reduce into, or
                // * We've already attempted this associative reduction.
                self.in_group = true;
                break;
            }

            let rhs_valid = validator
                .validate(schema.as_ref(), next.head.root.get())
                .map_err(Error::SchemaError)?
                .ok()
                .map_err(|err| {
                    Error::FailedValidation(self.spec.names[next.head.meta.binding()].clone(), err)
                })?;

            match reduce::reduce::<crate::ArchivedNode>(
                match &reduced {
                    Some(root) => LazyNode::Heap(root),
                    None => LazyNode::Node(root.get()),
                },
                LazyNode::Node(next.head.root.get()),
                rhs_valid,
                &self.alloc,
                is_full,
            ) {
                Ok((node, deleted)) => {
                    meta.set_deleted(deleted);
                    reduced = Some(node);

                    // Discard the peeked entry, which was reduced into `reduced_root`.
                    let segment = self.heap.pop().unwrap().0;
                    let (_discard, segment) = segment.pop_head(&mut self.spill)?;
                    if let Some(segment) = segment {
                        self.heap.push(cmp::Reverse(segment));
                    }
                }
                Err(reduce::Error::NotAssociative) => {
                    meta.set_not_associative();
                    break;
                }
                Err(err) => return Err(Error::Reduction(err)),
            }
        }

        // Map `reduced` into an owned variant.
        let root = match reduced {
            None => {
                // `root` was spilled to disk and was not reduced again.
                // We validate !front() documents when spilling to disk and
                // can skip doing so now (this is the common case).
                if meta.front() {
                    validator
                        .validate(schema.as_ref(), root.get())
                        .map_err(Error::SchemaError)?
                        .ok()
                        .map_err(|err| {
                            Error::FailedValidation(self.spec.names[meta.binding()].clone(), err)
                        })?;
                }

                OwnedNode::Archived(root)
            }
            Some(reduced) => {
                // We built `reduced` via reduction and must re-validate it.
                validator
                    .validate(schema.as_ref(), &reduced)
                    .map_err(Error::SchemaError)?
                    .ok()
                    .map_err(|err| {
                        Error::FailedValidation(self.spec.names[meta.binding()].clone(), err)
                    })?;

                // Safety: we allocated `reduced` out of `self.alloc`.
                let reduced = unsafe { OwnedHeapNode::new(reduced, self.alloc.clone()) };

                // Safety: we must hold `alloc` constant for all reductions of a yielded entry.
                if bump_mem_used(&self.alloc) > BUMP_THRESHOLD {
                    self.alloc = Arc::new(Bump::new());
                }

                OwnedNode::Heap(reduced)
            }
        };

        Ok(Some(DrainedDoc { meta, root }))
    }
}

impl<F: io::Read + io::Seek> Iterator for SpillDrainer<F> {
    type Item = Result<DrainedDoc, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.drain_next().transpose()
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
            in_group: false,
            spec,
            spill,
        })
    }

    pub fn into_parts(self) -> (Spec, F) {
        let Self {
            alloc: _,
            heap: _,
            in_group: _,
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
        assert_eq!(ranges, vec![0..171]);

        insta::assert_snapshot!(to_hex(&spill.get_ref()), @r###"
        |5c000000 90000000 c0000000 00400000| \............@.. 00000000
        |006b6579 ff010070 08000000 6161610b| .key...p....aaa. 00000010
        |0010ff1c 0011760a 00031800 4370706c| ......v.....Cppl 00000020
        |65180090 06000000 ccffffff 0225007c| e............%.| 00000030
        |00000001 00008048 00306262 623b000d| .......H.0bbb;.. 00000040
        |48006262 616e616e 61180007 48005000| H.bbanana...H.P. 00000050
        |00000000 3f000000 48000000 c0020000| ....?...H....... 00000060
        |80400000 006b6579 ff010070 08000000| .@...key...p.... 00000070
        |6363630b 0061ff00 00000076 0a000318| ccc..a.....v.... 00000080
        |00526172 726f7418 00f00106 000000cc| .Rarrot......... 00000090
        |ffffff02 00000000 000000|            ...........      000000a0
                                                               000000ab
        "###);

        // Parse the region as a Segment.
        let mut segment = Segment::new(keys, &mut spill, ranges[0].clone()).unwrap();

        // First chunk has two documents.
        assert_eq!(segment.head.meta.binding(), 0);
        assert_eq!(segment.head.meta.front(), false);
        assert!(crate::compare(segment.head.root.get(), &fixture[0].1).is_eq());
        assert!(!segment.tail.is_empty());
        assert_eq!(segment.next, 100..171);

        let (_, next_segment) = segment.pop_head(&mut spill).unwrap();
        segment = next_segment.unwrap();

        assert_eq!(segment.head.meta.binding(), 1);
        assert_eq!(segment.head.meta.front(), true);
        assert!(crate::compare(segment.head.root.get(), &fixture[1].1).is_eq());
        assert!(segment.tail.is_empty()); // Chunk is empty.
        assert_eq!(segment.next, 100..171);

        // Next chunk is read and has one document.
        let (_, next_segment) = segment.pop_head(&mut spill).unwrap();
        segment = next_segment.unwrap();

        assert_eq!(segment.head.meta.binding(), 2);
        assert_eq!(segment.head.meta.front(), true);
        assert!(crate::compare(segment.head.root.get(), &fixture[2].1).is_eq());
        assert!(segment.tail.is_empty()); // Chunk is empty.
        assert_eq!(segment.next, 171..171);

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
                    true, // Full reduction.
                    vec![Extractor::with_default(
                        "/key",
                        &SerPolicy::noop(),
                        json!("def"),
                    )],
                    "source-name",
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
                    doc.meta.binding(),
                    serde_json::to_value(SerPolicy::noop().on_owned(&doc.root)).unwrap(),
                    doc.meta.front(),
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
                    true, // Full reduction.
                    vec![Extractor::new("/key", &SerPolicy::noop())],
                    "source-name",
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

        // "aaa" is front() & validated, and matches the schema.
        assert!(matches!(
            drainer.next().unwrap(),
            Ok(DrainedDoc { meta, .. }) if meta.front()
        ));
        // "bbb" doesn't match the schema but is !front() (we assume it was validated on spill).
        assert!(matches!(
            drainer.next().unwrap(),
            Ok(DrainedDoc { meta, .. }) if !meta.front()
        ));
        // "ccc" doesn't match the schema, is front(), and fails validation.
        assert!(matches!(
            drainer.next().unwrap(),
            Err(Error::FailedValidation(n, _)) if n == "source-name (binding 0)"
        ));
        // "ddd" has an invalid front() document, but is further reduced upon drain,
        // and the reduction output is itself valid.
        assert!(matches!(
            drainer.next().unwrap(),
            Ok(DrainedDoc { meta, .. }) if meta.front()
        ));
        // "eee" is reduced on drain, and its RHS doesn't match the schema.
        assert!(matches!(
            drainer.next().unwrap(),
            Err(Error::FailedValidation(n, _)) if n == "source-name (binding 0)"
        ));
        // Polling the iterator again pops the second "eee" document,
        // which was peeked but not yet popped during the prior failed validation.
        assert!(matches!(
            drainer.next().unwrap(),
            Ok(DrainedDoc { meta, .. }) if !meta.front()
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
            .map(|(binding, value, front)| HeapEntry {
                meta: Meta::new(*binding, *front),
                root: HeapNode::from_node(value, &alloc),
            })
            .collect()
    }
}
