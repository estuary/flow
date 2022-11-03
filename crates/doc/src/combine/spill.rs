use super::{Error, REDUCED_FLAG, REVALIDATE_FLAG};
use crate::{
    reduce,
    validation::{Validation, Validator},
    ArchivedDoc, ArchivedNode, AsNode, HeapDoc, HeapNode, LazyNode, Pointer,
};
use itertools::Itertools;
use rkyv::ser::Serializer;
use std::collections::BinaryHeap;
use std::io;
use std::ops::Range;
use std::{cmp, rc::Rc};

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

    /// Return the target number of documents-per-chunk such that each chunk
    /// is no more than about a megabyte in size.
    pub fn target_docs_per_chunk(alloc: &bumpalo::Bump, docs: usize) -> usize {
        const TARGET_SIZE: usize = 1 << 20; // 1MB.

        if docs == 0 {
            return 1;
        }

        let bytes_per_doc = alloc.allocated_bytes() / docs;
        (bytes_per_doc + TARGET_SIZE - 1) / TARGET_SIZE
    }

    /// Write a segment to the spill file. The segment iterator must yield
    /// documents in sorted key order. Documents will be grouped into chunks
    /// of the given size, and are then written in-order to the spill file.
    /// Each chunks is compressed using LZ4.
    /// The written size of the segment is returned.
    pub fn write_segment<'alloc, S>(
        &mut self,
        segment: S,
        docs_per_chunk: usize,
    ) -> Result<u64, io::Error>
    where
        S: Iterator<Item = HeapDoc<'alloc>>,
    {
        let chunks = segment.chunks(docs_per_chunk);

        let begin = self.spill.seek(io::SeekFrom::Current(0))?;
        let mut ser = rkyv::ser::serializers::AllocSerializer::<8192>::default();
        let mut lz4_buf = Vec::new();

        for chunk in chunks.into_iter() {
            let chunk = chunk.collect::<Vec<_>>();

            // Archive chunk into uncompressed "raw" buffer.
            ser.serialize_value(&chunk)
                .expect("serialize of HeapDoc to memory always succeeds");
            let (raw_buf, scratch, _shared) = ser.into_components();
            let mut raw_buf = raw_buf.into_inner();

            // Prepare a buffer to hold the compressed result, reserving the leading eight bytes for the chunk header.
            lz4_buf.clear();
            lz4_buf.reserve(8 + lz4::block::compress_bound(raw_buf.len())?);
            unsafe { lz4_buf.set_len(lz4_buf.capacity()) };

            // Compress the raw buffer, reserving the header.
            let n = lz4::block::compress_to_buffer(
                &raw_buf,
                Some(lz4::block::CompressionMode::DEFAULT),
                false,
                &mut lz4_buf[8..],
            )?;
            unsafe { lz4_buf.set_len(8 + n) }; // Safety: lz4 will not write beyond our given slice.

            // Update the header with the raw and lz4'd chunk lengths, then send to writer.
            let lz4_len = u32::to_ne_bytes(lz4_buf.len() as u32 - 8);
            let raw_len = u32::to_ne_bytes(raw_buf.len() as u32);
            lz4_buf[0..4].copy_from_slice(&lz4_len);
            lz4_buf[4..8].copy_from_slice(&raw_len);

            self.spill.write_all(&lz4_buf)?;

            // Re-compose the `rkyv` serializer, preserving allocated capacity.
            // rkyv::SharedSerializeMap doesn't provide an API to reset while preserving its allocations.
            raw_buf.clear();

            ser = rkyv::ser::serializers::CompositeSerializer::new(
                rkyv::ser::serializers::AlignedSerializer::new(raw_buf),
                scratch,
                Default::default(),
            );
        }

        let end = self.spill.seek(io::SeekFrom::Current(0))?;

        // Ignore segments which are empty.
        if begin != end {
            self.ranges.push(begin..end);
        }

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

/// Segment is a segment region of a spill file which is being incrementally read.
/// As documents are written to the spill file in sorted order within a segment,
/// this iterator-like object will also yield documents in ascending key order.
pub struct Segment {
    _backing: rkyv::AlignedVec,
    docs: &'static [ArchivedDoc],
    key: Rc<[Pointer]>,
    next: Range<u64>,
}

impl Segment {
    /// Build a new Segment covering the given range of the spill file.
    /// The given AlignedVec buffer, which may have pre-allocated capacity,
    /// is used to back the archived documents read from the spill file.
    pub fn new<R: io::Read + io::Seek>(
        key: Rc<[Pointer]>,
        r: &mut R,
        range: Range<u64>,
        mut backing: rkyv::AlignedVec,
        lz4_buf: &mut Vec<u8>,
    ) -> Result<Self, io::Error> {
        assert_ne!(range.start, range.end);
        lz4_buf.clear();
        backing.clear();

        // Read chunk header.
        let mut header = [0, 0, 0, 0, 0, 0, 0, 0];
        r.seek(io::SeekFrom::Start(range.start))?;
        r.read_exact(&mut header)?;

        let lz4_len = u32::from_ne_bytes(header[0..4].try_into().unwrap()) as u64;
        let raw_len = u32::from_ne_bytes(header[4..8].try_into().unwrap()) as u64;

        // Compute implied next chunk range and ensure it remains valid.
        let next = range.start + 8 + lz4_len..range.end;
        assert!(
            next.start <= next.end,
            "read header len {lz4_len} which is outside of region {next:?}"
        );

        // Allocate and read compressed chunk into `tmp`.
        // Safety: we're immediately reading into allocated memory, overwriting its uninitialized content.
        lz4_buf.reserve(lz4_len as usize);
        unsafe { lz4_buf.set_len(lz4_len as usize) }
        r.read_exact(lz4_buf)?;

        // Allocate and decompress into `backing`.
        // Safety: we're immediately decompressing into allocated memory, overwriting its uninitialized content.
        backing.reserve(raw_len as usize);
        unsafe { backing.set_len(raw_len as usize) }

        assert_eq!(
            lz4::block::decompress_to_buffer(&lz4_buf, Some(raw_len as i32), &mut backing)?,
            backing.len(),
            "bytes actually decompressed don't match the length encoded in the chunk header"
        );

        // Cast `backing` into its archived type.
        let docs = unsafe { rkyv::archived_root::<Vec<HeapDoc>>(&backing) };

        // Transmute from the implied Self lifetime of backing to &'static lifetime.
        // Safety: Segment is a guard which maintains the lifetime of `backing`
        // alongside its borrowed `docs` reference.
        let docs: &[ArchivedDoc] = unsafe { std::mem::transmute(docs.as_slice()) };
        assert_ne!(docs.len(), 0);

        Ok(Self {
            _backing: backing,
            docs,
            key,
            next,
        })
    }

    /// Head is the next ordered document of the Segment.
    /// Despite the static lifetime, the head() document cannot be referenced
    /// after a subsequent call to next().
    pub fn head(&self) -> &'static ArchivedDoc {
        &self.docs[0]
    }

    /// Next is called after the current head() has been fully processed.
    /// It is unsafe to access a previous head() after calling next().
    /// If no more documents remain in the Segment then Ok(None) is returned.
    pub fn next<R: io::Read + io::Seek>(
        self,
        r: &mut R,
        tmp: &mut Vec<u8>,
    ) -> Result<Option<Self>, io::Error> {
        let Segment {
            _backing: backing,
            docs,
            key,
            next,
        } = self;

        if docs.len() != 1 {
            Ok(Some(Segment {
                _backing: backing,
                docs: &docs[1..],
                key,
                next,
            }))
        } else if !next.is_empty() {
            Ok(Some(Self::new(key, r, next, backing, tmp)?))
        } else {
            Ok(None)
        }
    }
}

impl Ord for Segment {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        Pointer::compare(&self.key, &self.docs[0].root, &other.docs[0].root).then(
            // When keys are equal than take the Segment which was produced into the spill file first.
            // This maintains the left-to-right associative ordering of reductions.
            self.next.start.cmp(&other.next.start),
        )
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
/// yielding one document per key in ascending order.
pub struct SpillDrainer<F: io::Read + io::Write + io::Seek> {
    heap: BinaryHeap<cmp::Reverse<Segment>>,
    key: Rc<[Pointer]>,
    schema: url::Url,
    spill: F,
    tmp: Vec<u8>,
}

impl<F: io::Read + io::Write + io::Seek> SpillDrainer<F> {
    /// Build a new SpillDrainer which drains the given segment ranges previously
    /// written to the spill file.
    pub fn new(
        key: Rc<[Pointer]>,
        schema: url::Url,
        mut spill: F,
        ranges: &[Range<u64>],
    ) -> Result<Self, std::io::Error> {
        let mut heap = BinaryHeap::with_capacity(ranges.len());
        let mut tmp = Vec::new();

        for range in ranges {
            let segment = Segment::new(
                key.clone(),
                &mut spill,
                range.clone(),
                Default::default(),
                &mut tmp,
            )?;
            heap.push(cmp::Reverse(segment));
        }

        Ok(Self {
            heap,
            key,
            schema,
            spill,
            tmp,
        })
    }

    pub fn into_parts(self) -> (Rc<[Pointer]>, url::Url, F) {
        let Self {
            heap: _,
            key,
            schema,
            spill,
            tmp: _,
        } = self;
        (key, schema, spill)
    }

    /// Drain documents from this SpillDrainer by invoking the given callback.
    /// Documents passed to the callback MUST NOT be accessed after it returns.
    /// The callback returns true if it would like to be called further, or false
    /// if a present call to drain_while() should return, yielding back to the caller.
    ///
    /// A future call to drain_while() can then resume the drain operation at
    /// its next ordered document. drain_while() returns true while documents
    /// remain to drain, and false only after all documents have been drained.
    pub fn drain_while<C, CE>(
        &mut self,
        validator: &mut Validator,
        mut callback: C,
    ) -> Result<bool, CE>
    where
        C: for<'alloc> FnMut(LazyNode<'alloc, 'static, ArchivedNode>, bool) -> Result<bool, CE>,
        CE: From<Error>,
    {
        while let Some(cmp::Reverse(segment)) = self.heap.pop() {
            let alloc = HeapNode::new_allocator();

            let mut root = LazyNode::Node(&segment.head().root);
            let mut reduced = segment.head().flags & REDUCED_FLAG != 0;
            let mut revalidate = segment.head().flags & REVALIDATE_FLAG != 0;

            // Poll the heap to find additional documents in other segments which share root's key.
            // Note that there can be at-most one instance of a key within a single segment,
            // so we don't need to also check `segment`.
            while let Some(cmp::Reverse(other)) = self.heap.peek() {
                let other = match root.compare(&segment.key, &LazyNode::Node(&other.head().root)) {
                    cmp::Ordering::Less => break,
                    cmp::Ordering::Equal => self.heap.pop().unwrap().0,
                    cmp::Ordering::Greater => unreachable!("root and other are mis-ordered"),
                };

                (root, reduced) = match (
                    root,
                    reduced,
                    &other.head().root,
                    other.head().flags & REDUCED_FLAG != 0,
                ) {
                    // `segment` is a RHS which is being combined or reduced into `other`'s LHS.
                    (lhs, reduced, rhs, false) => {
                        let rhs_valid = Validation::validate(validator, &self.schema, rhs)
                            .map_err(Error::SchemaError)?
                            .ok()
                            .map_err(Error::PreReduceValidation)?;

                        (
                            LazyNode::Heap(
                                reduce::reduce(
                                    lhs,
                                    LazyNode::Node(rhs),
                                    rhs_valid,
                                    &alloc,
                                    reduced,
                                )
                                .map_err(Error::Reduction)?,
                            ),
                            reduced,
                        )
                    }
                    // `segment` is a reduced LHS which is being combined with `other`'s RHS.
                    (rhs, false, lhs, true) => {
                        let rhs_valid = rhs
                            .validate_ok(validator, &self.schema)
                            .map_err(Error::SchemaError)?
                            .map_err(Error::PreReduceValidation)?;

                        (
                            LazyNode::Heap(
                                reduce::reduce(LazyNode::Node(lhs), rhs, rhs_valid, &alloc, true)
                                    .map_err(Error::Reduction)?,
                            ),
                            true,
                        )
                    }
                    (_lhs, true, rhs, true) => {
                        return Err(Error::AlreadyFullyReduced(
                            serde_json::to_value(rhs.as_node()).unwrap(),
                        )
                        .into())
                    }
                };
                revalidate = true;

                if let Some(other) = other
                    .next(&mut self.spill, &mut self.tmp)
                    .map_err(Error::SpillIO)?
                {
                    self.heap.push(cmp::Reverse(other));
                }
            }

            if revalidate {
                // We've reduced multiple documents into this one.
                // Ensure it remains valid to its schema.
                root.validate_ok(validator, &self.schema)
                    .map_err(Error::SchemaError)?
                    .map_err(Error::PostReduceValidation)?;
            }

            let done = !callback(root, reduced)?;

            if let Some(segment) = segment
                .next(&mut self.spill, &mut self.tmp)
                .map_err(Error::SpillIO)?
            {
                self.heap.push(cmp::Reverse(segment));
            }

            if done {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    use crate::{Schema, Validator};
    use json::schema::{build::build_schema, index::IndexBuilder};

    #[test]
    fn test_spill_writes_to_segments() {
        let fixtures = vec![
            (json!({"key": "aaa", "v": "apple"}), 0),
            (json!({"key": "bbb", "v": "banana"}), REDUCED_FLAG),
            (
                json!({"key": "ccc", "v": "carrot"}),
                REDUCED_FLAG | REVALIDATE_FLAG,
            ),
        ];

        // Write fixtures into a SpillWriter.
        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
        let alloc = HeapNode::new_allocator();

        spill
            .write_segment(
                fixtures.into_iter().map(|(value, flags)| HeapDoc {
                    root: HeapNode::from_node(value.as_node(), &alloc),
                    flags,
                }),
                2,
            )
            .unwrap();
        let (mut spill, ranges) = spill.into_parts();

        // Assert we wrote the expected range and regression fixture.
        assert_eq!(ranges, vec![0..189]);

        insta::assert_snapshot!(to_hex(&spill.get_ref()), @r###"
        |67000000 98000000 f1006b65 79000000| g.........key... 00000000
        |00030800 00006161 610c0000 05001076| ......aaa......v 00000010
        |05003100 00011800 7070706c 65000005| ..1.....ppple... 00000020
        |11000830 00306262 62130010 03050008| ...0.0bbb....... 00000030
        |30008062 616e616e 61000618 00000500| 0..banana....... 00000040
        |509cffff ff020d00 07020000 180017b4| P............... 00000050
        |18001301 1c0080d0 ffffff02 00000046| ...............F 00000060
        |00000050 000000f1 006b6579 00000000| ...P.....key.... 00000070
        |03080000 00636363 0c000005 00107605| .....ccc......v. 00000080
        |00310000 01180070 6172726f 74000611| .1.....parrot... 00000090
        |00000500 50ccffff ff020d00 00390001| ....P........9.. 000000a0
        |0600a000 00e8ffff ff010000 00|       .............    000000b0
                                                               000000bd
        "###);

        // Parse the region as a Segment.
        let key: Rc<[Pointer]> = vec![Pointer::from_str("/key")].into();
        let mut tmp = Vec::new();
        let mut actual = Vec::new();
        let mut segment = Segment::new(
            key,
            &mut spill,
            ranges[0].clone(),
            Default::default(),
            &mut tmp,
        )
        .unwrap();

        // First chunk has two documents.
        assert_eq!(segment.docs.len(), 2);
        assert_eq!(segment._backing.len(), 152);
        assert_eq!(segment.next, 111..189);

        actual.push(serde_json::to_value(&segment.head().root.as_node()).unwrap());

        segment = segment.next(&mut spill, &mut tmp).unwrap().unwrap();
        actual.push(serde_json::to_value(&segment.head().root.as_node()).unwrap());

        // Next chunk is read and has one document.
        segment = segment.next(&mut spill, &mut tmp).unwrap().unwrap();

        assert_eq!(segment.docs.len(), 1);
        assert_eq!(segment._backing.len(), 80);
        assert_eq!(segment.next, 189..189);

        actual.push(serde_json::to_value(&segment.head().root.as_node()).unwrap());

        // Stepping the segment again consumes it, as no chunks remain.
        assert!(segment.next(&mut spill, &mut tmp).unwrap().is_none());

        insta::assert_json_snapshot!(actual, @r###"
        [
          {
            "key": "aaa",
            "v": "apple"
          },
          {
            "key": "bbb",
            "v": "banana"
          },
          {
            "key": "ccc",
            "v": "carrot"
          }
        ]
        "###);
    }

    #[test]
    fn test_heap_merge() {
        let schema = json!({
            "properties": {
                "key": { "type": "string" },
                "v": {
                    "type": "array",
                    "reduce": { "strategy": "append" }
                }
            },
            "reduce": { "strategy": "merge" }
        });
        let key: Rc<[Pointer]> = vec![Pointer::from_str("/key")].into();
        let curi = url::Url::parse("http://example/schema").unwrap();
        let schema: Schema = build_schema(curi.clone(), &schema).unwrap();

        let mut index = IndexBuilder::new();
        index.add(&schema).unwrap();
        index.verify_references().unwrap();
        let index = index.into_index();

        let fixtures = vec![
            vec![
                (json!({"key": "aaa", "v": ["apple"]}), REDUCED_FLAG),
                (json!({"key": "bbb", "v": ["banana"]}), 0),
                (json!({"key": "ccc", "v": ["carrot"]}), 0),
            ],
            vec![
                (json!({"key": "bbb", "v": ["avocado"]}), REDUCED_FLAG),
                (json!({"key": "ccc", "v": ["raisin"]}), REDUCED_FLAG),
                (json!({"key": "ddd", "v": ["tomato"]}), REDUCED_FLAG),
            ],
            vec![
                (json!({"key": "ccc", "v": ["dill"]}), 0),
                (json!({"key": "ddd", "v": ["pickle"]}), 0),
                (json!({"key": "eee", "v": ["squash"]}), 0),
            ],
        ];

        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();

        for segment in fixtures {
            let alloc = HeapNode::new_allocator();

            spill
                .write_segment(
                    segment.into_iter().map(|(value, flags)| HeapDoc {
                        root: HeapNode::from_node(value.as_node(), &alloc),
                        flags,
                    }),
                    2,
                )
                .unwrap();
        }

        // Map from SpillWriter => SpillDrainer.
        let (spill, ranges) = spill.into_parts();
        let mut drainer = SpillDrainer::new(key, curi, spill, &ranges).unwrap();

        let mut validator = Validator::new(&index);
        let mut actual = Vec::new();

        loop {
            if !drainer
                .drain_while(&mut validator, |node, full| {
                    let node = serde_json::to_value(&node).unwrap();

                    actual.push((node, full));
                    Ok::<_, Error>(actual.len() % 2 != 0)
                })
                .unwrap()
            {
                break;
            }
        }

        insta::assert_json_snapshot!(actual, @r###"
        [
          [
            {
              "key": "aaa",
              "v": [
                "apple"
              ]
            },
            true
          ],
          [
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
    fn test_bumpalo_chunk_capacity() {
        let alloc = bumpalo::Bump::with_capacity(1 << 15);
        assert_eq!(alloc.chunk_capacity(), 36800);

        // Allocation which fits within the current chunk.
        alloc.alloc_str("hello world");

        // Expect chunk capacity is lower than before, because of the allocation.
        // TODO(johnny): This is broken currently.
        // Filed issue: https://github.com/fitzgen/bumpalo/issues/185
        // I'm leaving this test in to identify when it's fixed,
        // because we'll want to update our MemTable spill logic to reflect the new behavior.
        assert_eq!(alloc.chunk_capacity(), 36800); // <- Should be assert_ne!().
    }

    fn to_hex(b: &[u8]) -> String {
        hexdump::hexdump_iter(b)
            .map(|line| format!("{line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}
