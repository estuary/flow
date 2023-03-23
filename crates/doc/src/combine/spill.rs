use super::{Error, FLAG_REDUCED};
use crate::{
    validation::Validator, ArchivedDoc, ArchivedNode, HeapDoc, HeapNode, LazyNode, Pointer,
};
use rkyv::ser::Serializer;
use std::cmp;
use std::collections::BinaryHeap;
use std::io;
use std::ops::Range;

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

    /// Write a segment to the spill file. The segment iterator must yield
    /// documents in sorted key order. Documents will be grouped into chunks
    /// of the given size, and are then written in-order to the spill file.
    /// Each chunks is compressed using LZ4.
    /// The written size of the segment is returned.
    pub fn write_segment<'alloc>(
        &mut self,
        mut segment: &[HeapDoc<'alloc>],
        chunk_target_size: Range<usize>,
    ) -> Result<u64, io::Error> {
        if segment.is_empty() {
            return Ok(0);
        }
        // Estimate an initial bytes per document from the first document. This
        // can change as we process the segment - consider a grouping key over
        // a user's union type that bakes in a "kind of document" component.
        let mut last_bytes_per_doc = segment[0].root.to_archive().len();

        let begin = self.spill.seek(io::SeekFrom::Current(0))?;
        let mut ser = rkyv::ser::serializers::AllocSerializer::<8192>::default();
        let mut lz4_buf = Vec::new();

        while !segment.is_empty() {
            // Project `last_bytes_per_doc` into a number of documents for this chunk,
            // in order to achieve an archived size equal to `chunk_target_size.begin`.
            // It can't be larger than the remaining documents, and can't be smaller than one.
            let chunk_docs = cmp::min(
                cmp::max(1, chunk_target_size.start / last_bytes_per_doc),
                segment.len(),
            );

            // Archive chunk into uncompressed "raw" buffer.
            ser.serialize_unsized_value(&segment[..chunk_docs])
                .expect("serialize of HeapDoc to memory always succeeds");
            let (raw_buf, scratch, _shared) = ser.into_components();
            let mut raw_buf = raw_buf.into_inner();

            let cur_bytes_per_doc = raw_buf.len() / chunk_docs;

            // If `raw_buf` is outside of our upper `chunk_target_size` range
            // and it's possible to make it smaller, then do so. This check lets
            // us bound how much a reader must keep in memory when later
            // processing a current chunk across all segments.
            if chunk_docs > 1 && raw_buf.len() > chunk_target_size.end {
                tracing::debug!(
                    chunk_docs,
                    %cur_bytes_per_doc,
                    %last_bytes_per_doc,
                    raw_buf_len = %raw_buf.len(),
                    "archived buffer is too large (trying again)",
                );

                // We allocated an over-large `raw_buf`: don't re-use it.
                // This should be rare.
                ser = Default::default();

                // By construction, `cur_bytes_per_doc` must be larger that before.
                // Otherwise we wouldn't be here. Update and try again.
                assert!(cur_bytes_per_doc > last_bytes_per_doc);
                last_bytes_per_doc = cur_bytes_per_doc;
                continue;
            }

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
            // Safety: lz4 will not write beyond our given slice.
            unsafe { lz4_buf.set_len(8 + n) };

            // Update the header with the raw and lz4'd chunk lengths, then send to writer.
            let lz4_len = u32::to_ne_bytes(lz4_buf.len() as u32 - 8);
            let raw_len = u32::to_ne_bytes(raw_buf.len() as u32);
            lz4_buf[0..4].copy_from_slice(&lz4_len);
            lz4_buf[4..8].copy_from_slice(&raw_len);

            self.spill.write_all(&lz4_buf)?;

            tracing::trace!(
                chunk_docs,
                bytes_per_doc = %cur_bytes_per_doc,
                raw_len = %raw_buf.len(),
                lz4_len = %lz4_buf.len(),
                remaining = %(segment.len() - chunk_docs),
                "wrote chunk",
            );

            // Re-compose the `rkyv` serializer, preserving allocated capacity of `raw_buf`.
            raw_buf.clear();
            ser = rkyv::ser::serializers::CompositeSerializer::new(
                rkyv::ser::serializers::AlignedSerializer::new(raw_buf),
                scratch,
                Default::default(),
            );

            last_bytes_per_doc = cur_bytes_per_doc;
            segment = &segment[chunk_docs..];
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

/// Segment is a segment region of a spill file which is being incrementally read.
/// As documents are written to the spill file in sorted order within a segment,
/// this iterator-like object will also yield documents in ascending key order.
pub struct Segment {
    docs: &'static [ArchivedDoc],
    key: Box<[Pointer]>,
    next: Range<u64>,
    zz_backing: rkyv::AlignedVec,
}

impl Segment {
    /// Build a new Segment covering the given range of the spill file.
    /// The given AlignedVec buffer, which may have pre-allocated capacity,
    /// is used to back the archived documents read from the spill file.
    pub fn new<R: io::Read + io::Seek>(
        key: Box<[Pointer]>,
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
        assert!(
            next.start <= next.end,
            "read header len {lz4_len} which is outside of region {next:?}"
        );

        // Allocate and read compressed chunk into `lz4_buf`.
        // Safety: we're immediately reading into allocated memory, overwriting its uninitialized content.
        let mut lz4_buf = Vec::with_capacity(lz4_len as usize);
        unsafe { lz4_buf.set_len(lz4_len as usize) }
        r.read_exact(&mut lz4_buf)?;

        // Allocate and decompress into `backing`.
        // Safety: we're immediately decompressing into allocated memory, overwriting its uninitialized content.
        let mut backing = rkyv::AlignedVec::with_capacity(raw_len as usize);
        unsafe { backing.set_len(raw_len as usize) }

        assert_eq!(
            lz4::block::decompress_to_buffer(&lz4_buf, Some(raw_len as i32), &mut backing)?,
            backing.len(),
            "bytes actually decompressed don't match the length encoded in the chunk header"
        );

        // Cast `backing` into its archived type.
        let docs = unsafe { rkyv::archived_unsized_root::<[HeapDoc]>(&backing) };

        // Transmute from the implied Self lifetime of backing to &'static lifetime.
        // Safety: Segment is a guard which maintains the lifetime of `backing`
        // alongside its borrowed `docs` reference.
        let docs: &[ArchivedDoc] = unsafe { std::mem::transmute(docs) };
        assert_ne!(docs.len(), 0);

        Ok(Self {
            zz_backing: backing,
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
    pub fn next<R: io::Read + io::Seek>(self, r: &mut R) -> Result<Option<Self>, io::Error> {
        let Segment {
            docs,
            key,
            next,
            zz_backing,
        } = self;

        if docs.len() != 1 {
            Ok(Some(Segment {
                docs: &docs[1..],
                key,
                next,
                zz_backing,
            }))
        } else if !next.is_empty() {
            Ok(Some(Self::new(key, r, next)?))
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
    key: Box<[Pointer]>,
    schema: Option<url::Url>,
    spill: F,
    validator: Validator,
}

impl<F: io::Read + io::Write + io::Seek> SpillDrainer<F> {
    /// Build a new SpillDrainer which drains the given segment ranges previously
    /// written to the spill file.
    pub fn new(
        key: Box<[Pointer]>,
        schema: Option<url::Url>,
        mut spill: F,
        ranges: &[Range<u64>],
        validator: Validator,
    ) -> Result<Self, std::io::Error> {
        let mut heap = BinaryHeap::with_capacity(ranges.len());

        for range in ranges {
            let segment = Segment::new(key.clone(), &mut spill, range.clone())?;
            heap.push(cmp::Reverse(segment));
        }

        Ok(Self {
            heap,
            key,
            schema,
            spill,
            validator,
        })
    }

    pub fn into_parts(self) -> (Box<[Pointer]>, Option<url::Url>, F, Validator) {
        let Self {
            heap: _,
            key,
            schema,
            spill,
            validator,
        } = self;
        (key, schema, spill, validator)
    }

    /// Drain documents from this SpillDrainer by invoking the given callback.
    /// Documents passed to the callback MUST NOT be accessed after it returns.
    /// The callback returns true if it would like to be called further, or false
    /// if a present call to drain_while() should return, yielding back to the caller.
    ///
    /// A future call to drain_while() can then resume the drain operation at
    /// its next ordered document. drain_while() returns true while documents
    /// remain to drain, and false only after all documents have been drained.
    pub fn drain_while<C, CE>(&mut self, mut callback: C) -> Result<bool, CE>
    where
        C: for<'alloc> FnMut(LazyNode<'alloc, 'static, ArchivedNode>, bool) -> Result<bool, CE>,
        CE: From<Error>,
    {
        while let Some(cmp::Reverse(cur)) = self.heap.pop() {
            let alloc = HeapNode::new_allocator();

            let mut cur_root = LazyNode::Node(&cur.head().root);
            let mut cur_flags = cur.head().flags;

            // Poll the heap to find additional documents in other segments which share root's key.
            // Note that there can be at-most one instance of a key within a single segment,
            // so we don't need to also check `segment`.
            while matches!(self.heap.peek(), Some(cmp::Reverse(peek))
                if cur_root.compare(&self.key, &LazyNode::Node(&peek.head().root)).is_eq())
            {
                let other = self.heap.pop().unwrap().0;

                let ArchivedDoc {
                    root: next_root,
                    flags: next_flags,
                } = other.head();

                let out = super::smash(
                    &alloc,
                    cur_root,
                    cur_flags,
                    LazyNode::Node(next_root),
                    *next_flags,
                    self.schema.as_ref(),
                    &mut self.validator,
                )?;
                (cur_root, cur_flags) = (LazyNode::Heap(out.root), out.flags);

                if let Some(other) = other.next(&mut self.spill).map_err(Error::SpillIO)? {
                    self.heap.push(cmp::Reverse(other));
                }
            }

            cur_root
                .validate_ok(&mut self.validator, self.schema.as_ref())
                .map_err(Error::SchemaError)?
                .map_err(Error::FailedValidation)?;

            let done = !callback(cur_root, cur_flags & FLAG_REDUCED != 0)?;

            if let Some(segment) = cur.next(&mut self.spill).map_err(Error::SpillIO)? {
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
    use crate::{combine::CHUNK_MAX_LEN, validation::build_schema, AsNode, Validator};
    use serde_json::{json, Value};

    #[test]
    fn test_spill_writes_to_segments() {
        let alloc = HeapNode::new_allocator();
        let segment = segment_fixture(
            &[
                (json!({"key": "aaa", "v": "apple"}), 0),
                (json!({"key": "bbb", "v": "banana"}), FLAG_REDUCED),
                (json!({"key": "ccc", "v": "carrot"}), FLAG_REDUCED),
            ],
            &alloc,
        );

        // Write segment fixture into a SpillWriter.
        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();

        // 130 is calibrated to include two, but not three documents in a chunk.
        spill.write_segment(&segment, 130..CHUNK_MAX_LEN).unwrap();
        let (mut spill, ranges) = spill.into_parts();

        // Assert we wrote the expected range and regression fixture.
        assert_eq!(ranges, vec![0..190]);

        insta::assert_snapshot!(to_hex(&spill.get_ref()), @r###"
        |67000000 98000000 f1006b65 79000000| g.........key... 00000000
        |00030800 00006161 610c0000 05001076| ......aaa......v 00000010
        |05003100 00011800 7070706c 65000005| ..1.....ppple... 00000020
        |11000830 00306262 62130010 03050008| ...0.0bbb....... 00000030
        |30008062 616e616e 61000618 00000500| 0..banana....... 00000040
        |509cffff ff020d00 07020000 180017b4| P............... 00000050
        |18001301 1c0080d0 ffffff02 00000047| ...............G 00000060
        |00000050 000000f1 006b6579 00000000| ...P.....key.... 00000070
        |03080000 00636363 0c000005 00107605| .....ccc......v. 00000080
        |00310000 01180070 6172726f 74000611| .1.....parrot... 00000090
        |00000500 50ccffff ff020d00 41000000| ....P.......A... 000000a0
        |010600a0 0000e8ff ffff0100 0000|     ..............   000000b0
                                                               000000be
        "###);

        // Parse the region as a Segment.
        let key: Box<[Pointer]> = vec![Pointer::from_str("/key")].into();
        let mut actual = Vec::new();
        let mut segment = Segment::new(key, &mut spill, ranges[0].clone()).unwrap();

        // First chunk has two documents.
        assert_eq!(segment.docs.len(), 2);
        assert_eq!(segment.zz_backing.len(), 152);
        assert_eq!(segment.next, 111..190);

        actual.push(serde_json::to_value(&segment.head().root.as_node()).unwrap());

        segment = segment.next(&mut spill).unwrap().unwrap();
        actual.push(serde_json::to_value(&segment.head().root.as_node()).unwrap());

        // Next chunk is read and has one document.
        segment = segment.next(&mut spill).unwrap().unwrap();

        assert_eq!(segment.docs.len(), 1);
        assert_eq!(segment.zz_backing.len(), 80);
        assert_eq!(segment.next, 190..190);

        actual.push(serde_json::to_value(&segment.head().root.as_node()).unwrap());

        // Stepping the segment again consumes it, as no chunks remain.
        assert!(segment.next(&mut spill).unwrap().is_none());

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
        let key: Box<[Pointer]> = vec![Pointer::from_str("/key")].into();
        let curi = url::Url::parse("http://example/schema").unwrap();
        let validator = Validator::new(build_schema(curi, &schema).unwrap()).unwrap();

        let alloc = HeapNode::new_allocator();
        let fixtures = vec![
            segment_fixture(
                &[
                    (json!({"key": "aaa", "v": ["apple"]}), FLAG_REDUCED),
                    (json!({"key": "bbb", "v": ["banana"]}), 0),
                    (json!({"key": "ccc", "v": ["carrot"]}), 0),
                ],
                &alloc,
            ),
            segment_fixture(
                &[
                    (json!({"key": "bbb", "v": ["avocado"]}), FLAG_REDUCED),
                    (json!({"key": "ccc", "v": ["raisin"]}), FLAG_REDUCED),
                    (json!({"key": "ddd", "v": ["tomato"]}), FLAG_REDUCED),
                ],
                &alloc,
            ),
            segment_fixture(
                &[
                    (json!({"key": "ccc", "v": ["dill"]}), 0),
                    (json!({"key": "ddd", "v": ["pickle"]}), 0),
                    (json!({"key": "eee", "v": ["squash"]}), 0),
                ],
                &alloc,
            ),
        ];

        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
        for segment in fixtures {
            spill.write_segment(&segment, 2..4).unwrap();
        }

        // Map from SpillWriter => SpillDrainer.
        let (spill, ranges) = spill.into_parts();
        let mut drainer = SpillDrainer::new(key, None, spill, &ranges, validator).unwrap();

        let mut actual = Vec::new();
        loop {
            if !drainer
                .drain_while(|node, full| {
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

    #[test]
    fn test_spill_chunk_too_large() {
        let alloc = HeapNode::new_allocator();
        let segment = segment_fixture(
            &[
                (json!("one"), 0),
                (json!("twotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwotwo"), 0),
                (json!("three"), 0),
                (json!("four"), 0),
                (json!("five"), 0),
            ],
            &alloc,
        );

        let mut spill = SpillWriter::new(io::Cursor::new(Vec::new())).unwrap();
        spill.write_segment(&segment, 109..110).unwrap();
        let (mut spill, ranges) = spill.into_parts();

        let key: Box<[Pointer]> = vec![Pointer::from_str("")].into();
        let mut segment = Segment::new(key, &mut spill, ranges[0].clone()).unwrap();

        // First chunk is retried until its narrowed to a single document.
        assert_eq!(segment.docs.len(), 1);
        assert_eq!(segment.zz_backing.len(), 32);
        segment = segment.next(&mut spill).unwrap().unwrap();

        // Second chunk can only proceed by encoding the document by itself.
        // It's still too large given our maximum chunk size, but we let it through.
        assert_eq!(segment.docs.len(), 1);
        assert_eq!(segment.zz_backing.len(), 120);
        segment = segment.next(&mut spill).unwrap().unwrap();

        // Third chunk is very conservative because `last_bytes_per_doc` is so large,
        // but then re-sets the expectation for the fourth chunk.
        assert_eq!(segment.docs.len(), 1);
        assert_eq!(segment.zz_backing.len(), 32);
        segment = segment.next(&mut spill).unwrap().unwrap();

        // Fourth chunk includes multiple documents.
        assert_eq!(segment.docs.len(), 2);
        assert_eq!(segment.zz_backing.len(), 56);
        segment = segment.next(&mut spill).unwrap().unwrap();
        assert!(segment.next(&mut spill).unwrap().is_none());
    }

    fn to_hex(b: &[u8]) -> String {
        hexdump::hexdump_iter(b)
            .map(|line| format!("{line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn segment_fixture<'alloc>(
        fixture: &[(Value, u8)],
        alloc: &'alloc bumpalo::Bump,
    ) -> Vec<HeapDoc<'alloc>> {
        fixture
            .into_iter()
            .map(|(value, flags)| HeapDoc {
                root: HeapNode::from_node(value.as_node(), &alloc),
                flags: *flags,
            })
            .collect()
    }
}
