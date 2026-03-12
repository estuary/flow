use crate::{
    Extractor, HeapRoot, OwnedNode, redact, reduce,
    validation::{FailedValidation, Validator},
};
use std::io::{self, Seek};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to redact portions of the document")]
    Redact(#[from] redact::Error),
    #[error("failed to combine documents having shared key")]
    Reduce(#[from] reduce::Error),
    #[error("{0} document failed validation against its collection JSON Schema")]
    FailedValidation(String, #[source] FailedValidation),
    #[error(transparent)]
    SchemaError(#[from] json::schema::index::Error),
    #[error("spill file IO error")]
    SpillIO(#[from] io::Error),
}

/// Specification of how Combine operations are to be done
/// over one or more bindings.
pub struct Spec {
    is_full: Vec<bool>,
    keys: Arc<[Box<[Extractor]>]>,
    names: Vec<String>,
    redact_salt: Vec<u8>,
    validators: Vec<Validator>,
}

impl Spec {
    /// Build a Spec from a single binding.
    /// * When `full` is true, the Combiner performs full reductions to group
    ///   each key to a single output document.
    /// * Or, when `full` is false, the Combiner performs all possible
    ///   associative reductions to group over distinct keys. Where an
    ///   associative reduction isn't possible, it will yield multiple
    ///   documents for a grouped key in the left-to-right order with
    ///   which they reduce into an unknown left-most document.
    pub fn with_one_binding(
        full: bool,
        key: impl Into<Box<[Extractor]>>,
        name: impl Into<String>,
        redact_salt: Vec<u8>,
        validator: Validator,
    ) -> Self {
        Self {
            is_full: vec![full],
            keys: vec![key.into()].into(),
            names: vec![name.into()],
            redact_salt,
            validators: vec![validator],
        }
    }

    /// Build a Spec from an Iterator of (is-full-reduction, key, schema, validator).
    pub fn with_bindings<I, K, N>(bindings: I, redact_salt: Vec<u8>) -> Self
    where
        I: IntoIterator<Item = (bool, K, N, Validator)>,
        K: Into<Box<[Extractor]>>,
        N: Into<String>,
    {
        let mut full = Vec::new();
        let mut keys = Vec::new();
        let mut names = Vec::new();
        let mut validators = Vec::new();

        for (index, (is_full, key, name, validator)) in bindings.into_iter().enumerate() {
            full.push(is_full);
            keys.push(key.into());
            names.push(format!("{} (binding {index})", name.into()));
            validators.push(validator);
        }

        Self {
            is_full: full,
            keys: keys.into(),
            names,
            redact_salt,
            validators,
        }
    }
}

/// Meta is metadata about an entry:
/// - It's u16 binding index.
/// - First 13 bytes of it's extracted key tuple.
/// - Flags
#[derive(Eq, PartialEq, Clone, Copy)]
pub struct Meta(
    // Packed bytes of:
    // - bytes [0,2): u16_be binding index (most-significant byte first)
    // - bytes [2,15): first 13 bytes of extracted key tuple
    //
    // This structure is designed to have a natural order that aligns with
    // ordering under (binding, key): if two entries compare != 0, then their
    // full (binding, key) order will compare identically.
    //
    // This lets us quickly determine that two entries are definitely NOT equal,
    // and their relative order, without thrashing the CPU cache or awaiting the
    // memory latency penalty to fetch their full keys.
    [u8; 15],
    // Flags
    u8,
);

/// HeapEntry is a combiner entry that exists in memory.
/// It's produced by MemTable and is consumed by SpillWriter.
pub struct HeapEntry<'s> {
    meta: Meta,
    root: HeapRoot<'s>,
}

pub mod memtable;
pub use memtable::{MemDrainer, MemTable};

pub mod spill;
pub use spill::{SpillDrainer, SpillWriter};

/// Accumulator is a MemTable paired with a File-backed SpillWriter.
/// As the caller utilizes the MemTable the Accumulator will transparently
/// spill table contents to its SpillWriter and then re-initializes a new and
/// empty MemTable, bounding overall memory usage.
pub struct Accumulator {
    memtable: Option<MemTable>,
    spill: SpillWriter<std::fs::File>,
}

impl Accumulator {
    pub fn new(spec: Spec, spill: std::fs::File) -> Result<Self, Error> {
        Ok(Self {
            memtable: Some(MemTable::new(spec)),
            spill: SpillWriter::new(spill)?,
        })
    }

    /// Obtain an MemTable with available capacity.
    /// If the held MemTable is already over-capacity, it is first spilled and
    /// then replaced with a new instance, which is then returned.
    pub fn memtable(&mut self) -> Result<&MemTable, Error> {
        let Self {
            memtable: Some(memtable),
            spill,
        } = self
        else {
            unreachable!("memtable is always Some");
        };

        if bump_mem_used(memtable.alloc()) > BUMP_THRESHOLD {
            let spec = self
                .memtable
                .take()
                .unwrap()
                .spill(spill, CHUNK_TARGET_SIZE)?;
            self.memtable = Some(MemTable::new(spec));
        }

        Ok(self.memtable.as_ref().unwrap())
    }

    /// Map this combine Accumulator into a Drainer, which will drain directly
    /// from the inner MemTable (if no spill occurred) or from an inner SpillDrainer.
    pub fn into_drainer(self) -> Result<Drainer, Error> {
        let Self {
            memtable: Some(memtable),
            mut spill,
        } = self
        else {
            unreachable!("memtable must be Some");
        };

        if spill.segment_ranges().is_empty() {
            let (spill, _ranges) = spill.into_parts();

            Ok(Drainer::Mem {
                spill,
                drainer: memtable.try_into_drainer()?,
            })
        } else {
            // Spill the final MemTable segment.
            let spec = memtable.spill(&mut spill, CHUNK_TARGET_SIZE)?;
            let (spill, ranges) = spill.into_parts();

            Ok(Drainer::Spill {
                drainer: SpillDrainer::new(spec, spill, &ranges)?,
            })
        }
    }
}

/// Drainer drains from either a wrapped MemTable or SpillDrainer.
pub enum Drainer {
    Mem {
        spill: std::fs::File,
        drainer: MemDrainer,
    },
    Spill {
        drainer: SpillDrainer<std::fs::File>,
    },
}

/// DrainedDoc is a document drained from a Drainer.
pub struct DrainedDoc {
    pub meta: Meta,
    pub root: OwnedNode,
}

impl Iterator for Drainer {
    type Item = Result<DrainedDoc, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Mem { drainer, .. } => drainer.next(),
            Self::Spill { drainer } => drainer.next(),
        }
    }
}

impl Drainer {
    /// Drain the next document of this Drainer.
    pub fn drain_next(&mut self) -> Result<Option<DrainedDoc>, Error> {
        match self {
            Self::Mem { drainer, .. } => drainer.drain_next(),
            Self::Spill { drainer } => drainer.drain_next(),
        }
    }

    /// Map this Drainer into a new and empty Accumulator.
    /// Any un-drained documents are dropped.
    pub fn into_new_accumulator(self) -> Result<Accumulator, Error> {
        match self {
            Drainer::Mem { spill, drainer } => {
                let spec = drainer.into_spec();
                Ok(Accumulator::new(spec, spill)?)
            }
            Drainer::Spill { drainer } => {
                let (spec, mut spill) = drainer.into_parts();

                spill.seek(io::SeekFrom::Start(0))?; // Reset to start.
                spill.set_len(0)?; // Release allocated size to OS.

                Ok(Accumulator::new(spec, spill)?)
            }
        }
    }
}

/// Combiner wraps an Accumulator or Drainer, reflecting the two stages of
/// a combiner life-cycle.
pub enum Combiner {
    Accumulator(Accumulator),
    Drainer(Drainer),
}

impl Combiner {
    /// Build a Combiner initialized as an empty, new Accumulator.
    pub fn new(spec: Spec, spill: std::fs::File) -> Result<Self, Error> {
        Ok(Self::Accumulator(Accumulator::new(spec, spill)?))
    }
}

impl Meta {
    #[inline]
    fn new(binding: u16, key: &[u8], front: bool, known_valid: bool) -> Self {
        let b = binding.to_be_bytes();
        let mut packed = [b[0], b[1], 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        // Copy up to 13 bytes of `key` into the packed representation.
        // Shorter keys leave trailing zeroes.
        for (s, t) in key.iter().zip(packed[2..].iter_mut()) {
            *t = *s;
        }

        let mut flags = 0u8;
        if front {
            flags |= META_FLAG_FRONT;
        }
        if known_valid {
            flags |= META_FLAG_KNOWN_VALID;
        }
        Self(packed, flags)
    }

    /// Build a Meta from a pre-extracted 16-byte packed key prefix.
    /// We take the first 13 bytes for Meta.
    #[inline]
    pub fn from_packed_prefix(
        binding: u16,
        packed_key_prefix: &[u8; 16],
        front: bool,
        known_valid: bool,
    ) -> Self {
        let b = binding.to_be_bytes();
        let p = packed_key_prefix;

        let packed = [
            b[0], b[1], p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10], p[11],
            p[12],
        ];

        let mut flags = 0u8;
        if front {
            flags |= META_FLAG_FRONT;
        }
        if known_valid {
            flags |= META_FLAG_KNOWN_VALID;
        }
        Self(packed, flags)
    }

    /// The binding for this entry.
    #[inline]
    pub fn binding(&self) -> usize {
        u16::from_be_bytes([self.0[0], self.0[1]]) as usize
    }

    /// Was this entry added at the front of the list of documents?
    /// This is commonly used to add a previously reduced, "left-hand" document
    /// to a combiner after other right-hand documents have already been added.
    #[inline]
    pub fn front(&self) -> bool {
        self.1 & META_FLAG_FRONT != 0
    }

    /// Is this entry known to be valid? Known-valid entries can skip validation
    /// during spill/drain. The shuffle pipeline validates documents at read time.
    /// Known-valid entries are assumed to not need redaction (validation drives redaction).
    #[inline]
    pub fn known_valid(&self) -> bool {
        self.1 & META_FLAG_KNOWN_VALID != 0
    }

    /// Is this entry marked as deleted by its reduction annotation?
    /// Deleted entries are conceptually a "tombstone" that can be used to
    /// delete a document from a downstream system (instead of doing an upsert).
    #[inline]
    pub fn deleted(&self) -> bool {
        self.1 & META_FLAG_DELETED != 0
    }

    // This LHS entry does not associatively reduce with its RHS entry.
    #[inline]
    fn not_associative(&self) -> bool {
        self.1 & META_FLAG_NOT_ASSOCIATIVE != 0
    }

    #[inline]
    fn set_known_valid(&mut self, known_valid: bool) {
        if known_valid {
            self.1 |= META_FLAG_KNOWN_VALID;
        } else {
            self.1 &= !META_FLAG_KNOWN_VALID;
        }
    }

    #[inline]
    fn set_deleted(&mut self, deleted: bool) {
        if deleted {
            self.1 |= META_FLAG_DELETED;
        } else {
            self.1 &= !META_FLAG_DELETED;
        }
    }

    #[inline]
    fn set_not_associative(&mut self) {
        self.1 |= META_FLAG_NOT_ASSOCIATIVE;
    }
}

impl std::fmt::Debug for Meta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_tuple("Meta");
        s.field(&self.binding());

        if self.front() {
            s.field(&"F");
        }
        if self.not_associative() {
            s.field(&"NA");
        }
        if self.deleted() {
            s.field(&"D");
        }
        if self.known_valid() {
            s.field(&"V");
        }
        s.finish()
    }
}

// Flag marking entry is at the front of the associative list.
const META_FLAG_FRONT: u8 = 0x01;
// Flag marking that this LHS entry doesn't associatively reduce with a following RHS.
const META_FLAG_NOT_ASSOCIATIVE: u8 = 0x02;
// Flag marking this entry is a deletion tombstone.
const META_FLAG_DELETED: u8 = 0x04;
// Flag marking this entry is known to be valid against its schema.
const META_FLAG_KNOWN_VALID: u8 = 0x08;

// The number of used bytes within a Bump allocator.
fn bump_mem_used(alloc: &bumpalo::Bump) -> usize {
    alloc.allocated_bytes() - alloc.chunk_capacity()
}

// The bump-allocator threshold after which we'll spill a MemTable to a SpillWriter.
// We _could_ make this a knob, but empirically using larger values doesn't increase
// performance in common cases where little reduction is happening, because we're
// essentially trading a merge-sort of in-memory HeapDocs for a heap merge-sort of
// ArchivedDocs.
//
// If we're writing too many segments -- enough that keeping all of their chunks
// resident in memory is a problem -- then larger values will decrease the number
// of segments written.
//
// There may be hypothetical use cases that benefit from more in-memory reduction.
// At the moment, I suspect this would still be pretty marginal.
const BUMP_THRESHOLD: usize = 1 << 28; // 256MB.

// The chunk target determines the amortization of archiving and
// compressing documents. We want chunks to be:
//  - Small, since SpillDrainer must keep one uncompressed chunk in memory
//    for every segment of the spill file.
//  - Larger, so that LZ4 can compress it reasonably (we typically see 5x
//    reduction, 10x is probably the theoretical max).
//  - Larger, so that filesystem reads and writes are amortized for the
//    LZ4-compressed.
//
const CHUNK_TARGET_SIZE: usize = 1 << 18; // 256KB.

// These are compile-time assertions that document and enforce that Combiner
// and friends implement Send.
fn _assert_accumulator_is_send(t: Accumulator) {
    _assert_send(t)
}
fn _assert_spill_drainer_is_send(t: SpillDrainer<std::fs::File>) {
    _assert_send(t)
}
fn _assert_mem_drainer_is_send(t: MemDrainer) {
    _assert_send(t)
}
fn _assert_drainer_is_send(t: Drainer) {
    _assert_send(t)
}
fn _assert_combiner_is_send(t: Combiner) {
    _assert_send(t)
}
fn _assert_send<T: Send>(_t: T) {}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{Extractor, HeapNode, SerPolicy, Validator};
    use itertools::Itertools;
    use serde_json::json;

    fn make_spec() -> Spec {
        let schema = json::schema::build::build_schema(
            &url::Url::parse("http://example/schema").unwrap(),
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

        Spec::with_one_binding(
            true,
            vec![Extractor::with_default(
                "/key",
                &SerPolicy::noop(),
                json!("def"),
            )],
            "test-source",
            Vec::new(),
            Validator::new(schema).unwrap(),
        )
    }

    fn drain_all(drainer: &mut Drainer) -> Vec<(usize, serde_json::Value, bool)> {
        std::iter::from_fn(|| drainer.drain_next().transpose())
            .map_ok(|doc| {
                (
                    doc.meta.binding(),
                    serde_json::to_value(SerPolicy::noop().on_owned(&doc.root)).unwrap(),
                    doc.meta.front(),
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    #[test]
    fn test_accumulator_mem_only() {
        let spill = tempfile::tempfile().unwrap();
        let mut acc = Accumulator::new(make_spec(), spill).unwrap();

        // Add documents through memtable().
        for doc_json in [
            json!({"key": "aaa", "v": ["apple"]}),
            json!({"key": "bbb", "v": ["banana"]}),
            json!({"key": "aaa", "v": ["avocado"]}),
        ] {
            let mt = acc.memtable().unwrap();
            let node = HeapNode::from_node(&doc_json, mt.alloc());
            mt.add(0, node, false).unwrap();
        }

        // No spill occurred — drain via the Mem variant.
        let mut drainer = acc.into_drainer().unwrap();
        assert!(matches!(&drainer, Drainer::Mem { .. }));

        let actual = drain_all(&mut drainer);
        insta::assert_json_snapshot!(actual, @r###"
        [
          [
            0,
            {
              "key": "aaa",
              "v": [
                "apple",
                "avocado"
              ]
            },
            false
          ],
          [
            0,
            {
              "key": "bbb",
              "v": [
                "banana"
              ]
            },
            false
          ]
        ]
        "###);

        // Recycle the drainer into a new accumulator.
        let mut acc = drainer.into_new_accumulator().unwrap();

        // Add more docs and drain again to verify the recycled accumulator works.
        let mt = acc.memtable().unwrap();
        let node = HeapNode::from_node(&json!({"key": "ccc", "v": ["carrot"]}), mt.alloc());
        mt.add(0, node, false).unwrap();

        let mut drainer = acc.into_drainer().unwrap();
        let actual = drain_all(&mut drainer);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].1["key"], "ccc");
    }

    #[test]
    fn test_accumulator_with_spill() {
        let spill = tempfile::tempfile().unwrap();
        let spec = make_spec();
        let mut acc = Accumulator::new(spec, spill).unwrap();

        // Add documents through memtable, then manually spill.
        for doc_json in [
            json!({"key": "aaa", "v": ["apple"]}),
            json!({"key": "bbb", "v": ["banana"]}),
        ] {
            let mt = acc.memtable().unwrap();
            let node = HeapNode::from_node(&doc_json, mt.alloc());
            mt.add(0, node, false).unwrap();
        }

        // Force a spill by taking the memtable and spilling it.
        let spec = acc
            .memtable
            .take()
            .unwrap()
            .spill(&mut acc.spill, CHUNK_TARGET_SIZE)
            .unwrap();
        acc.memtable = Some(MemTable::new(spec));

        // Add more documents to a fresh memtable (second segment).
        for doc_json in [
            json!({"key": "aaa", "v": ["avocado"]}),
            json!({"key": "ccc", "v": ["carrot"]}),
        ] {
            let mt = acc.memtable().unwrap();
            let node = HeapNode::from_node(&doc_json, mt.alloc());
            mt.add(0, node, false).unwrap();
        }

        // Drain via the Spill variant.
        let mut drainer = acc.into_drainer().unwrap();
        assert!(matches!(&drainer, Drainer::Spill { .. }));

        let actual = drain_all(&mut drainer);
        insta::assert_json_snapshot!(actual, @r###"
        [
          [
            0,
            {
              "key": "aaa",
              "v": [
                "apple",
                "avocado"
              ]
            },
            false
          ],
          [
            0,
            {
              "key": "bbb",
              "v": [
                "banana"
              ]
            },
            false
          ],
          [
            0,
            {
              "key": "ccc",
              "v": [
                "carrot"
              ]
            },
            false
          ]
        ]
        "###);

        // Recycle the spill drainer into a new accumulator.
        let mut acc = drainer.into_new_accumulator().unwrap();

        let mt = acc.memtable().unwrap();
        let node = HeapNode::from_node(&json!({"key": "ddd", "v": ["dill"]}), mt.alloc());
        mt.add(0, node, false).unwrap();

        let mut drainer = acc.into_drainer().unwrap();
        let actual = drain_all(&mut drainer);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].1["key"], "ddd");
    }

    #[test]
    fn test_drainer_iterator() {
        let spill = tempfile::tempfile().unwrap();
        let mut acc = Accumulator::new(make_spec(), spill).unwrap();

        let mt = acc.memtable().unwrap();
        let node = HeapNode::from_node(&json!({"key": "aaa", "v": ["apple"]}), mt.alloc());
        mt.add(0, node, false).unwrap();

        // Exercise the Iterator impl (which delegates to drain_next).
        let drainer = acc.into_drainer().unwrap();
        let actual: Vec<_> = drainer
            .map(|r| {
                let doc = r.unwrap();
                serde_json::to_value(SerPolicy::noop().on_owned(&doc.root)).unwrap()
            })
            .collect();
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0]["key"], "aaa");
    }

    #[test]
    fn test_combiner_new() {
        let spill = tempfile::tempfile().unwrap();
        let combiner = Combiner::new(make_spec(), spill).unwrap();
        assert!(matches!(combiner, Combiner::Accumulator(_)));
    }
}
