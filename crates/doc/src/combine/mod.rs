use crate::{
    reduce, transform,
    validation::{FailedValidation, Validator},
    Extractor, HeapNode, OwnedNode,
};
use std::io::{self, Seek};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to combine documents having shared key")]
    Reduction(#[from] reduce::Error),
    #[error("{0} document failed validation against its collection JSON Schema")]
    FailedValidation(String, #[source] FailedValidation),
    #[error(transparent)]
    SchemaError(#[from] json::schema::index::Error),
    #[error("spill file IO error")]
    SpillIO(#[from] io::Error),
    #[error("failed to transform document")]
    Transform(#[from] transform::Error),
}

/// Specification of how Combine operations are to be done
/// over one or more bindings.
pub struct Spec {
    is_full: Vec<bool>,
    keys: Arc<[Box<[Extractor]>]>,
    names: Vec<String>,
    validators: Vec<(Validator, Option<url::Url>)>,
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
        schema: Option<url::Url>,
        validator: Validator,
    ) -> Self {
        Self {
            is_full: vec![full],
            keys: vec![key.into()].into(),
            names: vec![name.into()],
            validators: vec![(validator, schema)],
        }
    }

    /// Build a Spec from an Iterator of (is-full-reduction, key, schema, validator).
    pub fn with_bindings<I, K, N>(bindings: I) -> Self
    where
        I: IntoIterator<Item = (bool, K, N, Option<url::Url>, Validator)>,
        K: Into<Box<[Extractor]>>,
        N: Into<String>,
    {
        let mut full = Vec::new();
        let mut keys = Vec::new();
        let mut names = Vec::new();
        let mut validators = Vec::new();

        for (index, (is_full, key, name, schema, validator)) in bindings.into_iter().enumerate() {
            full.push(is_full);
            keys.push(key.into());
            names.push(format!("{} (binding {index})", name.into()));
            validators.push((validator, schema));
        }

        Self {
            is_full: full,
            keys: keys.into(),
            names,
            validators,
        }
    }
}

/// Meta is metadata about an entry: its binding index and flags.
#[derive(Eq, PartialEq, Clone, Copy)]
pub struct Meta(u32);

/// HeapEntry is a combiner entry that exists in memory.
/// It's produced by MemTable and is consumed by SpillWriter.
pub struct HeapEntry<'s> {
    meta: Meta,
    root: HeapNode<'s>,
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
    fn new(mut binding: u32, front: bool) -> Self {
        if front {
            binding |= META_FLAG_FRONT;
        }
        Self(binding)
    }

    /// The binding for this entry.
    #[inline]
    pub fn binding(&self) -> usize {
        (self.0 & META_BINDING_MASK) as usize
    }

    /// Was this entry added at the front of the list of documents?
    /// This is commonly used to add a previously reduced, "left-hand" document
    /// to a combiner after other right-hand documents have already been added.
    #[inline]
    pub fn front(&self) -> bool {
        self.0 & META_FLAG_FRONT != 0
    }

    /// Is this entry marked as deleted by its reduction annotation?
    /// Deleted entries are conceptually a "tombstone" that can be used to
    /// delete a document from a downstream system (instead of doing an upsert).
    #[inline]
    pub fn deleted(&self) -> bool {
        self.0 & META_FLAG_DELETED != 0
    }

    // This LHS entry does not associatively reduce with its RHS entry.
    #[inline]
    fn not_associative(&self) -> bool {
        self.0 & META_FLAG_NOT_ASSOCIATIVE != 0
    }

    #[inline]
    fn set_deleted(&mut self, deleted: bool) {
        if deleted {
            self.0 = self.0 | META_FLAG_DELETED;
        } else {
            self.0 = self.0 & !META_FLAG_DELETED;
        }
    }

    #[inline]
    fn set_not_associative(&mut self) {
        self.0 = self.0 | META_FLAG_NOT_ASSOCIATIVE;
    }

    fn to_bytes(&self) -> [u8; 4] {
        self.0.to_le_bytes()
    }

    fn from_bytes(b: [u8; 4]) -> Self {
        Self(u32::from_le_bytes(b))
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
        s.finish()
    }
}

// Binding is the lower 24 bits of Meta (max value is 16MM).
const META_BINDING_MASK: u32 = 0x00ffffff;
// Flag marking entry is at the front of the associative list.
const META_FLAG_FRONT: u32 = 1 << 31;
// Flag marking that this LHS entry doesn't associatively reduce with a following RHS.
const META_FLAG_NOT_ASSOCIATIVE: u32 = 1 << 30;
// Flag marking this entry is a deletion tombstone.
const META_FLAG_DELETED: u32 = 1 << 29;

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
