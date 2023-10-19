use crate::{
    reduce,
    validation::{FailedValidation, Validator},
    ArchivedNode, Extractor, HeapNode, LazyNode, OwnedNode, SerPolicy,
};
use std::io::{self, Seek};
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to combine documents having shared key")]
    Reduction(#[from] reduce::Error),
    #[error("document failed validation against its collection JSON Schema")]
    FailedValidation(#[source] FailedValidation),
    #[error("asked to left-combine, but right-hand document is already fully reduced: {0}")]
    AlreadyFullyReduced(serde_json::Value),
    #[error(transparent)]
    SchemaError(#[from] json::schema::index::Error),
    #[error("spill file IO error")]
    SpillIO(#[from] io::Error),
}

/// Specification of how Combine operations are to be done
/// over one or more bindings.
pub struct Spec {
    keys: Arc<[Box<[Extractor]>]>,
    validators: Vec<(Validator, Option<url::Url>)>,
}

impl Spec {
    /// Build a Spec from a single binding.
    pub fn with_one_binding(
        key: impl Into<Box<[Extractor]>>,
        schema: Option<url::Url>,
        validator: Validator,
    ) -> Self {
        let key = key.into();
        assert!(!key.is_empty());

        Self {
            keys: vec![key].into(),
            validators: vec![(validator, schema)],
        }
    }

    /// Build a Spec from an Iterator of bindings.
    pub fn with_bindings<I, K>(bindings: I) -> Self
    where
        I: IntoIterator<Item = (K, Option<url::Url>, Validator)>,
        K: Into<Box<[Extractor]>>,
    {
        let (keys, validators): (Vec<_>, _) = bindings
            .into_iter()
            .map(|(key, schema, validator)| (key.into(), (validator, schema)))
            .unzip();

        Self {
            keys: keys.into(),
            validators,
        }
    }
}

/// HeapEntry is a combiner entry that exists in memory.
/// It's produced by MemTable and is consumed by SpillWriter.
pub struct HeapEntry<'s> {
    binding: u32,
    reduced: bool,
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
    pub binding: u32,
    pub reduced: bool,
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

// Smash two documents together.
fn smash<'alloc>(
    alloc: &'alloc bumpalo::Bump,
    lhs_doc: LazyNode<'alloc, '_, ArchivedNode>,
    lhs_reduced: bool,
    rhs_doc: LazyNode<'alloc, '_, ArchivedNode>,
    rhs_reduced: bool,
    schema: Option<&url::Url>,
    validator: &mut Validator,
) -> Result<(HeapNode<'alloc>, bool), Error> {
    match (lhs_doc, lhs_reduced, rhs_doc, rhs_reduced) {
        // `rhs_doc` is being combined into `lhs_doc`, which may or may not be fully reduced.
        (lhs, lhs_reduced, rhs, false) => {
            let rhs_valid = rhs
                .validate_ok(validator, schema)
                .map_err(Error::SchemaError)?
                .map_err(Error::FailedValidation)?;

            Ok((
                reduce::reduce(lhs, rhs, rhs_valid, &alloc, lhs_reduced)
                    .map_err(Error::Reduction)?,
                lhs_reduced,
            ))
        }
        // `rhs_doc` is actually a fully-reduced LHS, which is reduced with `lhs_doc`.
        (rhs, false, lhs, true) => {
            let rhs_valid = rhs
                .validate_ok(validator, schema)
                .map_err(Error::SchemaError)?
                .map_err(Error::FailedValidation)?;

            Ok((
                reduce::reduce(lhs, rhs, rhs_valid, &alloc, true).map_err(Error::Reduction)?,
                true,
            ))
        }
        (_lhs, true, rhs, true) => {
            return Err(Error::AlreadyFullyReduced(
                serde_json::to_value(SerPolicy::debug().on_lazy(&rhs)).unwrap(),
            )
            .into())
        }
    }
}

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
