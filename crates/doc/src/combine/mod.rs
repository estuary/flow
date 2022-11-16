use crate::{
    reduce,
    validation::{FailedValidation, Validator},
    ArchivedNode, HeapDoc, LazyNode, Pointer,
};
use std::io::{self, Seek};
use std::rc::Rc;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("failed to combine documents having shared key")]
    Reduction(#[from] reduce::Error),
    #[error("document is invalid: {0:#}")]
    FailedValidation(#[source] FailedValidation),
    #[error("asked to left-combine, but right-hand document is already fully reduced: {0}")]
    AlreadyFullyReduced(serde_json::Value),
    #[error(transparent)]
    SchemaError(#[from] json::schema::index::Error),
    #[error("spill file IO error")]
    #[serde(serialize_with = "serialize_as_display")]
    SpillIO(#[from] io::Error),
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
    pub fn new(
        key: Rc<[Pointer]>,
        schema: Option<url::Url>,
        spill: std::fs::File,
        validator: Validator,
    ) -> Result<Self, Error> {
        Ok(Self {
            memtable: Some(MemTable::new(key, schema, validator)),
            spill: SpillWriter::new(spill)?,
        })
    }

    /// Obtain an MemTable with available capacity.
    /// If the held MemTable is already over-capacity, it is first spilled and
    /// then replaced with a new instance, which is then returned.
    pub fn memtable(&mut self) -> Result<&MemTable, Error> {
        let Self { memtable: Some(memtable), spill } = self else {
            unreachable!("memtable is always Some");
        };

        // TODO(johnny): This is somewhat broken because chunk_capacity() is broken.
        // Currently this means the mem_used value is very quantized and doubles ~quadratically.
        // See: https://github.com/fitzgen/bumpalo/issues/185
        let mem_used = memtable.alloc().allocated_bytes() - memtable.alloc().chunk_capacity();
        if mem_used > SPILL_THRESHOLD {
            let (key, schema, validator) = self
                .memtable
                .take()
                .unwrap()
                .spill(spill, CHUNK_TARGET_LEN..CHUNK_MAX_LEN)?;
            self.memtable = Some(MemTable::new(key, schema, validator));
        }

        Ok(self.memtable.as_ref().unwrap())
    }

    /// Map this combine Accumulator into a Drainer, which will drain directly
    /// from the inner MemTable (if no spill occurred) or from an inner SpillDrainer.
    pub fn into_drainer(self) -> Result<Drainer, Error> {
        let Self {
            memtable: Some(memtable),
            mut spill,
        } = self else {
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
            let (key, schema, validator) =
                memtable.spill(&mut spill, CHUNK_TARGET_LEN..CHUNK_MAX_LEN)?;
            let (spill, ranges) = spill.into_parts();

            Ok(Drainer::Spill {
                drainer: SpillDrainer::new(key, schema, spill, &ranges, validator)?,
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

impl Drainer {
    /// Drain documents from this Drainer by invoking the given callback.
    /// Documents passed to the callback MUST NOT be accessed after it returns.
    /// The callback returns true if it would like to be called further, or false
    /// if a present call to drain_while() should return, yielding back to the caller.
    ///
    /// A future call to drain_while() can then resume the drain operation at
    /// its next ordered document. drain_while() returns true while documents
    /// remain to drain, and false only after all documents have been drained.
    pub fn drain_while<C, CE>(&mut self, callback: C) -> Result<bool, CE>
    where
        C: for<'alloc> FnMut(LazyNode<'alloc, 'static, ArchivedNode>, bool) -> Result<bool, CE>,
        CE: From<Error>,
    {
        match self {
            Drainer::Mem { drainer, .. } => drainer.drain_while(callback),
            Drainer::Spill { drainer } => drainer.drain_while(callback),
        }
    }

    /// Map this Drainer into a new and empty Accumulator.
    /// Any un-drained documents are dropped.
    pub fn into_new_accumulator(self) -> Result<Accumulator, Error> {
        match self {
            Drainer::Mem { spill, drainer } => {
                let (key, schema, validator) = drainer.into_parts();
                Ok(Accumulator::new(key, schema, spill, validator)?)
            }
            Drainer::Spill { drainer } => {
                let (key, schema, mut spill, validator) = drainer.into_parts();

                spill.seek(io::SeekFrom::Start(0))?; // Reset to start.
                spill.set_len(0)?; // Release allocated size to OS.

                Ok(Accumulator::new(key, schema, spill, validator)?)
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
    pub fn new(
        key: Rc<[Pointer]>,
        schema: Option<url::Url>,
        spill: std::fs::File,
        validator: Validator,
    ) -> Result<Self, Error> {
        Ok(Self::Accumulator(Accumulator::new(
            key, schema, spill, validator,
        )?))
    }
}

// Bit flags set on HeapDoc::flags to mark combiner processing status.
// A the moment there's just one: a bit that indicates the document
// is fully reduced, and not merely right-combined.
const FLAG_REDUCED: u8 = 1;

// Smash two documents together.
fn smash<'alloc>(
    alloc: &'alloc bumpalo::Bump,
    lhs_doc: LazyNode<'alloc, '_, ArchivedNode>,
    lhs_flags: u8,
    rhs_doc: LazyNode<'alloc, '_, ArchivedNode>,
    rhs_flags: u8,
    schema: Option<&url::Url>,
    validator: &mut Validator,
) -> Result<HeapDoc<'alloc>, Error> {
    match (
        lhs_doc,
        lhs_flags & FLAG_REDUCED != 0,
        rhs_doc,
        rhs_flags & FLAG_REDUCED != 0,
    ) {
        // `rhs_doc` is being combined into `lhs_doc`, which may or may not be fully reduced.
        (lhs, lhs_reduced, rhs, false) => {
            let rhs_valid = rhs
                .validate_ok(validator, schema)
                .map_err(Error::SchemaError)?
                .map_err(Error::FailedValidation)?;

            Ok(HeapDoc {
                root: reduce::reduce(lhs, rhs, rhs_valid, &alloc, lhs_reduced)
                    .map_err(Error::Reduction)?,
                flags: if lhs_reduced { FLAG_REDUCED } else { 0 },
            })
        }
        // `rhs_doc` is actually a fully-reduced LHS, which is reduced with `lhs_doc`.
        (rhs, false, lhs, true) => {
            let rhs_valid = rhs
                .validate_ok(validator, schema)
                .map_err(Error::SchemaError)?
                .map_err(Error::FailedValidation)?;

            Ok(HeapDoc {
                root: reduce::reduce(lhs, rhs, rhs_valid, &alloc, true)
                    .map_err(Error::Reduction)?,
                flags: FLAG_REDUCED,
            })
        }
        (_lhs, true, rhs, true) => {
            return Err(Error::AlreadyFullyReduced(serde_json::to_value(&rhs).unwrap()).into())
        }
    }
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
const SPILL_THRESHOLD: usize = 8 * (1 << 28) / 10; // 80% of 256MB.

// The chunk target determines the amortization of archiving and
// compressing documents. We want chunks to be:
//  - Small, since SpillDrainer must keep one uncompressed chunk in memory
//    for every segment of the spill file.
//  - Larger, so that LZ4 can compress it reasonably (we typically see 5x
//    reduction, 10x is probably the theoretical max).
//  - Larger, so that filesystem reads and writes are amortized for the
//    LZ4-compressed.
//
// We also want a maximum bound so that SpillDrainer isn't required to
// hold many large chunks in memory, which could push us over our allocation.
const CHUNK_TARGET_LEN: usize = 1 << 18; // 256KB.
const CHUNK_MAX_LEN: usize = 1 << 20; // 1MB.

fn serialize_as_display<T, S>(thing: T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: std::fmt::Display,
    S: serde::ser::Serializer,
{
    let s = thing.to_string();
    serializer.serialize_str(&s)
}
