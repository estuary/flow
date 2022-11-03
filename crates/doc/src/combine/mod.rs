use crate::{
    reduce,
    validation::{FailedValidation, Validator},
    ArchivedNode, LazyNode, Pointer,
};
use std::io::{self, Seek};
use std::rc::Rc;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("failed to combine documents having shared key")]
    Reduction(#[from] reduce::Error),
    #[error("document is invalid: {0:#}")]
    PreReduceValidation(#[source] FailedValidation),
    #[error("combined document is invalid: {0:#}")]
    PostReduceValidation(#[source] FailedValidation),
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
    memtable: MemTable,
    spill: SpillWriter<std::fs::File>,
}

impl Accumulator {
    pub fn new(key: Rc<[Pointer]>, schema: url::Url, spill: std::fs::File) -> Result<Self, Error> {
        Ok(Self {
            memtable: MemTable::new(key, schema),
            spill: SpillWriter::new(spill)?,
        })
    }

    /// Obtain an MemTable with available capacity.
    /// If the held MemTable is already over-capacity, it is first spilled and
    /// then replaced with a new instance, which is then returned.
    pub fn memtable(&mut self) -> Result<&MemTable, Error> {
        let Self { memtable, spill } = self;

        let mem_used = memtable.alloc().allocated_bytes() - memtable.alloc().chunk_capacity();
        if mem_used > SPILL_THRESHOLD {
            std::mem::replace(
                memtable,
                MemTable::new(memtable.key().clone(), memtable.schema().clone()),
            )
            .spill(spill)?;
        }
        Ok(memtable)
    }

    /// Map this combine Accumulator into a Drainer, which will drain directly
    /// from the inner MemTable (if no spill occurred) or from an inner SpillDrainer.
    pub fn into_drainer(self) -> Result<Drainer, Error> {
        let Self {
            memtable,
            mut spill,
        } = self;

        if spill.segment_ranges().is_empty() {
            let (spill, _ranges) = spill.into_parts();

            Ok(Drainer::Mem {
                spill,
                drainer: memtable.into_drainer(),
            })
        } else {
            // Spill the final MemTable segment.
            let (key, schema) = memtable.spill(&mut spill)?;
            let (spill, ranges) = spill.into_parts();

            Ok(Drainer::Spill {
                drainer: SpillDrainer::new(key, schema, spill, &ranges)?,
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
    pub fn drain_while<C, CE>(&mut self, validator: &mut Validator, callback: C) -> Result<bool, CE>
    where
        C: for<'alloc> FnMut(LazyNode<'alloc, 'static, ArchivedNode>, bool) -> Result<bool, CE>,
        CE: From<Error>,
    {
        match self {
            Drainer::Mem { drainer, .. } => drainer.drain_while(validator, callback),
            Drainer::Spill { drainer } => drainer.drain_while(validator, callback),
        }
    }

    /// Map this Drainer into a new and empty Accumulator.
    /// Any un-drained documents are dropped.
    pub fn into_new_accumulator(self) -> Result<Accumulator, Error> {
        match self {
            Drainer::Mem { spill, drainer } => {
                let (key, schema) = drainer.into_parts();
                Ok(Accumulator::new(key, schema, spill)?)
            }
            Drainer::Spill { drainer } => {
                let (key, schema, mut spill) = drainer.into_parts();

                spill.seek(io::SeekFrom::Start(0))?; // Reset to start.
                spill.set_len(0)?; // Release allocated size to OS.

                Ok(Accumulator::new(key, schema, spill)?)
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
    pub fn new(key: Rc<[Pointer]>, schema: url::Url, spill: std::fs::File) -> Result<Self, Error> {
        Ok(Self::Accumulator(Accumulator::new(key, schema, spill)?))
    }
}

// Bit flags set on HeapDoc::flags to mark combiner processing status:

// The document is fully reduced, and not merely right-combined.
const REDUCED_FLAG: u8 = 1;
// The document has had reductions applied and must be revalidated prior to combiner drain.
const REVALIDATE_FLAG: u8 = 2;
// The bump-allocator threshold after which we'll spill a MemTable to a SpillWriter.
const SPILL_THRESHOLD: usize = 8 * (1 << 27) / 10; // 80% of 128MB.

fn serialize_as_display<T, S>(thing: T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: std::fmt::Display,
    S: serde::ser::Serializer,
{
    let s = thing.to_string();
    serializer.serialize_str(&s)
}
