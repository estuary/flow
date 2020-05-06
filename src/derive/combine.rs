use super::{transform::Context, Error, RecordBatch};
use crate::doc::{extract_reduce_annotations, reduce, Pointer, Validator};
use estuary_json::de::walk;
use estuary_json::validator::FullContext;
use estuary_json::NoopWalker;
use serde_json::Value;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

#[derive(Eq)]
struct KeyedDoc<'a> {
    key: &'a [Pointer],
    doc: Value,
}

impl<'a> Hash for KeyedDoc<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for key in self.key {
            let value = key.query(&self.doc).unwrap_or(&Value::Null);
            let span = walk(value, &mut NoopWalker).unwrap();
            state.write_u64(span.hashed);
        }
    }
}

impl<'a> PartialEq for KeyedDoc<'a> {
    fn eq(&self, other: &Self) -> bool {
        for key in self.key {
            let lhs = key.query(&self.doc).unwrap_or(&Value::Null);
            let rhs = key.query(&other.doc).unwrap_or(&Value::Null);

            if estuary_json::json_cmp(lhs, rhs) != std::cmp::Ordering::Equal {
                return false;
            }
        }
        true
    }
}

pub struct Combiner<'a> {
    entries: HashSet<KeyedDoc<'a>>,
}

impl<'a> Combiner<'a> {
    pub fn new() -> Combiner<'a> {
        Combiner {
            entries: HashSet::new(),
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = Value> + 'a {
        self.entries.into_iter().map(|kd| kd.doc)
    }
}

pub fn process_derived_batch<'a>(
    ctx: &'a Context,
    combiner: &mut Combiner<'a>,
    batch: RecordBatch,
) -> Result<(), Error> {
    let batch = batch.to_bytes();

    // Split records on newline boundaries.
    // TODO(johnny): Convince serde-json to expose Deserializer::byte_offset()?
    // Then it's not necessary to pre-scan for newlines.
    let splits = batch
        .iter()
        .enumerate()
        .filter(|(_, &b)| b == b'\n')
        .map(|(ind, _)| ind + 1);

    let mut last_pivot = 0;

    for next_pivot in splits {
        let doc: Value = serde_json::from_slice(&batch[last_pivot..next_pivot])?;
        last_pivot = next_pivot;

        let mut validator = Validator::<FullContext>::new(&ctx.schema_index, &ctx.derived_schema)?;
        walk(&doc, &mut validator)?;

        if validator.invalid() {
            let errors = validator
                .outcomes()
                .iter()
                .filter(|(o, _)| o.is_error())
                .collect::<Vec<_>>();
            log::error!("derived doc is invalid: {:?}", errors);
            Err(Error::DerivedValidationFailed)?;
        }

        let doc = KeyedDoc {
            key: &ctx.derived_key,
            doc,
        };

        let reduced = match combiner.entries.take(&doc) {
            Some(mut prior) => {
                let strategy_idx = extract_reduce_annotations(validator.outcomes());

                reduce::Reducer {
                    at: 0,
                    val: doc.doc,
                    into: &mut prior.doc,
                    created: false,
                    idx: &strategy_idx,
                }
                .reduce();

                prior
            }
            None => doc,
        };
        combiner.entries.insert(reduced);
    }

    Ok(())
}
