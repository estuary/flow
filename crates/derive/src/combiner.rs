use super::DebugJson;

use doc::{reduce, Pointer, Validation, Validator};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::rc::Rc;
use url::Url;

#[derive(thiserror::Error, Debug, serde::Serialize)]
pub enum Error {
    #[error("failed to combine documents having shared key")]
    Reduction(#[from] reduce::Error),
    #[error("document is invalid: {0:#}")]
    PreReduceValidation(#[source] doc::FailedValidation),
    #[error("combined document is invalid: {0:#}")]
    PostReduceValidation(#[source] doc::FailedValidation),
    #[error("asked to left-combine, but right-hand document is already fully reduced: {0}")]
    AlreadyFullyReduced(Value),

    #[error(transparent)]
    SchemaError(#[from] json::schema::index::Error),
}

/// KeyedDoc is a Value document and the composite JSON-Pointers over which it's combined.
struct KeyedDoc {
    key: Rc<[Pointer]>,
    doc: Value,
    fully_reduced: bool,
}

// KeyedDoc is ordered on its document's extracted key.
impl Ord for KeyedDoc {
    fn cmp(&self, other: &Self) -> Ordering {
        Pointer::compare(&self.key, &self.doc, &other.doc)
    }
}

impl PartialOrd for KeyedDoc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for KeyedDoc {}

impl PartialEq for KeyedDoc {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

pub struct Combiner {
    key: Rc<[Pointer]>,
    schema: Url,
    entries: BTreeSet<KeyedDoc>,
}

pub struct DrainIter {
    it: std::collections::btree_set::IntoIter<KeyedDoc>,
}

impl Iterator for DrainIter {
    type Item = (Value, bool);

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next().map(|kd| (kd.doc, kd.fully_reduced))
    }
}

impl Combiner {
    pub fn new(schema: Url, key: Rc<[Pointer]>) -> Combiner {
        assert!(!key.is_empty());

        Combiner {
            schema,
            entries: BTreeSet::new(),
            key,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Reduce the fully reduced left-hand document with a partially reduced right-hand
    /// document that's already in the Combiner. It's an error if there is already a fully
    /// reduced right-hand document.
    pub fn reduce_left(&mut self, lhs: Value, validator: &mut Validator) -> Result<(), Error> {
        let lookup = KeyedDoc {
            key: self.key.clone(),
            doc: lhs,
            fully_reduced: false,
        };

        let rhs = match self.entries.take(&lookup) {
            Some(entry) if entry.fully_reduced => {
                return Err(Error::AlreadyFullyReduced(lookup.doc))
            }
            Some(entry) => Some(entry.doc),
            None => None,
        };

        let lhs = Validation::validate(validator, &self.schema, lookup.doc)?
            .ok()
            .map_err(Error::PreReduceValidation)?;

        let reduced = if let Some(rhs) = rhs {
            let lhs = Some(lhs.0.document);

            // Validate RHS (again) to gather annotations. Note that it must have already
            // validated in order to have been in the Combiner.
            let rhs = Validation::validate(validator, &self.schema, rhs)
                .unwrap()
                .ok()
                .unwrap();

            reduce::reduce(lhs, rhs, true)?
        } else {
            reduce::reduce(None, lhs, true)?
        };

        let reduced = Validation::validate(validator, &self.schema, reduced)?
            .ok()
            .map_err(Error::PostReduceValidation)?;

        self.entries.insert(KeyedDoc {
            key: lookup.key,
            doc: reduced.0.document,
            fully_reduced: true,
        });

        Ok(())
    }

    /// Combine the partial right-hand side document into the left-hand document held by the Combiner.
    pub fn combine_right(&mut self, rhs: Value, validator: &mut Validator) -> Result<(), Error> {
        let lookup = KeyedDoc {
            key: self.key.clone(),
            doc: rhs,
            fully_reduced: false,
        };

        let (lhs, fully_reduced) = match self.entries.take(&lookup) {
            Some(entry) => (Some(entry.doc), entry.fully_reduced),
            None => (None, false),
        };

        let rhs = Validation::validate(validator, &self.schema, lookup.doc)?
            .ok()
            .map_err(Error::PreReduceValidation)?;

        let reduced = reduce::reduce(lhs, rhs, fully_reduced)?;

        let reduced = Validation::validate(validator, &self.schema, reduced)?
            .ok()
            .map_err(Error::PostReduceValidation)?;

        self.entries.insert(KeyedDoc {
            key: lookup.key,
            doc: reduced.0.document,
            fully_reduced,
        });

        Ok(())
    }

    // Drain all entries of the Combiner via the returned iterator.
    // Entries are immediately removed even if the iterator is not consumed.
    pub fn drain_entries(&mut self) -> DrainIter {
        DrainIter {
            it: std::mem::take(&mut self.entries).into_iter(),
        }
    }

    pub fn key(&self) -> &Rc<[Pointer]> {
        &self.key
    }
}

impl std::fmt::Debug for Combiner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Combiner")
            .field("schema", &self.schema.as_str())
            .field("key", &self.key)
            .field(
                "entries",
                &self
                    .entries
                    .iter()
                    .map(|k| DebugJson(&k.doc))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::super::test::build_min_max_sum_schema;
    use super::super::ValidatorGuard;
    use super::*;
    use serde_json::json;

    #[test]
    fn test_lifecycle() {
        let mut guard = ValidatorGuard::new(&build_min_max_sum_schema()).unwrap();
        let key: Vec<Pointer> = vec!["/key/1".into(), "/key/0".into()];
        let key: Rc<[Pointer]> = key.into();

        let docs = vec![
            (
                false,
                json!({"key": ["key", "one"], "min": 3, "max": 3.3, "lww": 1}),
            ),
            (
                true,
                json!({"key": ["key", "two"], "min": 4, "max": 4.4, "lww": 2}),
            ),
            (
                false,
                json!({"key": ["key", "two"], "min": 2, "max": 2.2, "lww": 3}),
            ),
            (
                true,
                json!({"key": ["key", "one"], "min": 5, "max": 5.5, "lww": 4}),
            ),
            (
                false,
                json!({"key": ["key", "three"], "min": 6, "max": 6.6, "lww": 5}),
            ),
        ];

        let mut combiner = Combiner::new(guard.schema.curi.clone(), key.clone());
        for (left, doc) in docs {
            if left {
                combiner.reduce_left(doc, &mut guard.validator)
            } else {
                combiner.combine_right(doc, &mut guard.validator)
            }
            .unwrap();
        }
        assert_eq!(combiner.entries.len(), 3);

        // Expect min / max reflect all combines, and that "lww" (last-write-wins) respects
        // the left vs right ordering of applications.
        assert_eq!(
            combiner.drain_entries().collect::<Vec<_>>(),
            vec![
                (
                    json!({"key": ["key", "one"], "min": 3, "max": 5.5, "lww": 1}),
                    true
                ),
                (
                    json!({"key": ["key", "three"], "min": 6, "max": 6.6, "lww": 5}),
                    false
                ),
                (
                    json!({"key": ["key", "two"], "min": 2, "max": 4.4, "lww": 3}),
                    true
                ),
            ]
        );
    }

    #[test]
    fn test_errors() {
        let mut guard = ValidatorGuard::new(&build_min_max_sum_schema()).unwrap();
        let key: Vec<Pointer> = vec!["/key".into()];
        let key: Rc<[Pointer]> = key.into();

        // Case: documents to combine don't validate.
        let mut combiner = Combiner::new(guard.schema.curi.clone(), key.clone());
        matches!(
            combiner
                .reduce_left(json!({"key": 1, "min": "whoops"}), &mut guard.validator)
                .unwrap_err(),
            Error::PreReduceValidation(_)
        );
        matches!(
            combiner
                .combine_right(json!({"key": 1, "min": "whoops"}), &mut guard.validator)
                .unwrap_err(),
            Error::PreReduceValidation(_)
        );

        // Case: reduce LHS & combine RHS which each validate, but don't together.
        let mut combiner = Combiner::new(guard.schema.curi.clone(), key.clone());
        combiner
            .reduce_left(json!({"key": 1, "sum": -2}), &mut guard.validator)
            .unwrap();
        matches!(
            combiner
                .combine_right(
                    json!({"key": 1, "sum": 1, "positive": 1}),
                    &mut guard.validator
                )
                .unwrap_err(),
            Error::PostReduceValidation(_)
        );

        // Case: combine RHS & reduce LHS which don't validate together.
        let mut combiner = Combiner::new(guard.schema.curi.clone(), key.clone());
        combiner
            .combine_right(json!({"key": 1, "sum": -2}), &mut guard.validator)
            .unwrap();
        matches!(
            combiner
                .reduce_left(
                    json!({"key": 1, "sum": 1, "positive": 1}),
                    &mut guard.validator
                )
                .unwrap_err(),
            Error::PostReduceValidation(_)
        );

        // Case: two LHS reductions are prohibited.
        let mut combiner = Combiner::new(guard.schema.curi.clone(), key.clone());
        combiner
            .reduce_left(json!({"key": 1, "sum": 1}), &mut guard.validator)
            .unwrap();
        matches!(
            combiner
                .reduce_left(json!({"key": 1, "sum": 1}), &mut guard.validator)
                .unwrap_err(),
            Error::AlreadyFullyReduced(_)
        );
    }
}
