use super::DebugJson;

use doc::{reduce, Pointer, SchemaIndex, Validation, Validator};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;
use url::Url;

#[derive(thiserror::Error, Debug)]
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
    key: Arc<[Pointer]>,
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
    schema: Url,
    validator: Validator<'static>,
    entries: BTreeSet<KeyedDoc>,
    key: Arc<[Pointer]>,
}

impl Combiner {
    pub fn new(
        schema_index: &'static SchemaIndex<'static>,
        schema: &Url,
        key: Arc<[Pointer]>,
    ) -> Combiner {
        Combiner {
            schema: schema.clone(),
            validator: Validator::new(schema_index),
            entries: BTreeSet::new(),
            key,
        }
    }

    /// Reduce the fully reduced left-hand document with a partially reduced right-hand
    /// document that's already in the Combiner. It's an error if there is already a fully
    /// reduced right-hand document.
    pub fn reduce_left(&mut self, lhs: Value) -> Result<(), Error> {
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

        let lhs = Validation::validate(&mut self.validator, &self.schema, lookup.doc)?
            .ok()
            .map_err(Error::PreReduceValidation)?;

        let reduced = if let Some(rhs) = rhs {
            let lhs = Some(lhs.0.document);

            // Validate RHS (again) to gather annotations. Note that it must have already
            // validated in order to have been in the Combiner.
            let rhs = Validation::validate(&mut self.validator, &self.schema, rhs)
                .unwrap()
                .ok()
                .unwrap();

            reduce::reduce(lhs, rhs, true)?
        } else {
            reduce::reduce(None, lhs, true)?
        };

        let reduced = Validation::validate(&mut self.validator, &self.schema, reduced)?
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
    pub fn combine_right(&mut self, rhs: Value) -> Result<(), Error> {
        let lookup = KeyedDoc {
            key: self.key.clone(),
            doc: rhs,
            fully_reduced: false,
        };

        let (lhs, fully_reduced) = match self.entries.take(&lookup) {
            Some(entry) => (Some(entry.doc), entry.fully_reduced),
            None => (None, false),
        };

        let rhs = Validation::validate(&mut self.validator, &self.schema, lookup.doc)?
            .ok()
            .map_err(Error::PreReduceValidation)?;

        let reduced = reduce::reduce(lhs, rhs, fully_reduced)?;

        let reduced = Validation::validate(&mut self.validator, &self.schema, reduced)?
            .ok()
            .map_err(Error::PostReduceValidation)?;

        self.entries.insert(KeyedDoc {
            key: lookup.key,
            doc: reduced.0.document,
            fully_reduced,
        });

        Ok(())
    }

    // Return all entries of the Combiner. If the UUID placeholder JSON pointer is non-empty,
    // then UUID_PLACEHOLDER is inserted into returned documents at the specified location.
    // Iff the document shape is incompatible with the pointer, it's returned unmodified.
    pub fn into_entries(self, uuid_placeholder_ptr: &str) -> impl Iterator<Item = (Value, bool)> {
        let uuid_placeholder = match uuid_placeholder_ptr {
            "" => None,
            s => Some(Pointer::from(s)),
        };

        self.entries.into_iter().map(move |mut kd| {
            if let Some(uuid_ptr) = &uuid_placeholder {
                if let Some(uuid_value) = uuid_ptr.create(&mut kd.doc) {
                    *uuid_value = Value::String(UUID_PLACEHOLDER.to_owned());
                }
            }
            (kd.doc, kd.fully_reduced)
        })
    }

    pub fn key(&self) -> &Arc<[Pointer]> {
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
    use super::{super::test::build_min_max_sum_schema, *};
    use serde_json::json;

    #[test]
    fn test_lifecycle() {
        let (schema_index, schema) = build_min_max_sum_schema();
        let key: Vec<Pointer> = vec!["/key/1".into(), "/key/0".into()];
        let key: Arc<[Pointer]> = key.into();

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

        let mut combiner = Combiner::new(schema_index, &schema, key.clone());
        for (left, doc) in docs {
            if left {
                combiner.reduce_left(doc)
            } else {
                combiner.combine_right(doc)
            }
            .unwrap();
        }
        assert_eq!(combiner.entries.len(), 3);

        // Expect min / max reflect all combines, and that "lww" (last-write-wins) respects
        // the left vs right ordering of applications.
        assert_eq!(
            combiner.into_entries("/foo").collect::<Vec<_>>(),
            vec![
                (
                    json!({"foo": UUID_PLACEHOLDER, "key": ["key", "one"], "min": 3, "max": 5.5, "lww": 1}),
                    true
                ),
                (
                    json!({"foo": UUID_PLACEHOLDER, "key": ["key", "three"], "min": 6, "max": 6.6, "lww": 5}),
                    false
                ),
                (
                    json!({"foo": UUID_PLACEHOLDER, "key": ["key", "two"], "min": 2, "max": 4.4, "lww": 3}),
                    true
                ),
            ]
        );
    }

    #[test]
    fn test_errors() {
        let (schema_index, schema) = build_min_max_sum_schema();
        let key: Vec<Pointer> = vec!["/key".into()];
        let key: Arc<[Pointer]> = key.into();

        // Case: documents to combine don't validate.
        let mut combiner = Combiner::new(schema_index, &schema, key.clone());
        matches!(
            combiner
                .reduce_left(json!({"key": 1, "min": "whoops"}))
                .unwrap_err(),
            Error::PreReduceValidation(_)
        );
        matches!(
            combiner
                .combine_right(json!({"key": 1, "min": "whoops"}))
                .unwrap_err(),
            Error::PreReduceValidation(_)
        );

        // Case: reduce LHS & combine RHS which each validate, but don't together.
        let mut combiner = Combiner::new(schema_index, &schema, key.clone());
        combiner.reduce_left(json!({"key": 1, "sum": -2})).unwrap();
        matches!(
            combiner
                .combine_right(json!({"key": 1, "sum": 1, "positive": 1}))
                .unwrap_err(),
            Error::PostReduceValidation(_)
        );

        // Case: combine RHS & reduce LHS which don't validate together.
        let mut combiner = Combiner::new(schema_index, &schema, key.clone());
        combiner
            .combine_right(json!({"key": 1, "sum": -2}))
            .unwrap();
        matches!(
            combiner
                .reduce_left(json!({"key": 1, "sum": 1, "positive": 1}))
                .unwrap_err(),
            Error::PostReduceValidation(_)
        );

        // Case: two LHS reductions are prohibited.
        let mut combiner = Combiner::new(schema_index, &schema, key.clone());
        combiner.reduce_left(json!({"key": 1, "sum": 1})).unwrap();
        matches!(
            combiner
                .reduce_left(json!({"key": 1, "sum": 1}))
                .unwrap_err(),
            Error::AlreadyFullyReduced(_)
        );
    }
}

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";
