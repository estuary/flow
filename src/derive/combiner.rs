use crate::doc::reduce::Reducer;
use crate::doc::{
    extract_reduce_annotations, validate, FailedValidation, Pointer, SchemaIndex, Validator,
};
use estuary_json::de::walk;
use estuary_json::validator::FullContext;
use estuary_json::NoopWalker;
use serde_json::Value;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use url::Url;

/// KeyedDoc is a Value document and the composite JSON-Pointers over which it's combined.
#[derive(Eq)]
struct KeyedDoc {
    key: Arc<[Pointer]>,
    doc: Value,
}

impl Hash for KeyedDoc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for key in self.key.iter() {
            let value = key.query(&self.doc).unwrap_or(&Value::Null);
            let span = walk(value, &mut NoopWalker).unwrap();
            state.write_u64(span.hashed);
        }
    }
}

impl PartialEq for KeyedDoc {
    fn eq(&self, other: &Self) -> bool {
        for key in self.key.iter() {
            let lhs = key.query(&self.doc).unwrap_or(&Value::Null);
            let rhs = key.query(&other.doc).unwrap_or(&Value::Null);

            if estuary_json::json_cmp(lhs, rhs) != std::cmp::Ordering::Equal {
                return false;
            }
        }
        true
    }
}

pub struct Combiner {
    schema: Url,
    validator: Validator<'static, FullContext>,
    entries: HashSet<KeyedDoc>,
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
            entries: HashSet::new(),
            key,
        }
    }

    pub fn combine(&mut self, doc: Value) -> Result<(), FailedValidation> {
        validate(&mut self.validator, &self.schema, &doc)?;
        let doc = KeyedDoc {
            key: self.key.clone(),
            doc,
        };

        let reduced = match self.entries.take(&doc) {
            Some(mut prior) => {
                Reducer {
                    at: 0,
                    val: doc.doc,
                    into: &mut prior.doc,
                    created: false,
                    idx: &extract_reduce_annotations(self.validator.outcomes()),
                }
                .reduce();

                prior
            }
            None => doc,
        };
        self.entries.insert(reduced);

        Ok(())
    }

    // Return all entries of the Combiner. If the UUID placeholder is non-empty,
    // then UUID_PLACEHOLDER is inserted into returned documents at the specified location.
    // Iff the document shape is incompatible with the pointer, it's returned unmodified.
    pub fn into_entries(self, uuid_placeholder: &str) -> impl Iterator<Item = Value> {
        let uuid_placeholder = match uuid_placeholder {
            "" => None,
            s => Some(Pointer::from(s)),
        };

        self.entries.into_iter().map(move |mut kd| {
            if let Some(uuid_ptr) = &uuid_placeholder {
                if let Some(uuid_value) = uuid_ptr.create(&mut kd.doc) {
                    *uuid_value = Value::String(UUID_PLACEHOLDER.to_owned());
                }
            }
            kd.doc
        })
    }
}

impl<'a> std::fmt::Debug for Combiner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Combiner")
    }
}

#[cfg(test)]
mod test {
    use super::{super::test::build_min_max_schema, *};
    use serde_json::json;

    #[test]
    fn test_lifecycle() {
        let (schema_index, schema) = build_min_max_schema();
        let key: Vec<Pointer> = vec!["/key/1".into(), "/key/0".into()];
        let key: Arc<[Pointer]> = key.into();

        let docs = vec![
            json!({"key": ["key", "one"], "min": 3, "max": 3.3}),
            json!({"key": ["key", "two"], "min": 4, "max": 4.4}),
            json!({"key": ["key", "two"], "min": 2, "max": 2.2}),
            json!({"key": ["key", "one"], "min": 5, "max": 5.5}),
            json!({"key": ["key", "three"], "min": 6, "max": 6.6}),
        ];

        let mut combiner = Combiner::new(schema_index, &schema, key.clone());
        for doc in docs {
            combiner.combine(doc).unwrap();
        }
        assert_eq!(combiner.entries.len(), 3);

        let mut entries = combiner.into_entries("/foo").collect::<Vec<_>>();
        entries.sort_by_key(|v| key[0].query(v).unwrap().as_str().unwrap().to_owned());

        assert_eq!(
            entries,
            vec![
                json!({"foo": UUID_PLACEHOLDER, "key": ["key", "one"], "min": 3, "max": 5.5}),
                json!({"foo": UUID_PLACEHOLDER, "key": ["key", "three"], "min": 6, "max": 6.6}),
                json!({"foo": UUID_PLACEHOLDER, "key": ["key", "two"], "min": 2, "max": 4.4}),
            ]
        );
    }
}

pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";
