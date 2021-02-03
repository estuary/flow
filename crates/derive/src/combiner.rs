use super::DebugJson;

use doc::{reduce, Pointer, SchemaIndex, Validator};
use json::validator::FullContext;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;
use url::Url;

/// KeyedDoc is a Value document and the composite JSON-Pointers over which it's combined.
#[derive(Eq)]
struct KeyedDoc {
    key: Arc<[Pointer]>,
    doc: Value,
}

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

impl PartialEq for KeyedDoc {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

pub struct Combiner {
    schema: Url,
    validator: Validator<'static, FullContext>,
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

    pub fn combine(&mut self, rhs: Value, prune: bool) -> Result<(), reduce::Error> {
        let mut entry = KeyedDoc {
            key: self.key.clone(),
            doc: rhs,
        };
        let lhs = self.entries.take(&entry).map(|kd| kd.doc);

        entry.doc = reduce::reduce(&mut self.validator, &self.schema, lhs, entry.doc, prune)?;
        self.entries.insert(entry);

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
            combiner.combine(doc, false).unwrap();
        }
        assert_eq!(combiner.entries.len(), 3);

        assert_eq!(
            combiner.into_entries("/foo").collect::<Vec<_>>(),
            vec![
                json!({"foo": UUID_PLACEHOLDER, "key": ["key", "one"], "min": 3, "max": 5.5}),
                json!({"foo": UUID_PLACEHOLDER, "key": ["key", "three"], "min": 6, "max": 6.6}),
                json!({"foo": UUID_PLACEHOLDER, "key": ["key", "two"], "min": 2, "max": 4.4}),
            ]
        );
    }
}

// This constant is shared between Rust and Go code.
// See go/protocols/flow/document_extensions.go.
pub const UUID_PLACEHOLDER: &str = "DocUUIDPlaceholder-329Bb50aa48EAa9ef";
