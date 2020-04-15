use crate::derive::state::DocStore;
use crate::specs::store::Document;
use serde_json::value::RawValue;
use std::borrow::Cow;
use std::collections::{btree_map, BTreeMap};

// Store is a DocStore implementation which uses an in-memory BTreeMap.
pub struct Store(BTreeMap<String, Box<RawValue>>);

impl Store {
    // Build an empty memory Store.
    pub fn new() -> Store {
        Store(BTreeMap::new())
    }
}

impl DocStore for Store {
    fn put(&mut self, doc: &Document<'_>) {
        self.0
            .insert(doc.key.to_owned().into(), doc.value.to_owned());
    }

    fn get<'s>(&'s self, key: &str) -> Option<Document<'s>> {
        self.0.get_key_value(key).and_then(|(key, value)| {
            Some(Document {
                key: Cow::from(key),
                value: value,
                expire_at: None,
            })
        })
    }

    fn iter_prefix<'s>(
        &'s self,
        prefix: &str,
    ) -> Box<dyn Iterator<Item = Document<'s>> + Send + Sync + 's> {
        Box::new(Prefix {
            prefix: prefix.to_owned(),
            inner: self.0.iter(),
        })
    }
}

pub struct Prefix<'s> {
    prefix: String,
    inner: btree_map::Iter<'s, String, Box<RawValue>>,
}

impl<'s> Iterator for Prefix<'s> {
    type Item = Document<'s>;

    #[inline]
    fn next(&mut self) -> Option<Document<'s>> {
        while let Some((key, value)) = self.inner.next() {
            if key.starts_with(&self.prefix) {
                return Some(Document {
                    key: Cow::from(key),
                    value: value,
                    expire_at: None,
                });
            }
        }
        None
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
