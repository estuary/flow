use crate::derive::state::DocStore;
use crate::specs::store::Document;
use serde_json::value::RawValue;
use std::collections::{btree_map, BTreeMap};
use std::ops::Bound;

// Store is a DocStore implementation which uses an in-memory BTreeMap.
pub struct Store(BTreeMap<Box<[u8]>, Box<RawValue>>);

impl Store {
    // Build an empty memory Store.
    pub fn new() -> Store {
        Store(BTreeMap::new())
    }
}

impl DocStore for Store {
    fn put(&mut self, doc: &Document<'_>) {
        self.0
            .insert(doc.key.as_bytes().clone().into(), doc.value.to_owned());
    }

    fn get<'s>(&'s self, key: &str) -> Option<Document<'s>> {
        self.0
            .get_key_value(key.as_bytes())
            .and_then(|(key, value)| {
                Some(Document {
                    key: String::from_utf8_lossy(key),
                    value: value,
                    expire_at: None,
                })
            })
    }

    fn iter_prefix<'s>(
        &'s self,
        prefix: &str,
    ) -> Box<dyn Iterator<Item = Document<'s>> + Send + Sync + 's> {
        let rng = (
            Bound::Included(prefix.as_bytes().clone().into()),
            super::prefix_range_end(prefix.as_bytes())
                .map_or(Bound::Unbounded, |e| Bound::Excluded(e)),
        );

        Box::new(Prefix {
            inner: self.0.range(rng),
        })
    }
}

pub struct Prefix<'s> {
    inner: btree_map::Range<'s, Box<[u8]>, Box<RawValue>>,
}

impl<'s> Iterator for Prefix<'s> {
    type Item = Document<'s>;

    #[inline]
    fn next(&mut self) -> Option<Document<'s>> {
        while let Some((key, value)) = self.inner.next() {
            return Some(Document {
                key: String::from_utf8_lossy(key),
                value: value,
                expire_at: None,
            });
        }
        None
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
