use super::{extract_reduce_annotations, reduce::Reducer, Annotation};
use estuary_json::{self as ej, validator};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::hash_map;
use std::hash::{BuildHasher, Hasher};
use std::iter;

pub struct Ring {
    key: Vec<String>,
    rs: hash_map::RandomState,

    v: Vec<Value>,
    head: usize,
    tail: usize,

    // Map from hash -> ring index.
    idx: hash_map::HashMap<u64, u32>,
}

impl Ring {
    pub fn new(key: Vec<String>, size: u32) -> Ring {
        Ring {
            key: key,
            rs: hash_map::RandomState::new(),
            v: iter::repeat(Value::Null).take(size as usize).collect(),
            head: 0,
            tail: 0,
            idx: hash_map::HashMap::with_capacity(size as usize),
        }
    }

    pub fn accumulate<C>(
        &mut self,
        doc: Value,
        validator: &validator::Validator<Annotation, C>,
    ) -> Option<Value>
    where
        C: validator::Context,
    {
        let hash = self.doc_hash(&doc);
        let entry = self.idx.entry(hash).or_insert(self.tail as u32);

        let mut create = *entry == self.tail as u32;

        // If an entry exists for this hash but is of a different key,
        // then de-index the existing value in favor of our new one.
        if !create && ej::json_cmp_at(&self.key, &doc, &self.v[*entry as usize]) != Ordering::Equal
        {
            *entry = self.tail as u32;
            create = true;
        }

        Reducer {
            at: 0,
            val: doc,
            into: &mut self.v[*entry as usize],
            created: create,
            idx: &extract_reduce_annotations(validator.outcomes()),
        }
        .reduce();

        if create {
            self.tail = (self.tail + 1) % self.v.len();

            if self.tail == self.head {
                let evicted = Some(self.v[self.head].take());
                self.head = (self.head + 1) % self.v.len();
                evicted
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn evict_head(&mut self) -> Option<Value> {
        if self.head == self.tail {
            None
        } else {
            let v = Some(self.v[self.head].take());
            self.head = (self.head + 1) % self.v.len();
            v
        }
    }

    fn doc_hash(&self, doc: &Value) -> u64 {
        let mut h = self.rs.build_hasher();

        for ptr in self.key.iter() {
            let d = doc.pointer(ptr).unwrap_or(&Value::Null);
            let span = ej::de::walk(d, &mut ej::NoopWalker).unwrap();
            h.write_u64(span.hashed);
        }
        h.finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    //use serde_json::json;

    #[test]
    fn test_foobar() {
        let _r = Ring::new(vec!["/1".to_owned(), "/0".to_owned()], 10);

        // assert_eq!(r.doc_hash(&json!(["foo", 1.0])), 123);
    }
}
