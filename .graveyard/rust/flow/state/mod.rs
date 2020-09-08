use crate::specs::store::Document;

// DocStore is a store of documents.
pub trait DocStore: Send + Sync {
    // Put a Document into the store, replacing an existing entry.
    fn put(&mut self, doc: &Document<'_>);
    // Get a Document from the store by its key.
    fn get<'s>(&'s self, key: &str) -> Option<Document<'s>>;
    // Iterate over store Documents having the given prefix.
    fn iter_prefix<'s>(
        &'s self,
        prefix: &str,
    ) -> Box<dyn Iterator<Item = Document<'s>> + Send + Sync + 's>;
}

mod memory;
pub use memory::Store as MemoryStore;

mod service;
pub use service::build as build_service;

fn prefix_range_end(prefix: &[u8]) -> Option<Box<[u8]>> {
    // Find index of last byte that isn't 0xff (the "pivot").
    let pivot = prefix
        .iter()
        .enumerate()
        .rev()
        .filter(|(_, &b)| b != 0xff)
        .next();

    // If found, return a slice through the pivot, which is incremented by one.
    match pivot {
        Some((ind, _)) => {
            let mut e = prefix[..ind + 1].to_owned();
            *e.last_mut().unwrap() += 1;
            Some(e.into_boxed_slice())
        }
        None => None,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::value::RawValue;
    use std::borrow::Cow;

    #[test]
    fn test_prefix_range() {
        // Empty string prefixes all possible keys.
        assert_eq!(prefix_range_end(&[]), None);
        // Similarly, a maximum-value string prefixes all other strings
        // which order after it.
        assert_eq!(prefix_range_end(&[0xff, 0xff, 0xff]), None);
        // Otherwise, expect the prefix is incremented and trimmed to the first larger key.
        assert_eq!(
            prefix_range_end(&[0xff, 0xff, 0xfe]).unwrap().as_ref(),
            &[0xff, 0xff, 0xff]
        );
        assert_eq!(
            prefix_range_end(&[0xff, 0xfe, 0xff]).unwrap().as_ref(),
            &[0xff, 0xff]
        );
        assert_eq!(
            prefix_range_end(&[0xfe, 0xff, 0xff]).unwrap().as_ref(),
            &[0xff]
        );
    }

    #[test]
    fn test_memory_store() {
        let mut store = MemoryStore::new();
        run_store_sequence(&mut store);
    }

    fn run_store_sequence(store: &mut dyn DocStore) {
        assert_eq!(store.iter_prefix("").count(), 0);
        assert!(store.get("missing").is_none());

        let fixtures = [
            ("foo/0", "0"),
            ("foo/2", "2"),
            ("fo0/1", "1"),
            ("bar/3", "false"),
            ("bar/3", "true"), // Replace.
            ("baz/4", "null"),
        ];

        for (key, doc) in fixtures.iter().copied() {
            store.put(&Document {
                key: Cow::from(key),
                value: &RawValue::from_string(doc.to_owned()).unwrap(),
                expire_at: None,
            });
        }

        assert_eq!(store.get("foo/0").unwrap().value.get(), "0");
        assert_eq!(store.get("bar/3").unwrap().value.get(), "true");

        assert_eq!(
            store
                .iter_prefix("foo")
                .map(|doc| serde_json::to_string(&doc).unwrap())
                .collect::<Vec<String>>(),
            vec![
                r#"{"key":"foo/0","value":0}"#.to_owned(),
                r#"{"key":"foo/2","value":2}"#.to_owned(),
            ],
        );
        assert_eq!(
            store
                .iter_prefix("ba")
                .map(|doc| serde_json::to_string(&doc).unwrap())
                .collect::<Vec<String>>(),
            vec![
                r#"{"key":"bar/3","value":true}"#.to_owned(),
                r#"{"key":"baz/4","value":null}"#.to_owned(),
            ],
        );

        assert_eq!(store.iter_prefix("").count(), 5);
        assert!(store.get("missing").is_none()); // Still missing.
    }
}
