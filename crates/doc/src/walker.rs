use super::{AsNode, Field, Fields, Node};

use fxhash::{hash64, FxHasher64};
use json::Span;
use std::hash::{Hash, Hasher};

/// walk_document is an integration joint between the (newer) AsNode trait
/// and it's representations, and the (older) Walker trait used to walk documents
/// in the course of schema validation.
///
/// Eventually we may want to update our JSON schema validation implementation
/// to directly use the AsNode trait, removing the need for this function.
pub fn walk_document<'l, N: AsNode, W: json::Walker>(
    document: &N,
    walker: &mut W,
    location: &json::Location<'l>,
    span_begin: usize,
) -> json::Span {
    match document.as_node() {
        Node::Array(arr) => {
            let mut span = Span::new(span_begin, ARRAY_SEED);
            let mut hasher = FxHasher64::default();

            for (index, item) in arr.iter().enumerate() {
                let item_span = Span::new(span.end, 0);
                let item_loc = json::LocatedItem {
                    parent: location,
                    index,
                };
                walker.push_item(&item_span, &item_loc);

                let item_loc = json::Location::Item(item_loc);
                let sub_span = walk_document(item, walker, &item_loc, span.end);

                hasher.write_u64(sub_span.hashed);
                span.end = sub_span.end;
            }

            // Hash of this span is the result of hashing over the ordered
            // hashes of each sub-span.
            span.hashed ^= hasher.finish();

            walker.pop_array(&span, location, arr.len());
            span
        }
        Node::Bool(v) => {
            let span = Span::new(span_begin, if v { BOOL_TRUE_HASH } else { BOOL_FALSE_HASH });
            walker.pop_bool(&span, location, v);
            span
        }
        Node::Bytes(_b) => {
            unimplemented!("bytes are not supported for validation yet")
        }
        Node::Null => {
            let span = Span::new(span_begin, UNIT_HASH);
            walker.pop_null(&span, location);
            span
        }
        Node::Number(v) => {
            let hash = match v {
                json::Number::Float(f) => {
                    // Separately hash integral and fractional hash parts to maintain equality
                    // between integer f64 values and u64/i64 types.
                    let vt = f.trunc();
                    hash64(&(vt as i64)) ^ hash64(&(f - vt).to_bits())
                }
                json::Number::Unsigned(u) => hash64(&u),
                json::Number::Signed(s) => hash64(&s),
            };

            let span = Span::new(span_begin, hash);
            walker.pop_numeric(&span, location, v);
            span
        }
        Node::String(v) => {
            let span = Span::new(span_begin, STRING_SEED ^ hash64(v));
            walker.pop_str(&span, location, v);
            span
        }
        Node::Object(fields) => {
            let mut span = Span::new(span_begin, OBJECT_SEED);

            for (index, field) in fields.iter().enumerate() {
                let prop_loc = json::LocatedProperty {
                    parent: location,
                    name: field.property(),
                    index,
                };
                let prop_span = Span::new(span.end, hash64(prop_loc.name));
                walker.push_property(&prop_span, &prop_loc);

                let prop_loc = json::Location::Property(prop_loc);
                let sub_span = walk_document(field.value(), walker, &prop_loc, span.end);

                span.end = sub_span.end;

                // Update the hash of our span by XOR'ing in a composed hash
                // of the property name and sub-span value. The XOR is required
                // in order to produce hash values which are invariant to the
                // order in which properties are enumerated.
                let mut h = FxHasher64::default();
                prop_span.hashed.hash(&mut h);
                sub_span.hashed.hash(&mut h);
                span.hashed ^= h.finish();
            }

            walker.pop_object(&span, location, fields.len());
            span
        }
    }
}

// Seeds to distinguish zero-valued types from one another.
// Numbers are not seeded.
const UNIT_HASH: u64 = 0xe0fe5d21a7c19aeb;
const BOOL_TRUE_HASH: u64 = 0x3bd83018139b2c4d;
const BOOL_FALSE_HASH: u64 = 0x4cd6d6c279e081c0;
const ARRAY_SEED: u64 = 0xca910bdc0b6441dd;
const OBJECT_SEED: u64 = 0x76662a22f0a45102;
const STRING_SEED: u64 = 0x5570bb24d6cdeee2;

#[cfg(test)]
mod test {

    use super::walk_document;
    use crate::HeapNode;
    use json::{LocatedItem, LocatedProperty, Location, Number, Span};

    struct MockWalker {
        calls: Vec<String>,
    }

    impl json::Walker for MockWalker {
        fn push_property<'a>(&mut self, span: &Span, loc: &'a LocatedProperty<'a>) {
            self.calls.push(format!(
                "push_property(span: {span:?}, loc: {})",
                json::Location::Property(*loc).pointer_str().to_string()
            ))
        }
        fn push_item<'a>(&mut self, span: &Span, loc: &'a LocatedItem<'a>) {
            self.calls.push(format!(
                "push_item(span: {span:?}, loc: {})",
                json::Location::Item(*loc).pointer_str().to_string()
            ))
        }
        fn pop_object<'a>(&mut self, span: &Span, loc: &'a Location<'a>, num_properties: usize) {
            self.calls.push(format!(
                "pop_object(span: {span:?}, loc: {}, num_properties: {num_properties})",
                loc.pointer_str().to_string()
            ))
        }
        fn pop_array<'a>(&mut self, span: &Span, loc: &'a Location<'a>, num_items: usize) {
            self.calls.push(format!(
                "pop_array(span: {span:?}, loc: {}, num_items: {num_items})",
                loc.pointer_str().to_string()
            ))
        }
        fn pop_bool<'a>(&mut self, span: &Span, loc: &'a Location<'a>, val: bool) {
            self.calls.push(format!(
                "pop_bool(span: {span:?}, loc: {}, val: {val})",
                loc.pointer_str().to_string()
            ))
        }
        fn pop_numeric<'a>(&mut self, span: &Span, loc: &'a Location<'a>, val: Number) {
            self.calls.push(format!(
                "pop_numeric(span: {span:?}, loc: {}, val: {val})",
                loc.pointer_str().to_string()
            ))
        }
        fn pop_str<'a>(&mut self, span: &Span, loc: &'a Location<'a>, val: &'a str) {
            self.calls.push(format!(
                "pop_str(span: {span:?}, loc: {}, val: {val})",
                loc.pointer_str().to_string()
            ))
        }
        fn pop_null<'a>(&mut self, span: &Span, loc: &'a Location<'a>) {
            self.calls.push(format!(
                "pop_null(span: {span:?}, loc: {})",
                loc.pointer_str().to_string()
            ))
        }
    }

    #[test]
    fn test_walk_regression() {
        let fixture = serde_json::json!({
            "numbers": [ 0x1111111111111111 as u64, -1234, 56.7891122334455],
            "repeat": "repeat",
            "some": {"bytes":"c29tZSBieXRlcw=="},
            "null": null,
            "foo": {
                "true": true,
                "two": 2,
                "repeat": {"repeat": "repeat"},
            },
            "bar": {
                "false": false,
                "flat-array": [{"object":"value", "ninish": 9.999}, 32, {"third": 3}],
                "nested-array": [[[1,2],[3],4],5,[6]]
            }
        });

        let alloc = HeapNode::new_allocator();
        let dedup = HeapNode::new_deduper(&alloc);
        let doc = HeapNode::from_serde(&fixture, &alloc, &dedup).unwrap();

        let mut walker = MockWalker { calls: Vec::new() };

        // Uncomment me to run the fixture through the original deserialization-based
        // Walker implementation. This ensures that that implementation and this one
        // produce identical outputs.
        //json::de::walk(&fixture, &mut walker).unwrap();

        walk_document(&doc, &mut walker, &json::Location::Root, 0);

        insta::assert_debug_snapshot!(&walker.calls);
    }
}
