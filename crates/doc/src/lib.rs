/// Node is the generic form of a document node as understood by Flow.
/// It's implemented by HeapNode, ArchivedNode, and serde_json::Value.
#[derive(Debug)]
pub enum Node<'a, N: AsNode> {
    Array(&'a [N]),
    Bool(bool),
    Bytes(&'a [u8]),
    Float(f64),
    NegInt(i64),
    Null,
    Object(&'a N::Fields),
    PosInt(u64),
    String(&'a str),
}

/// AsNode is the trait by which a specific document representation is accessed through a generic Node.
pub trait AsNode: Sized {
    type Fields: Fields<Self> + ?Sized;

    /// Convert an AsNode into a Node.
    fn as_node<'a>(&'a self) -> Node<'a, Self>;

    /// Convert an AsNode into a serde_json::Value using the debug SerPolicy.
    fn to_debug_json_value(&self) -> serde_json::Value {
        serde_json::to_value(&SerPolicy::debug().on(self)).unwrap()
    }
}

/// Fields is the generic form of a Document object representation.
pub trait Fields<N: AsNode> {
    type Field<'a>: Field<'a, N>
    where
        Self: 'a;

    type Iter<'a>: ExactSizeIterator<Item = Self::Field<'a>>
    where
        Self: 'a;

    fn get<'a>(&'a self, property: &str) -> Option<Self::Field<'a>>;
    fn len(&self) -> usize;
    fn iter<'a>(&'a self) -> Self::Iter<'a>;
}

/// Field is the generic form of a Document object Field as understood by Flow.
pub trait Field<'a, N: AsNode> {
    fn property(&self) -> &'a str;
    fn value(&self) -> &'a N;
}

// Documents are built on the heap using a bump allocator.
// Re-export the concrete allocator type, as most clients don't care.
pub use bumpalo::Bump as Allocator;

// This crate has three implementations of AsNode: a mutable HeapNode,
// an ArchivedNode serialized by the `rkyv` crate,
// and an implementation upon serde_json::Value.
mod archived;
pub use archived::{ArchivedField, ArchivedNode};
pub mod heap;
pub use heap::{HeapField, HeapNode};
mod value;

// BumpStr is a low-level String type built upon a Bump allocator.
mod bump_str;
pub use bump_str::BumpStr;

// BumpVec is a low-level Vector type built upon a Bump allocator.
mod bump_vec;
pub use bump_vec::BumpVec;

// HeapNode may be directly deserialized using serde.
mod heap_de;

// We provide serde::Serialize covering all Doc implementations.
mod ser;
pub use ser::SerPolicy;

// All implementations of AsNode may be compared with one another.
mod compare;
pub use compare::compare;

// A JSON Pointer implementation that works with all AsNode implementations,
// and allows creation of documents using serde_json::Value and HeapNode.
pub mod ptr;
pub use ptr::Pointer;

// Extractor extracts locations from documents.
mod extractor;
pub use extractor::{Extractor, TRUNCATION_INDICATOR_PTR};

// Walker is a medium-term integration joint between AsNode implementations
// and our JSON-schema validator. We may seek to get rid of this and have
// JSON-schema validation evaluate directly over AsNode.
pub mod walker;

// Optimized conversions from AsNode implementations into HeapNode.
pub mod lazy;
pub use lazy::LazyNode;

// OwnedNode owns its HeapNode or ArchivedNode.
mod owned;
pub use owned::{OwnedArchivedNode, OwnedHeapNode, OwnedNode};

// JSON-schema annotation extensions supported by Flow documents.
mod annotation;
pub use annotation::Annotation;

// Validation is a higher-order API for driving JSON-schema validation
// over AsNode implementations.
pub mod validation;
pub use validation::{
    FailedValidation, RawValidator, Schema, SchemaIndex, SchemaIndexBuilder, Valid, Validation,
    Validator,
};

// Doc implementations may be reduced.
pub mod reduce;

// Doc implementations may be transformed.
pub mod transform;

// Documents may be combined.
#[cfg(feature = "combine")]
pub mod combine;
#[cfg(feature = "combine")]
pub use combine::Combiner;

// Shape is a description of the valid shapes that a document may take.
// It's similar to (and built from) a JSON Schema, but includes only
// those inferences which can be statically proven for all documents.
pub mod shape;
pub use shape::Shape;

// Fancy diff support for documents.
pub mod diff;
pub use diff::diff;

#[cfg(test)]
mod test {

    use super::{ArchivedNode, BumpStr, BumpVec, HeapNode, Node, SerPolicy};
    use serde_json::json;

    #[test]
    fn test_round_trip() {
        let fixture = json!({
            "numbers": [ 0x1111111111111111 as u64, -1234, 56.7891122334455],
            "shared string": "shared string",
            "some": {"bytes":"c29tZSBieXRlcw=="},
            "null": null,
            "nested": {
                "true": true,
                "false": false,
                "two": 2,
                "shared string": {"shared string": "shared string"},
            },
            "big string": "a bigger string",
            "small string": "smol",
            // Key which cannot be borrowed upon deserialization.
            "key\nwith\t\"escapes\"": "escapey\\value\\is\"escaping",
            "": "empty property"
        });

        // Deserialize from bytes to exercise deserialization escapes.
        let fixture_bytes = fixture.to_string().into_bytes();
        let mut fixture_de = serde_json::Deserializer::from_slice(&fixture_bytes);

        // We can deserialize into a HeapNode.
        let alloc = HeapNode::new_allocator();
        let doc = HeapNode::from_serde(&mut fixture_de, &alloc).unwrap();
        insta::assert_debug_snapshot!(doc);

        // The document can be archived with a stable byte layout.
        let archive_buf = doc.to_archive();

        let archive_buf_hexdump = hexdump::hexdump_iter(&archive_buf)
            .map(|line| format!("{line}"))
            .collect::<Vec<_>>()
            .join("\n");
        insta::assert_snapshot!(archive_buf_hexdump);

        // We can directly serialize an ArchivedNode into a serde_json::Value,
        // which exactly matches our original fixture.
        let archived_doc = ArchivedNode::from_archive(&archive_buf);
        let recovered = serde_json::to_value(SerPolicy::noop().on(archived_doc)).unwrap();
        assert_eq!(fixture, recovered);

        // The live document also serializes to an identical Value.
        let recovered = serde_json::to_value(SerPolicy::noop().on(&doc)).unwrap();
        assert_eq!(fixture, recovered);

        // A serde_json::Value can also be serialized as an AsNode.
        let recovered = serde_json::to_value(SerPolicy::noop().on(&fixture)).unwrap();
        assert_eq!(fixture, recovered);

        // Confirm number of bump-allocated bytes doesn't regress.
        assert_eq!(alloc.allocated_bytes(), 3392);
    }

    #[test]
    fn test_data_serialization() {
        let alloc = bumpalo::Bump::new();
        let doc = HeapNode::Bytes(super::BumpVec::from_slice(&[8, 6, 7, 5, 3, 0, 9], &alloc));
        let human_doc = serde_json::to_value(SerPolicy::noop().on(&doc)).unwrap();

        insta::assert_debug_snapshot!(human_doc, @r###"String("CAYHBQMACQ==")"###);
    }

    #[test]
    fn test_sizes() {
        // HeapNode is about as efficient as it can be, considering it's an enum
        // with many variants, most of which are 8-byte aligned.
        assert_eq!(std::mem::size_of::<HeapNode<'static>>(), 16);

        // String references are "fat" pointers which is why we don't use them.
        // If we did, it would increase wasted space by 33%.
        assert_eq!(std::mem::size_of::<&str>(), 16);

        pub enum NaiveStr<'a> {
            _String(&'a str),
            _XXX(bool),
            _YYY(u64),
        }
        assert_eq!(std::mem::size_of::<NaiveStr<'static>>(), 24);
        assert_eq!(std::mem::align_of::<NaiveStr<'static>>(), 8);

        // Instead, BumpStr is 8 bytes.
        assert_eq!(std::mem::size_of::<BumpStr>(), 8);

        // bumpalo's Vec type, the obvious alternative to BumpVec, is worse:
        assert_eq!(std::mem::size_of::<bumpalo::collections::Vec<bool>>(), 32);

        pub enum NaiveVec<'a> {
            _Vec(bumpalo::collections::Vec<'a, bool>),
            _XXX(bool),
            _YYY(u64),
        }
        assert_eq!(std::mem::size_of::<NaiveVec<'static>>(), 40); // Ouch!
        assert_eq!(std::mem::align_of::<NaiveVec<'static>>(), 8);

        // Instead, BumpVec is 8 bytes.
        assert_eq!(std::mem::size_of::<BumpVec<bool>>(), 8);

        // Node is 24 bytes.
        assert_eq!(std::mem::size_of::<Node<'static, HeapNode<'static>>>(), 24);
        assert_eq!(std::mem::align_of::<Node<'static, HeapNode<'static>>>(), 8);
    }
}
