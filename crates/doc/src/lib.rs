/// Node is the generic form of a document node as understood by Flow.
/// It's implemented by HeapNode, ArchivedNode, and serde_json::Value.
#[derive(Debug)]
pub enum Node<'a, N: AsNode> {
    Array(&'a [N]),
    Bool(bool),
    Bytes(&'a [u8]),
    Null,
    Number(json::Number),
    Object(&'a N::Fields),
    String(&'a str),
}

/// AsNode is the trait by which a specific document representation is accessed through a generic Node.
pub trait AsNode: Sized {
    type Fields: Fields<Self> + ?Sized;

    fn as_node<'a>(&'a self) -> Node<'a, Self>;
}

/// Fields is the generic form of a Document object representation.
pub trait Fields<N: AsNode> {
    type Field<'a>: Field<'a, N>
    where
        Self: 'a;

    type Iter<'a>: Iterator<Item = Self::Field<'a>>
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

// This crate has three implementations of AsNode: a mutable HeapNode,
// an ArchivedNode serialized by the `rkyv` crate,
// and an implementation upon serde_json::Value.
mod archived;
pub use archived::{ArchivedDoc, ArchivedField, ArchivedNode};
pub mod heap;
pub use heap::{HeapDoc, HeapField, HeapNode};
mod value;

// Dedup de-duplicates strings used in the construction of HeapNodes.
// This also reduces the size of serialized ArchivedNodes, as the archival format
// stores one copy of each deduplicated string.
pub mod dedup;

// HeapNode may be directly deserialized using serde.
mod heap_de;

// We provide serde::Serialize covering all Doc implementations.
mod ser;

// All implementations of AsNode may be compared with one another.
mod compare;
pub use compare::compare;

// A JSON Pointer implementation that works with all AsNode implementations,
// and allows creation of documents using serde_json::Value and HeapNode.
pub mod ptr;
pub use ptr::Pointer;

// Walker is a medium-term integration joint between AsNode implementations
// and our JSON-schema validator. We may seek to get rid of this and have
// JSON-schema validation evaluate directly over AsNode.
pub mod walker;

// Optimized conversions from AsNode implementations into HeapNode.
pub mod lazy;
pub use lazy::LazyNode;

// JSON-schema annotation extensions supported by Flow documents.
mod annotation;
pub use annotation::Annotation;

// Validation is a higher-order API for driving JSON-schema validation
// over AsNode implementations.
pub mod validation;
pub use validation::{
    FailedValidation, Schema, SchemaIndex, SchemaIndexBuilder, Valid, Validation, Validator,
};

// Doc implementations may be reduced.
pub mod reduce;

// Documents may be combined.
pub mod combine;
pub use combine::Combiner;

// Nodes may be packed as FoundationDB tuples.
pub mod tuple_pack;

pub mod inference;

#[cfg(test)]
mod test {

    use super::{ArchivedNode, AsNode, HeapNode};
    use serde_json::json;

    #[test]
    fn test_round_trip() {
        let big_string = std::iter::repeat("a big string")
            .take(30)
            .collect::<Vec<_>>()
            .join(" ");

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
            "big string": big_string,
            "small string": "smol", // Not de-duplicated because it's so small.
        });

        // We can deserialize into a Doc.
        let alloc = HeapNode::new_allocator();
        let dedup = HeapNode::new_deduper(&alloc);
        let doc = HeapNode::from_serde(&fixture, &alloc, &dedup).unwrap();
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
        let recovered = serde_json::to_value(archived_doc.as_node()).unwrap();
        assert_eq!(fixture, recovered);

        // The live document also serializes to an identical Value.
        let recovered = serde_json::to_value(doc.as_node()).unwrap();
        assert_eq!(fixture, recovered);

        // A serde_json::Value can also be serialized as an AsNode.
        let recovered = serde_json::to_value(fixture.as_node()).unwrap();
        assert_eq!(fixture, recovered);

        // Confirm number of bump-allocated bytes doesn't regress.
        assert_eq!(alloc.allocated_bytes(), 1408);
    }

    #[test]
    fn test_data_serialization() {
        let bump = bumpalo::Bump::new();

        let doc = HeapNode::Bytes(super::heap::BumpVec(
            bumpalo::vec![in &bump; 8, 6, 7, 5, 3, 0, 9],
        ));
        let human_doc = serde_json::to_value(doc.as_node()).unwrap();

        insta::assert_debug_snapshot!(human_doc, @r###"String("CAYHBQMACQ==")"###);
    }
}
