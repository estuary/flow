use super::{BumpStr, BumpVec};
use json::{AsNode, Field, Fields};

/// HeapNode is a document node representation stored in the heap.
// The additional archive bounds are required to satisfy the compiler due to
// the recursive nature of this structure. For more explanation see:
// https://github.com/rkyv/rkyv/blob/master/examples/json/src/main.rs
#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
#[rkyv(
    archived = ArchivedNode,
    serialize_bounds(
        __S: rkyv::ser::Writer + rkyv::ser::Allocator,
        __S::Error: rkyv::rancor::Source,
    ),
    bytecheck(
        bounds(__C: rkyv::validation::ArchiveContext, <__C as rkyv::rancor::Fallible>::Error: rkyv::rancor::Source)
    )
)]
pub enum HeapNode<'alloc> {
    Array(i32, #[rkyv(omit_bounds)] BumpVec<'alloc, HeapNode<'alloc>>),
    Bool(bool),
    Bytes(BumpVec<'alloc, u8>),
    Float(f64),
    NegInt(i64),
    Null,
    Object(i32, BumpVec<'alloc, HeapField<'alloc>>),
    PosInt(u64),
    String(BumpStr<'alloc>),
}

/// HeapField is a field representation stored in the heap.
#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
#[rkyv(
    archived = ArchivedField,
    serialize_bounds(
        __S: rkyv::ser::Writer + rkyv::ser::Allocator,
        __S::Error: rkyv::rancor::Source,
    ),
    bytecheck(
        bounds(__C: rkyv::validation::ArchiveContext, <__C as rkyv::rancor::Fallible>::Error: rkyv::rancor::Source)
    )
)]
pub struct HeapField<'alloc> {
    pub property: BumpStr<'alloc>,
    #[rkyv(omit_bounds)]
    pub value: HeapNode<'alloc>,
}

impl<'alloc> HeapNode<'alloc> {
    // new_allocator builds a bumpalo::Bump allocator for use in building HeapNodes.
    // It's a trivial helper which can reduce type imports.
    pub fn new_allocator() -> bumpalo::Bump {
        Self::allocator_with_capacity(0)
    }

    // allocator_with_capacity builds a bumpalo::Bump allocator with the designated capacity.
    pub fn allocator_with_capacity(capacity: usize) -> bumpalo::Bump {
        bumpalo::Bump::with_capacity(capacity)
    }

    // Recursively clone the argument AsNode into a HeapNode.
    #[inline]
    pub fn from_node<N: json::AsNode>(node: &N, alloc: &'alloc bumpalo::Bump) -> Self {
        Self::from_node_with_length(node, alloc).0
    }

    // Recursively clone the argument AsNode into a HeapNode, also returning its tape length.
    pub fn from_node_with_length<N: json::AsNode>(
        node: &N,
        alloc: &'alloc bumpalo::Bump,
    ) -> (Self, i32) {
        use json::Node;
        match node.as_node() {
            Node::Array(arr) => {
                let mut built_length = 1;
                let items = BumpVec::with_contents(
                    alloc,
                    arr.iter().map(|item| {
                        let (item, child_delta) = Self::from_node_with_length(item, alloc);
                        built_length += child_delta;
                        item
                    }),
                );
                (HeapNode::Array(built_length, items), built_length)
            }
            Node::Bool(b) => (HeapNode::Bool(b), 1),
            Node::Bytes(b) => (HeapNode::Bytes(BumpVec::from_slice(b, alloc)), 1),
            Node::Null => (HeapNode::Null, 1),
            Node::Float(n) => (HeapNode::Float(n), 1),
            Node::PosInt(n) => (HeapNode::PosInt(n), 1),
            Node::NegInt(n) => (HeapNode::NegInt(n), 1),
            Node::Object(fields) => {
                let mut built_length = 1;
                let fields = BumpVec::with_contents(
                    alloc,
                    fields.iter().map(|field| {
                        let (value, child_delta) =
                            Self::from_node_with_length(field.value(), alloc);
                        built_length += child_delta;

                        HeapField {
                            property: BumpStr::from_str(field.property(), alloc),
                            value,
                        }
                    }),
                );
                (HeapNode::Object(built_length, fields), built_length)
            }
            Node::String(s) => (HeapNode::String(BumpStr::from_str(s, alloc)), 1),
        }
    }

    pub fn new_array<I>(alloc: &'alloc bumpalo::Bump, iter: I) -> Self
    where
        I: ExactSizeIterator<Item = HeapNode<'alloc>>,
    {
        let items = BumpVec::with_contents(alloc, iter);
        let built_length = 1 + items.iter().map(|i| i.tape_length()).sum::<i32>();
        HeapNode::Array(built_length, items)
    }

    pub fn new_object<I>(alloc: &'alloc bumpalo::Bump, iter: I) -> Self
    where
        I: ExactSizeIterator<Item = HeapField<'alloc>>,
    {
        let fields = BumpVec::with_contents(alloc, iter);
        let built_length = 1 + fields.iter().map(|f| f.value.tape_length()).sum::<i32>();
        HeapNode::Object(built_length, fields)
    }

    /// Try to set `value` at the designated Pointer within this HeapNode,
    /// creating intermediate objects and arrays along the way as necessary.
    /// Returns Ok on success with the tape-length delta, or Err if unable to
    /// set `value`, also with the tape-length delta.
    /// Note this routine may modify self even if the operation fails
    /// due to introductions of intermediate nodes.
    pub fn try_set(
        self: &mut Self,
        ptr: &json::Pointer,
        value: Self,
        alloc: &'alloc bumpalo::Bump,
    ) -> Result<i32, i32> {
        use json::ptr::Token;

        let mut tail = ptr.0.as_slice();
        let mut stack = Vec::new();
        let mut node = self;

        let (matched, mut built_delta) = loop {
            let Some((token, new_tail)) = tail.split_first() else {
                // Base case: replace `node` with `value`.
                let built_delta = value.tape_length() - node.tape_length();
                *node = value;
                break (true, built_delta);
            };
            tail = new_tail;

            // If the current value is null but more tokens remain in the pointer,
            // instantiate it as an object or array (depending on token type) into
            // which we'll create the next child location.
            if let HeapNode::Null = node {
                match token {
                    Token::Property(_) => {
                        *node = HeapNode::Object(1, BumpVec::new());
                    }
                    Token::Index(_) => {
                        *node = HeapNode::Array(1, BumpVec::new());
                    }
                    Token::NextProperty | Token::NextIndex => break (false, 0),
                };
            };

            match node {
                HeapNode::Object(tape_length, fields) => {
                    let property = match token {
                        Token::Index(ind) => BumpStr::from_str(&ind.to_string(), alloc),
                        Token::Property(property) => BumpStr::from_str(property, alloc),
                        Token::NextProperty | Token::NextIndex => break (false, 0),
                    };

                    let (local_delta, index) =
                        match fields.binary_search_by(|l| l.property.cmp(&property)) {
                            Ok(index) => (0i32, index),
                            Err(index) => {
                                let value = HeapField {
                                    property,
                                    value: HeapNode::Null,
                                };
                                fields.insert(index, value, alloc);
                                (1, index)
                            }
                        };

                    stack.push((tape_length, local_delta));
                    node = &mut fields[index].value
                }
                HeapNode::Array(tape_length, items) => {
                    let index = match token {
                        Token::Index(index) => *index,
                        Token::NextIndex => items.len(),
                        Token::NextProperty | Token::Property(_) => break (false, 0),
                    };
                    // Create any required indices [0..ind) as HeapNode::Null.
                    let local_delta = (1 + index).saturating_sub(items.len());
                    items.extend(
                        std::iter::repeat_with(|| HeapNode::Null).take(local_delta),
                        alloc,
                    );

                    stack.push((tape_length, local_delta as i32));
                    node = &mut items[index]
                }
                HeapNode::Bool(_)
                | HeapNode::Bytes(_)
                | HeapNode::Float(_)
                | HeapNode::NegInt(_)
                | HeapNode::PosInt(_)
                | HeapNode::String(_) => {
                    break (false, 0);
                }
                HeapNode::Null => unreachable!("null already handled"),
            };
        };

        // Walk back up the stack, adjusting tape lengths as we go.
        for (tape_length, local_delta) in stack.into_iter().rev() {
            built_delta += local_delta;
            *tape_length += built_delta;
        }

        matched.then_some(built_delta).ok_or(built_delta)
    }
}

impl<'alloc> json::AsNode for HeapNode<'alloc> {
    type Fields = [HeapField<'alloc>];

    // We *always* want this inline, because the caller will next match
    // over our returned Node, and (when inline'd) the optimizer can
    // collapse the chained `match` blocks into one.
    #[inline(always)]
    fn as_node<'a>(&'a self) -> json::Node<'a, Self> {
        use json::Node;
        match self {
            HeapNode::Array(_tape_length, a) => Node::Array(a),
            HeapNode::Bool(b) => Node::Bool(*b),
            HeapNode::Bytes(b) => Node::Bytes(b),
            HeapNode::Float(n) => Node::Float(*n),
            HeapNode::NegInt(n) => Node::NegInt(*n),
            HeapNode::Null => Node::Null,
            HeapNode::Object(_tape_length, o) => Node::Object(o.as_slice()),
            HeapNode::PosInt(n) => Node::PosInt(*n),
            HeapNode::String(s) => Node::String(s),
        }
    }
    #[inline]
    fn tape_length(&self) -> i32 {
        match self {
            HeapNode::Array(tape_length, _) => *tape_length,
            HeapNode::Object(tape_length, _) => *tape_length,
            _ => 1,
        }
    }
}

impl<'alloc> json::Fields<HeapNode<'alloc>> for [HeapField<'alloc>] {
    type Field<'a>
        = &'a HeapField<'alloc>
    where
        'alloc: 'a;
    type Iter<'a>
        = std::slice::Iter<'a, HeapField<'alloc>>
    where
        'alloc: 'a;

    #[inline]
    fn get<'a>(&'a self, property: &str) -> Option<Self::Field<'a>> {
        match self.binary_search_by(|l| l.property.cmp(property)) {
            Ok(ind) => Some(&self[ind]),
            Err(_) => None,
        }
    }
    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
    #[inline]
    fn iter<'a>(&'a self) -> Self::Iter<'a> {
        self.iter()
    }
}

impl<'a, 'alloc> json::Field<'a, HeapNode<'alloc>> for &'a HeapField<'alloc> {
    #[inline(always)]
    fn property(&self) -> &'a str {
        &self.property
    }
    #[inline(always)]
    fn value(&self) -> &'a HeapNode<'alloc> {
        &self.value
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::ArchivedNode;
    use json::node::compare;
    use json::Pointer;
    use serde_json::json;
    use std::cmp::Ordering;

    #[test]
    fn test_ptr_query() {
        // Extended document fixture from RFC-6901.
        let doc = json!({
            "foo": ["bar", "baz"],
            "": 0,
            "a/b": 1,
            "c%d": 2,
            "e^f": 3,
            "g|h": 4,
            "i\\j": 5,
            "k\"l": 6,
            " ": 7,
            "m~n": 8,
            "9": 10,
            "-": 11,
        });

        let alloc = HeapNode::new_allocator();
        let heap_doc = HeapNode::from_serde(&doc, &alloc).unwrap();

        let archive = heap_doc.to_archive();
        let arch_doc = ArchivedNode::from_archive(&archive);

        // Query document locations which exist (cases from RFC-6901).
        for case in [
            ("", json!(doc)),
            ("/foo", json!(["bar", "baz"])),
            ("/foo/0", json!("bar")),
            ("/foo/1", json!("baz")),
            ("/", json!(0)),
            ("/a~1b", json!(1)),
            ("/c%d", json!(2)),
            ("/e^f", json!(3)),
            ("/g|h", json!(4)),
            ("/i\\j", json!(5)),
            ("/k\"l", json!(6)),
            ("/ ", json!(7)),
            ("/m~0n", json!(8)),
            ("/9", json!(10)),
            ("/-", json!(11)),
        ]
        .iter()
        {
            let ptr = Pointer::from(case.0);

            assert_eq!(
                compare(ptr.query(&heap_doc).unwrap(), &case.1),
                Ordering::Equal
            );
            assert_eq!(
                compare(ptr.query(arch_doc).unwrap(), &case.1),
                Ordering::Equal
            );
        }

        // Locations which don't exist.
        for case in [
            "/bar",      // Missing property.
            "/foo/2",    // Missing index.
            "/foo/prop", // Cannot take property of array.
            "/e^f/3",    // Not an object or array.
        ]
        .iter()
        {
            let ptr = Pointer::from(*case);
            assert!(ptr.query(&heap_doc).is_none());
            assert!(ptr.query(arch_doc).is_none());
        }
    }

    #[test]
    fn test_ptr_create() {
        // Modify a Null root by applying a succession of upserts.
        let alloc = HeapNode::new_allocator();
        let mut root_heap_doc = HeapNode::Null;

        for (ptr, value, expect_delta) in [
            // Creates Object root, Array at /foo, and Object at /foo/2.
            ("/foo/2/a", json!("hello"), 5), // Creates: root obj + foo array + 2 nulls + obj at [2] + "hello"
            // Add property to existing object.
            ("/foo/2/b", json!(3), 1),   // Adds one property value
            ("/foo/0", json!(false), 0), // Update existing Null (both have tape_length = 1).
            ("/bar", json!(null), 1),    // Add property to doc root (adds null).
            ("/foo/0", json!(true), 0),  // Update from 'false' (both have tape_length = 1).
            // Index token is interpreted as property because object exists.
            ("/foo/2/4", json!(5), 1), // Adds one property value
            // NextIndex token is also interpreted as property.
            ("/foo/2/-", json!(false), 1), // Adds one property value
        ]
        .iter_mut()
        {
            let ptr = Pointer::from(ptr);
            let child = HeapNode::from_serde(&*value, &alloc).unwrap();

            let built_delta = root_heap_doc.try_set(&ptr, child, &alloc).unwrap();
            assert_eq!(built_delta, *expect_delta);
        }

        let expect = json!({
            "foo": [true, null, {"-": false, "a": "hello", "b": 3, "4": 5}],
            "bar": null,
        });

        assert_eq!(compare(&root_heap_doc, &expect), Ordering::Equal);

        // Verify correct tape lengths at interesting locations within the tree.
        for (ptr, length) in [("", 10), ("/foo", 8), ("/foo/2", 5)] {
            let ptr = Pointer::from(ptr);
            assert_eq!(ptr.query(&expect).unwrap().tape_length(), length);
            assert_eq!(ptr.query(&root_heap_doc).unwrap().tape_length(), length);
        }

        // Cases which return None.
        for case in [
            "/foo/2/a/3", // Attempt to index string scalar.
            "/foo/bar",   // Attempt to take property of array.
            "/foo/-",     // Attempt to take property of array
        ]
        .iter()
        {
            let ptr = Pointer::from(*case);

            assert!(root_heap_doc.try_set(&ptr, HeapNode::Null, &alloc).is_err());
        }
    }
}
