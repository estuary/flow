use super::{AsNode, Field, Fields, Node};

/// HeapDoc is a document representation stored in the heap.
#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
#[archive(archived = "ArchivedDoc")]
pub struct HeapDoc<'alloc> {
    /// Root node of the document.
    pub root: HeapNode<'alloc>,
    /// Arbitrary flags used to persist document processing status.
    pub flags: u8,
}

/// HeapNode is a document node representation stored in the heap.
// The additional archive bounds are required to satisfy the compiler due to
// the recursive nature of this structure. For more explanation see:
// https://github.com/rkyv/rkyv/blob/master/examples/json/src/main.rs
#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
#[archive(
    archived = "ArchivedNode",
    bound(
        serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer + rkyv::ser::SharedSerializeRegistry"
    )
)]
pub enum HeapNode<'alloc> {
    Array(#[omit_bounds] BumpVec<'alloc, HeapNode<'alloc>>),
    Bool(bool),
    Bytes(BumpVec<'alloc, u8>),
    Float(f64),
    NegInt(i64),
    Null,
    Object(BumpVec<'alloc, HeapField<'alloc>>),
    PosInt(u64),
    String(HeapString<'alloc>),
}

/// HeapField is a field representation stored in the heap.
#[derive(Debug, rkyv::Archive, rkyv::Serialize)]
#[archive(
    archived = "ArchivedField",
    bound(
        serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer + rkyv::ser::SharedSerializeRegistry"
    )
)]
pub struct HeapField<'alloc> {
    pub property: HeapString<'alloc>,
    #[omit_bounds]
    pub value: HeapNode<'alloc>,
}

/// HeapString is a string representation stored in the heap.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeapString<'alloc>(pub &'alloc str);

/// BumpVec is a generic Vec<T> that's bound to a bump allocator.
#[derive(Debug)]
pub struct BumpVec<'alloc, T: std::fmt::Debug>(pub bumpalo::collections::Vec<'alloc, T>);

impl<'alloc> HeapNode<'alloc> {
    // new_allocator builds a bumpalo::Bump allocator for use in building HeapNodes.
    // It's a trivial helper which can reduce type imports.
    pub fn new_allocator() -> bumpalo::Bump {
        bumpalo::Bump::new()
    }
}

impl<'alloc> AsNode for HeapNode<'alloc> {
    type Fields = [HeapField<'alloc>];

    fn as_node<'a>(&'a self) -> Node<'a, Self> {
        match self {
            HeapNode::Array(a) => Node::Array(a.0.as_slice()),
            HeapNode::Bool(b) => Node::Bool(*b),
            HeapNode::Bytes(b) => Node::Bytes(&b.0),
            HeapNode::Float(n) => Node::Number(json::Number::Float(*n)),
            HeapNode::NegInt(n) => Node::Number(json::Number::Signed(*n)),
            HeapNode::Null => Node::Null,
            HeapNode::Object(o) => Node::Object(o.0.as_slice()),
            HeapNode::PosInt(n) => Node::Number(json::Number::Unsigned(*n)),
            HeapNode::String(s) => Node::String(&s.0),
        }
    }
}

impl<'alloc> Fields<HeapNode<'alloc>> for [HeapField<'alloc>] {
    type Field<'a> = &'a HeapField<'alloc> where 'alloc: 'a;
    type Iter<'a> = std::slice::Iter<'a, HeapField<'alloc>> where 'alloc: 'a;

    fn get<'a>(&'a self, property: &str) -> Option<Self::Field<'a>> {
        match self.binary_search_by(|l| l.property.0.cmp(property)) {
            Ok(ind) => Some(&self[ind]),
            Err(_) => None,
        }
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn iter<'a>(&'a self) -> Self::Iter<'a> {
        self.iter()
    }
}

impl<'a, 'alloc> Field<'a, HeapNode<'alloc>> for &'a HeapField<'alloc> {
    fn property(&self) -> &'a str {
        &self.property.0
    }
    fn value(&self) -> &'a HeapNode<'alloc> {
        &self.value
    }
}

impl<'alloc, T: std::fmt::Debug> BumpVec<'alloc, T> {
    pub fn new(alloc: &'alloc bumpalo::Bump) -> Self {
        Self(bumpalo::collections::vec::Vec::new_in(alloc))
    }
    pub fn with_capacity_in(capacity: usize, alloc: &'alloc bumpalo::Bump) -> Self {
        Self(bumpalo::collections::vec::Vec::with_capacity_in(
            capacity, alloc,
        ))
    }
}

impl<'alloc> BumpVec<'alloc, HeapField<'alloc>> {
    /// Insert or obtain a mutable reference to a child HeapNode with the given property.
    pub fn insert_mut(&mut self, property: HeapString<'alloc>) -> &mut HeapNode<'alloc> {
        let ind = match self.0.binary_search_by(|l| l.property.0.cmp(&property.0)) {
            Ok(ind) => ind,
            Err(ind) => {
                self.0.insert(
                    ind,
                    HeapField {
                        property,
                        value: HeapNode::Null,
                    },
                );
                ind
            }
        };
        &mut self.0[ind].value
    }

    // Remove the named property, returning its removed HeapField if found.
    pub fn remove(&mut self, property: &str) -> Option<HeapField<'alloc>> {
        match self.0.binary_search_by(|l| l.property.0.cmp(property)) {
            Ok(ind) => Some(self.0.remove(ind)),
            Err(_) => None,
        }
    }
}
