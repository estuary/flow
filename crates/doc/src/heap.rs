use super::{AsNode, BumpStr, BumpVec, Field, Fields, Node};

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
    Array(#[rkyv(omit_bounds)] BumpVec<'alloc, HeapNode<'alloc>>),
    Bool(bool),
    Bytes(BumpVec<'alloc, u8>),
    Float(f64),
    NegInt(i64),
    Null,
    Object(BumpVec<'alloc, HeapField<'alloc>>),
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

    // from_node builds a HeapNode from another AsNode implementation.
    pub fn from_node<N: AsNode>(node: &N, alloc: &'alloc bumpalo::Bump) -> Self {
        match node.as_node() {
            Node::Array(arr) => HeapNode::Array(BumpVec::with_contents(
                alloc,
                arr.iter().map(|item| Self::from_node(item, alloc)),
            )),
            Node::Bool(b) => HeapNode::Bool(b),
            Node::Bytes(b) => HeapNode::Bytes(BumpVec::from_slice(b, alloc)),
            Node::Null => HeapNode::Null,
            Node::Float(n) => HeapNode::Float(n),
            Node::PosInt(n) => HeapNode::PosInt(n),
            Node::NegInt(n) => HeapNode::NegInt(n),
            Node::Object(fields) => HeapNode::Object(BumpVec::with_contents(
                alloc,
                fields.iter().map(|field| HeapField {
                    property: BumpStr::from_str(field.property(), alloc),
                    value: Self::from_node(field.value(), alloc),
                }),
            )),
            Node::String(s) => HeapNode::String(BumpStr::from_str(s, alloc)),
        }
    }
}

impl<'alloc> AsNode for HeapNode<'alloc> {
    type Fields = [HeapField<'alloc>];

    // We *always* want this inline, because the caller will next match
    // over our returned Node, and (when inline'd) the optimizer can
    // collapse the chained `match` blocks into one.
    #[inline(always)]
    fn as_node<'a>(&'a self) -> Node<'a, Self> {
        match self {
            HeapNode::Array(a) => Node::Array(a),
            HeapNode::Bool(b) => Node::Bool(*b),
            HeapNode::Bytes(b) => Node::Bytes(b),
            HeapNode::Float(n) => Node::Float(*n),
            HeapNode::NegInt(n) => Node::NegInt(*n),
            HeapNode::Null => Node::Null,
            HeapNode::Object(o) => Node::Object(o.as_slice()),
            HeapNode::PosInt(n) => Node::PosInt(*n),
            HeapNode::String(s) => Node::String(s),
        }
    }
}

impl<'alloc> Fields<HeapNode<'alloc>> for [HeapField<'alloc>] {
    type Field<'a>
        = &'a HeapField<'alloc>
    where
        'alloc: 'a;
    type Iter<'a>
        = std::slice::Iter<'a, HeapField<'alloc>>
    where
        'alloc: 'a;

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

impl<'a, 'alloc> Field<'a, HeapNode<'alloc>> for &'a HeapField<'alloc> {
    #[inline(always)]
    fn property(&self) -> &'a str {
        &self.property
    }
    #[inline(always)]
    fn value(&self) -> &'a HeapNode<'alloc> {
        &self.value
    }
}

impl<'alloc> BumpVec<'alloc, HeapField<'alloc>> {
    /// Insert or obtain a mutable reference to a child HeapNode with the given property.
    pub fn insert_property(
        &mut self,
        property: &str,
        alloc: &'alloc bumpalo::Bump,
    ) -> &mut HeapNode<'alloc> {
        let ind = match self.binary_search_by(|l| l.property.cmp(property)) {
            Ok(ind) => ind,
            Err(ind) => {
                self.insert(
                    ind,
                    HeapField {
                        property: BumpStr::from_str(property, alloc),
                        value: HeapNode::Null,
                    },
                    alloc,
                );
                ind
            }
        };
        &mut self[ind].value
    }

    // Remove the named property, returning its removed HeapField if found.
    pub fn remove_property(&mut self, property: &str) -> Option<HeapField<'alloc>> {
        match self.binary_search_by(|l| l.property.cmp(property)) {
            Ok(ind) => Some(self.remove(ind)),
            Err(_) => None,
        }
    }
}
