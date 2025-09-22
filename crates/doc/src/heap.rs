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
    pub fn from_node<N: AsNode>(node: &N, alloc: &'alloc bumpalo::Bump) -> Self {
        Self::from_node_with_length(node, alloc).0
    }

    // Recursively clone the argument AsNode into a HeapNode, also returning its tape length.
    pub fn from_node_with_length<N: AsNode>(node: &N, alloc: &'alloc bumpalo::Bump) -> (Self, i32) {
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
}

impl<'alloc> AsNode for HeapNode<'alloc> {
    type Fields = [HeapField<'alloc>];

    // We *always* want this inline, because the caller will next match
    // over our returned Node, and (when inline'd) the optimizer can
    // collapse the chained `match` blocks into one.
    #[inline(always)]
    fn as_node<'a>(&'a self) -> Node<'a, Self> {
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

impl<'alloc> Fields<HeapNode<'alloc>> for [HeapField<'alloc>] {
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
