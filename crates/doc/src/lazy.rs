use super::{
    AsNode, BumpStr, FailedValidation, Field, Fields, HeapField, HeapNode, Node, Valid, Validator,
};

/// LazyNode is either a HeapNode, or is an AsNode which may be promoted to a HeapNode.
pub enum LazyNode<'alloc, 'n, N: AsNode> {
    Node(&'n N),
    Heap(&'n HeapNode<'alloc>),
}

/// LazyArray is either a [AsNode] slice, or is a vec of HeapNode.
pub enum LazyArray<'alloc, 'n, N: AsNode> {
    Node(&'n [N]),
    Heap(&'n [HeapNode<'alloc>]),
}

/// LazyObject is either an AsNode::Fields, or is a vec of HeapField.
pub enum LazyObject<'alloc, 'n, N: AsNode> {
    Node(&'n N::Fields),
    Heap(&'n [HeapField<'alloc>]),
}

/// LazyField is either an AsNode::Fields::Field, or is a HeapField.
pub enum LazyField<'alloc, 'n, N: AsNode + 'n> {
    Node(<N::Fields as Fields<N>>::Field<'n>),
    Heap(&'n HeapField<'alloc>),
}

/// LazyDestructured is an unpacked Node or HeapNode.
pub enum LazyDestructured<'alloc, 'n, N: AsNode> {
    Array(LazyArray<'alloc, 'n, N>),
    ScalarNode(Node<'n, N>),
    ScalarHeap(&'n HeapNode<'alloc>),
    Object(LazyObject<'alloc, 'n, N>),
}

impl<'alloc, 'n, N: AsNode> LazyNode<'alloc, 'n, N> {
    // Map this LazyNode into an owned HeapNode, either by re-hydrating a non-heap Node,
    // or by taking a shallow copy of a HeapNode. Note that the returned HeapNode may
    // reference structure which is shared with other HeapNodes.
    // Returns both the HeapNode and its tape length.
    pub fn into_heap_node(self, alloc: &'alloc bumpalo::Bump) -> (HeapNode<'alloc>, i32) {
        match self {
            Self::Node(doc) => HeapNode::from_node_with_length(doc, alloc),
            Self::Heap(doc) => (Self::borrow(doc), doc.tape_length()),
        }
    }

    fn borrow(doc: &'n HeapNode<'alloc>) -> HeapNode<'alloc> {
        // Directly transmute &HeapNode into an owned HeapNode by bit-copy.
        //
        // Safety: LazyNode holds immutable references of documents being
        // built up using append-only reduction. By design, and enforced
        // through its immutability, all built up HeapNode structure is
        // allocated into new memory, though it may reference borrowed
        // HeapNodes without the possibility of mutating them.
        //
        // Recursive HeapNode references are exclusively backed by a shared
        // bump allocator having lifetime 'alloc, and are freed as a unit,
        // meaning a use-after-free due to disjoint lifetimes isn't possible.
        unsafe { std::mem::transmute_copy::<HeapNode, HeapNode>(&doc) }
    }

    pub fn destructure(&self) -> LazyDestructured<'alloc, 'n, N> {
        match self {
            Self::Node(doc) => match doc.as_node() {
                Node::Array(arr) => LazyDestructured::Array(LazyArray::Node(arr)),
                Node::Object(fields) => LazyDestructured::Object(LazyObject::Node(fields)),
                doc @ _ => LazyDestructured::ScalarNode(doc),
            },
            Self::Heap(HeapNode::Array(_tape_length, arr)) => {
                LazyDestructured::Array(LazyArray::Heap(arr.as_slice()))
            }
            Self::Heap(HeapNode::Object(_tape_length, fields)) => {
                LazyDestructured::Object(LazyObject::Heap(fields.as_slice()))
            }
            Self::Heap(doc) => LazyDestructured::ScalarHeap(doc),
        }
    }

    /// validate_ok is a convenience which validates a wrapped HeapNode or
    /// AsNode and then attempts to extract a Valid outcome. This is helpful
    /// because a Validation is generic over the AsNode type but Valid erases
    /// it, allowing for single-path handle for the Self::Heap and Self::Node cases.
    pub fn validate_ok<'v>(
        &self,
        validator: &'v mut Validator,
        schema: Option<&'v url::Url>,
    ) -> Result<Result<Valid<'static, 'v>, FailedValidation>, json::schema::index::Error> {
        match self {
            Self::Heap(n) => Ok(validator
                .validate(schema, *n)?
                .ok()
                .map_err(|invalid| invalid.revalidate_with_context(*n))),
            Self::Node(n) => Ok(validator
                .validate(schema, *n)?
                .ok()
                .map_err(|invalid| invalid.revalidate_with_context(*n))),
        }
    }

    pub fn tape_length(&self) -> i32 {
        match self {
            Self::Heap(n) => n.tape_length(),
            Self::Node(n) => n.tape_length(),
        }
    }
}

impl<'alloc, 'n, N: AsNode> LazyArray<'alloc, 'n, N> {
    pub fn len(&self) -> usize {
        match self {
            Self::Node(arr) => arr.len(),
            Self::Heap(arr) => arr.len(),
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = LazyNode<'alloc, 'n, N>> {
        let (it1, it2) = match self {
            Self::Node(arr) => (Some(arr.iter().map(|d| LazyNode::Node(d))), None),
            Self::Heap(arr) => (None, Some(arr.iter().map(LazyNode::Heap))),
        };
        it1.into_iter().flatten().chain(it2.into_iter().flatten())
    }
}

impl<'alloc, 'n, N: AsNode + 'n> LazyObject<'alloc, 'n, N> {
    pub fn len(&self) -> usize {
        match self {
            Self::Node(fields) => fields.len(),
            Self::Heap(fields) => fields.len(),
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = LazyField<'alloc, 'n, N>> {
        let (it1, it2) = match self {
            Self::Node(fields) => (Some(fields.iter().map(LazyField::Node)), None),
            Self::Heap(fields) => (None, Some(fields.iter().map(LazyField::Heap))),
        };
        it1.into_iter().flatten().chain(it2.into_iter().flatten())
    }
}

impl<'alloc, 'n, N: AsNode> LazyField<'alloc, 'n, N> {
    pub fn property(&self) -> &str {
        match self {
            LazyField::Node(field) => field.property(),
            LazyField::Heap(field) => &field.property,
        }
    }

    // Map this LazyField into an owned HeapField, either by re-hydrating a non-heap Field,
    // or by taking a shallow copy of a HeapField. Note that the returned HeapField may
    // reference structure which is shared with other HeapNodes.
    // Returns both the HeapField and the tape length of its value.
    pub fn into_heap_field(self, alloc: &'alloc bumpalo::Bump) -> (HeapField<'alloc>, i32) {
        match self {
            Self::Node(field) => {
                let (value, built_length) = HeapNode::from_node_with_length(field.value(), alloc);
                let field = HeapField {
                    property: BumpStr::from_str(field.property(), alloc),
                    value,
                };
                (field, built_length)
            }
            Self::Heap(HeapField { property, value }) => {
                let built_length = value.tape_length();
                let field = HeapField {
                    property: *property,
                    value: LazyNode::<N>::borrow(value),
                };
                (field, built_length)
            }
        }
    }

    /// into_parts returns the separate property and value components of the field.
    /// The property is returned as a Result to reflect its borrowed or owned nature.
    pub fn into_parts(self) -> (Result<&'n str, BumpStr<'alloc>>, LazyNode<'alloc, 'n, N>) {
        match self {
            Self::Node(field) => (Ok(field.property()), LazyNode::Node(field.value())),
            Self::Heap(field) => (Err(field.property), LazyNode::Heap(&field.value)),
        }
    }
}
