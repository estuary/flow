use super::{
    AsNode, BumpStr, BumpVec, FailedValidation, Field, Fields, HeapField, HeapNode, Node, Valid,
    Validator,
};

/// LazyNode is either a HeapNode, or is an AsNode which may be promoted to a HeapNode.
pub enum LazyNode<'alloc, 'n, N: AsNode> {
    Node(&'n N),
    Heap(HeapNode<'alloc>),
}

/// LazyArray is either a [AsNode] slice, or is a vec of HeapNode.
pub enum LazyArray<'alloc, 'n, N: AsNode> {
    Node(&'n [N]),
    Heap(BumpVec<'alloc, HeapNode<'alloc>>),
}

/// LazyObject is either an AsNode::Fields, or is a vec of HeapField.
pub enum LazyObject<'alloc, 'n, N: AsNode + 'n> {
    Node(&'n N::Fields),
    Heap(BumpVec<'alloc, HeapField<'alloc>>),
}

/// LazyDestructured is an unpacked Node or HeapNode.
pub enum LazyDestructured<'alloc, 'n, N: AsNode> {
    Array(LazyArray<'alloc, 'n, N>),
    ScalarNode(Node<'n, N>),
    ScalarHeap(HeapNode<'alloc>),
    Object(LazyObject<'alloc, 'n, N>),
}

/// LazyField is either an AsNode::Fields::Field, or is a HeapField.
pub enum LazyField<'alloc, 'n, N: AsNode + 'n> {
    Node(<N::Fields as Fields<N>>::Field<'n>),
    Heap(HeapField<'alloc>),
}

impl<'alloc> HeapNode<'alloc> {
    // from_node builds a HeapNode from another AsNode implementation.
    pub fn from_node<N: AsNode>(node: &N, alloc: &'alloc bumpalo::Bump) -> Self {
        match node.as_node() {
            Node::Array(arr) => {
                let mut vec = BumpVec::with_capacity_in(arr.len(), alloc);
                vec.extend(arr.iter().map(|item| Self::from_node(item, alloc)), alloc);
                HeapNode::Array(vec)
            }
            Node::Bool(b) => HeapNode::Bool(b),
            Node::Bytes(b) => HeapNode::Bytes(BumpVec::from_slice(b, alloc)),
            Node::Null => HeapNode::Null,
            Node::Float(n) => HeapNode::Float(n),
            Node::PosInt(n) => HeapNode::PosInt(n),
            Node::NegInt(n) => HeapNode::NegInt(n),
            Node::Object(fields) => {
                let mut vec = BumpVec::with_capacity_in(fields.len(), alloc);
                vec.extend(
                    fields.iter().map(|field| HeapField {
                        property: BumpStr::from_str(field.property(), alloc),
                        value: Self::from_node(field.value(), alloc),
                    }),
                    alloc,
                );
                HeapNode::Object(vec)
            }
            Node::String(s) => HeapNode::String(BumpStr::from_str(s, alloc)),
        }
    }
}

impl<'alloc, 'n, N: AsNode> LazyNode<'alloc, 'n, N> {
    pub fn unwrap_node(self) -> &'n N {
        match self {
            Self::Node(n) => n,
            Self::Heap(_) => panic!("not a LazyNode::Node"),
        }
    }

    pub fn unwrap_heap(self) -> HeapNode<'alloc> {
        match self {
            Self::Node(_) => panic!("not a LazyNode::Heap"),
            Self::Heap(n) => n,
        }
    }

    pub fn into_heap_node(self, alloc: &'alloc bumpalo::Bump) -> HeapNode<'alloc> {
        match self {
            Self::Node(doc) => HeapNode::from_node(doc, alloc),
            Self::Heap(doc) => doc,
        }
    }

    pub fn destructure(self) -> LazyDestructured<'alloc, 'n, N> {
        match self {
            Self::Node(doc) => match doc.as_node() {
                Node::Array(arr) => LazyDestructured::Array(LazyArray::Node(arr)),
                Node::Object(fields) => LazyDestructured::Object(LazyObject::Node(fields)),
                doc @ _ => LazyDestructured::ScalarNode(doc),
            },
            Self::Heap(HeapNode::Array(arr)) => LazyDestructured::Array(LazyArray::Heap(arr)),
            Self::Heap(HeapNode::Object(fields)) => {
                LazyDestructured::Object(LazyObject::Heap(fields))
            }
            Self::Heap(doc) => LazyDestructured::ScalarHeap(doc),
        }
    }

    /// validate_ok is a convenience which validates a wrapped HeapNode or
    /// AsNode and then attempts to extract a Valid outcome. This is helpful
    /// because a Validation is generic over the AsNode type but Valid erases
    /// it, allowing for single-path handle for the Self::Heap and Self::Node cases.
    pub fn validate_ok<'doc, 'v>(
        &'doc self,
        validator: &'v mut Validator,
        schema: Option<&'v url::Url>,
    ) -> Result<Result<Valid<'static, 'v>, FailedValidation>, json::schema::index::Error> {
        match self {
            Self::Heap(n) => Ok(validator.validate(schema, n)?.ok()),
            Self::Node(n) => Ok(validator.validate(schema, *n)?.ok()),
        }
    }
}

impl<'alloc, 'n, N: AsNode> LazyDestructured<'alloc, 'n, N> {
    /// restructure the LazyDestructured into either a HeapNode or Node.
    pub fn restructure(self) -> Result<HeapNode<'alloc>, Node<'n, N>> {
        match self {
            Self::Array(LazyArray::Node(arr)) => Err(Node::Array(arr)),
            Self::Array(LazyArray::Heap(arr)) => Ok(HeapNode::Array(arr)),
            Self::Object(LazyObject::Node(fields)) => Err(Node::Object(fields)),
            Self::Object(LazyObject::Heap(fields)) => Ok(HeapNode::Object(fields)),
            Self::ScalarNode(doc) => Err(doc),
            Self::ScalarHeap(doc) => Ok(doc),
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
            Self::Heap(arr) => (None, Some(arr.into_iter().map(LazyNode::Heap))),
        };
        it1.into_iter().flatten().chain(it2.into_iter().flatten())
    }
}

impl<'alloc, 'n, N: AsNode> LazyObject<'alloc, 'n, N> {
    pub fn len(&self) -> usize {
        match self {
            Self::Node(fields) => fields.len(),
            Self::Heap(fields) => fields.len(),
        }
    }

    pub fn into_iter(self) -> impl Iterator<Item = LazyField<'alloc, 'n, N>> {
        let (it1, it2) = match self {
            Self::Node(fields) => (Some(fields.iter().map(LazyField::Node)), None),
            Self::Heap(fields) => (None, Some(fields.into_iter().map(LazyField::Heap))),
        };
        it1.into_iter().flatten().chain(it2.into_iter().flatten())
    }
}

impl<'alloc, 'n, N: AsNode> LazyField<'alloc, 'n, N> {
    pub fn property(&self) -> &str {
        match self {
            LazyField::Node(field) => field.property(),
            LazyField::Heap(field) => field.property(),
        }
    }

    pub fn into_heap_field(self, alloc: &'alloc bumpalo::Bump) -> HeapField<'alloc> {
        match self {
            Self::Node(field) => HeapField {
                property: BumpStr::from_str(field.property(), alloc),
                value: HeapNode::from_node(field.value(), alloc),
            },
            Self::Heap(field) => field,
        }
    }

    /// into_parts returns the separate property and value components of the field.
    /// The property is returned as a Result to reflect its borrowed or owned nature.
    pub fn into_parts(self) -> (Result<&'n str, BumpStr<'alloc>>, LazyNode<'alloc, 'n, N>) {
        match self {
            Self::Node(field) => (Ok(field.property()), LazyNode::Node(field.value())),
            Self::Heap(field) => (Err(field.property), LazyNode::Heap(field.value)),
        }
    }
}
