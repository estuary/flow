/// Node is the fundamental representation of a JSON document node.
/// It's implemented by serde_json::Value, doc::HeapNode, and doc::ArchivedNode.
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

    /// Return the total number of nodes contained within this node, inclusive.
    /// Under a "tape" interpretation of a document, this is the total number
    /// of entries utilized by this node and all its children.
    /// This number is always positive, but is a signed type to facilitate easy
    /// calculation of tape-length deltas.
    fn tape_length(&self) -> i32;
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

// Implementation for serde_json::Value
impl AsNode for serde_json::Value {
    type Fields = serde_json::Map<String, serde_json::Value>;

    // Not inline(always) because Value is infrequently used.
    #[inline]
    fn as_node<'a>(&'a self) -> Node<'a, Self> {
        match self {
            Self::Array(a) => Node::Array(a),
            Self::Bool(b) => Node::Bool(*b),
            Self::Null => Node::Null,
            Self::Number(n) => {
                if let Some(n) = n.as_u64() {
                    Node::PosInt(n)
                } else if let Some(n) = n.as_i64() {
                    Node::NegInt(n)
                } else {
                    Node::Float(n.as_f64().unwrap())
                }
            }
            Self::Object(o) => Node::Object(o),
            Self::String(s) => Node::String(s),
        }
    }
    fn tape_length(&self) -> i32 {
        match self {
            Self::Array(a) => 1 + a.iter().map(|v| v.tape_length()).sum::<i32>(),
            Self::Object(o) => 1 + o.iter().map(|(_, v)| v.tape_length()).sum::<i32>(),
            _ => 1,
        }
    }
}

impl Fields<serde_json::Value> for serde_json::Map<String, serde_json::Value> {
    type Field<'a> = (&'a String, &'a serde_json::Value);
    type Iter<'a> = serde_json::map::Iter<'a>;

    fn get<'a>(&'a self, property: &str) -> Option<Self::Field<'a>> {
        <serde_json::Map<String, serde_json::Value>>::get_key_value(self, property)
    }

    fn len(&self) -> usize {
        <serde_json::Map<String, serde_json::Value>>::len(self)
    }

    fn iter<'a>(&'a self) -> Self::Iter<'a> {
        <serde_json::Map<String, serde_json::Value>>::iter(self)
    }
}

impl<'a> Field<'a, serde_json::Value> for (&'a String, &'a serde_json::Value) {
    #[inline]
    fn property(&self) -> &'a str {
        self.0
    }
    #[inline]
    fn value(&self) -> &'a serde_json::Value {
        self.1
    }
}
