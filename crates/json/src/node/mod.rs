mod compare;
mod value; // Implement AsNode for serde_json::Value.

pub use compare::{compare, compare_node};

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

/// Fields is the trait by which fields of an object representation are accessed.
pub trait Fields<N: AsNode> {
    type Field<'a>: Field<'a, N>
    where
        Self: 'a;

    // Iterator over fields, ordered by ascending lexicographic property.
    type Iter<'a>: ExactSizeIterator<Item = Self::Field<'a>>
    where
        Self: 'a;

    // Get a field by property name.
    fn get<'a>(&'a self, property: &str) -> Option<Self::Field<'a>>;
    // Number of fields.
    fn len(&self) -> usize;
    // Iterator over fields, in ascending lexicographic property order.
    fn iter<'a>(&'a self) -> Self::Iter<'a>;
}

/// Field is the trait by which the property and value of a field representation are accessed.
pub trait Field<'a, N: AsNode> {
    fn property(&self) -> &'a str;
    fn value(&self) -> &'a N;
}
