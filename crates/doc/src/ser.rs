use super::{AsNode, Field, Fields, LazyNode, Node};

// Implement Serialize over all Node implementations.
// We must implement over Node and not AsNode due to the Rust foreign type restriction.

impl<'n, N: AsNode> serde::Serialize for Node<'n, N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        match self {
            Node::Array(arr) => serializer.collect_seq(arr.into_iter().map(|d| d.as_node())),
            Node::Bool(b) => serializer.serialize_bool(*b),
            Node::Bytes(b) => {
                if serializer.is_human_readable() {
                    serializer.collect_str(&base64::display::Base64Display::with_config(
                        b,
                        base64::STANDARD,
                    ))
                } else {
                    serializer.serialize_bytes(b)
                }
            }
            Node::Null => serializer.serialize_unit(),
            Node::Number(json::Number::Float(n)) => serializer.serialize_f64(*n),
            Node::Number(json::Number::Signed(n)) => serializer.serialize_i64(*n),
            Node::Number(json::Number::Unsigned(n)) => serializer.serialize_u64(*n),
            Node::Object(fields) => serializer.collect_map(
                fields
                    .iter()
                    .map(|field| (field.property(), field.value().as_node())),
            ),
            Node::String(s) => serializer.serialize_str(s),
        }
    }
}

impl<'alloc, 'n, N: AsNode> serde::Serialize for LazyNode<'alloc, 'n, N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        match self {
            Self::Heap(n) => n.as_node().serialize(serializer),
            Self::Node(n) => n.as_node().serialize(serializer),
        }
    }
}
