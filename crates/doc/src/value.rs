use super::{AsNode, Field, Fields, Node};

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
