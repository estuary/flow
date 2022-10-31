use super::{AsNode, Field, Fields, Node};

impl AsNode for serde_json::Value {
    type Fields = serde_json::Map<String, serde_json::Value>;

    fn as_node<'a>(&'a self) -> Node<'a, Self> {
        match self {
            Self::Array(a) => Node::Array(a),
            Self::Bool(b) => Node::Bool(*b),
            Self::Null => Node::Null,
            Self::Number(n) => Node::Number(n.into()),
            Self::Object(o) => Node::Object(o),
            Self::String(s) => Node::String(s),
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
    fn property(&self) -> &'a str {
        self.0
    }

    fn value(&self) -> &'a serde_json::Value {
        self.1
    }
}
