use super::{AsNode, Node};
use std::io;

impl<'n, N: AsNode> tuple::TuplePack for Node<'n, N> {
    fn pack<W: io::Write>(
        &self,
        w: &mut W,
        tuple_depth: tuple::TupleDepth,
    ) -> io::Result<tuple::VersionstampOffset> {
        match self {
            Node::Array(_) | Node::Object(_) => {
                serde_json::to_vec(self).unwrap().pack(w, tuple_depth)
            }
            Node::Bool(b) => b.pack(w, tuple_depth),
            Node::Bytes(b) => b.pack(w, tuple_depth),
            Node::Null => Option::<()>::None.pack(w, tuple_depth),
            Node::Number(json::Number::Float(n)) => n.pack(w, tuple_depth),
            Node::Number(json::Number::Signed(n)) => n.pack(w, tuple_depth),
            Node::Number(json::Number::Unsigned(n)) => n.pack(w, tuple_depth),
            Node::String(s) => s.pack(w, tuple_depth),
        }
    }
}
