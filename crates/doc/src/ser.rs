use super::{AsNode, Field, Fields, LazyNode, Node, OwnedNode};
use std::io;

/// SerPolicy is a policy for serialization of AsNode instances.
/// It tweaks serialization behavior, such as by truncating long strings.
#[derive(Debug, Clone)]
pub struct SerPolicy {
    /// Truncate strings which are longer than this limit.
    pub str_truncate_after: usize,
}

impl SerPolicy {
    pub fn new(str_truncate_after: usize) -> Self {
        Self { str_truncate_after }
    }

    /// Apply the policy to an AsNode instance, returning a serializable SerNode.
    pub fn on<'p, 'n, N: AsNode>(&'p self, node: &'p N) -> SerNode<'p, N> {
        SerNode { node, policy: self }
    }

    /// Apply the policy to a LazyNode instance, returning a serializable SerLazy.
    pub fn on_lazy<'p, 'alloc, 'n, N: AsNode>(
        &'p self,
        node: &'p LazyNode<'alloc, 'n, N>,
    ) -> SerLazy<'p, 'alloc, 'n, N> {
        SerLazy { node, policy: self }
    }

    /// Apply the policy to an OwnedNode instance, returning a serializable SerOwned.
    pub fn on_owned<'p>(&'p self, node: &'p OwnedNode) -> SerOwned<'p> {
        SerOwned { node, policy: self }
    }

    // Return a SerPolicy appropriate for error messages and other debugging cases.
    pub fn debug() -> Self {
        Self {
            str_truncate_after: 512,
        }
    }
}

impl Default for SerPolicy {
    fn default() -> Self {
        Self::new(usize::MAX)
    }
}

pub struct SerNode<'p, N: AsNode> {
    node: &'p N,
    policy: &'p SerPolicy,
}

pub struct SerLazy<'p, 'alloc, 'n, N: AsNode> {
    node: &'p LazyNode<'alloc, 'n, N>,
    policy: &'p SerPolicy,
}

pub struct SerOwned<'p> {
    node: &'p OwnedNode,
    policy: &'p SerPolicy,
}

impl<'p, N: AsNode> serde::Serialize for SerNode<'p, N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        match self.node.as_node() {
            Node::Array(arr) => serializer.collect_seq(arr.iter().map(|d| Self {
                node: d,
                policy: self.policy,
            })),
            Node::Bool(b) => serializer.serialize_bool(b),
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
            Node::Float(n) => serializer.serialize_f64(n),
            Node::NegInt(n) => serializer.serialize_i64(n),
            Node::PosInt(n) => serializer.serialize_u64(n),
            Node::Object(fields) => serializer.collect_map(fields.iter().map(|field| {
                (
                    field.property(),
                    Self {
                        node: field.value(),
                        policy: self.policy,
                    },
                )
            })),
            Node::String(mut s) => {
                if s.len() > self.policy.str_truncate_after {
                    s = &s[..self.policy.str_truncate_after];
                }
                serializer.serialize_str(s)
            }
        }
    }
}

// SerNode may be packed as a FoundationDB tuple.
impl<'p, N: AsNode> tuple::TuplePack for SerNode<'p, N> {
    fn pack<W: io::Write>(
        &self,
        w: &mut W,
        tuple_depth: tuple::TupleDepth,
    ) -> io::Result<tuple::VersionstampOffset> {
        match self.node.as_node() {
            Node::Array(_) | Node::Object(_) => {
                serde_json::to_vec(self).unwrap().pack(w, tuple_depth)
            }
            Node::Bool(b) => b.pack(w, tuple_depth),
            Node::Bytes(b) => b.pack(w, tuple_depth),
            Node::Null => Option::<()>::None.pack(w, tuple_depth),
            Node::Float(n) => n.pack(w, tuple_depth),
            Node::NegInt(n) => n.pack(w, tuple_depth),
            Node::PosInt(n) => n.pack(w, tuple_depth),
            Node::String(mut s) => {
                if s.len() > self.policy.str_truncate_after {
                    s = &s[..self.policy.str_truncate_after];
                }
                s.pack(w, tuple_depth)
            }
        }
    }
}

impl<N: AsNode> serde::Serialize for SerLazy<'_, '_, '_, N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        match &self.node {
            LazyNode::Heap(n) => SerNode {
                node: *n,
                policy: self.policy,
            }
            .serialize(serializer),

            LazyNode::Node(n) => SerNode {
                node: *n,
                policy: self.policy,
            }
            .serialize(serializer),
        }
    }
}

impl serde::Serialize for SerOwned<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.node {
            OwnedNode::Heap(n) => SerNode {
                node: n.get(),
                policy: self.policy,
            }
            .serialize(serializer),

            OwnedNode::Archived(n) => SerNode {
                node: n.get(),
                policy: self.policy,
            }
            .serialize(serializer),
        }
    }
}
