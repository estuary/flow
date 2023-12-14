use super::{AsNode, Field, Fields, LazyNode, Node, OwnedNode};
use std::io;

/// SerPolicy is a policy for serialization of AsNode instances.
/// It tweaks serialization behavior, such as by truncating long strings.
#[derive(Debug, Clone)]
pub struct SerPolicy {
    /// Truncate strings which are longer than this limit.
    pub str_truncate_after: usize,
    pub array_truncate_after: usize,
    pub obj_truncate_after: usize,
    pub nested_obj_truncate_after: usize,
}

impl SerPolicy {
    pub fn truncate_strings(str_truncate_after: usize) -> Self {
        Self {
            str_truncate_after,
            array_truncate_after: usize::MAX,
            obj_truncate_after: usize::MAX,
            nested_obj_truncate_after: usize::MAX,
        }
    }

    /// Apply the policy to an AsNode instance, returning a serializable SerNode.
    pub fn on<'p, 'n, N: AsNode>(&'p self, node: &'n N) -> SerNode<'p, 'n, N> {
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
            array_truncate_after: 200,
            obj_truncate_after: 400,
            nested_obj_truncate_after: 100,
        }
    }

    fn apply_to_str<'a>(&self, raw: &'a str) -> &'a str {
        if raw.len() > self.str_truncate_after {
            // Find the greatest index that is <= `str_truncate_after` and falls at a utf8
            // character boundary
            let mut truncate_at = self.str_truncate_after;
            while !raw.is_char_boundary(truncate_at) {
                truncate_at -= 1;
            }
            &raw[..truncate_at]
        } else {
            raw
        }
    }

    fn for_child(&self) -> SerPolicy {
        let mut cp = self.clone();
        cp.obj_truncate_after = cp.nested_obj_truncate_after;
        cp
    }
}

impl Default for SerPolicy {
    fn default() -> Self {
        Self::truncate_strings(usize::MAX)
    }
}

pub struct SerNode<'p, 'n, N: AsNode> {
    node: &'n N,
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

impl<'p, 'n, N: AsNode> serde::Serialize for SerNode<'p, 'n, N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        match self.node.as_node() {
            Node::Array(arr) => {
                let child_policy = self.policy.for_child();
                serializer.collect_seq(arr.iter().take(self.policy.array_truncate_after).map(|d| {
                    SerNode {
                        node: d,
                        policy: &child_policy,
                    }
                }))
            }
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
            Node::Object(fields) => {
                let child_policy = self.policy.for_child();
                serializer.collect_map(fields.iter().take(self.policy.obj_truncate_after).map(
                    |field| {
                        (
                            field.property(),
                            SerNode {
                                node: field.value(),
                                policy: &child_policy,
                            },
                        )
                    },
                ))
            }
            Node::String(mut s) => {
                s = self.policy.apply_to_str(s);
                serializer.serialize_str(s)
            }
        }
    }
}

// SerNode may be packed as a FoundationDB tuple.
impl<'p, 'n, N: AsNode> tuple::TuplePack for SerNode<'p, 'n, N> {
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
                s = self.policy.apply_to_str(s);
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

#[cfg(test)]
mod test {
    use serde_json::{json, Value};

    use super::*;

    #[test]
    fn test_ser_policy() {
        // We use ordered maps, so object keys should be truncated
        // deterministically. The big_obj properties all start with `p`, so
        // those ought to be the ones truncated.
        let mut yuge_tracks_of_land = big_obj(100);
        yuge_tracks_of_land.insert("bigNestedObj".to_string(), big_obj(100).into());
        yuge_tracks_of_land.insert("bigStr".to_string(), big_str(200));
        yuge_tracks_of_land.insert("bigArray".to_string(), big_array(100));
        yuge_tracks_of_land.insert(
            "nested".to_string(),
            json!({
                "still": big_obj(100),
                "more": big_str(100),
                "stuff": big_array(100),
                "smolStr": "i am smol",
                "smolArray": [1, 2, 3],
            }),
        );
        // For good measure, assert that this property is removed, since it
        // comes after all the `p{n}` keys lexicographically.
        yuge_tracks_of_land.insert("z".to_string(), json!("this should be removed"));

        let policy = SerPolicy {
            str_truncate_after: 80,
            array_truncate_after: 80,
            obj_truncate_after: 80,
            nested_obj_truncate_after: 40,
        };

        let val: Value = yuge_tracks_of_land.into();
        let ser = serde_json::to_string(&policy.on(&val)).unwrap();
        let result: Value = serde_json::from_str(&ser).unwrap();

        assert_obj_len(&result, "", 80);
        assert_obj_len(&result, "/bigNestedObj", 40);

        // Smaller than 80 because truncation must be done only at character boundaries
        assert_str_len(&result, "/bigStr", 78);

        assert_array_len(&result, "/bigArray", 80);

        assert_obj_len(&result, "/nested/still", 40);
        assert_str_len(&result, "/nested/more", 78); // char boundary
        assert_array_len(&result, "/nested/stuff", 80);
        assert_str_len(&result, "/nested/smolStr", 9);
        assert_array_len(&result, "/nested/smolArray", 3);

        assert!(result.pointer("/z").is_none());
    }

    fn assert_array_len(val: &Value, ptr: &str, expect_len: usize) {
        let inner = val.pointer(ptr).unwrap();
        let arr = inner.as_array().unwrap();
        assert_eq!(expect_len, arr.len(), "wrong array len for: '{ptr}'");
    }

    fn assert_str_len(val: &Value, ptr: &str, expect_len: usize) {
        let inner = val.pointer(ptr).unwrap();
        let s = inner.as_str().unwrap();
        assert_eq!(expect_len, s.len(), "wrong str len for: '{ptr}'");
    }

    fn assert_obj_len(val: &Value, ptr: &str, expect_len: usize) {
        let inner_val = val.pointer(ptr).expect(ptr);
        let obj = inner_val.as_object().expect(ptr);
        assert_eq!(expect_len, obj.len(), "wrong object len for ptr: '{ptr}'");
    }

    fn big_str(len: usize) -> Value {
        // Use a multi-byte character so that we can assert that truncation
        // only happens at character boundaries.
        let s = std::iter::repeat('长').take(len).collect();
        Value::String(s)
    }

    fn big_obj(len: usize) -> serde_json::Map<String, Value> {
        (0..)
            .into_iter()
            .take(len)
            .map(|i| (format!("p{i}"), json!(i)))
            .collect()
    }

    fn big_array(len: usize) -> Value {
        let vals = std::iter::repeat(Value::Bool(true)).take(len).collect();
        Value::Array(vals)
    }
}
