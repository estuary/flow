use super::{AsNode, Field, Fields, LazyNode, Node, OwnedNode};
use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
};

/// SerPolicy is a policy for serialization of AsNode instances.
/// It tweaks serialization behavior, such as by truncating long strings.
#[derive(Debug, Clone)]
pub struct SerPolicy {
    /// Truncate strings which are longer than this limit.
    pub str_truncate_after: usize,
    /// Truncate arrays with more items than this limit.
    pub array_truncate_after: usize,
    /// Truncate nested objects after this number of properties.
    /// Object truncation is done by taking the first `nested_obj_truncate_after`
    /// properties. Whether or not this is deterministic will depend on whether the
    /// underlying object iterator provides the keys in a deterministic order.
    /// We generally use sorted maps, which works for this.
    /// The root object is never truncated.
    pub nested_obj_truncate_after: usize,
}

impl SerPolicy {
    /// Maximum node depth which SerPolicy will serialize.
    /// Properties and array items below this depth will be omitted.
    // This depth is effectively constrained by serde_json, which has a generous
    // but hard limit. We follow their lead.
    const MAX_DEPTH: usize = 126;

    pub const fn noop() -> Self {
        Self {
            str_truncate_after: usize::MAX,
            array_truncate_after: usize::MAX,
            nested_obj_truncate_after: usize::MAX,
        }
    }

    pub fn truncate_strings(str_truncate_after: usize) -> Self {
        Self {
            str_truncate_after,
            ..SerPolicy::noop()
        }
    }

    /// Apply the policy to an AsNode instance, returning a serializable SerNode.
    pub fn with_truncation_indicator<'p, 'n, 's, N: AsNode>(
        &'p self,
        node: &'n N,
        truncation_indicator: &'s AtomicBool,
    ) -> SerNode<'p, 'n, 's, N> {
        SerNode {
            node,
            depth: 0,
            policy: self,
            truncation_indicator: Some(truncation_indicator),
        }
    }

    pub fn on<'p, 'n, N: AsNode>(&'p self, node: &'n N) -> SerNode<'p, 'n, 'static, N> {
        SerNode {
            node,
            depth: 0,
            policy: self,
            truncation_indicator: None,
        }
    }

    /// Apply the policy to a LazyNode instance, returning a serializable SerLazy.
    pub fn on_lazy<'p, 'alloc, 'n, N: AsNode>(
        &'p self,
        node: &'p LazyNode<'alloc, 'n, N>,
    ) -> SerLazy<'p, 'alloc, 'n, N> {
        SerLazy { node, policy: self }
    }

    /// Apply the policy to an OwnedNode instance, returning a serializable SerOwned.
    pub fn on_owned_with_truncation_indicator<'p, 's>(
        &'p self,
        node: &'p OwnedNode,
        truncation_indicator: &'s AtomicBool,
    ) -> SerOwned<'p, 's> {
        SerOwned {
            node,
            policy: self,
            truncation_indicator: Some(truncation_indicator),
        }
    }

    pub fn on_owned<'p>(&'p self, node: &'p OwnedNode) -> SerOwned<'p, 'static> {
        SerOwned {
            node,
            policy: self,
            truncation_indicator: None,
        }
    }

    // Return a SerPolicy appropriate for error messages and other debugging cases.
    pub fn debug() -> Self {
        Self {
            str_truncate_after: 512,
            array_truncate_after: 200,
            nested_obj_truncate_after: 100,
        }
    }

    fn apply_to_str<'a, 'b>(
        &self,
        raw: &'a str,
        truncation_indicator: Option<&'b AtomicBool>,
    ) -> &'a str {
        if raw.len() > self.str_truncate_after {
            if let Some(indicator) = truncation_indicator {
                indicator.store(true, Ordering::SeqCst);
            }
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
}

pub struct SerNode<'p, 'n, 's, N: AsNode> {
    node: &'n N,
    depth: usize,
    policy: &'p SerPolicy,
    truncation_indicator: Option<&'s AtomicBool>,
}

pub struct SerLazy<'p, 'alloc, 'n, N: AsNode> {
    node: &'p LazyNode<'alloc, 'n, N>,
    policy: &'p SerPolicy,
}

pub struct SerOwned<'p, 's> {
    node: &'p OwnedNode,
    policy: &'p SerPolicy,
    truncation_indicator: Option<&'s AtomicBool>,
}

impl<'p, 'n, 's, N: AsNode> serde::Serialize for SerNode<'p, 'n, 's, N> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::Serializer,
    {
        match self.node.as_node() {
            Node::Array(arr) => {
                let item_limit = if self.depth < SerPolicy::MAX_DEPTH {
                    self.policy.array_truncate_after
                } else {
                    0
                };
                if let Some(indicator) = self.truncation_indicator {
                    if arr.len() > item_limit {
                        indicator.store(true, Ordering::SeqCst);
                    }
                }
                serializer.collect_seq(arr.iter().take(item_limit).map(|d| SerNode {
                    node: d,
                    depth: self.depth + 1,
                    policy: self.policy,
                    truncation_indicator: self.truncation_indicator,
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
                let key_limit = if self.depth == 0 {
                    usize::MAX
                } else if self.depth < SerPolicy::MAX_DEPTH {
                    self.policy.nested_obj_truncate_after
                } else {
                    0
                };
                if let Some(indicator) = self.truncation_indicator {
                    if fields.len() > key_limit {
                        indicator.store(true, Ordering::SeqCst);
                    }
                }
                serializer.collect_map(fields.iter().take(key_limit).map(|field| {
                    (
                        field.property(),
                        SerNode {
                            node: field.value(),
                            depth: self.depth + 1,
                            policy: self.policy,
                            truncation_indicator: self.truncation_indicator,
                        },
                    )
                }))
            }
            Node::String(mut s) => {
                s = self.policy.apply_to_str(s, self.truncation_indicator);
                serializer.serialize_str(s)
            }
        }
    }
}

// SerNode may be packed as a FoundationDB tuple.
impl<'p, 'n, 's, N: AsNode> tuple::TuplePack for SerNode<'p, 'n, 's, N> {
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
                s = self.policy.apply_to_str(s, self.truncation_indicator);
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
                depth: 0,
                policy: self.policy,
                truncation_indicator: None,
            }
            .serialize(serializer),

            LazyNode::Node(n) => SerNode {
                node: *n,
                depth: 0,
                policy: self.policy,
                truncation_indicator: None,
            }
            .serialize(serializer),
        }
    }
}

impl serde::Serialize for SerOwned<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.node {
            OwnedNode::Heap(n) => SerNode {
                node: n.get(),
                depth: 0,
                policy: self.policy,
                truncation_indicator: self.truncation_indicator,
            }
            .serialize(serializer),

            OwnedNode::Archived(n) => SerNode {
                node: n.get(),
                depth: 0,
                policy: self.policy,
                truncation_indicator: self.truncation_indicator,
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
    fn test_ser_policy_all_types() {
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

        let policy = SerPolicy {
            str_truncate_after: 80,
            array_truncate_after: 80,
            nested_obj_truncate_after: 40,
        };

        let was_pruned = AtomicBool::new(false);
        let result = round_trip_serde(&policy, yuge_tracks_of_land.into(), &was_pruned);
        assert!(
            was_pruned.load(Ordering::SeqCst),
            "document should have been pruned during ser"
        );

        assert_obj_len(&result, "", 104); // root should not be truncated
        assert_obj_len(&result, "/bigNestedObj", 40);

        // Smaller than 80 because truncation must be done only at character boundaries
        assert_str_len(&result, "/bigStr", 78);

        assert_array_len(&result, "/bigArray", 80);

        assert_obj_len(&result, "/nested/still", 40);
        assert_str_len(&result, "/nested/more", 78); // char boundary
        assert_array_len(&result, "/nested/stuff", 80);
        assert_str_len(&result, "/nested/smolStr", 9);
        assert_array_len(&result, "/nested/smolArray", 3);
    }

    // Below tests are all checking that we set the truncation_indicator if we truncate any values.

    #[test]
    fn test_ser_policy_truncation_indicator_strings() {
        let policy = SerPolicy {
            str_truncate_after: 3,
            ..SerPolicy::noop()
        };
        let input = json!({
            "a": "foo"
        });
        let indicator = AtomicBool::new(false);
        let str_val =
            serde_json::to_string(&policy.with_truncation_indicator(&input, &indicator)).unwrap();
        assert!(!indicator.load(Ordering::SeqCst));
        assert_eq!(r#"{"a":"foo"}"#, &str_val);

        let input = json!({
            "a": big_str(9),
        });
        let indicator = AtomicBool::new(false);
        let str_val =
            serde_json::to_string(&policy.with_truncation_indicator(&input, &indicator)).unwrap();
        assert!(indicator.load(Ordering::SeqCst));
        assert_eq!(r#"{"a":"长"}"#, &str_val);
    }

    #[test]
    fn test_ser_policy_truncation_indicator_nested_objects() {
        let policy = SerPolicy {
            nested_obj_truncate_after: 3,
            ..SerPolicy::noop()
        };
        let input = json!({
            "a": big_obj(3),
            "b": big_obj(3),
            "c": big_obj(3),
            "d": big_obj(3),
        });
        let indicator = AtomicBool::new(false);
        let result = round_trip_serde(&policy, input, &indicator);
        assert!(!indicator.load(Ordering::SeqCst));
        assert_obj_len(&result, "", 4);
        assert_obj_len(&result, "/a", 3);
        assert_obj_len(&result, "/b", 3);
        assert_obj_len(&result, "/c", 3);
        assert_obj_len(&result, "/d", 3);

        let input = json!({
            "a": big_obj(3),
            "b": big_obj(3),
            "c": big_obj(3),
            "d": big_obj(99),
        });
        let indicator = AtomicBool::new(false);
        let result = round_trip_serde(&policy, input, &indicator);
        assert!(indicator.load(Ordering::SeqCst));
        assert_obj_len(&result, "", 4);
        assert_obj_len(&result, "/a", 3);
        assert_obj_len(&result, "/b", 3);
        assert_obj_len(&result, "/c", 3);
        assert_obj_len(&result, "/d", 3);
    }

    #[test]
    fn test_ser_policy_truncation_indicator_arrays() {
        let policy = SerPolicy {
            array_truncate_after: 3,
            ..SerPolicy::noop()
        };
        let input = json!({
            "a": big_array(3),
        });
        let indicator = AtomicBool::new(false);
        let result = round_trip_serde(&policy, input, &indicator);
        assert!(!indicator.load(Ordering::SeqCst));
        assert_array_len(&result, "/a", 3);

        let input = json!({
            "a": big_array(4),
        });
        let indicator = AtomicBool::new(false);
        let result = round_trip_serde(&policy, input, &indicator);
        assert!(indicator.load(Ordering::SeqCst));
        assert_array_len(&result, "/a", 3);
    }

    #[test]
    fn test_ser_policy_deep_nesting() {
        let (mut arr, mut obj) = (json!(1234), json!(1234));
        for _ in 0..SerPolicy::MAX_DEPTH + 20 {
            arr = json!([arr]);
            obj = json!({"p": obj});
        }
        let input = json!({"arr": arr, "obj": obj});

        let policy = SerPolicy {
            ..SerPolicy::noop()
        };
        let indicator = AtomicBool::new(false);

        // Expect that our overly nested `input` document is truncated and
        // is able to be parsed by serde_json.
        let _result = round_trip_serde(&policy, input, &indicator);
        assert!(indicator.load(Ordering::SeqCst));
    }

    fn round_trip_serde(policy: &SerPolicy, input: Value, indicator: &AtomicBool) -> Value {
        assert!(
            !indicator.load(Ordering::SeqCst),
            "indicator must start out false"
        );
        let str_val =
            serde_json::to_string(&policy.with_truncation_indicator(&input, indicator)).unwrap();
        serde_json::from_str(&str_val).expect("failed to deserialize round tripped doc")
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

    fn big_str(at_least_len: usize) -> Value {
        // Use a multi-byte character so that we can assert that truncation
        // only happens at character boundaries.
        let mut s = String::new();
        while s.len() < at_least_len {
            s.push('长');
        }
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
