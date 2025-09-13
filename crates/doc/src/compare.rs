use itertools::{EitherOrBoth, Itertools};
use json::{Field, Fields};
use std::cmp::Ordering;

/// compare evaluates the deep ordering of |lhs| and |rhs|.
/// This function establishes an arbitrary ordering over
/// Documents in order to provide a total ordering. Arrays and
/// objects are compared lexicographically, and the natural
/// Object order is used (by default, sorted on property name).
pub fn compare<L: json::AsNode, R: json::AsNode>(lhs: &L, rhs: &R) -> Ordering {
    match (lhs.as_node(), rhs.as_node()) {
        (json::Node::Array(lhs), json::Node::Array(rhs)) => lhs
            .iter()
            .zip_longest(rhs)
            .map(|eob| match eob {
                EitherOrBoth::Both(lhs, rhs) => compare(lhs, rhs),
                EitherOrBoth::Right(_) => Ordering::Less,
                EitherOrBoth::Left(_) => Ordering::Greater,
            })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal),
        (json::Node::Bool(lhs), json::Node::Bool(rhs)) => lhs.cmp(&rhs),
        (json::Node::Bytes(lhs), json::Node::Bytes(rhs)) => lhs.cmp(rhs),
        (json::Node::Null, json::Node::Null) => Ordering::Equal,
        (json::Node::Object(lhs), json::Node::Object(rhs)) => lhs
            .iter()
            .zip_longest(rhs.iter())
            .map(|eob| match eob {
                EitherOrBoth::Both(lhs, rhs) => {
                    let prop_ord = lhs.property().cmp(rhs.property());
                    match prop_ord {
                        Ordering::Equal => compare(lhs.value(), rhs.value()),
                        _ => prop_ord,
                    }
                }
                EitherOrBoth::Right(_) => Ordering::Less,
                EitherOrBoth::Left(_) => Ordering::Greater,
            })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal),
        (json::Node::String(lhs), json::Node::String(rhs)) => lhs.cmp(rhs),

        // Trivial numeric comparisons.
        (json::Node::NegInt(lhs), json::Node::NegInt(rhs)) => lhs.cmp(&rhs),
        (json::Node::PosInt(lhs), json::Node::PosInt(rhs)) => lhs.cmp(&rhs),
        (json::Node::NegInt(_), json::Node::PosInt(_)) => Ordering::Less,
        (json::Node::PosInt(_), json::Node::NegInt(_)) => Ordering::Greater,

        // Cross-type numeric comparisons that fall back to json::Number.
        (json::Node::Float(lhs), json::Node::Float(rhs)) => json::Number::Float(lhs).cmp(&json::Number::Float(rhs)),
        (json::Node::PosInt(lhs), json::Node::Float(rhs)) => json::Number::Unsigned(lhs).cmp(&json::Number::Float(rhs)),
        (json::Node::Float(lhs), json::Node::PosInt(rhs)) => json::Number::Float(lhs).cmp(&json::Number::Unsigned(rhs)),
        (json::Node::NegInt(lhs), json::Node::Float(rhs)) => json::Number::Signed(lhs).cmp(&json::Number::Float(rhs)),
        (json::Node::Float(lhs), json::Node::NegInt(rhs)) => json::Number::Float(lhs).cmp(&json::Number::Signed(rhs)),

        // Types are not comparable. Define an (arbitrary) total ordering.
        (json::Node::Null, _) => Ordering::Less,
        (_, json::Node::Null) => Ordering::Greater,
        (json::Node::Bool(_), _) => Ordering::Less,
        (_, json::Node::Bool(_)) => Ordering::Greater,
        (json::Node::Bytes(_), _) => Ordering::Less,
        (_, json::Node::Bytes(_)) => Ordering::Greater,
        (json::Node::Float(_), _) => Ordering::Less,
        (_, json::Node::Float(_)) => Ordering::Greater,
        (json::Node::NegInt(_), _) => Ordering::Less,
        (_, json::Node::NegInt(_)) => Ordering::Greater,
        (json::Node::PosInt(_), _) => Ordering::Less,
        (_, json::Node::PosInt(_)) => Ordering::Greater,
        (json::Node::String(_), _) => Ordering::Less,
        (_, json::Node::String(_)) => Ordering::Greater,
        (json::Node::Array(_), _) => Ordering::Less,
        (_, json::Node::Array(_)) => Ordering::Greater,
    }
}

#[cfg(test)]
mod test {
    use crate::{compare, ArchivedNode, HeapNode};

    use serde_json::{json, Value};
    use std::cmp::Ordering;

    #[test]
    fn test_null_ordering() {
        is_eq(Value::Null, Value::Null);
    }

    #[test]
    fn test_bool_ordering() {
        is_eq(json!(true), json!(true));
        is_eq(json!(false), json!(false));
        is_lt(json!(false), json!(true));

        is_lt(Value::Null, json!(false)); // Bool > Null.
    }

    #[test]
    fn test_number_ordering() {
        is_eq(json!(10), json!(10)); // u64.
        is_eq(json!(-10), json!(-10)); // i64.
        is_eq(json!(20), json!(20.00)); // u64 & f64.
        is_eq(json!(-20), json!(-20.00)); // i64 & f64.

        is_lt(json!(10), json!(20)); // u64.
        is_lt(json!(-20), json!(-10)); // i64.
        is_lt(json!(10), json!(20.00)); // u64 & f64.
        is_lt(json!(-20), json!(-10.00)); // i64 & f64.
        is_lt(json!(-1), json!(1)); // i64 & u64.

        is_lt(Value::Null, json!(1)); // Number > Null.
        is_lt(json!(true), json!(1)); // Number > Bool.
    }

    #[test]
    fn test_string_ordering() {
        is_eq(json!(""), json!(""));
        is_eq(json!("foo"), json!("foo"));

        is_lt(json!(""), json!("foo"));
        is_lt(json!("foo"), json!("foobar"));
        is_lt(json!("foo"), json!("fp"));

        is_lt(Value::Null, json!("1")); // String > Null.
        is_lt(json!(true), json!("1")); // String > Bool.
        is_lt(json!(1), json!("1")); // String > Number.
    }

    #[test]
    fn test_array_ordering() {
        is_eq(json!([]), json!([]));
        is_eq(json!([1, 2]), json!([1, 2]));

        is_lt(json!([]), json!([1, 2]));
        is_lt(json!([1, 2]), json!([1, 2, 3]));
        is_lt(json!([1, 2, 3]), json!([1, 3]));

        is_lt(Value::Null, json!([1])); // Array > Null.
        is_lt(json!(true), json!([1])); // Array > Bool.
        is_lt(json!(1), json!([1])); // Array > Number.
        is_lt(json!("1"), json!([1])); // Array > String.
    }

    #[test]
    fn test_object_ordering() {
        is_eq(json!({}), json!({}));
        is_eq(json!({"a": 1, "b": 2}), json!({"a": 1, "b": 2}));

        is_lt(json!({}), json!({"a": 1}));
        is_lt(json!({"a": 1}), json!({"b": 2}));

        is_lt(json!({"a": 1}), json!({"a": 1, "b": 2}));
        is_lt(json!({"a": 1, "b": 2}), json!({"a": 1, "c": 1}));
        is_lt(json!({"a": 1, "b": 2}), json!({"a": 1, "b": 3}));

        is_lt(Value::Null, json!({"1": 1})); // Object > Null.
        is_lt(json!(true), json!({"1": 1})); // Object > Bool.
        is_lt(json!(1), json!({"1": 1})); // Object > Number.
        is_lt(json!("1"), json!({"1": 1})); // Object > String.
        is_lt(json!([1]), json!({"1": 1})); // Object > Array.
    }

    fn multi_compare(lhs: &Value, rhs: &Value) -> Ordering {
        // Determine the compared value.
        let out = compare(lhs, rhs);

        // Now assert that all flavors of Value, HeapNode,
        // and ArchiveNode give a consistent answer.
        let alloc = HeapNode::new_allocator();

        let lhs_heap = HeapNode::from_serde(lhs, &alloc).unwrap();
        let rhs_heap = HeapNode::from_serde(rhs, &alloc).unwrap();

        let buf = lhs_heap.to_archive();
        let lhs_arch = ArchivedNode::from_archive(&buf);
        let buf = rhs_heap.to_archive();
        let rhs_arch = ArchivedNode::from_archive(&buf);

        assert_eq!(compare(lhs, &rhs_heap), out);
        assert_eq!(compare(lhs, rhs_arch), out);

        assert_eq!(compare(&lhs_heap, rhs), out);
        assert_eq!(compare(lhs_arch, rhs), out);

        assert_eq!(compare(&lhs_heap, rhs_arch), out);
        assert_eq!(compare(lhs_arch, &rhs_heap), out);
        out
    }

    fn is_lt(lhs: Value, rhs: Value) {
        assert_eq!(multi_compare(&lhs, &rhs), Ordering::Less);
        assert_eq!(multi_compare(&rhs, &lhs), Ordering::Greater);
    }
    fn is_eq(lhs: Value, rhs: Value) {
        assert_eq!(multi_compare(&lhs, &rhs), Ordering::Equal);
        assert_eq!(multi_compare(&rhs, &lhs), Ordering::Equal);
    }
}
