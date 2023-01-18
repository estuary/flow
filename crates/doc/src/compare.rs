use super::{AsNode, Field, Fields, Node};
use itertools::{EitherOrBoth, Itertools};
use std::cmp::Ordering;

/// compare evaluates the deep ordering of |lhs| and |rhs|.
/// This function establishes an arbitrary ordering over
/// Documents in order to provide a total ordering. Arrays and
/// objects are compared lexicographically, and the natural
/// Object order is used (by default, sorted on property name).
pub fn compare<L: AsNode, R: AsNode>(lhs: &L, rhs: &R) -> Ordering {
    match (lhs.as_node(), rhs.as_node()) {
        (Node::Array(lhs), Node::Array(rhs)) => lhs
            .iter()
            .zip_longest(rhs)
            .map(|eob| match eob {
                EitherOrBoth::Both(lhs, rhs) => compare(lhs, rhs),
                EitherOrBoth::Right(_) => Ordering::Less,
                EitherOrBoth::Left(_) => Ordering::Greater,
            })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal),
        (Node::Bool(lhs), Node::Bool(rhs)) => lhs.cmp(&rhs),
        (Node::Bytes(lhs), Node::Bytes(rhs)) => lhs.cmp(rhs),
        (Node::Null, Node::Null) => Ordering::Equal,
        (Node::Number(lhs), Node::Number(rhs)) => lhs.cmp(&rhs),
        (Node::Object(lhs), Node::Object(rhs)) => lhs
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
        (Node::String(lhs), Node::String(rhs)) => lhs.cmp(rhs),

        // Types are not equal. Define an (arbitrary) total ordering.
        (Node::Null, _) => Ordering::Less,
        (_, Node::Null) => Ordering::Greater,
        (Node::Bool(_), _) => Ordering::Less,
        (_, Node::Bool(_)) => Ordering::Greater,
        (Node::Bytes(_), _) => Ordering::Less,
        (_, Node::Bytes(_)) => Ordering::Greater,
        (Node::Number(_), _) => Ordering::Less,
        (_, Node::Number(_)) => Ordering::Greater,
        (Node::String(_), _) => Ordering::Less,
        (_, Node::String(_)) => Ordering::Greater,
        (Node::Array(_), _) => Ordering::Less,
        (_, Node::Array(_)) => Ordering::Greater,
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
