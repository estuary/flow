use super::{AsNode, Field, Fields, Node};
use itertools::{EitherOrBoth, Itertools};
use std::cmp::Ordering;

/// compare evaluates the deep ordering of `lhs` and `rhs`.
/// This function establishes an arbitrary, total ordering over
/// which is stable across AsNode implementations. Arrays and
/// objects are compared lexicographically by walking their ordered
/// items or lexicographic properties.
pub fn compare<L: AsNode, R: AsNode>(lhs: &L, rhs: &R) -> Ordering {
    compare_node(&lhs.as_node(), &rhs.as_node())
}

/// compare_node evaluates the deep ordering of `lhs` and `rhs`,
/// which have already been unwrapped into Node instances.
/// Generally you should use compare() instead, which allows the compiler
/// to collapse an internal match statement of as_node() with the match
/// statement within compare_node().
#[inline]
pub fn compare_node<'l, 'r, L: AsNode, R: AsNode>(
    lhs: &Node<'l, L>,
    rhs: &Node<'r, R>,
) -> Ordering {
    match (lhs, rhs) {
        (Node::Array(lhs), Node::Array(rhs)) => lhs
            .iter()
            .zip_longest(rhs.iter())
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

        // Trivial numeric comparisons.
        (Node::NegInt(lhs), Node::NegInt(rhs)) => lhs.cmp(&rhs),
        (Node::PosInt(lhs), Node::PosInt(rhs)) => lhs.cmp(&rhs),
        (Node::Float(lhs), Node::Float(rhs)) => lhs.total_cmp(&rhs),
        (Node::NegInt(_), Node::PosInt(_)) => Ordering::Less,
        (Node::PosInt(_), Node::NegInt(_)) => Ordering::Greater,

        // Cross-type numeric comparisons that project to f64.
        (Node::PosInt(lhs), Node::Float(rhs)) => (*lhs as f64).total_cmp(&rhs),
        (Node::Float(lhs), Node::PosInt(rhs)) => lhs.total_cmp(&(*rhs as f64)),
        (Node::NegInt(lhs), Node::Float(rhs)) => (*lhs as f64).total_cmp(&rhs),
        (Node::Float(lhs), Node::NegInt(rhs)) => lhs.total_cmp(&(*rhs as f64)),

        // Types are not comparable. Define an (arbitrary) total ordering.
        (Node::Null, _) => Ordering::Less,
        (_, Node::Null) => Ordering::Greater,
        (Node::Bool(_), _) => Ordering::Less,
        (_, Node::Bool(_)) => Ordering::Greater,
        (Node::Bytes(_), _) => Ordering::Less,
        (_, Node::Bytes(_)) => Ordering::Greater,
        (Node::Float(_), _) => Ordering::Less,
        (_, Node::Float(_)) => Ordering::Greater,
        (Node::NegInt(_), _) => Ordering::Less,
        (_, Node::NegInt(_)) => Ordering::Greater,
        (Node::PosInt(_), _) => Ordering::Less,
        (_, Node::PosInt(_)) => Ordering::Greater,
        (Node::String(_), _) => Ordering::Less,
        (_, Node::String(_)) => Ordering::Greater,
        (Node::Array(_), _) => Ordering::Less,
        (_, Node::Array(_)) => Ordering::Greater,
    }
}

/*
pub fn is_multiple_of<'a, N: AsNode>(node: &Node<'a, N>, of: &Node<'a, N>) -> bool {
    match of {
        Node::PosInt(d) => match node {
            Node::PosInt(n) => n % d == 0,
            Node::NegInt(n) => n % (*d as i64) == 0,
            Node::Float(n) => (n / (*d as f64)).fract() == 0.0,
            _ => false,
        },
        Node::NegInt(d) => match node {
            Node::PosInt(n) => (*n as i64) % d == 0,
            Node::NegInt(n) => n % d == 0,
            Node::Float(n) => (n / (*d as f64)).fract() == 0.0,
            _ => false,
        },
        Node::Float(d) => match node {
            Node::PosInt(n) => (*n as f64) % d == 0.0,
            Node::NegInt(n) => (*n as f64) % d == 0.0,
            Node::Float(n) => (n / d).fract() == 0.0,
        },
        _ => false,
    }
}
*/

#[cfg(test)]
mod test {
    use super::compare;
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

    fn is_lt(lhs: Value, rhs: Value) {
        assert_eq!(compare(&lhs, &rhs), Ordering::Less);
        assert_eq!(compare(&rhs, &lhs), Ordering::Greater);
    }
    fn is_eq(lhs: Value, rhs: Value) {
        assert_eq!(compare(&lhs, &rhs), Ordering::Equal);
        assert_eq!(compare(&rhs, &lhs), Ordering::Equal);
    }
}
