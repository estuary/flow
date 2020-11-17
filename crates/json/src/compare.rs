use super::Number;
use itertools::{EitherOrBoth, Itertools};
use serde_json::Value;
use std::cmp::Ordering;

/// json_cmp evaluates the deep ordering of |lhs| and |rhs|.
/// This function establishes an arbitrary ordering over JSON
/// types, in order to provide a total ordering. Arrays and
/// objects are compared lexicographically, and the natural
/// Object order is used (by default, sorted on property name).
pub fn json_cmp(lhs: &Value, rhs: &Value) -> Ordering {
    match (lhs, rhs) {
        // Simple scalar comparisons:
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(lhs), Value::Bool(rhs)) => lhs.cmp(rhs),
        (Value::Number(lhs), Value::Number(rhs)) => {
            let lhs = Number::from(lhs);
            let rhs = Number::from(rhs);
            lhs.cmp(&rhs)
        }
        (Value::String(lhs), Value::String(rhs)) => lhs.cmp(rhs),
        (Value::Array(lhs), Value::Array(rhs)) => lhs
            .iter()
            .zip_longest(rhs)
            .map(|eob| match eob {
                EitherOrBoth::Both(lhs, rhs) => json_cmp(lhs, rhs),
                EitherOrBoth::Right(_) => Ordering::Less,
                EitherOrBoth::Left(_) => Ordering::Greater,
            })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal),
        // Deeply compare object (sorted, or otherwise ordered) properties
        // and values in lexicographic order.
        (Value::Object(lhs), Value::Object(rhs)) => lhs
            .iter()
            .zip_longest(rhs)
            .map(|eob| match eob {
                EitherOrBoth::Both((lhs_p, lhs_v), (rhs_p, rhs_v)) => {
                    let prop_ord = lhs_p.cmp(rhs_p);
                    match prop_ord {
                        Ordering::Equal => json_cmp(lhs_v, rhs_v),
                        _ => prop_ord,
                    }
                }
                EitherOrBoth::Right(_) => Ordering::Less,
                EitherOrBoth::Left(_) => Ordering::Greater,
            })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal),

        // Types are not equal. Define an (arbitrary) total ordering.
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(_), _) => Ordering::Less,
        (_, Value::Bool(_)) => Ordering::Greater,
        (Value::Number(_), _) => Ordering::Less,
        (_, Value::Number(_)) => Ordering::Greater,
        (Value::String(_), _) => Ordering::Less,
        (_, Value::String(_)) => Ordering::Greater,
        (Value::Array(_), _) => Ordering::Less,
        (_, Value::Array(_)) => Ordering::Greater,
    }
}

/// json_cmp_at evaluates the deep ordering of |lhs| and |rhs| with respect
/// to a composite key, specified as a slice of JSON-Pointers relative to the
/// respective document roots. JSON-Pointers which point to a document location
/// that does not exist assume an implicit "null" value. In other words, they
/// behave identically to a document where the location *does* exist but with
/// an explicit null value.
pub fn json_cmp_at<S>(key_ptrs: &[S], lhs: &Value, rhs: &Value) -> Ordering
where
    S: AsRef<str>,
{
    key_ptrs
        .iter()
        .map(|ptr| {
            json_cmp(
                lhs.pointer(ptr.as_ref()).unwrap_or(&Value::Null),
                rhs.pointer(ptr.as_ref()).unwrap_or(&Value::Null),
            )
        })
        .find(|o| *o != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}

#[cfg(test)]
mod test {
    use super::{json_cmp, json_cmp_at};
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
        assert_eq!(json_cmp(&lhs, &rhs), Ordering::Less);
        assert_eq!(json_cmp(&rhs, &lhs), Ordering::Greater);
    }
    fn is_eq(lhs: Value, rhs: Value) {
        assert_eq!(json_cmp(&lhs, &rhs), Ordering::Equal);
        assert_eq!(json_cmp(&rhs, &lhs), Ordering::Equal);
    }

    #[test]
    fn test_pointer_compare_objects() {
        let d1 = &json!({"a": 1, "b": 2, "c": 3});
        let d2 = &json!({"a": 2, "b": 1, "c": 3});

        // No pointers => always equal.
        assert_eq!(json_cmp_at(&[] as &[&str], d1, d2), Ordering::Equal);
        // Deep compare of document roots.
        assert_eq!(json_cmp_at(&["".to_owned()], d1, d2), Ordering::Less);
        // Simple key ordering.
        assert_eq!(json_cmp_at(&["/a"], d1, d2), Ordering::Less);
        assert_eq!(json_cmp_at(&["/b"], d1, d2), Ordering::Greater);
        assert_eq!(json_cmp_at(&["/c"], d1, d2), Ordering::Equal);
        // Composite key ordering.
        assert_eq!(json_cmp_at(&["/c", "/a"], d1, d2), Ordering::Less);
        assert_eq!(json_cmp_at(&["/c", "/b"], d1, d2), Ordering::Greater);
        assert_eq!(json_cmp_at(&["/c", "/c"], d1, d2), Ordering::Equal);
        assert_eq!(
            json_cmp_at(&["/c", "/c", "/c", "/a"], d1, d2),
            Ordering::Less
        );
    }

    #[test]
    fn test_pointer_compare_arrays() {
        let d1 = &json!([1, 2, 3]);
        let d2 = &json!([2, 1, 3]);

        // No pointers => always equal.
        assert_eq!(json_cmp_at(&[] as &[&str], d1, d2), Ordering::Equal);
        // Deep compare of document roots.
        assert_eq!(json_cmp_at(&["".to_owned()], d1, d2), Ordering::Less);
        // Simple key ordering.
        assert_eq!(json_cmp_at(&["/0"], d1, d2), Ordering::Less);
        assert_eq!(json_cmp_at(&["/1"], d1, d2), Ordering::Greater);
        assert_eq!(json_cmp_at(&["/2"], d1, d2), Ordering::Equal);
        // Composite key ordering.
        assert_eq!(json_cmp_at(&["/2", "/0"], d1, d2), Ordering::Less);
        assert_eq!(json_cmp_at(&["/2", "/1"], d1, d2), Ordering::Greater);
        assert_eq!(json_cmp_at(&["/2", "/2"], d1, d2), Ordering::Equal);
    }

    #[test]
    fn test_pointer_compare_missing() {
        let d1 = &json!({"a": Value::Null, "c": 3});
        let d2 = &json!({"b": 2});

        assert_eq!(json_cmp_at(&["/does/not/exist"], d1, d2), Ordering::Equal);
        // Key exists at |d1| but not |d2|. |d2| value is implicitly null.
        assert_eq!(json_cmp_at(&["/c"], d1, d2), Ordering::Greater);
        // Key exists at |d2| but not |d1|. |d1| value is implicitly null.
        assert_eq!(json_cmp_at(&["/b"], d1, d2), Ordering::Less);
        // Key exists at |d1| but not |d2|. Both are null (implicit and explicit).
        assert_eq!(json_cmp_at(&["/a"], d1, d2), Ordering::Equal);
    }
}
