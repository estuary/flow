use super::Number;
use itertools::{EitherOrBoth, Itertools};
use serde_json::Value;
use std::cmp::Ordering;

pub fn json_cmp(lhs: &Value, rhs: &Value) -> Option<Ordering> {
    match (lhs, rhs) {
        // Simple scalar comparisons:
        (Value::String(lhs), Value::String(rhs)) => Some(lhs.cmp(rhs)),
        (Value::Bool(lhs), Value::Bool(rhs)) => Some(lhs.cmp(rhs)),
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        // Compared numbers regardless of underlying representation (u64, f64, i64).
        (Value::Number(lhs), Value::Number(rhs)) => {
            let lhs = Number::from(lhs);
            let rhs = Number::from(rhs);
            lhs.partial_cmp(&rhs)
        }
        // Deeply compare array items in lexicographic order.
        (Value::Array(lhs), Value::Array(rhs)) => lhs
            .iter()
            .zip_longest(rhs)
            .map(|eob| match eob {
                EitherOrBoth::Both(lhs, rhs) => json_cmp(lhs, rhs),
                EitherOrBoth::Right(_) => Some(Ordering::Less),
                EitherOrBoth::Left(_) => Some(Ordering::Greater),
            })
            .find(|o| {
                if let Some(Ordering::Equal) = o {
                    false
                } else {
                    true
                }
            })
            .unwrap_or(Some(Ordering::Equal)),
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
                        _ => Some(prop_ord),
                    }
                }
                EitherOrBoth::Right(_) => Some(Ordering::Less),
                EitherOrBoth::Left(_) => Some(Ordering::Greater),
            })
            .find(|o| {
                if let Some(Ordering::Equal) = o {
                    false
                } else {
                    true
                }
            })
            .unwrap_or(Some(Ordering::Equal)),
        // Incompatible types.
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use super::json_cmp;
    use serde_json::{json, Value};
    use std::cmp::Ordering;

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

        is_none(json!(1), json!("1"));
        is_none(json!(1), json!({"1": 1}));
    }

    #[test]
    fn test_string_ordering() {
        is_eq(json!(""), json!(""));
        is_eq(json!("foo"), json!("foo"));

        is_lt(json!(""), json!("foo"));
        is_lt(json!("foo"), json!("foobar"));
        is_lt(json!("foo"), json!("fp"));

        is_none(json!(1), Value::Null);
    }

    #[test]
    fn test_bool_ordering() {
        is_eq(json!(true), json!(true));
        is_eq(json!(false), json!(false));
        is_lt(json!(false), json!(true));

        is_none(json!(false), json!(0));
        is_none(json!(true), json!(1));
    }

    #[test]
    fn test_array_ordering() {
        is_eq(json!([]), json!([]));
        is_eq(json!([1, 2]), json!([1, 2]));

        is_lt(json!([]), json!([1, 2]));
        is_lt(json!([1, 2]), json!([1, 2, 3]));
        is_lt(json!([1, 2, 3]), json!([1, 3]));

        is_none(json!([]), Value::Null);
        is_none(json!([1]), json!("[1]"));
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

        is_none(json!({}), Value::Null);
        is_none(json!({"a": 1}), json!("{\"a\": 1}"));
    }

    fn is_lt(lhs: Value, rhs: Value) {
        assert_eq!(json_cmp(&lhs, &rhs), Some(Ordering::Less));
        assert_eq!(json_cmp(&rhs, &lhs), Some(Ordering::Greater));
    }
    fn is_eq(lhs: Value, rhs: Value) {
        assert_eq!(json_cmp(&lhs, &rhs), Some(Ordering::Equal));
        assert_eq!(json_cmp(&rhs, &lhs), Some(Ordering::Equal));
    }
    fn is_none(lhs: Value, rhs: Value) {
        assert_eq!(json_cmp(&lhs, &rhs), None);
        assert_eq!(json_cmp(&rhs, &lhs), None);
    }
}
