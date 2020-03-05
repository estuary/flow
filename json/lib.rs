use itertools::{EitherOrBoth, Itertools};
use serde_json;
use std::cmp::Ordering;
use std::fmt;

/// `Number` holds possible numeric types of the JSON object model.
#[derive(Debug)]
pub enum Number {
    Unsigned(u64),
    Signed(i64),
    Float(f64),
}

impl Number {
    pub fn is_multiple_of(&self, d: &Self) -> bool {
        use Number::*;

        match *d {
            Unsigned(d) => match *self {
                Unsigned(n) => n % d == 0,
                Signed(n) => n % (d as i64) == 0,
                Float(n) => (n / (d as f64)).fract() == 0.0,
            },
            Signed(d) => match *self {
                Unsigned(n) => (n as i64) % d == 0,
                Signed(n) => n % d == 0,
                Float(n) => (n / (d as f64)).fract() == 0.0,
            },
            Float(d) => match *self {
                Unsigned(n) => (n as f64) % d == 0.0,
                Signed(n) => (n as f64) % d == 0.0,
                Float(n) => (n / d).fract() == 0.0,
            },
        }
    }
}

impl From<&serde_json::Number> for Number {
    fn from(n: &serde_json::Number) -> Number {
        if let Some(n) = n.as_u64() {
            Number::Unsigned(n)
        } else if let Some(n) = n.as_i64() {
            Number::Signed(n)
        } else {
            Number::Float(n.as_f64().unwrap())
        }
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use Number::*;

        match *self {
            Unsigned(lhs) => match other {
                Unsigned(rhs) => lhs.partial_cmp(rhs),
                Signed(rhs) => (lhs as i64).partial_cmp(rhs),
                Float(rhs) => (lhs as f64).partial_cmp(rhs),
            },
            Signed(lhs) => match other {
                Unsigned(rhs) => lhs.partial_cmp(&(*rhs as i64)),
                Signed(rhs) => lhs.partial_cmp(rhs),
                Float(rhs) => (lhs as f64).partial_cmp(rhs),
            },
            Float(lhs) => match other {
                Unsigned(rhs) => lhs.partial_cmp(&(*rhs as f64)),
                Signed(rhs) => lhs.partial_cmp(&(*rhs as f64)),
                Float(rhs) => lhs.partial_cmp(rhs),
            },
        }
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other)
            .map_or(false, |c| c == Ordering::Equal)
    }
}

/// `Span` describes a instance value of a visited JSON document, including
/// its [begin, end) value indexes in an ordered depth-first walk of the document.
#[derive(Debug, Eq, PartialEq)]
pub struct Span {
    /// Index of the first value of this Span. Indices start at zero and
    /// parent objects & arrays are indexed before each child value.
    pub begin: usize,
    /// Index immediately beyond the last value of this Span.
    pub end: usize,
    /// Hash value of the document span. Hashes are invariant to the
    /// specific ordering of encountered properties.
    pub hashed: u64,
}

impl Span {
    /// New returns a length-one Span with the given index and hash.
    pub fn new(at: usize, h: u64) -> Span {
        return Span {
            begin: at,
            end: at + 1,
            hashed: h,
        };
    }
}

/// `Location` of a value within a JSON document.
pub enum Location<'a> {
    Root,
    Property(LocatedProperty<'a>),
    Item(LocatedItem<'a>),
}

/// `LocatedProperty` is a property located within a JSON document.
#[derive(Copy, Clone)]
pub struct LocatedProperty<'a> {
    pub parent: &'a Location<'a>,
    pub name: &'a str,
    pub index: usize,
}

/// `LocatedItem` is an array item located within a JSON document.
#[derive(Copy, Clone)]
pub struct LocatedItem<'a> {
    pub parent: &'a Location<'a>,
    pub index: usize,
}

impl<'a> fmt::Display for Location<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Location::Root => write!(f, "#"),
            Location::Property(LocatedProperty { parent, name, .. }) => {
                write!(f, "{}/{}", parent, name)
            }
            Location::Item(LocatedItem { parent, index }) => write!(f, "{}/{}", *parent, index),
        }
    }
}

/// `Walker` visits values within JSON documents.
pub trait Walker {
    fn push_property<'a>(&mut self, _span: &Span, _loc: &'a LocatedProperty<'a>) {}
    fn push_item<'a>(&mut self, _span: &Span, _loc: &'a LocatedItem<'a>) {}

    fn pop_object<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _num_properties: usize) {}
    fn pop_array<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _num_items: usize) {}
    fn pop_bool<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _val: bool) {}
    fn pop_numeric<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _val: Number) {}
    fn pop_str<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _val: &'a str) {}
    fn pop_null<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>) {}
}

/// `NoopWalker` is as `Walker` implementation which does nothing.
pub struct NoopWalker;
impl Walker for NoopWalker {}

pub mod de;
pub mod schema;
pub mod validator;

pub fn json_cmp(lhs: &serde_json::Value, rhs: &serde_json::Value) -> Option<Ordering> {
    use serde_json::Value as sjv;

    match (lhs, rhs) {
        // Simple scalar comparisons:
        (sjv::String(lhs), sjv::String(rhs)) => Some(lhs.cmp(rhs)),
        (sjv::Bool(lhs), sjv::Bool(rhs)) => Some(lhs.cmp(rhs)),
        (sjv::Null, sjv::Null) => Some(Ordering::Equal),
        // Compared numbers regardless of underlying representation (u64, f64, i64).
        (sjv::Number(lhs), sjv::Number(rhs)) => {
            let lhs = Number::from(lhs);
            let rhs = Number::from(rhs);
            lhs.partial_cmp(&rhs)
        }
        // Deeply compare array items in lexicographic order.
        (sjv::Array(lhs), sjv::Array(rhs)) => lhs
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
        (sjv::Object(lhs), sjv::Object(rhs)) => lhs
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
    use super::{json_cmp, Number};
    use serde_json::{json, Value};
    use std::cmp::Ordering;

    #[test]
    fn test_multiple_of() {
        use Number::{Float, Signed, Unsigned};

        assert!(Unsigned(32).is_multiple_of(&Unsigned(4)));
        assert!(Unsigned(32).is_multiple_of(&Signed(-4)));
        assert!(Unsigned(32).is_multiple_of(&Float(4.0)));
        assert!(!Unsigned(32).is_multiple_of(&Unsigned(5)));
        assert!(!Unsigned(32).is_multiple_of(&Signed(-5)));
        assert!(!Unsigned(32).is_multiple_of(&Float(4.5)));

        assert!(Signed(32).is_multiple_of(&Unsigned(4)));
        assert!(Signed(-32).is_multiple_of(&Signed(-4)));
        assert!(Signed(-32).is_multiple_of(&Float(4.0)));
        assert!(!Signed(32).is_multiple_of(&Unsigned(5)));
        assert!(!Signed(-32).is_multiple_of(&Signed(-5)));
        assert!(!Signed(-32).is_multiple_of(&Float(4.5)));

        assert!(Float(32.0).is_multiple_of(&Unsigned(4)));
        assert!(Float(-32.0).is_multiple_of(&Signed(-4)));
        assert!(Float(-32.0).is_multiple_of(&Float(4.0)));
        assert!(!Float(32.1).is_multiple_of(&Unsigned(4)));
        assert!(!Float(-32.1).is_multiple_of(&Signed(-4)));
        assert!(!Float(-32.1).is_multiple_of(&Float(4.0)));
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
}
