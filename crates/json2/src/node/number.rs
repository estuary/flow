use std::cmp::Ordering;

// Number is a type-erased subset of the Node variants that hold a native number.
#[derive(Debug, Copy, Clone)]
pub enum Number {
    Float(f64),
    NegInt(i64),
    PosInt(u64),
}

impl Number {
    #[inline]
    pub fn from_node<N: crate::AsNode>(node: &N) -> Option<Self> {
        match node.as_node() {
            crate::Node::PosInt(n) => Some(Self::PosInt(n)),
            crate::Node::NegInt(n) => Some(Self::NegInt(n)),
            crate::Node::Float(n) => Some(Self::Float(n)),
            _ => None,
        }
    }
}

impl Ord for Number {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Trivial numeric comparisons.
            (Number::NegInt(lhs), Number::NegInt(rhs)) => lhs.cmp(&rhs),
            (Number::PosInt(lhs), Number::PosInt(rhs)) => lhs.cmp(&rhs),
            (Number::Float(lhs), Number::Float(rhs)) => lhs.total_cmp(&rhs),
            (Number::NegInt(_), Number::PosInt(_)) => Ordering::Less,
            (Number::PosInt(_), Number::NegInt(_)) => Ordering::Greater,

            // Cross-type numeric comparisons that project to f64.
            (Number::PosInt(lhs), Number::Float(rhs)) => (*lhs as f64).total_cmp(&rhs),
            (Number::Float(lhs), Number::PosInt(rhs)) => lhs.total_cmp(&(*rhs as f64)),
            (Number::NegInt(lhs), Number::Float(rhs)) => (*lhs as f64).total_cmp(&rhs),
            (Number::Float(lhs), Number::NegInt(rhs)) => lhs.total_cmp(&(*rhs as f64)),
        }
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Number {}

impl Number {
    pub fn is_multiple_of(&self, d: &Self) -> bool {
        match *d {
            Self::PosInt(d) => match *self {
                Self::PosInt(n) => n % d == 0,
                Self::NegInt(n) => n % (d as i64) == 0,
                Self::Float(n) => (n / (d as f64)).fract() == 0.0,
            },
            Self::NegInt(d) => match *self {
                Self::PosInt(n) => (n as i64) % d == 0,
                Self::NegInt(n) => n % d == 0,
                Self::Float(n) => (n / (d as f64)).fract() == 0.0,
            },
            Self::Float(d) => match *self {
                Self::PosInt(n) => (n as f64) % d == 0.0,
                Self::NegInt(n) => (n as f64) % d == 0.0,
                Self::Float(n) => (n / d).fract() == 0.0,
            },
        }
    }
}

/*

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::Value;
    use std::convert::TryInto;

    fn expect_eq<L, R>(l: L, r: R)
    where
        L: TryInto<Value>,
        R: TryInto<Value>,
        L::Error: std::fmt::Debug,
        R::Error: std::fmt::Debug,
    {
        assert_eq!(l.try_into().unwrap(), r.try_into().unwrap());
    }

    #[test]
    fn test_number_conversion() {
        fn from(s: &str) -> Number {
            let n: serde_json::Number = serde_json::from_str(s).unwrap();
            Number::from(&n)
        }

        assert_eq!(from("1234"), PosInt(1234));
        assert_eq!(from("-1234"), NegInt(-1234));
        assert_eq!(from("12.34"), Float(12.34));
        assert_eq!(from("18446744073709551615"), PosInt(18446744073709551615));

        // Signed / unsigned integer conversions always succeed.
        expect_eq(PosInt(1234), 1234 as u64);
        expect_eq(NegInt(-1234), -1234 as i64);

        expect_eq(Float(-12.34), -12.34 as f64);
        expect_eq(Float(0.0), 0.0 as f64);
        expect_eq(Float(std::f64::MIN), std::f64::MIN);
        expect_eq(Float(std::f64::MAX), std::f64::MAX);

        // Float conversions fail if it's not a representable number in JSON.
        assert!(Value::try_from(Float(std::f64::NAN)).is_err());
        assert!(Value::try_from(Float(std::f64::INFINITY)).is_err());
        assert!(Value::try_from(Float(std::f64::NEG_INFINITY)).is_err());
    }

    #[test]
    fn test_multiple_of() {
        assert!(PosInt(32).is_multiple_of(&PosInt(4)));
        assert!(PosInt(32).is_multiple_of(&NegInt(-4)));
        assert!(PosInt(32).is_multiple_of(&Float(4.0)));
        assert!(!PosInt(32).is_multiple_of(&PosInt(5)));
        assert!(!PosInt(32).is_multiple_of(&NegInt(-5)));
        assert!(!PosInt(32).is_multiple_of(&Float(4.5)));

        assert!(NegInt(32).is_multiple_of(&PosInt(4)));
        assert!(NegInt(-32).is_multiple_of(&NegInt(-4)));
        assert!(NegInt(-32).is_multiple_of(&Float(4.0)));
        assert!(!NegInt(32).is_multiple_of(&PosInt(5)));
        assert!(!NegInt(-32).is_multiple_of(&NegInt(-5)));
        assert!(!NegInt(-32).is_multiple_of(&Float(4.5)));

        assert!(Float(32.0).is_multiple_of(&PosInt(4)));
        assert!(Float(-32.0).is_multiple_of(&NegInt(-4)));
        assert!(Float(-32.0).is_multiple_of(&Float(4.0)));
        assert!(!Float(32.1).is_multiple_of(&PosInt(4)));
        assert!(!Float(-32.1).is_multiple_of(&NegInt(-4)));
        assert!(!Float(-32.1).is_multiple_of(&Float(4.0)));
    }

    #[test]
    fn test_equality() {
        is_eq(PosInt(10), PosInt(10));
        is_eq(NegInt(-10), NegInt(-10));
        is_eq(Float(1.0), Float(1.0));
        is_eq(PosInt(20), NegInt(20));
        is_eq(PosInt(20), Float(20.00));
        is_eq(NegInt(-20), Float(-20.00));

        // NaN is arbitrarily defined to be equal to
        // itself, in order to provide a total ordering.
        use std::f64::{INFINITY, NAN, NEG_INFINITY};
        is_eq(Float(NAN), Float(NAN));
        is_eq(Float(NEG_INFINITY), Float(NEG_INFINITY));
        is_eq(Float(INFINITY), Float(INFINITY));
    }

    #[test]
    fn test_ordering() {
        is_lt(PosInt(10), PosInt(11));
        is_lt(NegInt(-11), NegInt(-10));
        is_lt(Float(1.0), Float(1.1));

        is_lt(PosInt(10), Float(10.1));
        is_lt(NegInt(-10), Float(-9.9));

        is_lt(NegInt(10), PosInt(11));
        is_lt(NegInt(-1), PosInt(0));

        use std::f64::{INFINITY, NAN, NEG_INFINITY};
        is_lt(NegInt(10), Float(INFINITY));
        is_lt(Float(NEG_INFINITY), PosInt(100));
        is_lt(Float(NEG_INFINITY), Float(INFINITY));

        // NaN is arbitrarily defined to be less-than any other value,
        // and equal to itself, in order to provide a total ordering.
        is_lt(Float(NAN), NegInt(10));
        is_lt(Float(NAN), Float(NEG_INFINITY));

        // Test cases where an unsigned integer is greater than i64::MAX
        is_lt(NegInt(-20), PosInt(10000000000000000000u64));
        is_lt(NegInt(0), PosInt(10000000000000000000u64));
        is_lt(PosInt(12), NegInt(i64::MAX));
        is_lt(NegInt(i64::MIN), PosInt(u64::MAX));
        is_lt(NegInt(i64::MAX), PosInt(u64::MAX));
        is_lt(Float(-20.0), PosInt(10000000000000000000u64));
        is_lt(Float(NEG_INFINITY), PosInt(10000000000000000000u64));
        is_lt(
            PosInt(10000000000000000000u64),
            Float(11000000000000000000.0),
        );
    }

    fn is_lt(lhs: Number, rhs: Number) {
        assert_eq!(lhs.cmp(&rhs), Ordering::Less);
        assert_eq!(rhs.cmp(&lhs), Ordering::Greater);
    }

    fn is_eq(lhs: Number, rhs: Number) {
        assert_eq!(lhs, rhs);
        assert_eq!(rhs, lhs);
    }
}

*/
