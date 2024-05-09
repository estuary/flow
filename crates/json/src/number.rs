use serde_json;
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt::Display;

/// `Number` holds possible numeric types of the JSON object model.
#[derive(Debug, Copy, Clone)]
pub enum Number {
    Unsigned(u64),
    Signed(i64),
    Float(f64),
}
use Number::*;

impl Display for Number {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unsigned(n) => write!(f, "{}", n),
            Signed(n) => write!(f, "{}", n),
            Float(n) => write!(f, "{}", n),
        }
    }
}

impl From<&serde_json::Number> for Number {
    fn from(n: &serde_json::Number) -> Self {
        if let Some(n) = n.as_u64() {
            Unsigned(n)
        } else if let Some(n) = n.as_i64() {
            Signed(n)
        } else {
            Float(n.as_f64().unwrap())
        }
    }
}

impl From<u64> for Number {
    fn from(n: u64) -> Self {
        Self::Unsigned(n)
    }
}

impl From<i64> for Number {
    fn from(n: i64) -> Self {
        Self::Signed(n)
    }
}

impl From<f64> for Number {
    fn from(n: f64) -> Self {
        Self::Float(n)
    }
}

impl TryFrom<Number> for serde_json::Value {
    type Error = ();

    fn try_from(n: Number) -> Result<Self, Self::Error> {
        match n {
            Unsigned(n) => Ok(serde_json::Value::Number(n.into())),
            Signed(n) => Ok(serde_json::Value::Number(n.into())),
            Float(n) => match serde_json::Number::from_f64(n) {
                Some(n) => Ok(serde_json::Value::Number(n)),
                None => Err(()),
            },
        }
    }
}

impl Ord for Number {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Unsigned(lhs), Unsigned(rhs)) => lhs.cmp(rhs),
            (Unsigned(_), Signed(rhs)) if *rhs < 0 => Ordering::Greater,
            (Unsigned(lhs), Signed(rhs)) => lhs.cmp(&(*rhs as u64)),
            (Unsigned(lhs), Float(rhs)) => f64_cmp(&(*lhs as f64), rhs),

            (Signed(lhs), Unsigned(_)) if *lhs < 0 => Ordering::Less,
            (Signed(lhs), Unsigned(rhs)) => (*lhs as u64).cmp(&rhs),
            (Signed(lhs), Signed(rhs)) => lhs.cmp(rhs),
            (Signed(lhs), Float(rhs)) => f64_cmp(&(*lhs as f64), rhs),

            (Float(lhs), Unsigned(rhs)) => f64_cmp(lhs, &(*rhs as f64)),
            (Float(lhs), Signed(rhs)) => f64_cmp(lhs, &(*rhs as f64)),
            (Float(lhs), Float(rhs)) => f64_cmp(lhs, rhs),
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
    pub fn checked_add(self: Self, other: Self) -> Option<Self> {
        match (self, other) {
            (Unsigned(lhs), Unsigned(rhs)) => lhs.checked_add(rhs).map(Into::into),
            (Signed(lhs), Signed(rhs)) => lhs.checked_add(rhs).map(Into::into),
            (Float(lhs), Float(rhs)) => f64_checked_add(lhs, rhs).map(Into::into),

            // Promotion of u64 into i64. We require the value be representable.
            (Unsigned(lhs), Signed(rhs)) => i64::try_from(lhs)
                .ok()
                .and_then(|lhs| lhs.checked_add(rhs))
                .map(Into::into),
            (Signed(lhs), Unsigned(rhs)) => i64::try_from(rhs)
                .ok()
                .and_then(|rhs| lhs.checked_add(rhs))
                .map(Into::into),

            // Promotion into f64. We accept loss of precision in these cases.
            (Unsigned(lhs), Float(rhs)) => f64_checked_add(lhs as f64, rhs).map(Into::into),
            (Signed(lhs), Float(rhs)) => f64_checked_add(lhs as f64, rhs).map(Into::into),
            (Float(lhs), Unsigned(rhs)) => f64_checked_add(lhs, rhs as f64).map(Into::into),
            (Float(lhs), Signed(rhs)) => f64_checked_add(lhs, rhs as f64).map(Into::into),
        }
    }

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

fn f64_cmp(lhs: &f64, rhs: &f64) -> Ordering {
    lhs.partial_cmp(rhs).unwrap_or_else(|| {
        if lhs.is_nan() && rhs.is_nan() {
            Ordering::Equal
        } else if lhs.is_nan() {
            Ordering::Less
        } else if rhs.is_nan() {
            Ordering::Greater
        } else {
            panic!("couldn't compare {} and {}", lhs, rhs);
        }
    })
}

fn f64_checked_add(a: f64, b: f64) -> Option<f64> {
    Some(a + b).filter(|f| f.is_finite())
}

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

        assert_eq!(from("1234"), Unsigned(1234));
        assert_eq!(from("-1234"), Signed(-1234));
        assert_eq!(from("12.34"), Float(12.34));
        assert_eq!(from("18446744073709551615"), Unsigned(18446744073709551615));

        // Signed / unsigned integer conversions always succeed.
        expect_eq(Unsigned(1234), 1234 as u64);
        expect_eq(Signed(-1234), -1234 as i64);

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

    #[test]
    fn test_equality() {
        is_eq(Unsigned(10), Unsigned(10));
        is_eq(Signed(-10), Signed(-10));
        is_eq(Float(1.0), Float(1.0));
        is_eq(Unsigned(20), Signed(20));
        is_eq(Unsigned(20), Float(20.00));
        is_eq(Signed(-20), Float(-20.00));

        // NaN is arbitrarily defined to be equal to
        // itself, in order to provide a total ordering.
        use std::f64::{INFINITY, NAN, NEG_INFINITY};
        is_eq(Float(NAN), Float(NAN));
        is_eq(Float(NEG_INFINITY), Float(NEG_INFINITY));
        is_eq(Float(INFINITY), Float(INFINITY));
    }

    #[test]
    fn test_ordering() {
        is_lt(Unsigned(10), Unsigned(11));
        is_lt(Signed(-11), Signed(-10));
        is_lt(Float(1.0), Float(1.1));

        is_lt(Unsigned(10), Float(10.1));
        is_lt(Signed(-10), Float(-9.9));

        is_lt(Signed(10), Unsigned(11));
        is_lt(Signed(-1), Unsigned(0));

        use std::f64::{INFINITY, NAN, NEG_INFINITY};
        is_lt(Signed(10), Float(INFINITY));
        is_lt(Float(NEG_INFINITY), Unsigned(100));
        is_lt(Float(NEG_INFINITY), Float(INFINITY));

        // NaN is arbitrarily defined to be less-than any other value,
        // and equal to itself, in order to provide a total ordering.
        is_lt(Float(NAN), Signed(10));
        is_lt(Float(NAN), Float(NEG_INFINITY));

        // Test cases where an unsigned integer is greater than i64::MAX
        is_lt(Signed(-20), Unsigned(10000000000000000000u64));
        is_lt(Signed(0), Unsigned(10000000000000000000u64));
        is_lt(Unsigned(12), Signed(i64::MAX));
        is_lt(Signed(i64::MIN), Unsigned(u64::MAX));
        is_lt(Signed(i64::MAX), Unsigned(u64::MAX));
        is_lt(Float(-20.0), Unsigned(10000000000000000000u64));
        is_lt(Float(NEG_INFINITY), Unsigned(10000000000000000000u64));
        is_lt(
            Unsigned(10000000000000000000u64),
            Float(11000000000000000000.0),
        );
    }

    #[test]
    fn test_add() {
        assert_eq!(Unsigned(1).checked_add(Unsigned(2)), Some(Unsigned(3)));
        assert_eq!(Signed(-1).checked_add(Signed(-2)), Some(Signed(-3)));
        assert_eq!(Float(1.0).checked_add(Float(2.0)), Some(Float(3.0)));

        assert_eq!(Unsigned(1).checked_add(Signed(-2)), Some(Signed(-1)));
        assert_eq!(Signed(-2).checked_add(Unsigned(3)), Some(Signed(1)));

        assert_eq!(Unsigned(1).checked_add(Float(0.1)), Some(Float(1.1)));
        assert_eq!(Float(-0.1).checked_add(Unsigned(1)), Some(Float(0.9)));

        assert_eq!(Signed(-1).checked_add(Float(2.1)), Some(Float(1.1)));
        assert_eq!(Float(0.1).checked_add(Signed(-2)), Some(Float(-1.9)));
    }

    #[test]
    fn test_add_overflows() {
        // Representable u64 => i64 promotions work.
        assert_eq!(
            Signed(-1).checked_add(Unsigned(u64::MAX / 2)),
            Some(Signed(i64::MAX - 1))
        );
        assert_eq!(
            Unsigned(u64::MAX / 2).checked_add(Signed(-1)),
            Some(Signed(i64::MAX - 1))
        );
        // Un-representable ones don't.
        assert_eq!(Signed(-1).checked_add(Unsigned(1 + (u64::MAX / 2))), None);
        assert_eq!(Unsigned(1 + u64::MAX / 2).checked_add(Signed(-1)), None);

        const MAX_F64_INT: i64 = 1 << f64::MANTISSA_DIGITS;

        // Representable u64 & i64 => f64 promotions work.
        assert_eq!(
            Unsigned(MAX_F64_INT as u64 - 1).checked_add(Float(1.0)),
            Some(Float(MAX_F64_INT as f64))
        );
        assert_eq!(
            Signed(-MAX_F64_INT + 1).checked_add(Float(-1.0)),
            Some(Float(-MAX_F64_INT as f64))
        );

        // We begin to lose precision at the boundaries of f64 integer representation.
        assert_eq!(
            Unsigned(MAX_F64_INT as u64).checked_add(Float(1.0)),
            Some(Float(MAX_F64_INT as f64))
        );
        assert_eq!(
            Signed(-MAX_F64_INT).checked_add(Float(-1.0)),
            Some(Float(-MAX_F64_INT as f64))
        );

        // Cases of overflow.
        assert_eq!(Unsigned(1).checked_add(Unsigned(u64::MAX)), None);
        assert_eq!(Signed(1).checked_add(Signed(i64::MAX)), None);
        assert_eq!(Signed(-1).checked_add(Signed(i64::MIN)), None);
        assert_eq!(Float(f64::MIN).checked_add(Float(f64::MIN / 2.0)), None);
        assert_eq!(Float(f64::MAX).checked_add(Float(f64::MAX / 2.0)), None);
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
