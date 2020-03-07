use serde_json;
use std::cmp::Ordering;

/// `Number` holds possible numeric types of the JSON object model.
#[derive(Debug)]
pub enum Number {
    Unsigned(u64),
    Signed(i64),
    Float(f64),
}
use Number::*;

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

// convert to value instead, and NULL if not a valid float ?
impl From<Number> for serde_json::Value {
    fn from(n: Number) -> Self {
        match n {
            Unsigned(n) => serde_json::Value::Number(n.into()),
            Signed(n) => serde_json::Value::Number(n.into()),
            Float(n) => serde_json::Number::from_f64(n)
                .map(|n| serde_json::Value::Number(n))
                .unwrap_or(serde_json::Value::Null),
        }
    }
}

impl std::ops::AddAssign<u64> for Number {
    fn add_assign(&mut self, rhs: u64) {
        match self {
            Unsigned(lhs) => *lhs += rhs,
            Signed(lhs) => *lhs += rhs as i64,
            Float(lhs) => *lhs += rhs as f64,
        }
    }
}

impl std::ops::AddAssign<i64> for Number {
    fn add_assign(&mut self, rhs: i64) {
        match self {
            Unsigned(lhs) => *self = Signed((*lhs as i64) + rhs),
            Signed(lhs) => *lhs += rhs,
            Float(lhs) => *lhs += rhs as f64,
        }
    }
}

impl std::ops::AddAssign<f64> for Number {
    fn add_assign(&mut self, rhs: f64) {
        match self {
            Unsigned(lhs) => *self = Float((*lhs as f64) + rhs),
            Signed(lhs) => *self = Float((*lhs as f64) + rhs),
            Float(lhs) => *lhs += rhs,
        }
    }
}

impl std::ops::AddAssign<Number> for Number {
    fn add_assign(&mut self, rhs: Number) {
        match rhs {
            Unsigned(rhs) => *self += rhs,
            Signed(rhs) => *self += rhs,
            Float(rhs) => *self += rhs,
        }
    }
}

impl std::ops::Add for Number {
    type Output = Self;

    fn add(self, rhs: Number) -> Self::Output {
        let mut lhs = self;
        lhs += rhs;
        lhs
    }
}

impl Ord for Number {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Unsigned(lhs), Unsigned(rhs)) => lhs.cmp(rhs),
            (Unsigned(lhs), Signed(rhs)) => (*lhs as i64).cmp(rhs),
            (Unsigned(lhs), Float(rhs)) => f64_cmp(&(*lhs as f64), rhs),

            (Signed(lhs), Unsigned(rhs)) => lhs.cmp(&(*rhs as i64)),
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_number_conversion() {
        fn from(s: &str) -> Number {
            let n: serde_json::Number = serde_json::from_str(s).unwrap();
            Number::from(&n)
        }

        assert_eq!(from("1234"), Unsigned(1234));
        assert_eq!(from("-1234"), Signed(-1234));
        assert_eq!(from("12.34"), Float(12.34));

        fn to(n: serde_json::Value) -> serde_json::Value {
            n
        }

        // Signed / unsigned integer conversions always succeed.
        assert_eq!(to(Unsigned(1234).into()), to((1234 as u64).into()));
        assert_eq!(to(Signed(-1234).into()), to((-1234 as i64).into()));

        assert_eq!(to(Float(-12.34).into()), to((-12.34 as f64).into()));
        assert_eq!(to(Float(0.0).into()), to((0.0 as f64).into()));
        assert_eq!(to(Float(std::f64::MIN).into()), to(std::f64::MIN.into()));
        assert_eq!(to(Float(std::f64::MAX).into()), to(std::f64::MAX.into()));

        // Float converts to NULL if it's not a representable number in JSON.
        assert_eq!(to(Float(std::f64::NAN).into()), serde_json::Value::Null);
        assert_eq!(
            to(Float(std::f64::INFINITY).into()),
            serde_json::Value::Null
        );
        assert_eq!(
            to(Float(std::f64::NEG_INFINITY).into()),
            serde_json::Value::Null
        );
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
    }

    #[test]
    fn test_add() {
        is_eq(Unsigned(1) + Unsigned(2), Unsigned(3));
        is_eq(Signed(-1) + Signed(-2), Signed(-3));
        is_eq(Float(1.0) + Float(2.0), Float(3.0));

        is_eq(Unsigned(1) + Signed(-2), Signed(-1));
        is_eq(Signed(-2) + Unsigned(3), Signed(1));

        is_eq(Unsigned(1) + Float(0.1), Float(1.1));
        is_eq(Float(-0.1) + Unsigned(1), Float(0.9));

        is_eq(Signed(-1) + Float(2.1), Float(1.1));
        is_eq(Float(0.1) + Signed(-2), Float(-1.9));
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
