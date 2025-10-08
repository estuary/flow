use crate::{Allocator, HeapNode};
use bigdecimal::BigDecimal;

#[derive(Debug, Clone, PartialEq)]
pub enum ParsedNumber {
    Arbitrary(BigDecimal),
    Float(f64),
    NegInt(i64),
    PosInt(u64),
}
use ParsedNumber::*;

impl ParsedNumber {
    pub fn checked_add(self, other: Self) -> Option<Self> {
        fn f64_checked_add(a: f64, b: f64) -> Option<ParsedNumber> {
            Some(a + b).filter(|f| f.is_finite()).map(Float)
        }

        match (self, other) {
            (PosInt(lhs), PosInt(rhs)) => lhs.checked_add(rhs).map(PosInt),
            (NegInt(lhs), NegInt(rhs)) => lhs.checked_add(rhs).map(NegInt),
            (PosInt(pos), NegInt(neg)) | (NegInt(neg), PosInt(pos)) => {
                let neg = neg.checked_neg()? as u64;

                if pos >= neg {
                    Some(PosInt(pos - neg))
                } else {
                    Some(NegInt(-((neg - pos) as i64)))
                }
            }
            (Float(lhs), Float(rhs)) => f64_checked_add(lhs, rhs),

            // Promotion into f64. We accept loss of precision in these cases.
            (PosInt(lhs), Float(rhs)) => f64_checked_add(lhs as f64, rhs),
            (NegInt(lhs), Float(rhs)) => f64_checked_add(lhs as f64, rhs),
            (Float(lhs), PosInt(rhs)) => f64_checked_add(lhs, rhs as f64),
            (Float(lhs), NegInt(rhs)) => f64_checked_add(lhs, rhs as f64),

            // Promotion into arbitrary precision.
            (Arbitrary(a), b) | (b, Arbitrary(a)) => match b {
                Arbitrary(n) => Some(n),
                Float(n) => n.try_into().ok(),
                NegInt(n) => Some(n.into()),
                PosInt(n) => Some(n.into()),
            }
            .map(|b| Arbitrary(a + b)),
        }
    }

    pub fn into_heap_node<'alloc>(self, alloc: &'alloc Allocator) -> HeapNode<'alloc> {
        match self {
            PosInt(n) => HeapNode::PosInt(n),
            NegInt(n) => HeapNode::NegInt(n),
            Float(n) => HeapNode::Float(n),
            Arbitrary(n) => {
                HeapNode::String(crate::BumpStr::from_str(&n.normalized().to_string(), alloc))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(PosInt(1).checked_add(PosInt(2)), Some(PosInt(3)));
        assert_eq!(NegInt(-1).checked_add(NegInt(-2)), Some(NegInt(-3)));
        assert_eq!(Float(1.0).checked_add(Float(2.0)), Some(Float(3.0)));
        assert_eq!(
            Arbitrary("9000000000000000000".parse().unwrap())
                .checked_add(Arbitrary("10000000000000000000".parse().unwrap())),
            Some(Arbitrary("19000000000000000000".parse().unwrap()))
        );

        // Cases which mix positive and negative integers.
        assert_eq!(PosInt(1).checked_add(NegInt(-2)), Some(NegInt(-1)));
        assert_eq!(NegInt(-2).checked_add(PosInt(3)), Some(PosInt(1)));

        assert_eq!(
            PosInt((i64::MAX as u64) + 3).checked_add(NegInt(-2)),
            Some(PosInt((i64::MAX as u64) + 1))
        );
        assert_eq!(
            NegInt(-5).checked_add(PosInt((i64::MAX as u64) + 3)),
            Some(PosInt((i64::MAX as u64) - 2))
        );
        assert_eq!(
            NegInt(i64::MIN + 1).checked_add(PosInt(2)),
            Some(NegInt(i64::MIN + 3))
        );
        assert_eq!(
            PosInt(i64::MAX as u64).checked_add(NegInt(i64::MIN + 1)),
            Some(PosInt(0))
        );

        // Cases which promote into f64.
        assert_eq!(PosInt(1).checked_add(Float(0.1)), Some(Float(1.1)));
        assert_eq!(Float(-0.1).checked_add(PosInt(1)), Some(Float(0.9)));
        assert_eq!(NegInt(-1).checked_add(Float(2.1)), Some(Float(1.1)));
        assert_eq!(Float(0.1).checked_add(NegInt(-2)), Some(Float(-1.9)));

        // Cases which promote into arbitrary.
        assert_eq!(
            PosInt(32).checked_add(Arbitrary(5.into())),
            Some(Arbitrary("37".parse().unwrap()))
        );
        assert_eq!(
            Arbitrary(u64::MAX.into()).checked_add(Float(32.5)),
            Some(Arbitrary("18446744073709551647.5".parse().unwrap()))
        );
        assert_eq!(
            NegInt(i64::MIN).checked_add(Arbitrary((-125).into())),
            Some(Arbitrary("-9223372036854775933".parse().unwrap()))
        );
    }

    #[test]
    fn test_add_overflows() {
        // Representable u64 => i64 promotions work.
        assert_eq!(
            NegInt(-1).checked_add(PosInt(u64::MAX / 2)),
            Some(PosInt(i64::MAX as u64 - 1))
        );
        assert_eq!(
            PosInt(u64::MAX / 2).checked_add(NegInt(-1)),
            Some(PosInt(i64::MAX as u64 - 1))
        );

        const MAX_F64_INT: i64 = 1 << f64::MANTISSA_DIGITS;

        // Representable u64 & i64 => f64 promotions work.
        assert_eq!(
            PosInt(MAX_F64_INT as u64 - 1).checked_add(Float(1.0)),
            Some(Float(MAX_F64_INT as f64))
        );
        assert_eq!(
            NegInt(-MAX_F64_INT + 1).checked_add(Float(-1.0)),
            Some(Float(-MAX_F64_INT as f64))
        );

        // We begin to lose precision at the boundaries of f64 integer representation.
        assert_eq!(
            PosInt(MAX_F64_INT as u64).checked_add(Float(1.0)),
            Some(Float(MAX_F64_INT as f64))
        );
        assert_eq!(
            NegInt(-MAX_F64_INT).checked_add(Float(-1.0)),
            Some(Float(-MAX_F64_INT as f64))
        );

        // Cases of overflow.
        assert_eq!(PosInt(1).checked_add(PosInt(u64::MAX)), None);
        assert_eq!(NegInt(1).checked_add(NegInt(i64::MAX)), None);
        assert_eq!(NegInt(-1).checked_add(NegInt(i64::MIN)), None);
        assert_eq!(Float(f64::MIN).checked_add(Float(f64::MIN / 2.0)), None);
        assert_eq!(Float(f64::MAX).checked_add(Float(f64::MAX / 2.0)), None);
        assert_eq!(NegInt(i64::MIN).checked_add(PosInt(1)), None); // Cannot negate i64::MIN.
    }
}
