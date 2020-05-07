use std::fmt;

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
pub struct Set(u32);

pub const INVALID: Set = Set(0b0000000);
pub const ARRAY: Set = Set(0b0000001);
pub const BOOLEAN: Set = Set(0b0000010);
pub const INTEGER: Set = Set(0b0000100);
pub const NULL: Set = Set(0b0001000);
pub const NUMBER: Set = Set(0b0010000);
pub const OBJECT: Set = Set(0b0100000);
pub const STRING: Set = Set(0b1000000);
pub const ANY: Set = Set(ARRAY.0 | BOOLEAN.0 | INTEGER.0 | NULL.0 | NUMBER.0 | OBJECT.0 | STRING.0);

impl std::ops::BitOr for Set {
    type Output = Self;

    fn bitor(self, other: Self) -> Self::Output {
        Set(self.0 | other.0)
    }
}

impl std::ops::BitAnd for Set {
    type Output = Self;

    fn bitand(self, other: Self) -> Self::Output {
        Set(self.0 & other.0)
    }
}

impl Set {
    pub fn as_str<'a>(&self, mut s: Vec<&'static str>) -> Vec<&'static str> {
        if self.0 & ARRAY.0 != 0 {
            s.push("array")
        }
        if self.0 & BOOLEAN.0 != 0 {
            s.push("boolean")
        }
        if self.0 & INTEGER.0 != 0 {
            s.push("integer")
        }
        if self.0 & NULL.0 != 0 {
            s.push("null")
        }
        if self.0 & NUMBER.0 != 0 {
            s.push("number")
        }
        if self.0 & OBJECT.0 != 0 {
            s.push("object")
        }
        if self.0 & STRING.0 != 0 {
            s.push("string")
        }
        s
    }

    #[inline]
    pub fn overlaps(&self, other: Self) -> bool {
        *self & other != INVALID
    }
}

impl fmt::Debug for Set {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str(Vec::new()).fmt(f)
    }
}
