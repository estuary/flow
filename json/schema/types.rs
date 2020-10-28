use super::Number;
use serde_json::Value;
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

const ALL: &[Set] = &[ARRAY, BOOLEAN, INTEGER, NULL, NUMBER, OBJECT, STRING];

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

impl std::ops::Not for Set {
    type Output = Self;

    fn not(self) -> Self::Output {
        // AND with ANY to ensure that none of the unused bits are set. Just a bit of caution to
        // prevent garbage data leaking out.
        Set((!self.0) & ANY.0)
    }
}

/// Iterator that returns the type names for all of the types in a `Set`. You get this iterator by
/// calling `Set::iter`.
pub struct Iter {
    types: Set,
    index: usize,
}
impl Iterator for Iter {
    type Item = &'static str;

    fn next(&mut self) -> Option<Self::Item> {
        let Iter {
            types,
            ref mut index,
        } = self;
        loop {
            let ty = ALL.get(*index)?;
            *index += 1;

            if types.overlaps(*ty) {
                match *ty {
                    ARRAY => return Some("array"),
                    BOOLEAN => return Some("boolean"),
                    INTEGER => return Some("integer"),
                    NULL => return Some("null"),
                    NUMBER => return Some("number"),
                    OBJECT => return Some("object"),
                    STRING => return Some("string"),
                    _ => unreachable!(),
                }
            }
        }
    }
}

impl<A> std::iter::FromIterator<A> for Set
where
    A: AsRef<str>,
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = A>,
    {
        let mut s = INVALID;
        for ty in iter {
            if let Some(t) = Set::for_type_name(ty.as_ref()) {
                s = s | t;
            } else {
                return INVALID;
            }
        }
        s
    }
}

impl Set {
    /// Returns an iterator over the type names as static strings.
    ///
    /// ```
    /// use estuary_json::schema::types::*;
    ///
    /// let ty = ARRAY | OBJECT | NULL;
    ///
    /// let names = ty.iter().collect::<Vec<&'static str>>();
    /// assert_eq!(vec!["array", "null", "object"], names);
    ///
    /// let ty = INVALID;
    /// let mut iter = ty.iter();
    /// assert!(iter.next().is_none());
    /// ```
    pub fn iter(&self) -> Iter {
        Iter {
            types: *self,
            index: 0,
        }
    }

    /// Returns a vec containing owned strings representing the types in this set.
    ///
    /// ```
    /// use estuary_json::schema::types::*;
    ///
    /// let ty = ARRAY | OBJECT | NULL;
    ///
    /// let names = ty.to_vec();
    /// assert_eq!(vec!["array".to_owned(), "null".to_owned(), "object".to_owned()], names);
    ///
    /// ```
    pub fn to_vec(&self) -> Vec<String> {
        self.iter().map(String::from).collect()
    }

    pub fn to_json_array(&self) -> String {
        format!("[{}]", self)
    }

    /// Returns the `Set` value for a single type with the given name.
    ///
    /// ```
    /// use estuary_json::schema::types::*;
    /// assert_eq!(Some(NUMBER), Set::for_type_name("number"));
    /// assert_eq!(Some(INTEGER), Set::for_type_name("integer"));
    /// assert_eq!(Some(BOOLEAN), Set::for_type_name("boolean"));
    /// assert_eq!(Some(OBJECT), Set::for_type_name("object"));
    /// assert_eq!(Some(ARRAY), Set::for_type_name("array"));
    /// assert_eq!(Some(NULL), Set::for_type_name("null"));
    /// assert!(Set::for_type_name("not a real type").is_none());
    /// ```
    pub fn for_type_name(str_val: &str) -> Option<Set> {
        match str_val {
            "array" => Some(ARRAY),
            "boolean" => Some(BOOLEAN),
            "integer" => Some(INTEGER),
            "null" => Some(NULL),
            "number" => Some(NUMBER),
            "object" => Some(OBJECT),
            "string" => Some(STRING),
            _ => None,
        }
    }

    pub fn for_value(val: &Value) -> Set {
        match val {
            Value::Array(_) => ARRAY,
            Value::Bool(_) => BOOLEAN,
            Value::Null => NULL,
            Value::Number(n) => match Number::from(n) {
                Number::Float(_) => NUMBER,
                Number::Signed(_) | Number::Unsigned(_) => NUMBER | INTEGER,
            },
            Value::Object(_) => OBJECT,
            Value::String(_) => STRING,
        }
    }

    #[inline]
    pub fn overlaps(&self, other: Self) -> bool {
        *self & other != INVALID
    }

    /// Returns true if this Set represents exactly one scalar type besides null.
    ///
    /// ```
    /// use estuary_json::schema::types::*;
    ///
    /// assert!(STRING.is_single_scalar_type());
    /// assert!(INTEGER.is_single_scalar_type());
    /// assert!(BOOLEAN.is_single_scalar_type());
    /// assert!(NUMBER.is_single_scalar_type());
    /// assert!((STRING | NULL).is_single_scalar_type());
    ///
    /// assert!(!(NULL.is_single_scalar_type()));
    /// assert!(!(OBJECT.is_single_scalar_type()));
    /// assert!(!(ARRAY.is_single_scalar_type()));
    /// assert!(!(INVALID.is_single_scalar_type()));
    ///
    /// assert!(!((OBJECT | INTEGER).is_single_scalar_type()));
    /// assert!(!((STRING | BOOLEAN).is_single_scalar_type()));
    /// ```
    pub fn is_single_scalar_type(&self) -> bool {
        let without_null = *self & (!NULL);
        match without_null {
            INTEGER | BOOLEAN | STRING | NUMBER => true,
            _ => false,
        }
    }
}

impl fmt::Debug for Set {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use itertools::Itertools;
        write!(f, "{:?}", self.iter().format(", "))
    }
}

impl fmt::Display for Set {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use itertools::Itertools;
        write!(f, "{:?}", self.iter().format(", "))
    }
}

impl serde::Serialize for Set {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_seq(self.iter())
    }
}

struct SetVisitor;
impl<'de> serde::de::Visitor<'de> for SetVisitor {
    type Value = Set;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "an array of strings")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut s = INVALID;
        while let Some(type_str) = seq.next_element::<&str>()? {
            if let Some(t) = Set::for_type_name(type_str) {
                s = s | t;
            } else {
                return Err(serde::de::Error::custom(format!(
                    "invalid type name: '{}'",
                    type_str
                )));
            }
        }
        Ok(s)
    }
}
impl<'de> serde::Deserialize<'de> for Set {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_seq(SetVisitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn round_trip_set_serde() {
        let input = ARRAY | NULL | INTEGER;

        let json = serde_json::to_string(&input).unwrap();
        assert_eq!(r##"["array", "null", "integer"]"##, &json);

        let result = serde_json::from_str::<Set>(&json).unwrap();
        assert_eq!(input, result);
    }
}
