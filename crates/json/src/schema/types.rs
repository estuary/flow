use super::Number;
use serde_json::Value;
use std::fmt;

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
pub struct Set(u32);

pub const INVALID: Set = Set(0b0000000);
pub const ARRAY: Set = Set(0b0000001);
pub const BOOLEAN: Set = Set(0b0000010);
pub const FRACTIONAL: Set = Set(0b0000100);
pub const INTEGER: Set = Set(0b0001000);
pub const NULL: Set = Set(0b0010000);
pub const OBJECT: Set = Set(0b0100000);
pub const STRING: Set = Set(0b1000000);
// INT_OR_FRACT is a composite for "number". It's not called NUMBER to avoid
// giving the impression that this is a fundamental type.
pub const INT_OR_FRAC: Set = Set(INTEGER.0 | FRACTIONAL.0);
// ANY is a composite for all possible types.
pub const ANY: Set =
    Set(ARRAY.0 | BOOLEAN.0 | FRACTIONAL.0 | INTEGER.0 | NULL.0 | OBJECT.0 | STRING.0);

impl std::ops::BitOr for Set {
    type Output = Self;

    #[inline]
    fn bitor(self, other: Self) -> Self::Output {
        Set(self.0 | other.0)
    }
}

impl std::ops::BitAnd for Set {
    type Output = Self;

    #[inline]
    fn bitand(self, other: Self) -> Self::Output {
        Set(self.0 & other.0)
    }
}

impl std::ops::Sub for Set {
    type Output = Self;

    #[inline]
    fn sub(self, other: Self) -> Self::Output {
        Set(self.0 & !other.0)
    }
}

/// Iterator that returns the type names for all of the types in a `Set`.
/// You get this iterator by calling `Set::iter`.
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

        const ITER_ORDER: &[Set] = &[
            ARRAY,
            BOOLEAN,
            FRACTIONAL,
            INTEGER,
            NULL,
            INT_OR_FRAC, // "number" sorts after "null".
            OBJECT,
            STRING,
        ];

        loop {
            let ty = ITER_ORDER.get(*index)?;
            *index += 1;

            // Is |ty| a subset of |types|?
            if *ty - *types == INVALID {
                match *ty {
                    ARRAY => return Some("array"),
                    BOOLEAN => return Some("boolean"),
                    FRACTIONAL if !types.overlaps(INTEGER) => return Some("fractional"),
                    INTEGER if !types.overlaps(FRACTIONAL) => return Some("integer"),
                    FRACTIONAL | INTEGER => (),
                    NULL => return Some("null"),
                    INT_OR_FRAC => return Some("number"),
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
    /// use json::schema::types::*;
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
    /// use json::schema::types::*;
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
    /// use json::schema::types::*;
    /// assert_eq!(Some(FRACTIONAL | INTEGER), Set::for_type_name("number"));
    /// assert_eq!(Some(FRACTIONAL), Set::for_type_name("fractional"));
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
            "fractional" => Some(FRACTIONAL),
            "integer" => Some(INTEGER),
            "null" => Some(NULL),
            "number" => Some(INT_OR_FRAC),
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
            Value::Number(n) => Self::for_number(&Number::from(n)),
            Value::Object(_) => OBJECT,
            Value::String(_) => STRING,
        }
    }

    pub fn for_number(num: &Number) -> Set {
        match num {
            // The json schema spec says that the "integer" type must match
            // "any number with a zero fractional part":
            // https://json-schema.org/draft/2019-09/json-schema-validation.html#rfc.section.6.1.1
            // So if there's an actual fractional part, then only "number" is valid,
            // but for any other numeric value, then "integer" is also valid.
            Number::Float(value) if value.fract() != 0.0 => FRACTIONAL,
            _ => INTEGER,
        }
    }

    #[inline]
    pub fn overlaps(&self, other: Self) -> bool {
        *self & other != INVALID
    }

    /// Returns true if this Set represents exactly one type beside null.
    ///
    /// ```
    /// use json::schema::types::*;
    ///
    /// assert!(STRING.is_single_type());
    /// assert!(INTEGER.is_single_type());
    /// assert!(FRACTIONAL.is_single_type());
    /// assert!(BOOLEAN.is_single_type());
    /// assert!(INT_OR_FRAC.is_single_type());
    /// assert!((STRING | NULL).is_single_type());
    /// assert!((ARRAY | NULL).is_single_type());
    /// assert!(OBJECT.is_single_type());
    ///
    /// assert!(!((STRING | BOOLEAN).is_single_type()));
    /// assert!(!((OBJECT | INTEGER).is_single_type()));
    /// assert!(!(INVALID.is_single_scalar_type()));
    /// assert!(!(NULL.is_single_scalar_type()));
    /// ```
    pub fn is_single_type(&self) -> bool {
        match *self - NULL {
            BOOLEAN | INT_OR_FRAC | FRACTIONAL | INTEGER | STRING | OBJECT | ARRAY => true,
            _ => false,
        }
    }

    /// Returns true if this Set represents exactly one scalar type besides null.
    ///
    /// ```
    /// use json::schema::types::*;
    ///
    /// assert!(STRING.is_single_scalar_type());
    /// assert!(INTEGER.is_single_scalar_type());
    /// assert!(FRACTIONAL.is_single_scalar_type());
    /// assert!(BOOLEAN.is_single_scalar_type());
    /// assert!(INT_OR_FRAC.is_single_scalar_type());
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
        match *self - NULL {
            BOOLEAN | INT_OR_FRAC | FRACTIONAL | INTEGER | STRING => true,
            _ => false,
        }
    }

    /// Returns true if this Set represents a key-able type,
    /// which is restricted to integers, strings, booleans,
    /// where each is further allowed to be null.
    ///
    /// ```
    /// use json::schema::types::*;
    ///
    /// assert!(STRING.is_keyable_type());
    /// assert!(INTEGER.is_keyable_type());
    /// assert!(FRACTIONAL.is_keyable_type());
    /// assert!(BOOLEAN.is_keyable_type());
    /// assert!(INT_OR_FRAC.is_keyable_type());
    /// assert!((STRING | NULL).is_keyable_type());
    ///
    /// assert!(!(NULL.is_keyable_type()));
    /// assert!(!(OBJECT.is_keyable_type()));
    /// assert!(!(ARRAY.is_keyable_type()));
    /// assert!(!(INVALID.is_keyable_type()));
    ///
    /// assert!(!((OBJECT | INTEGER).is_keyable_type()));
    /// assert!(!((STRING | BOOLEAN).is_keyable_type()));
    /// ```
    pub fn is_keyable_type(&self) -> bool {
        match *self - NULL {
            BOOLEAN | INTEGER | INT_OR_FRAC | FRACTIONAL | STRING => true,
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

/// Deserializes a Set, which may be represented either as a single string, or as an array of
/// strings.
struct SetVisitor;
impl<'de> serde::de::Visitor<'de> for SetVisitor {
    type Value = Set;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a string or an array of strings")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        TypeStrVisitor.visit_str(value).map(TypeStr::into_set)
    }

    // used when calling serde_json::from_value, since the string will be owned in that case
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(value.as_str())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        TypeStrVisitor
            .visit_unit()
            .map(|_| unreachable!("visit_unit always returns error"))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut s = INVALID;
        while let Some(type_str) = seq.next_element::<TypeStr>()? {
            s = s | type_str.into_set()
        }
        Ok(s)
    }
}
impl<'de> serde::Deserialize<'de> for Set {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(SetVisitor)
    }
}

/// Exists to provide a deserialize impl that can only accept a single string and provides a more
/// helpful error message when it encounters a null value. A common mistake in YAML is to forget to
/// put quotes around "null", which causes it to intrepreted as the null keyword instead of the
/// string "null".
struct TypeStr(Set);
impl TypeStr {
    fn into_set(self) -> Set {
        self.0
    }
}
struct TypeStrVisitor;
impl<'de> serde::de::Visitor<'de> for TypeStrVisitor {
    type Value = TypeStr;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "a string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if let Some(ty) = Set::for_type_name(value) {
            Ok(TypeStr(ty))
        } else {
            Err(serde::de::Error::custom(format!(
                "invalid type name: '{}'",
                value
            )))
        }
    }
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(v.as_str())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Err(serde::de::Error::custom(
            "the type \"null\" must be written as a quoted string (null is a keyword in YAML).",
        ))
    }
}
impl<'de> serde::Deserialize<'de> for TypeStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(TypeStrVisitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn set_is_deserialized_from_a_single_string() {
        assert_eq!(ARRAY, serde_json::from_str("\"array\"").unwrap());
        assert_eq!(BOOLEAN, serde_json::from_str("\"boolean\"").unwrap());
        assert_eq!(OBJECT, serde_json::from_str("\"object\"").unwrap());
        assert_eq!(INT_OR_FRAC, serde_json::from_str("\"number\"").unwrap());
        assert_eq!(NULL, serde_json::from_str("\"null\"").unwrap());
        assert_eq!(INTEGER, serde_json::from_str("\"integer\"").unwrap());
        assert_eq!(FRACTIONAL, serde_json::from_str("\"fractional\"").unwrap());
        assert_eq!(STRING, serde_json::from_str("\"string\"").unwrap());
    }

    #[test]
    fn set_deserialize_returns_error_when_null_is_unquoted() {
        let err = serde_json::from_str::<Set>(r#"["string", null]"#)
            .expect_err("expected deserialize to return an error");
        assert!(
            err.to_string()
                .contains("the type \"null\" must be written as a quoted string"),
            "err is not what was expected: {:?}",
            err
        );

        let err = serde_json::from_str::<Set>("null")
            .expect_err("expected deserialize to return an error");
        assert!(
            err.to_string()
                .contains("the type \"null\" must be written as a quoted string"),
            "err is not what was expected: {:?}",
            err
        );
    }

    #[test]
    fn set_is_deserialized_from_an_owned_value() {
        let input = serde_json::json!("boolean");
        // Calling from_value requires `Visitor::visit_string` to be implemented
        let ty = serde_json::from_value(input).unwrap();
        assert_eq!(BOOLEAN, ty);
    }

    #[test]
    fn round_trip_set_serde() {
        let input = ARRAY | NULL | INTEGER;

        let json = serde_json::to_string(&input).unwrap();
        assert_eq!(r##"["array","integer","null"]"##, &json);

        let result = serde_json::from_str::<Set>(&json).unwrap();
        assert_eq!(input, result);
    }

    #[test]
    fn set_is_serialized_as_an_array_even_when_there_is_only_one_possible_type() {
        assert_eq!(
            r##"["integer"]"##,
            &serde_json::to_string(&INTEGER).unwrap()
        );
    }

    #[test]
    fn set_number_iteration() {
        assert_eq!(
            r##"["null","number"]"##,
            &serde_json::to_string(&(NULL | INT_OR_FRAC)).unwrap()
        );
        assert_eq!(
            r##"["fractional","null"]"##,
            &serde_json::to_string(&(NULL | FRACTIONAL)).unwrap()
        );
        assert_eq!(
            r##"["integer","null"]"##,
            &serde_json::to_string(&(NULL | INTEGER)).unwrap()
        );
    }
}
