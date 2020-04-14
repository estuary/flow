//! Workaround for https://github.com/serde-rs/json/issues/587 by @cormacrelf


use serde::de::{self, Deserialize, Deserializer, Visitor};
use std::borrow::Cow;
use std::fmt;

/// An internal wrapper on which to mount a custom Deserialize implementation.
struct CowStr<'a>(Cow<'a, str>);

impl<'de> Deserialize<'de> for CowStr<'de> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(CowStrVisitor)
    }
}

/// Does the heavy lifting of visiting borrowed strings
struct CowStrVisitor;

impl<'de> Visitor<'de> for CowStrVisitor {
    type Value = CowStr<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string")
    }

    // Borrowed directly from the input string, which has lifetime 'de
    // The input must outlive the resulting Cow.
    fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(CowStr(Cow::Borrowed(value)))
    }

    // A string that currently only lives in a temporary buffer -- we need a copy
    // (Example: serde is reading from a BufRead)
    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(CowStr(Cow::Owned(value.to_owned())))
    }

    // An optimisation of visit_str for situations where the deserializer has
    // already taken ownership. For example, the string contains escaped characters.
    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(CowStr(Cow::Owned(value)))
    }
}

/// Deserializes a Cow<str> as Borrowed where possible.
/// The default impl for Cow does not do this: https://github.com/serde-rs/json/issues/587
///
/// To benefit, you must use `serde_json::from_str` or another deserializer that supports borrowed data.
///
/// ## Usage
///
/// ```
/// use std::borrow::Cow;
/// #[derive(serde_derive::Deserialize)]
/// struct MyStruct<'a> {
///     #[serde(borrow, deserialize_with = "playground::deserialize_cow_str")]
///     field: Cow<'a, str>,
/// }
///
pub fn deserialize_cow_str<'de, D>(deserializer: D) -> Result<Cow<'de, str>, D::Error>
where
    D: Deserializer<'de>,
{
    let wrapper = CowStr::deserialize(deserializer)?;
    Ok(wrapper.0)
}

#[cfg(test)]
mod test {
    use serde_json;
    use super::*;

    #[test]
    fn test() {
        #[derive(Debug, serde::Deserialize)]
        struct A<'a> {
            normal: Cow<'a, str>,

            // Note that you need serde(borrow) as well as deserialize_with, so that the
            // Deserialize impl gives you lifetimes
            #[serde(borrow, deserialize_with = "deserialize_cow_str")]
            overridden: Cow<'a, str>,

            #[serde(borrow, deserialize_with = "deserialize_cow_str")]
            escaped: Cow<'a, str>,
        };
        
        let input = r#"{
            "normal": "value",
            "overridden": "value",
            "escaped": "\""
        }"#;
        let a: A = serde_json::from_str(input).unwrap();

        let is_owned = |s| match s {
            Cow::Owned(_) => true,
            Cow::Borrowed(_) => false,
        };

        assert!(
            is_owned(a.normal),
            "tests the default behaviour without potential specialization"
        );
        assert!(
            !is_owned(a.overridden),
            "using the deserialize_with should work"
        );
        assert!(is_owned(a.escaped), "escaped text can't be borrowed");
    }
}