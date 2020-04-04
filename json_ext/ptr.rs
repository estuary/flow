use serde_json as sj;
use std::cmp;
use std::convert::TryFrom;
use std::str::FromStr;
use thiserror;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("non-empty JSON pointer must have a leading '/'")]
    NotRooted,
}

/// Token is a parsed token of a JSON pointer.
#[derive(Debug, Eq, PartialEq)]
pub enum Token {
    /// Integer index of a JSON array.
    /// If applied to a JSON object, the index is may also serve as a property name.
    Index(usize, String),
    /// JSON object property name. Never an integer.
    Property(String),
    /// Next JSON index which is one beyond the current array extent.
    /// If applied to a JSON object, the property literal "-" is used.
    NextIndex,
}

/// Pointer is a parsed JSON pointer.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Pointer(Vec<Token>);

impl Ord for Token {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        use Token::*;
        match (self, other) {
            (Index(lhs, _), Index(rhs, _)) => lhs.cmp(&rhs),
            (Property(lhs), Property(rhs)) => lhs.cmp(&rhs),
            (NextIndex, NextIndex) => cmp::Ordering::Equal,

            // Index orders before NextIndex, which orders before Property.
            (Index(_, _), _) => cmp::Ordering::Less,
            (_, Index(_, _)) => cmp::Ordering::Greater,
            (NextIndex, _) => cmp::Ordering::Less,
            (_, NextIndex) => cmp::Ordering::Greater,
        }
    }
}

impl PartialOrd for Token {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<&str> for Pointer {
    type Error = Error;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.is_empty() {
            return Ok(Pointer(Vec::new()));
        } else if !s.starts_with('/') {
            return Err(Error::NotRooted);
        }

        Ok(Pointer(
            s.split('/')
                .skip(1)
                .map(|t| t.replace("~1", "/").replace("~0", "~"))
                .map(|t| {
                    use Token::*;

                    if t == "-" {
                        NextIndex
                    } else if t.starts_with('+') {
                        Property(t)
                    } else if t.starts_with('0') && t.len() > 1 {
                        Property(t)
                    } else if let Ok(ind) = usize::from_str(&t) {
                        Index(ind, t)
                    } else {
                        Property(t)
                    }
                })
                .collect(),
        ))
    }
}

impl Token {
    /// Returns the Token's interpretation as an Object property:
    /// - When a Property, its value is returned directly.
    /// - When an Index, the string form of the Index is returned.
    /// - When a NextIndex, returns "-".
    pub fn as_property(&self) -> &str {
        match self {
            Token::Index(_, prop) => &prop,
            Token::Property(prop) => &prop,
            Token::NextIndex => "-",
        }
    }
}

impl Pointer {
    /// Query an existing value at the pointer location within the document.
    /// Returns None if the pointed location (or a parent thereof) does not exist.
    pub fn query<'v>(&self, doc: &'v sj::Value) -> Option<&'v sj::Value> {
        use sj::Value::{Array, Object};
        use Token::*;

        let mut v = doc;

        for token in self.0.iter() {
            let next = match v {
                Object(map) => map.get(token.as_property()),
                Array(arr) => {
                    if let Index(ind, _) = token {
                        arr.get(*ind)
                    } else {
                        None
                    }
                }
                _ => None,
            };

            if let Some(vv) = next {
                v = vv;
            } else {
                return None;
            }
        }
        Some(v)
    }

    /// Query a mutable existing value at the pointer location within the document,
    /// recursively creating the location if it doesn't exist. Existing parent locations
    /// which are Null are instantiated as an Object or Array, depending on the type of
    /// Token at that location (Property or Index/NextIndex). An existing Array is
    /// extended with Nulls as required to instantiate a specified Index.
    /// Returns a mutable Value at the pointed location, or None only if the document
    /// structure is incompatible with the pointer (eg, because a parent location is
    /// a scalar type, or attempts to index an array by-property).
    pub fn create<'v>(&self, doc: &'v mut sj::Value) -> Option<&'v mut sj::Value> {
        use sj::Value as sjv;
        use Token::*;

        let mut v = doc;

        for token in self.0.iter() {
            // If the current value is null but more tokens remain in the pointer,
            // instantiate it as an object or array (depending on token type) in
            // which we'll create the next child location.
            if let sjv::Null = v {
                match token {
                    Property(_) => {
                        *v = sjv::Object(sj::map::Map::new());
                    }
                    Index(_, _) | NextIndex => {
                        *v = sjv::Array(Vec::new());
                    }
                };
            }

            v = match v {
                sjv::Object(map) => {
                    // Create or modify existing entry.
                    map.entry(token.as_property()).or_insert(sj::Value::Null)
                }
                sjv::Array(arr) => match token {
                    Index(ind, _) => {
                        // Create any required indices [0..ind) as Null.
                        if *ind >= arr.len() {
                            arr.extend(
                                std::iter::repeat(sj::Value::Null).take(1 + *ind - arr.len()),
                            );
                        }
                        // Create or modify |ind| entry.
                        arr.get_mut(*ind).unwrap()
                    }
                    NextIndex => {
                        // Append and return a Null.
                        arr.push(sj::Value::Null);
                        arr.last_mut().unwrap()
                    }
                    // Cannot match (attempt to query property of an array).
                    Property(_) => return None,
                },
                sjv::Number(_) | sjv::Bool(_) | sjv::String(_) => {
                    return None; // Cannot match (attempt to take child of scalar).
                }
                sjv::Null => panic!("unexpected null"),
            };
        }
        Some(v)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ptr_parsing() -> Result<(), Error> {
        use Token::*;

        // Basic example.
        let ptr = Pointer::try_from("/p1/2/p3/-")?;
        assert_eq!(
            ptr,
            Pointer(vec![
                Property("p1".to_owned()),
                Index(2, "2".to_owned()),
                Property("p3".to_owned()),
                NextIndex,
            ])
        );

        // Empty pointer.
        let ptr = Pointer::try_from("")?;
        assert_eq!(ptr, Pointer(vec![]));

        // Un-rooted pointers are an error.
        match Pointer::try_from("p1/2") {
            Err(Error::NotRooted) => (),
            _ => panic!("expected error"),
        }

        // Handles escapes.
        let ptr = Pointer::try_from("/p~01/~12")?;
        assert_eq!(
            ptr,
            Pointer(vec![Property("p~1".to_owned()), Property("/2".to_owned()),])
        );

        // Handles disallowed integer representations.
        let ptr = Pointer::try_from("/01/+2/-3/4/-")?;
        assert_eq!(
            ptr,
            Pointer(vec![
                Property("01".to_owned()),
                Property("+2".to_owned()),
                Property("-3".to_owned()),
                Index(4, "4".to_owned()),
                NextIndex,
            ])
        );

        Ok(())
    }

    #[test]
    fn test_ptr_query() -> Result<(), Error> {
        // Extended document fixture from RFC-6901.
        let doc = sj::json!({
            "foo": ["bar", "baz"],
            "": 0,
            "a/b": 1,
            "c%d": 2,
            "e^f": 3,
            "g|h": 4,
            "i\\j": 5,
            "k\"l": 6,
            " ": 7,
            "m~n": 8,
            "9": 10,
            "-": 11,
        });

        // Query document locations which exist (cases from RFC-6901).
        for case in [
            ("", sj::json!(doc)),
            ("/foo", sj::json!(["bar", "baz"])),
            ("/foo/0", sj::json!("bar")),
            ("/foo/1", sj::json!("baz")),
            ("/", sj::json!(0)),
            ("/a~1b", sj::json!(1)),
            ("/c%d", sj::json!(2)),
            ("/e^f", sj::json!(3)),
            ("/g|h", sj::json!(4)),
            ("/i\\j", sj::json!(5)),
            ("/k\"l", sj::json!(6)),
            ("/ ", sj::json!(7)),
            ("/m~0n", sj::json!(8)),
            ("/9", sj::json!(10)),
            ("/-", sj::json!(11)),
        ]
        .iter()
        {
            let ptr = Pointer::try_from(case.0)?;
            assert_eq!(ptr.query(&doc).unwrap(), &case.1);
        }

        // Locations which don't exist.
        for case in [
            "/bar",      // Missing property.
            "/foo/2",    // Missing index.
            "/foo/prop", // Cannot take property of array.
            "/e^f/3",    // Not an object or array.
        ]
        .iter()
        {
            let ptr = Pointer::try_from(*case)?;
            assert!(ptr.query(&doc).is_none());
        }

        Ok(())
    }

    #[test]
    fn test_ptr_create() -> Result<(), Error> {
        use estuary_json as ej;
        use sj::Value as sjv;

        // Modify a Null root by applying a succession of upserts.
        let mut root = sjv::Null;

        for case in [
            // Creates Object root, Array at /foo, and Object at /foo/1.
            ("/foo/2/a", sjv::String("hello".to_owned())),
            // Add property to existing object.
            ("/foo/2/b", ej::Number::Unsigned(3).into()),
            ("/foo/0", sjv::Bool(false)), // Update existing Null.
            ("/bar", sjv::Null),          // Add property to doc root.
            ("/foo/0", sjv::Bool(true)),  // Update from 'false'.
            ("/foo/-", sjv::String("world".to_owned())), // NextIndex extends Array.
            // Index token is interpreted as property because object exists.
            ("/foo/2/4", ej::Number::Unsigned(5).into()),
            // NextIndex token is also interpreted as property.
            ("/foo/2/-", sjv::Bool(false)),
        ]
        .iter_mut()
        {
            let ptr = Pointer::try_from(case.0)?;
            std::mem::swap(ptr.create(&mut root).unwrap(), &mut case.1);
        }

        assert_eq!(
            root,
            sj::json!({
                "foo": [true, sjv::Null, {"-": false, "a": "hello", "b": 3, "4": 5}, "world"],
                "bar": sjv::Null,
            })
        );

        // Cases which return None.
        for case in [
            "/foo/2/a/3", // Attempt to index string scalar.
            "/foo/bar",   // Attempt to take property of array.
        ]
        .iter()
        {
            let ptr = Pointer::try_from(*case)?;
            assert!(ptr.create(&mut root).is_none());
        }

        Ok(())
    }
}
