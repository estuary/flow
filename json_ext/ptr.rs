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
    pub fn as_property(&self) -> &str {
        match self {
            Token::Index(_, prop) => &prop,
            Token::Property(prop) => &prop,
            Token::NextIndex => "-",
        }
    }
}

impl Pointer {
    pub fn query<'v>(&self, mut v: &'v sj::Value) -> Option<&'v sj::Value> {
        use sj::Value::{Array, Object};
        use Token::*;

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

    pub fn create<'v>(&self, mut v: &'v mut sj::Value) -> Option<&'v mut sj::Value> {
        use sj::Value::{Array, Null, Object};
        use Token::*;

        for token in self.0.iter() {
            // If more tokens remain but this value is null,
            // instantiate an object or array (depending on token type)
            // to hold the created sub-value.
            if let Null = v {
                match token {
                    Property(_) => {
                        *v = Object(sj::map::Map::new());
                    }
                    Index(_, _) | NextIndex => {
                        *v = Array(Vec::new());
                    }
                };
            }

            let next = match v {
                Object(map) => Some(map.entry(token.as_property()).or_insert(sj::Value::Null)),
                Array(arr) => match token {
                    Index(ind, _) => {
                        if *ind >= arr.len() {
                            arr.extend(
                                std::iter::repeat(sj::Value::Null).take(1 + *ind - arr.len()),
                            );
                        }
                        arr.get_mut(*ind)
                    }
                    NextIndex => {
                        arr.push(sj::Value::Null);
                        arr.last_mut()
                    }
                    _ => None,
                },

                // Existing number, string, or bool.
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
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parsing() -> Result<(), Error> {
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
    fn test_ptr_create() -> Result<(), Error> {
        use estuary_json as ej;
        use sj::Value as sjv;
        use std::convert::TryInto;

        // Modify a Null root by applying a succession of upserts.
        let mut root = sjv::Null;

        for case in [
            ("/foo/1/a", sjv::String("hello".to_owned())),
            ("/foo/1/b", ej::Number::Unsigned(3).into()),
            ("/foo/0", sjv::Bool(false)),
            ("/bar", sjv::Null),
            ("/foo/0", sjv::Bool(true)),
            ("/foo/-", sjv::String("world".to_owned())),
        ]
        .iter_mut()
        {
            let ptr: Pointer = case.0.try_into()?;
            std::mem::swap(ptr.create(&mut root).unwrap(), &mut case.1);
        }

        assert_eq!(
            root,
            sj::json!({
                "foo": [true, {"a": "hello", "b": 3}, "world"],
                "bar": sjv::Null,
            })
        );

        Ok(())
    }
}
