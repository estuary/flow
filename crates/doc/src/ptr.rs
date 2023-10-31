use super::{AsNode, BumpVec, Field, Fields, HeapNode, Node};
use std::str::FromStr;

/// Token is a parsed token of a JSON pointer.
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub enum Token {
    /// Integer index of a JSON array.
    /// If applied to a JSON object, the index is may also serve as a property name.
    Index(usize),
    /// JSON object property name without escaping. Never an integer.
    Property(String),
    // Represents the concept of "the next property" to be added
    NextProperty,
    /// Next JSON index which is one beyond the current array extent.
    /// If applied to a JSON object, the property literal "-" is used.
    NextIndex,
}

impl Token {
    pub fn from_str(s: &str) -> Self {
        if s.starts_with('+') || (s.starts_with('0') && s.len() > 1) {
            Token::Property(s.to_string())
        } else if let Ok(ind) = usize::from_str(&s) {
            Token::Index(ind)
        } else {
            Token::Property(s.to_string())
        }
    }
}

impl<'t> std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::Index(ind) => write!(f, "{ind}"),
            Token::Property(prop) => write!(f, "{prop}"),
            Token::NextProperty => write!(f, "*"),
            Token::NextIndex => write!(f, "-"),
        }
    }
}

/// Pointer is a parsed JSON pointer.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Pointer(pub Vec<Token>);

impl Pointer {
    /// Builds an empty Pointer which references the document root.
    pub fn empty() -> Pointer {
        Pointer(Vec::new())
    }

    /// Builds a Pointer from the given string, which is an encoded JSON pointer.
    ///
    /// ```
    /// use doc::ptr::{Pointer, Token};
    ///
    /// let pointer = Pointer::from_str("/foo/ba~1ar/3");
    /// let expected_tokens = vec![
    ///     Token::Property("foo".to_string()),
    ///     Token::Property("ba/ar".to_string()),
    ///     Token::Index(3),
    /// ];
    /// assert_eq!(expected_tokens, pointer.0);
    /// ```
    pub fn from_str(s: &str) -> Pointer {
        if s.is_empty() {
            return Pointer(Vec::new());
        }
        let mut ptr = Self::empty();

        for token in s
            .split('/')
            .skip(if s.starts_with('/') { 1 } else { 0 })
            .map(|t| t.replace("~1", "/").replace("~0", "~"))
        {
            ptr.push(Token::from_str(&token));
        }
        ptr
    }

    /// Builds a `Pointer` from a `Location`. Since both `Location` and `Pointer`
    /// internally represent property names without any escaping, this function will
    /// always use the raw property names without performing any conversions.
    ///
    /// ```
    /// use json::Location;
    /// use doc::ptr::Token;
    ///
    /// let root = Location::Root;
    /// let foo = root.push_prop("foo");
    /// let eoa = foo.push_end_of_array();
    /// let bar = eoa.push_prop("bar");
    /// let index = bar.push_item(3);
    ///
    /// let pointer = doc::Pointer::from_location(&index);
    /// // equivalent to "/foo/-/bar/3"
    /// let expected_tokens = vec![
    ///     Token::Property("foo".to_string()),
    ///     Token::NextIndex,
    ///     Token::Property("bar".to_string()),
    ///     Token::Index(3)
    /// ];
    /// let actual_tokens = pointer.iter().cloned().collect::<Vec<_>>();
    /// assert_eq!(expected_tokens, actual_tokens);
    /// ```
    pub fn from_location(location: &json::Location) -> Pointer {
        location.fold(Pointer::empty(), |location, mut ptr| {
            match location {
                json::Location::Root => {}
                json::Location::Property(prop) => {
                    ptr.push(Token::Property(prop.name.to_string()));
                }
                json::Location::Item(item) => {
                    ptr.push(Token::Index(item.index));
                }
                json::Location::EndOfArray(_) => {
                    ptr.push(Token::NextIndex);
                }
                json::Location::NextProperty(_) => {
                    ptr.push(Token::NextProperty);
                }
            }
            ptr
        })
    }

    // Push a new Token onto the Pointer.
    pub fn push(&mut self, token: Token) -> &mut Pointer {
        self.0.push(token);
        self
    }

    /// Iterate over pointer tokens.
    pub fn iter(&self) -> impl Iterator<Item = &Token> {
        self.0.iter()
    }

    /// Query an existing value at the pointer location within the document.
    /// Returns None if the pointed location (or a parent thereof) does not exist.
    pub fn query<'n, N: AsNode>(&self, mut node: &'n N) -> Option<&'n N> {
        for token in self.iter() {
            let next: Option<&N> = match node.as_node() {
                Node::Object(fields) => match token {
                    Token::Index(ind) => fields.get(&ind.to_string()),
                    Token::Property(property) => fields.get(&property),
                    Token::NextProperty | Token::NextIndex => None,
                }
                .map(|field| field.value()),
                Node::Array(arr) => match token {
                    Token::Index(ind) => arr.get(*ind),
                    Token::Property(_) | Token::NextIndex | Token::NextProperty => None,
                },
                _ => None,
            };

            if let Some(next) = next {
                node = next;
            } else {
                return None;
            }
        }
        Some(node)
    }

    /// Query a mutable existing value at the pointer location within the document,
    /// recursively creating the location if it doesn't exist. Existing parent locations
    /// which are Null are instantiated as an Object or Array, depending on the type of
    /// Token at that location (Property or Index/NextIndex). An existing Array is
    /// extended with Nulls as required to instantiate a specified Index.
    /// Returns a mutable Value at the pointed location, or None only if the document
    /// structure is incompatible with the pointer (eg, because a parent location is
    /// a scalar type, or attempts to index an array by-property).
    pub fn create_value<'v>(
        &self,
        value: &'v mut serde_json::Value,
    ) -> Option<&'v mut serde_json::Value> {
        use serde_json::Value;

        let mut v = value;

        for token in self.iter() {
            // If the current value is null but more tokens remain in the pointer,
            // instantiate it as an object or array (depending on token type) in
            // which we'll create the next child location.
            if let Value::Null = v {
                match token {
                    Token::Property(_) | Token::NextProperty => {
                        *v = Value::Object(serde_json::map::Map::new());
                    }
                    Token::Index(_) | Token::NextIndex => {
                        *v = Value::Array(Vec::new());
                    }
                };
            }

            v = match v {
                Value::Object(map) => match token {
                    // Create or modify existing entry.
                    Token::Index(ind) => map.entry(ind.to_string()).or_insert(Value::Null),
                    Token::Property(prop) => map.entry(prop).or_insert(Value::Null),
                    Token::NextProperty | Token::NextIndex => return None,
                },
                Value::Array(arr) => match token {
                    Token::Index(ind) => {
                        // Create any required indices [0..ind) as Null.
                        if *ind >= arr.len() {
                            arr.extend(std::iter::repeat(Value::Null).take(1 + ind - arr.len()));
                        }
                        // Create or modify |ind| entry.
                        &mut arr[*ind]
                    }
                    Token::NextIndex => {
                        // Append and return a Null.
                        arr.push(Value::Null);
                        arr.last_mut().unwrap()
                    }
                    // Cannot match (attempt to query property of an array).
                    Token::Property(_) | Token::NextProperty => return None,
                },
                Value::Number(_) | Value::Bool(_) | Value::String(_) => {
                    return None; // Cannot match (attempt to take child of scalar).
                }
                Value::Null => unreachable!("null already handled"),
            };
        }
        Some(v)
    }

    pub fn create_heap_node<'alloc, 'n>(
        &self,
        node: &'n mut HeapNode<'alloc>,
        alloc: &'alloc bumpalo::Bump,
    ) -> Option<&'n mut HeapNode<'alloc>> {
        let mut v = node;

        for token in self.iter() {
            // If the current value is null but more tokens remain in the pointer,
            // instantiate it as an object or array (depending on token type) in
            // which we'll create the next child location.
            if let HeapNode::Null = v {
                match token {
                    Token::Property(_) => {
                        *v = HeapNode::Object(BumpVec::new());
                    }
                    Token::Index(_) => {
                        *v = HeapNode::Array(BumpVec::new());
                    }
                    Token::NextProperty | Token::NextIndex => return None,
                };
            }

            v = match v {
                HeapNode::Object(fields) => match token {
                    // Create or modify existing entry.
                    Token::Index(ind) => fields.insert_property(&ind.to_string(), alloc),
                    Token::Property(property) => fields.insert_property(property, alloc),
                    Token::NextProperty | Token::NextIndex => return None,
                },
                HeapNode::Array(arr) => match token {
                    Token::Index(ind) => {
                        // Create any required indices [0..ind) as Null.
                        if *ind >= arr.len() {
                            arr.extend(
                                std::iter::repeat_with(|| HeapNode::Null).take(1 + ind - arr.len()),
                                alloc,
                            );
                        }
                        // Create or modify |ind| entry.
                        &mut arr[*ind]
                    }
                    Token::NextIndex => {
                        // Append and return a Null.
                        arr.push(HeapNode::Null, alloc);
                        arr.last_mut().unwrap()
                    }
                    // Cannot match (attempt to query property of an array).
                    Token::Property(_) | Token::NextProperty => return None,
                },
                HeapNode::Bool(_)
                | HeapNode::Bytes(_)
                | HeapNode::Float(_)
                | HeapNode::NegInt(_)
                | HeapNode::PosInt(_)
                | HeapNode::String(_) => {
                    return None; // Cannot match (attempt to take child of scalar).
                }
                HeapNode::Null => unreachable!("null already handled"),
            };
        }
        Some(v)
    }
}

impl<S: AsRef<str>> From<S> for Pointer {
    fn from(s: S) -> Self {
        let s = s.as_ref();
        Pointer::from_str(s.as_ref())
    }
}

impl<'t> FromIterator<Token> for Pointer {
    fn from_iter<T: IntoIterator<Item = Token>>(iter: T) -> Self {
        let mut ptr = Self::empty();
        for token in iter {
            ptr.push(token);
        }
        ptr
    }
}

impl serde::Serialize for Pointer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&format!("{self}"))
    }
}

impl<'de> serde::Deserialize<'de> for Pointer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(Self::from_str(&s))
    }
}

impl std::fmt::Display for Pointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn replace_escapes(s: &str) -> String {
            s.replace("~", "~0").replace("/", "~1")
        }

        for item in self.iter() {
            write!(f, "/")?;
            match item {
                Token::NextIndex => write!(f, "-")?,
                Token::NextProperty => write!(f, "*")?,
                Token::Property(p) => write!(f, "{}", replace_escapes(p))?,
                Token::Index(ind) => write!(f, "{}", ind)?,
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{compare, ArchivedNode};
    use serde_json::json;
    use std::cmp::Ordering;

    #[test]
    fn test_ptr_parsing() {
        use Token::*;

        // Basic example.
        let ptr = Pointer::from("/p1/2/p3");
        assert!(vec![
            Property("p1".to_string()),
            Index(2),
            Property("p3".to_string())
        ]
        .iter()
        .eq(ptr.iter()));

        // Empty pointer.
        let ptr = Pointer::from("");
        assert_eq!(ptr.iter().next(), None);

        // Un-rooted pointers are treated as rooted. Note that such pointers
        // are in technical violation of the spec.
        let ptr = Pointer::from("p1/2");
        assert!(vec![Property("p1".to_string()), Index(2)]
            .iter()
            .eq(ptr.iter()));

        // Handles escapes.
        let ptr = Pointer::from("/p~01/~12");
        assert!(
            vec![Property("p~1".to_string()), Property("/2".to_string())]
                .iter()
                .eq(ptr.iter())
        );

        // Handles disallowed integer representations.
        let ptr = Pointer::from("/01/+2/-3/4");
        assert!(vec![
            Property("01".to_string()),
            Property("+2".to_string()),
            Property("-3".to_string()),
            Index(4)
        ]
        .iter()
        .eq(ptr.iter()));
    }

    #[test]
    fn test_ptr_query() {
        // Extended document fixture from RFC-6901.
        let doc = json!({
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

        let alloc = HeapNode::new_allocator();
        let heap_doc = HeapNode::from_serde(&doc, &alloc).unwrap();

        let archive = heap_doc.to_archive();
        let arch_doc = ArchivedNode::from_archive(&archive);

        // Query document locations which exist (cases from RFC-6901).
        for case in [
            ("", json!(doc)),
            ("/foo", json!(["bar", "baz"])),
            ("/foo/0", json!("bar")),
            ("/foo/1", json!("baz")),
            ("/", json!(0)),
            ("/a~1b", json!(1)),
            ("/c%d", json!(2)),
            ("/e^f", json!(3)),
            ("/g|h", json!(4)),
            ("/i\\j", json!(5)),
            ("/k\"l", json!(6)),
            ("/ ", json!(7)),
            ("/m~0n", json!(8)),
            ("/9", json!(10)),
            ("/-", json!(11)),
        ]
        .iter()
        {
            let ptr = Pointer::from(case.0);

            assert_eq!(compare(ptr.query(&doc).unwrap(), &case.1), Ordering::Equal);
            assert_eq!(
                compare(ptr.query(&heap_doc).unwrap(), &case.1),
                Ordering::Equal
            );
            assert_eq!(
                compare(ptr.query(arch_doc).unwrap(), &case.1),
                Ordering::Equal
            );
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
            let ptr = Pointer::from(*case);
            assert!(ptr.query(&doc).is_none());
            assert!(ptr.query(&heap_doc).is_none());
            assert!(ptr.query(arch_doc).is_none());
        }
    }

    #[test]
    fn test_ptr_create() {
        // Modify a Null root by applying a succession of upserts.
        let mut root_value = json!(null);

        let alloc = HeapNode::new_allocator();
        let mut root_heap_doc = HeapNode::Null;

        for case in [
            // Creates Object root, Array at /foo, and Object at /foo/1.
            ("/foo/2/a", json!("hello")),
            // Add property to existing object.
            ("/foo/2/b", json!(3)),
            ("/foo/0", json!(false)), // Update existing Null.
            ("/bar", json!(null)),    // Add property to doc root.
            ("/foo/0", json!(true)),  // Update from 'false'.
            // Index token is interpreted as property because object exists.
            ("/foo/2/4", json!(5)),
            // NextIndex token is also interpreted as property.
            ("/foo/2/-", json!(false)),
        ]
        .iter_mut()
        {
            let ptr = Pointer::from(case.0);
            let child = HeapNode::from_serde(&case.1, &alloc).unwrap();

            *ptr.create_heap_node(&mut root_heap_doc, &alloc).unwrap() = child;

            std::mem::swap(ptr.create_value(&mut root_value).unwrap(), &mut case.1);
        }

        let expect = json!({
            "foo": [true, null, {"-": false, "a": "hello", "b": 3, "4": 5}],
            "bar": null,
        });

        assert_eq!(compare(&root_value, &expect), Ordering::Equal);
        assert_eq!(compare(&root_heap_doc, &expect), Ordering::Equal);

        // Cases which return None.
        for case in [
            "/foo/2/a/3", // Attempt to index string scalar.
            "/foo/bar",   // Attempt to take property of array.
            "/foo/-",     // Attempt to take property of array
        ]
        .iter()
        {
            let ptr = Pointer::from(*case);

            assert!(ptr.create_value(&mut root_value).is_none());
            assert!(ptr.create_heap_node(&mut root_heap_doc, &alloc).is_none());
        }

        let next_index_ptr = Pointer::from_iter(
            vec![Token::Property("foo".to_string()), Token::NextProperty].into_iter(),
        );

        let res = next_index_ptr.create_value(&mut root_value);
        assert_eq!(res, None);
    }

    #[test]
    fn test_ptr_to_string() {
        // Turn JSON pointer strings to json::Pointer and back to string
        let cases = vec![
            "/foo/2/a~1b",
            "/foo/2/b~0",
            "/foo/0",
            "/bar",
            "/foo/0",
            "/foo/-",
            "/foo/2/4",
            "/foo/2/-",
        ];

        let results = cases
            .iter()
            .map(|case| {
                let ptr = Pointer::from(case);
                ptr.to_string()
            })
            .collect::<Vec<String>>();

        assert_eq!(cases, results);
    }
}
