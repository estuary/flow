use json::{json_cmp, Location};
use serde_json as sj;
use std::cmp::Ordering;
use std::fmt::Display;
use std::str::FromStr;
use tinyvec::TinyVec;

/// Pointer is a parsed JSON pointer.
#[derive(Eq, PartialEq, Clone)]
pub struct Pointer(TinyVec<[u8; 16]>);

/// Token is a parsed token of a JSON pointer.
#[derive(Debug, Eq, PartialEq)]
pub enum Token<'t> {
    /// Integer index of a JSON array.
    /// If applied to a JSON object, the index is may also serve as a property name.
    Index(usize),
    /// JSON object property name. Never an integer.
    Property(&'t str),
    /// Next JSON index which is one beyond the current array extent.
    /// If applied to a JSON object, the property literal "-" is used.
    NextIndex,
}

/// Iter is the iterator type over Tokens that's returned by Pointer::iter().
pub struct Iter<'t>(&'t [u8]);

impl Pointer {
    /// Builds an empty Pointer which references the document root.
    pub fn empty() -> Pointer {
        Pointer(TinyVec::new())
    }

    pub fn from_vec(v: &Vec<String>) -> Pointer {
        if v.is_empty() {
            return Pointer::empty();
        }

        let mut tape = Pointer(TinyVec::new());

        v.iter()
            .map(|t| t.replace("~1", "/").replace("~0", "~"))
            .for_each(|t| {
                if t == "-" {
                    tape.push(Token::NextIndex);
                } else if t.starts_with('+') || (t.starts_with('0') && t.len() > 1) {
                    tape.push(Token::Property(&t));
                } else if let Ok(ind) = usize::from_str(&t) {
                    tape.push(Token::Index(ind));
                } else {
                    tape.push(Token::Property(&t));
                }
            });

        tape
    }

    /// Builds a Pointer from the given string, which is an encoded JSON pointer.
    pub fn from_str(s: &str) -> Pointer {
        if s.is_empty() {
            return Pointer(TinyVec::new());
        }
        let mut tape = Pointer(TinyVec::new());

        s.split('/')
            .skip(if s.starts_with('/') { 1 } else { 0 })
            .map(|t| t.replace("~1", "/").replace("~0", "~"))
            .for_each(|t| {
                if t == "-" {
                    tape.push(Token::NextIndex);
                } else if t.starts_with('+') || (t.starts_with('0') && t.len() > 1) {
                    tape.push(Token::Property(&t));
                } else if let Ok(ind) = usize::from_str(&t) {
                    tape.push(Token::Index(ind));
                } else {
                    tape.push(Token::Property(&t));
                }
            });

        tape
    }

    /// Builds a `Pointer` from a `Location`. Since both `Location` and `Pointer`
    /// internally represent property names without any escaping, this function will
    /// always use the raw property names without performing any conversions.
    ///
    /// ```
    /// use json::Location;
    /// use doc::ptr::{Pointer, Token};
    ///
    /// let root = Location::Root;
    /// let foo = root.push_prop("foo");
    /// let eoa = foo.push_end_of_array();
    /// let bar = eoa.push_prop("bar");
    /// let index = bar.push_item(3);
    ///
    /// let pointer = Pointer::from_location(&index);
    /// // equivalent to "/foo/-/bar/3"
    /// let expected_tokens = vec![
    ///     Token::Property("foo"),
    ///     Token::NextIndex,
    ///     Token::Property("bar"),
    ///     Token::Index(3)
    /// ];
    /// let actual_tokens = pointer.iter().collect::<Vec<_>>();
    /// assert_eq!(expected_tokens, actual_tokens);
    /// ```
    pub fn from_location(location: &Location) -> Pointer {
        location.fold(Pointer::empty(), |location, mut ptr| {
            match location {
                Location::Root => {}
                Location::Property(prop) => {
                    ptr.push(Token::Property(prop.name));
                }
                Location::Item(item) => {
                    ptr.push(Token::Index(item.index));
                }
                Location::EndOfArray(_) => {
                    ptr.push(Token::NextIndex);
                }
            }
            ptr
        })
    }

    /// Push a new Token onto the Pointer.
    pub fn push<'t>(&mut self, token: Token<'t>) -> &mut Pointer {
        match token {
            Token::Index(ind) => {
                self.0.push(b'I');
                self.enc_varint(ind as u64);
            }
            Token::Property(prop) => {
                // Encode as 'P' control code,
                // followed by varint *byte* (not char) length,
                // followed by property UTF-8 bytes.
                self.0.push(b'P');
                let prop = prop.as_bytes();
                self.enc_varint(prop.len() as u64);
                self.0.extend(prop.iter().copied());
            }
            Token::NextIndex => {
                self.0.push(b'-');
            }
        }
        self
    }

    /// Pop last token from the pointer
    pub fn pop(&mut self) {
        self.0.pop();
    }

    /// Iterate over pointer tokens.
    pub fn iter(&'_ self) -> Iter<'_> {
        Iter(&self.0)
    }

    fn enc_varint(&mut self, n: u64) {
        let mut buf = [0 as u8; 10];
        let n = super::varint::write_varu64(&mut buf, n);
        self.0.extend(buf.iter().copied().take(n));
    }
}

impl Default for Pointer {
    fn default() -> Self {
        Self::empty()
    }
}

impl Display for Pointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn replace_escapes(s: &str) -> String {
            s.replace("~", "~0").replace("/", "~1")
        }

        for item in self.iter() {
            write!(f, "/")?;
            match item {
                Token::NextIndex => write!(f, "-")?,
                Token::Property(p) => write!(f, "{}", replace_escapes(p))?,
                Token::Index(ind) => write!(f, "{}", ind)?,
            };
        }

        Ok(())
    }
}

impl<S: AsRef<str>> From<S> for Pointer {
    fn from(s: S) -> Self {
        let s = s.as_ref();
        Pointer::from_str(s.as_ref())
    }
}

impl<'t> Iterator for Iter<'t> {
    type Item = Token<'t>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        // Match on next control code.
        Some(match self.0[0] as char {
            '-' => {
                self.0 = &self.0[1..]; // Pop control code.
                Token::NextIndex
            }
            'P' => {
                let (prop_len, prop_len_len) = super::varint::read_varu64(&self.0[1..]);
                let prop = &self.0[1 + prop_len_len..1 + prop_len_len + prop_len as usize];
                let prop = unsafe { std::str::from_utf8_unchecked(prop) };
                self.0 = &self.0[1 + prop_len_len + prop_len as usize..]; // Pop.
                Token::Property(prop)
            }
            'I' => {
                let (ind, ind_len) = super::varint::read_varu64(&self.0[1..]);
                self.0 = &self.0[1 + ind_len..]; // Pop.
                Token::Index(ind as usize)
            }
            c => panic!("unexpected tape control {:?}", c),
        })
    }
}

impl Pointer {
    /// Query an existing value at the pointer location within the document.
    /// Returns None if the pointed location (or a parent thereof) does not exist.
    pub fn query<'v>(&self, doc: &'v sj::Value) -> Option<&'v sj::Value> {
        use sj::Value::{Array, Object};
        use Token::*;

        let mut v = doc;

        for token in self.iter() {
            let next = match v {
                Object(map) => match token {
                    Index(ind) => map.get(&ind.to_string()),
                    Property(prop) => map.get(prop),
                    NextIndex => map.get("-"),
                },
                Array(arr) => match token {
                    Index(ind) => arr.get(ind),
                    Property(_) | NextIndex => None,
                },
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

    /// Compare the deep ordering of |lhs| and |rhs| with respect to a composite key,
    /// specified as a slice of Pointers relative to the respective document roots.
    /// Pointers which point to a document location that does not exist assume an
    /// implicit "null" value. In other words, they behave identically to a document
    /// where the location *does* exist but with an explicit null value.
    pub fn compare(ptrs: &[Self], lhs: &sj::Value, rhs: &sj::Value) -> Ordering {
        ptrs.iter()
            .map(|ptr| {
                json_cmp(
                    ptr.query(lhs).unwrap_or(&sj::Value::Null),
                    ptr.query(rhs).unwrap_or(&sj::Value::Null),
                )
            })
            .find(|o| *o != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
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

        for token in self.iter() {
            // If the current value is null but more tokens remain in the pointer,
            // instantiate it as an object or array (depending on token type) in
            // which we'll create the next child location.
            if let sjv::Null = v {
                match token {
                    Property(_) => {
                        *v = sjv::Object(sj::map::Map::new());
                    }
                    Index(_) | NextIndex => {
                        *v = sjv::Array(Vec::new());
                    }
                };
            }

            v = match v {
                sjv::Object(map) => match token {
                    // Create or modify existing entry.
                    Index(ind) => map.entry(ind.to_string()).or_insert(sj::Value::Null),
                    Property(prop) => map.entry(prop).or_insert(sj::Value::Null),
                    NextIndex => map.entry("-").or_insert(sj::Value::Null),
                },
                sjv::Array(arr) => match token {
                    Index(ind) => {
                        // Create any required indices [0..ind) as Null.
                        if ind >= arr.len() {
                            arr.extend(
                                std::iter::repeat(sj::Value::Null).take(1 + ind - arr.len()),
                            );
                        }
                        // Create or modify |ind| entry.
                        arr.get_mut(ind).unwrap()
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

impl std::fmt::Debug for Pointer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ptr_parsing() {
        use Token::*;

        // Basic example.
        let ptr = Pointer::from("/p1/2/p3/-");
        assert!(vec![Property("p1"), Index(2), Property("p3"), NextIndex]
            .into_iter()
            .eq(ptr.iter()));

        // Empty pointer.
        let ptr = Pointer::from("");
        assert_eq!(ptr.iter().next(), None);

        // Un-rooted pointers are treated as rooted. Note that such pointers
        // are in technical violation of the spec.
        let ptr = Pointer::from("p1/2");
        assert!(vec![Property("p1"), Index(2)].into_iter().eq(ptr.iter()));

        // Handles escapes.
        let ptr = Pointer::from("/p~01/~12");
        assert!(vec![Property("p~1"), Property("/2")]
            .into_iter()
            .eq(ptr.iter()));

        // Handles disallowed integer representations.
        let ptr = Pointer::from("/01/+2/-3/4/-");
        assert!(vec![
            Property("01"),
            Property("+2"),
            Property("-3"),
            Index(4),
            NextIndex,
        ]
        .into_iter()
        .eq(ptr.iter()));
    }

    #[test]
    fn test_ptr_size() {
        assert_eq!(std::mem::size_of::<Pointer>(), 32);

        let small = Pointer::from("/_estuary/uuid");
        assert_eq!(small.0.len(), 16);

        if let TinyVec::Heap(_) = small.0 {
            panic!("didn't expect fixture to spill to heap");
        }

        let large = Pointer::from("/large key/and child");
        assert_eq!(large.0.len(), 22);

        if let TinyVec::Inline(_) = large.0 {
            panic!("expected large fixture to spill to heap");
        }
    }

    #[test]
    fn test_ptr_query() {
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
            let ptr = Pointer::from(case.0);
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
            let ptr = Pointer::from(*case);
            assert!(ptr.query(&doc).is_none());
        }
    }

    #[test]
    fn test_ptr_create() {
        // Modify a Null root by applying a succession of upserts.
        let mut root = sj::json!(null);

        for case in [
            // Creates Object root, Array at /foo, and Object at /foo/1.
            ("/foo/2/a", sj::json!("hello")),
            // Add property to existing object.
            ("/foo/2/b", sj::json!(3)),
            ("/foo/0", sj::json!(false)),   // Update existing Null.
            ("/bar", sj::json!(null)),      // Add property to doc root.
            ("/foo/0", sj::json!(true)),    // Update from 'false'.
            ("/foo/-", sj::json!("world")), // NextIndex extends Array.
            // Index token is interpreted as property because object exists.
            ("/foo/2/4", sj::json!(5)),
            // NextIndex token is also interpreted as property.
            ("/foo/2/-", sj::json!(false)),
        ]
        .iter_mut()
        {
            let ptr = Pointer::from(case.0);
            std::mem::swap(ptr.create(&mut root).unwrap(), &mut case.1);
        }

        assert_eq!(
            root,
            sj::json!({
                "foo": [true, null, {"-": false, "a": "hello", "b": 3, "4": 5}, "world"],
                "bar": null,
            })
        );

        // Cases which return None.
        for case in [
            "/foo/2/a/3", // Attempt to index string scalar.
            "/foo/bar",   // Attempt to take property of array.
        ]
        .iter()
        {
            let ptr = Pointer::from(*case);
            assert!(ptr.create(&mut root).is_none());
        }
    }

    #[test]
    fn test_ptr_to_string() {
        // Turn JSON pointer strings to doc::Pointer and back to string
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
