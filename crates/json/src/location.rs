use crate::{Field, Fields, Node};
use std::fmt::{self, Write};

/// `Location` of a value within a JSON document.
/// Due to differences in string escaping in different representations, `Location` does not
/// implement `std::fmt::Display`, only `Debug`. To get a display representation, use either the
/// `pointer_str` or `url_escaped` function.
///
/// Examples:
/// ```
/// use json::Location;
///
/// let l0 = Location::Root;
/// let l1 = l0.push_prop("foo");
/// let l2 = l1.push_item(42);
/// let l3 = l2.push_end_of_array();
///
/// assert_eq!("/foo/42/-", l3.pointer_str().to_string());
///
/// let as_url = format!("http://foo.test/myschema.json#{}", l3.url_escaped());
/// assert_eq!("http://foo.test/myschema.json#/foo/42/-", as_url);
///
/// let l4 = l3.push_prop("ba~ ba/ 45");
/// assert_eq!("q=/foo/42/-/ba~0%20ba~1%2045", format!("q={}", l4.url_escaped()));
/// ```
#[derive(Copy, Clone)]
pub enum Location<'a> {
    Root,
    Property(LocatedProperty<'a>),
    Item(LocatedItem<'a>),
    EndOfArray(&'a Location<'a>),
    NextProperty(&'a Location<'a>),
}

impl<'a> Location<'a> {
    /// Returns a new Location that extends this one with the given property.
    pub fn push_prop(&'a self, name: &'a str) -> Location<'a> {
        Location::Property(LocatedProperty { parent: self, name })
    }

    /// Returns a new Location that extends this one with the given index.
    pub fn push_item(&'a self, index: usize) -> Location<'a> {
        Location::Item(LocatedItem {
            parent: self,
            index,
        })
    }

    // Returns a new Location that extends this one with a non-existent, trailing array item ("-").
    pub fn push_end_of_array(&'a self) -> Location<'a> {
        Location::EndOfArray(self)
    }

    // Returns a new Location that extends this one with a pointer to the object's additionalProperties ("*").
    pub fn push_next_property(&'a self) -> Location<'a> {
        Location::NextProperty(self)
    }

    /// Returns a struct that implements `std::fmt::Display` to provide a string representation of
    /// the location as a JSON pointer that does no escaping besides '~' and '/'.
    pub fn pointer_str(&'a self) -> PointerStr<'a> {
        PointerStr(*self)
    }

    /// Returns a struct that implements `std::fmt::Display` to provide a string representation of
    /// the location as a JSON pointer that is suitable for inclusion in a URL fragment.
    pub fn url_escaped(&'a self) -> UrlEscaped<'a> {
        UrlEscaped(*self)
    }

    /// Just like folding any other linked list. This one starts at the root and works
    /// from there, so the location that's passed is the one that will be visited last.
    pub fn fold<T, F>(&self, initial: T, mut fun: F) -> T
    where
        F: FnMut(Location<'a>, T) -> T,
    {
        self.fold_inner(initial, &mut fun)
    }

    /// Recursively passing a `&mut` reference requires that the function arguments accept the
    /// `&mut` reference rather than taking ownership of the argument. This function exists to
    /// allow `fold` to take ownership of a closure, so that you don't have to put `&mut` in front
    /// of the closures passed to `fold`.
    fn fold_inner<T, F>(&self, initial: T, fun: &mut F) -> T
    where
        F: FnMut(Location<'a>, T) -> T,
    {
        let mut acc = initial;
        match self {
            Location::Root => {}
            Location::Property(prop) => {
                acc = prop.parent.fold_inner(acc, fun);
            }
            Location::Item(item) => {
                acc = item.parent.fold_inner(acc, fun);
            }
            Location::EndOfArray(parent) => {
                acc = parent.fold_inner(acc, fun);
            }
            Location::NextProperty(parent) => {
                acc = parent.fold_inner(acc, fun);
            }
        }
        fun(*self, acc)
    }
}

impl<'a> fmt::Debug for Location<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.pointer_str())
    }
}

/// `LocatedProperty` is a property located within a JSON document.
#[derive(Copy, Clone)]
pub struct LocatedProperty<'a> {
    pub parent: &'a Location<'a>,
    pub name: &'a str,
}

/// `LocatedItem` is an array item located within a JSON document.
#[derive(Copy, Clone)]
pub struct LocatedItem<'a> {
    pub parent: &'a Location<'a>,
    pub index: usize,
}

/// Search the document for the node at the given tape index, and if found, call the `found`
/// function with the Location and Node and return its result, or None if not found.
/// This routine is O(log n) for AsNode implementations having an O(1) tape_length() method.
pub fn find_tape_index<'n, N, F, T>(doc: &'n N, tape_index: i32, found: F) -> Option<T>
where
    N: crate::AsNode,
    F: for<'l> FnOnce(Location<'l>, &'n N) -> T,
{
    fn inner<'n, 'l, N, F, T>(
        location: Location<'l>,
        node: &'n N,
        needle: i32,
        tape_index: &mut i32,
        found: F,
    ) -> Option<T>
    where
        N: crate::AsNode,
        F: for<'m> FnOnce(Location<'m>, &'n N) -> T,
    {
        if *tape_index == needle {
            return Some(found(location, node));
        }
        *tape_index += 1; // Step past self.

        match node.as_node() {
            Node::Array(arr) => {
                for (i, item) in arr.iter().enumerate() {
                    let tape_delta = item.tape_length();

                    if *tape_index + tape_delta <= needle {
                        *tape_index += tape_delta;
                    } else {
                        return inner(location.push_item(i), item, needle, tape_index, found);
                    }
                }
                *tape_index += 1; // for the end array node
            }
            Node::Object(fields) => {
                for field in fields.iter() {
                    let tape_delta = field.value().tape_length();

                    if *tape_index + tape_delta <= needle {
                        *tape_index += tape_delta;
                    } else {
                        return inner(
                            location.push_prop(field.property()),
                            field.value(),
                            needle,
                            tape_index,
                            found,
                        );
                    }
                }
                *tape_index += 1; // for the end array node
            }
            _ => (),
        }

        None
    }

    let mut cur_index = 0;
    inner(Location::Root, doc, tape_index, &mut cur_index, found)
}

/// Helper struct to format a location as a JSON Pointer as a rust String. This pointer will have
/// '~' and '/' escaped, but no other characters will be escaped. This is mostly likely what you
/// want, since serde will handle the rest of the escaping described in the "JSON String Representation"
/// section of [RFC-6901](https://tools.ietf.org/html/rfc6901#section-5). Note that `PointerStr`
/// would not be appropriat for any manual JSON serialization, since it does not handle such escapes.
pub struct PointerStr<'a>(Location<'a>);

/// Helper struct to format a location as a JSON Pointer suitable for use in a percent-encoded url
/// fragment as described in RFC-6901 Section 6:https://tools.ietf.org/html/rfc6901#section-6
pub struct UrlEscaped<'a>(Location<'a>);

impl<'a> fmt::Display for PointerStr<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fold(Ok(()), move |loc, result: std::fmt::Result| {
            result.and_then(|_| match loc {
                Location::Root => Ok(()),
                Location::Property(LocatedProperty { name, .. }) => {
                    f.write_char('/')?;
                    for c in name.chars() {
                        match c {
                            '~' => f.write_str("~0")?,
                            '/' => f.write_str("~1")?,
                            _ => f.write_char(c)?,
                        }
                    }
                    Ok(())
                }
                Location::Item(LocatedItem { index, .. }) => write!(f, "/{}", index),
                Location::EndOfArray(_) => write!(f, "/-"),
                Location::NextProperty(_) => write!(f, "/*"),
            })
        })
    }
}

impl<'a> fmt::Display for UrlEscaped<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fold(Ok(()), move |loc, result| {
            result.and_then(|_| match loc {
                Location::Root => Ok(()),
                Location::Property(LocatedProperty { name, .. }) => {
                    f.write_char('/')?;
                    for p in percent_encoding::utf8_percent_encode(name, PTR_ESCAPE_SET) {
                        for c in p.chars() {
                            match c {
                                '~' => f.write_str("~0")?,
                                '/' => f.write_str("~1")?,
                                _ => f.write_char(c)?,
                            }
                        }
                    }
                    Ok(())
                }
                Location::Item(LocatedItem { index, .. }) => write!(f, "/{}", index),
                Location::EndOfArray(_) => write!(f, "/-"),
                Location::NextProperty(_) => write!(f, "/*"),
            })
        })
    }
}

/// This is a superset of the required fragment and query percent-encode sets.
/// See: https://url.spec.whatwg.org/#fragment-percent-encode-set
const PTR_ESCAPE_SET: &percent_encoding::AsciiSet = &percent_encoding::CONTROLS
    .add(b'%')
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`')
    .add(b'#')
    .add(b'?')
    .add(b'&')
    .add(b'=');

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AsNode;
    use serde_json::json;

    #[test]
    fn test_find_tape_index() {
        // Complex document covering all JSON types and nesting scenarios
        let doc = json!({
            "array": [
                42,
                null,
                [true, false],
                {"inner": "value"}
            ],
            "empty_array": [],
            "empty_obj": {},
            "number": -3.14,
            "string": "hello",
            "z_last": [[[[99]]]]
        });

        // Table of (tape_index, expected_location, value_check)
        // Note: Object properties are alphabetically sorted
        let expectations = [
            (0, "", "root object"),
            (1, "/array", "array"),
            (2, "/array/0", "42"),
            (3, "/array/1", "null"),
            (4, "/array/2", "nested array"),
            (5, "/array/2/0", "true"),
            (6, "/array/2/1", "false"),
            (7, "/array/3", "object"),
            (8, "/array/3/inner", "value"),
            (9, "/empty_array", "empty array"),
            (10, "/empty_obj", "empty object"),
            (11, "/number", "-3.14"),
            (12, "/string", "hello"),
            (13, "/z_last", "4d array"),
            (14, "/z_last/0", "3d array"),
            (15, "/z_last/0/0", "2d array"),
            (16, "/z_last/0/0/0", "1d array"),
            (17, "/z_last/0/0/0/0", "99"),
        ];

        for (idx, expected_loc, description) in expectations {
            let result = find_tape_index(&doc, idx, |loc, node| {
                let loc_str = format!("{:?}", loc);

                // Verify node type matches description
                let matches = match (description, node.as_node()) {
                    ("root object", Node::Object(_)) => true,
                    ("array", Node::Array(a)) if a.len() == 4 => true,
                    ("42", Node::PosInt(42)) => true,
                    ("null", Node::Null) => true,
                    ("nested array", Node::Array(a)) if a.len() == 2 => true,
                    ("true", Node::Bool(true)) => true,
                    ("false", Node::Bool(false)) => true,
                    ("object", Node::Object(_)) => true,
                    ("value", Node::String(s)) if s == "value" => true,
                    ("empty array", Node::Array(a)) if a.is_empty() => true,
                    ("empty object", Node::Object(o)) if o.is_empty() => true,
                    ("-3.14", Node::Float(f)) if f == -3.14 => true,
                    ("hello", Node::String(s)) if s == "hello" => true,
                    ("4d array" | "3d array" | "2d array" | "1d array", Node::Array(a)) if a.len() == 1 => true,
                    ("99", Node::PosInt(99)) => true,
                    _ => false,
                };

                assert!(matches, "At index {}, expected {}, but node didn't match", idx, description);
                loc_str
            });

            assert_eq!(
                result,
                Some(expected_loc.to_string()),
                "Index {} should map to '{}' ({})",
                idx,
                expected_loc,
                description
            );
        }

        // Test out-of-bounds indices
        assert_eq!(find_tape_index(&doc, -1, |_, _| "unreachable"), None);
        assert_eq!(find_tape_index(&doc, 18, |_, _| "unreachable"), None);
        assert_eq!(find_tape_index(&doc, 100, |_, _| "unreachable"), None);
    }


}
