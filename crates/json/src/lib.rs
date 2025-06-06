use std::fmt::{self, Write};

use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
mod number;
pub use number::Number;
mod scope;
pub use scope::Scope;

/// `Span` describes a instance value of a visited JSON document, including
/// its [begin, end) value indexes in an ordered depth-first walk of the document.
#[derive(Debug, Eq, PartialEq)]
pub struct Span {
    /// Index of the first value of this Span. Indices start at zero and
    /// parent objects & arrays are indexed before each child value.
    pub begin: usize,
    /// Index immediately beyond the last value of this Span.
    pub end: usize,
    /// Hash value of the document span. Hashes are invariant to the
    /// specific ordering of encountered properties.
    pub hashed: u64,
}

impl Span {
    /// New returns a length-one Span with the given index and hash.
    pub fn new(at: usize, h: u64) -> Span {
        return Span {
            begin: at,
            end: at + 1,
            hashed: h,
        };
    }
}

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
    /// Returns a new Location that extends this one with the given property and index.
    pub fn push_prop_with_index(&'a self, name: &'a str, index: usize) -> Location<'a> {
        Location::Property(LocatedProperty {
            parent: self,
            name,
            index,
        })
    }

    /// Returns a new Location that extends this one with the given property.
    pub fn push_prop(&'a self, name: &'a str) -> Location<'a> {
        self.push_prop_with_index(name, usize::MAX)
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
    pub fn pointer_str(&self) -> PointerStr {
        PointerStr(*self)
    }

    /// Returns a struct that implements `std::fmt::Display` to provide a string representation of
    /// the location as a JSON pointer that is suitable for inclusion in a URL fragment.
    pub fn url_escaped(&self) -> UrlEscaped {
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
    pub index: usize,
}

/// `LocatedItem` is an array item located within a JSON document.
#[derive(Copy, Clone)]
pub struct LocatedItem<'a> {
    pub parent: &'a Location<'a>,
    pub index: usize,
}

/// Helper struct to format a location as a JSON Pointer as a rust String. This pointer will have
/// '~' and '/' escaped, but no other characters will be escaped. This is mostly likely what you
/// want, since serde will handle the rest of the escaping described in the "JSON String Representation"
/// section of [RFC-6901](https://tools.ietf.org/html/rfc6901#section-5). Note that `PointerStr`
/// would not be appropriat for any manual JSON serialization, since it does not handle such escapes.
pub struct PointerStr<'a>(Location<'a>);

/// The `Display` impl formats the Location as an escaped RFC 6901 JSON Pointer
/// (i.e., with '~' => "~0" and '/' => "~1").
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

/// Helper struct to format a location as a JSON Pointer suitable for use in a percent-encoded url
/// fragment as described in RFC-6901 Section 6:https://tools.ietf.org/html/rfc6901#section-6
pub struct UrlEscaped<'a>(Location<'a>);
impl<'a> fmt::Display for UrlEscaped<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fold(Ok(()), move |loc, result| {
            result.and_then(|_| match loc {
                Location::Root => Ok(()),
                Location::Property(LocatedProperty { name, .. }) => {
                    f.write_char('/')?;
                    for p in utf8_percent_encode(name, PTR_ESCAPE_SET) {
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
const PTR_ESCAPE_SET: &AsciiSet = &CONTROLS
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

/// `Walker` visits values within JSON documents.
pub trait Walker {
    fn push_property<'a>(&mut self, _span: &Span, _loc: &'a LocatedProperty<'a>) {}
    fn push_item<'a>(&mut self, _span: &Span, _loc: &'a LocatedItem<'a>) {}

    fn pop_object<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _num_properties: usize) {}
    fn pop_array<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _num_items: usize) {}
    fn pop_bool<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _val: bool) {}
    fn pop_numeric<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _val: Number) {}
    fn pop_str<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>, _val: &'a str) {}
    fn pop_null<'a>(&mut self, _span: &Span, _loc: &'a Location<'a>) {}
}

/// `NoopWalker` is as `Walker` implementation which does nothing.
pub struct NoopWalker;
impl Walker for NoopWalker {}

pub mod de;
pub mod schema;
pub mod validator;
