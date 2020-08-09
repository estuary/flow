use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use std::fmt::{self, Write};

mod number;
pub use number::Number;

mod compare;
pub use compare::{json_cmp, json_cmp_at};

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
/// Examples:
/// ```
/// use estuary_json::Location;
///
/// let l0 = Location::Root;
/// let l1 = l0.push_prop("foo");
/// let l2 = l1.push_item(42);
///
/// let as_url = format!("http://foo.test/myschema.json#{}", l2);
/// assert_eq!("http://foo.test/myschema.json#/foo/42", as_url);
///
/// let l3 = l2.push_prop("ba~ ba/ 45");
/// assert_eq!("q=/foo/42/ba~0%20ba~1%2045", format!("q={}", l3));
/// ```
#[derive(Copy, Clone)]
pub enum Location<'a> {
    Root,
    Property(LocatedProperty<'a>),
    Item(LocatedItem<'a>),
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

/// The `Display` impl formats the Location as an escaped RFC 6901 JSON Pointer
/// (i.e., with '~' => "~0" and '/' => "~1") which is additionally URL-encoded,
/// making it directly useable within URL query and fragment components.
impl<'a> fmt::Display for Location<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Location::Root => write!(f, ""),
            Location::Property(LocatedProperty { parent, name, .. }) => {
                write!(f, "{}/", parent)?;

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
            Location::Item(LocatedItem { parent, index }) => write!(f, "{}/{}", parent, index),
        }
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
