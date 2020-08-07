use std::fmt;

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

/// Wraps a `Location` and provides a `Display` impl that formats the location as a JSON pointer.
///
pub struct JsonPointer<'a, 'b>(&'b Location<'a>);

// TODO: Do we need to handle escaping '/' and '~' chars in display and debug impls
impl<'a, 'b> fmt::Display for JsonPointer<'a, 'b> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.format_pointer(f)
    }
}
impl<'a, 'b> fmt::Debug for JsonPointer<'a, 'b> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// `Location` of a value within a JSON document.
/// Examples:
/// ```
/// use estuary_json::{Location, JsonPointer};
///
/// let root = Location::Root;
/// let foo = root.child_property("foo", 1);
/// let elem_0 = foo.child_array_element(0);
///
/// let elem_0_fragment = format!("http://foo.test/myschema.json{}", elem_0);
/// assert_eq!("http://foo.test/myschema.json#/foo/0", elem_0_fragment.as_str());
///
/// let elem_0_pointer = format!("my_pointer={}", elem_0.as_json_pointer());
/// assert_eq!("my_pointer=/foo/0", elem_0_pointer.as_str());
/// ```
pub enum Location<'a> {
    Root,
    Property(LocatedProperty<'a>),
    Item(LocatedItem<'a>),
}

impl<'a> Location<'a> {
    /// Returns a new `Location` for the given field that is a child of this location.
    pub fn child_property(&'a self, field_name: &'a str, enumeration_index: usize) -> Location<'a> {
        let prop = LocatedProperty {
            parent: self,
            name: field_name,
            index: enumeration_index,
        };
        Location::Property(prop)
    }

    /// Returns a new `Location` for the given field that is a child of this location.
    pub fn child_array_element(&'a self, index: usize) -> Location<'a> {
        let item = LocatedItem {
            parent: self,
            index,
        };
        Location::Item(item)
    }

    /// Returns a string representation of this location as a JSON pointer.
    pub fn to_pointer(&self) -> String {
        self.as_json_pointer().to_string()
    }

    /// Returns a struct that implements `Display` and `Debug` to format the location as a json pointer.
    pub fn as_json_pointer<'b>(&'b self) -> JsonPointer<'a, 'b> {
        JsonPointer(self)
    }

    fn format_pointer(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Location::Root => Ok(()),
            Location::Property(LocatedProperty { parent, name, .. }) => {
                write!(f, "{}/{}", parent.as_json_pointer(), name)
            }
            Location::Item(LocatedItem { parent, index }) => {
                write!(f, "{}/{}", parent.as_json_pointer(), index)
            }
        }
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

/// The `Display` impl formats the Location as a fragment with a json pointer. If you just want the
/// pointer without the leading `#`, then use `Location::as_json_pointer`.
impl<'a> fmt::Display for Location<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "#{}", self.as_json_pointer())
    }
}

impl<'a> fmt::Debug for Location<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

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
