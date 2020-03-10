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

/// `Location` of a value within a JSON document.
pub enum Location<'a> {
    Root,
    Property(LocatedProperty<'a>),
    Item(LocatedItem<'a>),
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

impl<'a> fmt::Display for Location<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Location::Root => write!(f, "#"),
            Location::Property(LocatedProperty { parent, name, .. }) => {
                write!(f, "{}/{}", parent, name)
            }
            Location::Item(LocatedItem { parent, index }) => write!(f, "{}/{}", *parent, index),
        }
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
