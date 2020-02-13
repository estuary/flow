use std::cmp;
use std::fmt;

/// `Number` holds possible numeric types of the JSON object model.
#[derive(Debug)]
pub enum Number {
    Unsigned(u64),
    Signed(i64),
    Float(f64),
}

impl Number {
    pub fn is_multiple_of(&self, d: &Self) -> bool {
        use Number::*;

        match *d {
            Unsigned(d) => match *self {
                Unsigned(n) => n % d == 0,
                Signed(n) => n % (d as i64) == 0,
                Float(n) => (n / (d as f64)).fract() == 0.0,
            },
            Signed(d) => match *self {
                Unsigned(n) => (n as i64) % d == 0,
                Signed(n) => n % d == 0,
                Float(n) => (n / (d as f64)).fract() == 0.0,
            },
            Float(d) => match *self {
                Unsigned(n) => (n as f64) % d == 0.0,
                Signed(n) => (n as f64) % d == 0.0,
                Float(n) => (n / d).fract() == 0.0,
            },
        }
    }
}

impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        use Number::*;

        match *self {
            Unsigned(lhs) => match other {
                Unsigned(rhs) => lhs.partial_cmp(rhs),
                Signed(rhs) => (lhs as i64).partial_cmp(rhs),
                Float(rhs) => (lhs as f64).partial_cmp(rhs),
            },
            Signed(lhs) => match other {
                Unsigned(rhs) => lhs.partial_cmp(&(*rhs as i64)),
                Signed(rhs) => lhs.partial_cmp(rhs),
                Float(rhs) => (lhs as f64).partial_cmp(rhs),
            },
            Float(lhs) => match other {
                Unsigned(rhs) => lhs.partial_cmp(&(*rhs as f64)),
                Signed(rhs) => lhs.partial_cmp(&(*rhs as f64)),
                Float(rhs) => lhs.partial_cmp(rhs),
            },
        }
    }
}

impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other)
            .map_or(false, |c| c == cmp::Ordering::Equal)
    }
}

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
