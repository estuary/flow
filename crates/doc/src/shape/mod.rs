use fancy_regex::Regex;
use json::schema::{formats::Format, types};
use serde_json::Value;
use std::collections::BTreeMap;
use url::Url;

mod inference;
pub mod inspections;
mod intersect;
pub mod limits;
pub mod location;
pub mod schema;
mod union;
mod widen;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Shape {
    /// Types that this location may take.
    pub type_: types::Set,
    /// Explicit enumeration of allowed values.
    pub enum_: Option<Vec<Value>>,
    /// Annotated `title` of the location.
    pub title: Option<String>,
    /// Annotated `description` of the location.
    pub description: Option<String>,
    /// Location's `reduce` strategy.
    pub reduction: Reduction,
    /// Does this location's schema flow from a `$ref`?
    pub provenance: Provenance,
    /// Default value of this document location, if any. A validation error is recorded if the
    /// default value specified does not validate against the location's schema.
    pub default: Option<(Value, Option<super::FailedValidation>)>,
    /// Is this location sensitive? For example, a password or credential.
    pub secret: Option<bool>,
    /// Annotations are any keywords starting with `X-` or `x-`.
    /// Their keys and values are collected here, without performing any
    /// normalization of prefix case. Technically both `x-foo` and `X-foo` may be
    /// defined and included here.
    pub annotations: BTreeMap<String, Value>,

    // Further type-specific inferences:
    pub string: StringShape,
    pub array: ArrayShape,
    pub object: ObjShape,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StringShape {
    pub content_encoding: Option<String>,
    pub content_type: Option<String>,
    pub format: Option<Format>,
    pub max_length: Option<usize>,
    pub min_length: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArrayShape {
    pub min: Option<usize>,
    pub max: Option<usize>,
    pub tuple: Vec<Shape>,
    pub additional: Option<Box<Shape>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjShape {
    pub properties: Vec<ObjProperty>,
    pub patterns: Vec<ObjPattern>,
    pub additional: Option<Box<Shape>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjProperty {
    pub name: String,
    pub is_required: bool,
    pub shape: Shape,
}

#[derive(Clone, Debug)]
pub struct ObjPattern {
    pub re: Regex,
    pub shape: Shape,
}

impl Eq for ObjPattern {}

impl PartialEq for ObjPattern {
    fn eq(&self, other: &Self) -> bool {
        self.re.as_str() == other.re.as_str() && self.shape == other.shape
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reduction {
    // Equivalent to Option::None.
    Unset,

    Append,
    FirstWriteWins,
    JsonSchemaMerge,
    LastWriteWins,
    Maximize,
    Merge,
    Minimize,
    Set,
    Sum,

    // Multiple concrete strategies may apply at the location.
    Multiple,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Provenance {
    // Equivalent to Option::None.
    Unset,
    // Url of another Schema, which this Schema is wholly drawn from.
    Reference(Url),
    // This location has local applications which constrain its Shape.
    Inline,
}

impl StringShape {
    pub const fn new() -> Self {
        Self {
            content_encoding: None,
            content_type: None,
            format: None,
            max_length: None,
            min_length: 0,
        }
    }
}

impl ObjShape {
    pub const fn new() -> Self {
        Self {
            properties: Vec::new(),
            patterns: Vec::new(),
            additional: None,
        }
    }
}

impl ArrayShape {
    pub const fn new() -> Self {
        Self {
            min: None,
            max: None,
            tuple: Vec::new(),
            additional: None,
        }
    }
}

impl Shape {
    /// Anything returns a Shape that matches any documents,
    /// equivalent to the "true" JSON schema.
    pub const fn anything() -> Self {
        Self {
            type_: types::ANY,
            enum_: None,
            title: None,
            description: None,
            reduction: Reduction::Unset,
            provenance: Provenance::Unset,
            default: None,
            secret: None,
            annotations: BTreeMap::new(),
            string: StringShape::new(),
            array: ArrayShape::new(),
            object: ObjShape::new(),
        }
    }

    /// Nothing returns a Shape that matches no documents,
    /// equivalent to the "false" JSON schema.
    pub const fn nothing() -> Self {
        Self {
            type_: types::INVALID,
            enum_: None,
            title: None,
            description: None,
            reduction: Reduction::Unset,
            provenance: Provenance::Inline,
            default: None,
            secret: None,
            annotations: BTreeMap::new(),
            string: StringShape::new(),
            array: ArrayShape::new(),
            object: ObjShape::new(),
        }
    }
}

// Returns true if the text is a match for the given regex. This function exists primarily so we
// have a common place to put logging, since there's a weird edge case where `is_match` returns an
// `Err`. This can happen if a regex uses backtracking and overflows the `backtracking_limit` when
// matching. We _could_ return an error when that happens, but it's not clear what the caller
// would do with such an error besides consider the document invalid. The logging might be
// important, though, since some jerk could potentially use this in a DDOS attack.
fn regex_matches(re: &fancy_regex::Regex, text: &str) -> bool {
    re.is_match(text).unwrap_or_else(|err| {
        tracing::warn!("error testing for regex match during inference: {}", err);
        false
    })
}

// Map values into their combined type set.
fn value_types<'v, I: Iterator<Item = &'v Value>>(it: I) -> types::Set {
    it.fold(types::INVALID, |_type, val| {
        types::Set::for_value(val) | _type
    })
}

// Given Shapes for pattern properties and additional properties,
// compute the imputed shape for a property named `property`.
fn impute_property_shape(
    property: &str,
    patterns: &[ObjPattern],
    additional: Option<&Shape>,
) -> Option<Shape> {
    // Compute the intersection of all matching property patterns.
    let pattern = patterns.iter().fold(None, |prior, pattern| {
        if !regex_matches(&pattern.re, property) {
            prior
        } else if let Some(prior) = prior {
            Some(Shape::intersect(prior, pattern.shape.clone()))
        } else {
            Some(pattern.shape.clone())
        }
    });

    if let Some(pattern) = pattern {
        Some(pattern)
    } else if let Some(addl) = additional {
        Some(addl.clone())
    } else {
        None
    }
}

#[cfg(test)]
// Map a JSON schema, in YAML form, into a Shape.
fn shape_from(schema_yaml: &str) -> Shape {
    let url = url::Url::parse("http://example/schema").unwrap();
    let schema: Value = serde_yaml::from_str(schema_yaml).unwrap();
    let schema =
        json::schema::build::build_schema::<crate::Annotation>(url.clone(), &schema).unwrap();

    let mut index = json::schema::index::IndexBuilder::new();
    index.add(&schema).unwrap();
    index.verify_references().unwrap();
    let index = index.into_index();

    Shape::infer(index.must_fetch(&url).unwrap(), &index)
}
