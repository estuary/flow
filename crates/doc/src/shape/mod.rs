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

// NOTE(johnny): This struct is large enough that its size may impact cache
// efficiency in certain hot paths. Be careful about adding new fields,
// and consider using niches like Option<Box<T>>.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Shape {
    /// Types that this location may take.
    pub type_: types::Set,
    /// Explicit enumeration of allowed values.
    pub enum_: Option<Vec<Value>>,
    /// Annotated `title` of the location.
    pub title: Option<Box<str>>,
    /// Annotated `description` of the location.
    pub description: Option<Box<str>>,
    /// Location's `reduce` strategy.
    pub reduce: Reduce,
    /// Location's `redact` strategy.
    pub redact: Redact,
    /// Does this location's schema flow from a `$ref`?
    pub provenance: Provenance,
    /// Default value of this document location, if any. A validation error is recorded if the
    /// default value specified does not validate against the location's schema.
    pub default: Option<Box<(Value, Option<super::FailedValidation>)>>,
    /// Is this location sensitive? For example, a password or credential.
    pub secret: Option<bool>,
    /// Annotations are any keywords starting with `X-` or `x-`.
    /// Their keys and values are collected here, without performing any
    /// normalization of prefix case. Technically both `x-foo` and `X-foo` may be
    /// defined and included here.
    pub annotations: BTreeMap<String, Value>,

    // Further type-specific inferences:
    pub array: ArrayShape,
    pub numeric: NumericShape,
    pub object: ObjShape,
    pub string: StringShape,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StringShape {
    pub content_encoding: Option<Box<str>>,
    pub content_type: Option<Box<str>>,
    pub format: Option<Format>,
    pub max_length: Option<u32>,
    pub min_length: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArrayShape {
    pub additional_items: Option<Box<Shape>>,
    pub max_items: Option<u32>,
    pub min_items: u32,
    pub tuple: Vec<Shape>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjShape {
    pub additional_properties: Option<Box<Shape>>,
    pub pattern_properties: Vec<ObjPattern>,
    pub properties: Vec<ObjProperty>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjProperty {
    pub name: Box<str>,
    pub is_required: bool,
    pub shape: Shape,
}

#[derive(Clone, Debug)]
pub struct ObjPattern {
    pub re: regex::Regex,
    pub shape: Shape,
}

impl Eq for ObjPattern {}

impl PartialEq for ObjPattern {
    fn eq(&self, other: &Self) -> bool {
        self.re.as_str() == other.re.as_str() && self.shape == other.shape
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NumericShape {
    pub minimum: Option<json::Number>,
    pub maximum: Option<json::Number>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Reduce {
    // Equivalent to Option::None.
    Unset,
    // Reduce using a strategy.
    Strategy(crate::reduce::Strategy),
    // Multiple concrete strategies may apply at the location.
    Multiple,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Redact {
    // Equivalent to Option::None.
    Unset,
    // Redact using a strategy.
    Strategy(crate::redact::Strategy),
    // Multiple concrete strategies may apply at the location.
    Multiple,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Provenance {
    // Equivalent to Option::None.
    Unset,
    // Url of another Schema, which this Schema is wholly drawn from.
    Reference(Box<Url>),
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
            additional_properties: None,
            pattern_properties: Vec::new(),
            properties: Vec::new(),
        }
    }
}

impl ArrayShape {
    pub const fn new() -> Self {
        Self {
            additional_items: None,
            max_items: None,
            min_items: 0,
            tuple: Vec::new(),
        }
    }
}

impl NumericShape {
    pub const fn new() -> Self {
        Self {
            minimum: None,
            maximum: None,
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
            reduce: Reduce::Unset,
            redact: Redact::Unset,
            provenance: Provenance::Unset,
            default: None,
            secret: None,
            annotations: BTreeMap::new(),
            array: ArrayShape::new(),
            numeric: NumericShape::new(),
            object: ObjShape::new(),
            string: StringShape::new(),
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
            reduce: Reduce::Unset,
            redact: Redact::Unset,
            provenance: Provenance::Inline,
            default: None,
            secret: None,
            annotations: BTreeMap::new(),
            array: ArrayShape::new(),
            numeric: NumericShape::new(),
            object: ObjShape::new(),
            string: StringShape::new(),
        }
    }
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
        if !pattern.re.is_match(property) {
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

/// X_INFER_SCHEMA is a JSON-Schema annotation optionally added to binding schemas
/// emitted by connectors during discovery. If present, it will be union'd with the
/// collection's inferred schema ref to form the discovered collection's read schema.
pub const X_INFER_SCHEMA: &str = "x-infer-schema";

/// X_INITIAL_READ_SCHEMA is a JSON-Schema annotation optionally added to binding schemas
/// emitted by connectors during discovery. If present, it will be used as the
/// collection's initial read schema.
pub const X_INITIAL_READ_SCHEMA: &str = "x-initial-read-schema";

/// X_COMPLEXITY_LIMIT is a JSON-Schema annotation added to emitted inferred schemas that
/// allows for the modification of the default complexity limit applied to inferred schemas.
pub const X_COMPLEXITY_LIMIT: &str = "x-complexity-limit";

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

#[cfg(test)]
mod test {
    #[test]
    #[cfg(target_arch = "x86_64")]
    fn shape_size_regression() {
        use super::{ArrayShape, ObjShape, Shape, StringShape};
        assert_eq!(std::mem::size_of::<ObjShape>(), 56);
        assert_eq!(std::mem::size_of::<StringShape>(), 48);
        assert_eq!(std::mem::size_of::<ArrayShape>(), 48);
        assert_eq!(std::mem::size_of::<Shape>(), 328);
    }
}
