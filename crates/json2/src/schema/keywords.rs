use super::{formats, types, FrozenSlice, FrozenString, Schema};

/// CoreAnnotation represents annotations of the JSON-Schema validation specification.
/// C.f. https://json-schema.org/draft/2019-09/json-schema-validation.html#rfc.section.9
#[derive(Debug)]
pub enum CoreAnnotation {
    Title(FrozenString),
    Description(FrozenString),
    Default(Box<serde_json::Value>),
    Deprecated(bool),
    ReadOnly(bool),
    WriteOnly(bool),
    Examples(Box<[serde_json::Value]>),
    ContentEncoding(FrozenString),
    ContentMediaType(FrozenString),
    ContentSchema(FrozenString),
    Format(formats::Format),
}

/// Annotation is a parsed JSON-Schema annotation that's associated with a Schema instance.
/// An Annotation may wrap, and is potentially convertible to a CoreAnnotation.
pub trait Annotation: Sized + std::fmt::Debug {
    type KeywordError: std::error::Error + Send + Sync + 'static;

    /// Returns true if the Annotation knows how to extract itself from the given keyword.
    fn uses_keyword(keyword: &str) -> bool;
    /// from_keyword builds an Annotation from the given keyword & value,
    /// which MUST be a keyword for which uses_keyword() is true.
    fn from_keyword(keyword: &str, value: &serde_json::Value) -> Result<Self, Self::KeywordError>;
}

#[derive(Debug)]
pub enum Keyword<A>
where
    A: Annotation,
{
    // Annotation wraps a boxed annotation keyword.
    Annotation {
        annotation: Box<A>,
    },
    // False is not a keyword, but a schema that always fails validation.
    False,

    // JSON Schema 2020-12 keywords follow:
    AdditionalProperties {
        additional_properties: Box<Schema<A>>,
    },
    AllOf {
        all_of: FrozenSlice<Schema<A>>,
    },
    Anchor {
        // $anchor keyword indicates that this Schema should be indexed under
        // an additional canonical URI, which is computed as the base URI
        // extended with a URI fragment composed of the Anchor string.
        anchor: FrozenString,
    },
    AnyOf {
        any_of: FrozenSlice<Schema<A>>,
    },
    Comment {
        comment: FrozenString,
    },
    Const {
        r#const: Box<serde_json::Value>,
    },
    Contains {
        contains: Box<Schema<A>>,
    },
    Defs {
        // $def is a keyword which defines a schema playing no direct
        // role in validation, but which may be referenced by other schemas
        // (and is indexed).
        defs: FrozenSlice<(FrozenString, Schema<A>)>,
    },
    Definitions {
        // definitions is the legacy name for the $def keyword,
        // having identical semantics, which we continue to support.
        definitions: FrozenSlice<(FrozenString, Schema<A>)>,
    },
    DependentRequired {
        dependent_required: Box<(FrozenString, FrozenSlice<FrozenString>)>,
    },
    DependentSchemas {
        dependent_schema: Box<(FrozenString, Schema<A>)>,
    },
    DynamicAnchor {
        // dynamicAnchor indicates that, should this schema appears first in
        // the current *dynamic* scope, then its base URI should be used when
        // resolving a $dynamicRef of a sub-schema of the current scope.
        dynamic_anchor: FrozenString,
    },
    DynamicRef {
        dynamic_ref: FrozenString,
    },
    Else {
        r#else: Box<Schema<A>>,
    },
    Enum {
        r#enum: FrozenSlice<serde_json::Value>,
    },
    ExclusiveMaximumPosInt {
        exclusive_maximum: u64,
    },
    ExclusiveMaximumNegInt {
        exclusive_maximum: i64,
    },
    ExclusiveMaximumFloat {
        exclusive_maximum: f64,
    },
    ExclusiveMinimumPosInt {
        exclusive_minimum: u64,
    },
    ExclusiveMinimumNegInt {
        exclusive_minimum: i64,
    },
    ExclusiveMinimumFloat {
        exclusive_minimum: f64,
    },
    Format {
        format: formats::Format,
    },
    Id {
        curi: FrozenString,
        explicit: bool,
    },
    If {
        r#if: Box<Schema<A>>,
    },
    Items {
        items: Box<Schema<A>>,
    },
    MaximumPosInt {
        maximum: u64,
    },
    MaximumNegInt {
        maximum: i64,
    },
    MaximumFloat {
        maximum: f64,
    },
    MaxContains {
        max_contains: usize,
    },
    MaxItems {
        max_items: usize,
    },
    MaxLength {
        max_length: usize,
    },
    MaxProperties {
        max_properties: usize,
    },
    MinimumPosInt {
        minimum: u64,
    },
    MinimumNegInt {
        minimum: i64,
    },
    MinimumFloat {
        minimum: f64,
    },
    MinContains {
        min_contains: usize,
    },
    MinItems {
        min_items: usize,
    },
    MinLength {
        min_length: usize,
    },
    MinProperties {
        min_properties: usize,
    },
    MultipleOfPosInt {
        multiple_of: u64,
    },
    MultipleOfNegInt {
        multiple_of: i64,
    },
    MultipleOfFloat {
        multiple_of: f64,
    },
    Not {
        not: Box<Schema<A>>,
    },
    OneOf {
        one_of: FrozenSlice<Schema<A>>,
    },
    Pattern {
        pattern: Box<regex::Regex>,
    },
    PatternProperties {
        pattern_properties: FrozenSlice<(regex::Regex, Schema<A>)>,
    },
    PrefixItems {
        prefix_items: FrozenSlice<Schema<A>>,
    },
    Properties {
        properties: FrozenSlice<(FrozenString, Schema<A>)>,
    },
    PropertyNames {
        property_names: Box<Schema<A>>,
    },
    Ref {
        r#ref: FrozenString,
    },
    Required {
        required: FrozenSlice<FrozenString>,
    },
    Then {
        then: Box<Schema<A>>,
    },
    Type {
        r#type: types::Set,
    },
    UnevaluatedItems {
        unevaluated_items: Box<Schema<A>>,
    },
    UnevaluatedProperties {
        unevaluated_properties: Box<Schema<A>>,
    },
    UniqueItems {},
}

pub const ADDITIONAL_ITEMS: &str = "additionalItems"; // Legacy keyword for `items`.
pub const ADDITIONAL_PROPERTIES: &str = "additionalProperties";
pub const ALL_OF: &str = "allOf";
pub const ANCHOR: &str = "$anchor";
pub const ANY_OF: &str = "anyOf";
pub const COMMENT: &str = "$comment";
pub const CONST: &str = "const";
pub const CONTAINS: &str = "contains";
pub const CONTENT_ENCODING: &str = "contentEncoding";
pub const CONTENT_MEDIA_TYPE: &str = "contentMediaType";
pub const CONTENT_SCHEMA: &str = "contentSchema";
pub const DEFS: &str = "$defs";
pub const DEFAULT: &str = "default";
pub const DEFINITIONS: &str = "definitions"; // Alternate of $defs. Same semantics.
pub const DEPENDENT_REQUIRED: &str = "dependentRequired";
pub const DEPENDENT_SCHEMAS: &str = "dependentSchemas";
pub const DEPRECATED: &str = "deprecated";
pub const DESCRIPTION: &str = "description";
pub const DYNAMIC_ANCHOR: &str = "$dynamicAnchor";
pub const DYNAMIC_REF: &str = "$dynamicRef";
pub const ELSE: &str = "else";
pub const ENUM: &str = "enum";
pub const EXAMPLE: &str = "example"; // OpenAPI < 3.1. Merged with "examples".
pub const EXAMPLES: &str = "examples";
pub const EXCLUSIVE_MAXIMUM: &str = "exclusiveMaximum";
pub const EXCLUSIVE_MINIMUM: &str = "exclusiveMinimum";
pub const FORMAT: &str = "format";
pub const ID: &str = "$id";
pub const IF: &str = "if";
pub const ITEMS: &str = "items";
pub const MAXIMUM: &str = "maximum";
pub const MAX_CONTAINS: &str = "maxContains";
pub const MAX_ITEMS: &str = "maxItems";
pub const MAX_LENGTH: &str = "maxLength";
pub const MAX_PROPERTIES: &str = "maxProperties";
pub const MINIMUM: &str = "minimum";
pub const MIN_CONTAINS: &str = "minContains";
pub const MIN_ITEMS: &str = "minItems";
pub const MIN_LENGTH: &str = "minLength";
pub const MIN_PROPERTIES: &str = "minProperties";
pub const MULTIPLE_OF: &str = "multipleOf";
pub const NOT: &str = "not";
pub const NULLABLE: &str = "nullable"; // OpenAPI < 3.1. Merged into "type".
pub const ONE_OF: &str = "oneOf";
pub const PATTERN: &str = "pattern";
pub const PATTERN_PROPERTIES: &str = "patternProperties";
pub const PREFIX_ITEMS: &str = "prefixItems";
pub const PROPERTIES: &str = "properties";
pub const PROPERTY_NAMES: &str = "propertyNames";
pub const READ_ONLY: &str = "readOnly";
pub const REF: &str = "$ref";
pub const REQUIRED: &str = "required";
pub const SCHEMA: &str = "$schema";
pub const THEN: &str = "then";
pub const TITLE: &str = "title";
pub const TYPE: &str = "type";
pub const UNEVALUATED_ITEMS: &str = "unevaluatedItems";
pub const UNEVALUATED_PROPERTIES: &str = "unevaluatedProperties";
pub const UNIQUE_ITEMS: &str = "uniqueItems";
pub const VOCABULARY: &str = "$vocabulary";
pub const WRITE_ONLY: &str = "writeOnly";

impl Annotation for CoreAnnotation {
    type KeywordError = serde_json::Error;

    fn uses_keyword(kw: &str) -> bool {
        match kw {
            CONTENT_ENCODING | CONTENT_MEDIA_TYPE | DEFAULT | DEPRECATED | DESCRIPTION
            | EXAMPLE | EXAMPLES | FORMAT | READ_ONLY | TITLE | WRITE_ONLY => true,
            _ => false,
        }
    }

    fn from_keyword(kw: &str, v: &serde_json::Value) -> Result<Self, serde_json::Error> {
        let v = v.clone();

        let as_str = |v: serde_json::Value| -> Result<FrozenString, serde_json::Error> {
            Ok(serde_json::from_value::<String>(v)?.into())
        };
        let as_bool = |v: serde_json::Value| -> Result<bool, serde_json::Error> {
            Ok(serde_json::from_value::<bool>(v)?)
        };

        Ok(match kw {
            CONTENT_ENCODING => CoreAnnotation::ContentEncoding(as_str(v)?),
            CONTENT_MEDIA_TYPE => CoreAnnotation::ContentMediaType(as_str(v)?),
            DEFAULT => CoreAnnotation::Default(Box::new(v)),
            DEPRECATED => CoreAnnotation::Deprecated(as_bool(v)?),
            DESCRIPTION => CoreAnnotation::Description(as_str(v)?),
            EXAMPLE => CoreAnnotation::Examples(vec![v].into()),
            EXAMPLES => {
                CoreAnnotation::Examples(serde_json::from_value::<Box<[serde_json::Value]>>(v)?)
            }
            FORMAT => CoreAnnotation::Format(serde_json::from_value::<formats::Format>(v)?),
            READ_ONLY => CoreAnnotation::ReadOnly(as_bool(v)?),
            TITLE => CoreAnnotation::Title(as_str(v)?),
            WRITE_ONLY => CoreAnnotation::WriteOnly(as_bool(v)?),
            _ => panic!("unexpected keyword: '{}'", kw),
        })
    }
}
