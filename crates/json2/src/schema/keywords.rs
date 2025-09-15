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
    Format(formats::Format),
}

/// Annotation is a parsed JSON-Schema annotation that's associated with a Schema instance.
/// An Annotation may wrap, and is potentially convertible to a CoreAnnotation.
pub trait Annotation: Sized + std::fmt::Debug {
    fn as_core(&self) -> Option<&CoreAnnotation>;
}

// CoreAnnotation trivially implements Annotation.
impl Annotation for CoreAnnotation {
    fn as_core(&self) -> Option<&CoreAnnotation> {
        Some(self)
    }
}

#[derive(Debug)]
pub enum Keyword<A>
where
    A: Annotation,
{
    CanonicalUri {
        curi: FrozenString,
    },
    // $def is a keyword which defines a schema playing no direct
    // role in validation, but which may be referenced by other schemas
    // (and is indexed).
    Def {
        defs: FrozenSlice<(FrozenString, Schema<A>)>,
    },
    // definitions is the legacy name for the $def keyword,
    // having identical semantics, which we continue to support.
    Definitions {
        definitions: FrozenSlice<(FrozenString, Schema<A>)>,
    },
    // dynamicAnchor indicates that, should this schema appears first in
    // the current *dynamic* scope, then its base URI should be used when
    // resolving a $dynamicRef of a sub-schema of the current scope.
    DynamicAnchor,
    // $anchor keyword indicates that this Schema should be indexed under
    // an additional canonical URI, which is computed as the base URI
    // extended with a URI fragment composed of the Anchor string.
    Anchor {
        anchor: FrozenString,
    },
    // Annotation collected by a successful application of this Schema.
    Annotation {
        annotation: Box<A>,
    },

    // In-place applications.
    Ref {
        r#ref: FrozenString,
    },
    DynamicRef {
        dynamic_ref: FrozenString,
    },
    AnyOf {
        any_of: FrozenSlice<Schema<A>>,
    },
    AllOf {
        all_of: FrozenSlice<Schema<A>>,
    },
    OneOf {
        one_of: FrozenSlice<Schema<A>>,
    },
    Not {
        not: Box<Schema<A>>,
    },
    If {
        r#if: Box<Schema<A>>,
    },
    Then {
        then: Box<Schema<A>>,
    },
    Else {
        r#else: Box<Schema<A>>,
    },
    DependentSchemas {
        dependent_schemas: FrozenSlice<(FrozenString, Schema<A>)>,
    },

    // Property applications.
    PropertyNames {
        property_names: Box<Schema<A>>,
    },
    Properties {
        // Properties which are required are tagged.
        properties: FrozenSlice<(FrozenString, Schema<A>)>,
    },
    PatternProperties {
        pattern_properties: FrozenSlice<(regex::Regex, Schema<A>)>,
    },
    AdditionalProperties {
        additional_properties: Box<Schema<A>>,
    },
    UnevaluatedProperties {
        unevaluated_properties: Box<Schema<A>>,
    },

    // Item applications.
    Contains {
        contains: Box<Schema<A>>,
    },
    PrefixItems {
        items: FrozenSlice<Schema<A>>,
    },
    Items {
        additional_items: Box<Schema<A>>,
    },
    UnevaluatedItems {
        unevaluated_items: Box<Schema<A>>,
    },

    // Validation keyword verified by this Schema.
    False,
    Type {
        r#type: types::Set,
    },
    Const {
        r#const: Box<serde_json::Value>,
    },
    Enum {
        r#enum: FrozenSlice<serde_json::Value>,
    },

    // String-specific validations.
    MaxLength {
        max_length: usize,
    },
    MinLength {
        min_length: usize,
    },
    Pattern {
        pattern: Box<regex::Regex>,
    },
    Format {
        format: formats::Format,
    },

    // Number-specific validations.
    MultipleOfPosInt {
        multiple_of: u64,
    },
    MultipleOfNegInt {
        multiple_of: i64,
    },
    MultipleOfFloat {
        multiple_of: f64,
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

    ExclusiveMaximumPosInt {
        exclusive_maximum: u64,
    },
    ExclusiveMaximumNegInt {
        exclusive_maximum: i64,
    },
    ExclusiveMaximumFloat {
        exclusive_maximum: f64,
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

    ExclusiveMinimumPosInt {
        exclusive_minimum: u64,
    },
    ExclusiveMinimumNegInt {
        exclusive_minimum: i64,
    },
    ExclusiveMinimumFloat {
        exclusive_minimum: f64,
    },

    // Array-specific validations.
    MaxItems {
        max_items: usize,
    },
    MinItems {
        min_items: usize,
    },
    UniqueItems,
    MaxContains {
        max_contains: usize,
    },
    MinContains {
        min_contains: usize,
    },

    // Object-specific validations.
    // Note `required` has special handling within the Property application.
    MaxProperties {
        max_properties: usize,
    },
    MinProperties {
        min_properties: usize,
    },
    DependentRequired {
        dependent_required: FrozenSlice<(FrozenString, Vec<FrozenString>)>,
    },
}

pub const ADDITIONAL_ITEMS: &str = "additionalItems";
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
pub const DEF: &str = "$defs";
pub const DEFAULT: &str = "default";
pub const DEFINITIONS: &str = "definitions"; // Alternate of $defs. Same semantics.
pub const DEPENDENT_REQUIRED: &str = "dependentRequired";
pub const DEPENDENT_SCHEMAS: &str = "dependentSchemas";
pub const DEPRECATED: &str = "deprecated";
pub const DESCRIPTION: &str = "description";
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
pub const PROPERTIES: &str = "properties";
pub const PROPERTY_NAMES: &str = "propertyNames";
pub const READ_ONLY: &str = "readOnly";
pub const DYNAMIC_ANCHOR: &str = "$dynamicAnchor";
pub const DYNAMIC_REF: &str = "$dynamicRef";
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
