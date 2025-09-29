use super::{formats, types, PackedSlice, PackedStr, Schema};

/// CoreAnnotation represents annotations of the JSON-Schema validation specification.
/// C.f. https://json-schema.org/draft/2019-09/json-schema-validation.html#rfc.section.9
#[derive(Debug)]
pub enum CoreAnnotation {
    Comment(PackedStr),
    ContentEncoding(PackedStr),
    ContentMediaType(PackedStr),
    ContentSchema(Box<serde_json::Value>),
    Default(Box<serde_json::Value>),
    Deprecated(bool),
    Description(PackedStr),
    Examples(Box<[serde_json::Value]>),
    Format(formats::Format),
    ReadOnly(bool),
    Title(PackedStr),
    WriteOnly(bool),
}

/// Annotation is a parsed JSON-Schema annotation that's associated with a Schema instance.
/// An Annotation may wrap, and is potentially convertible to a CoreAnnotation.
pub trait Annotation: std::fmt::Debug + Sized + 'static {
    type KeywordError: std::error::Error;

    // Returns the keyword of this annotation.
    fn keyword(&self) -> &str;
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
        all_of: PackedSlice<Schema<A>>,
    },
    Anchor {
        // $anchor keyword indicates that this Schema should be indexed under
        // an additional canonical URI, which is computed as the base URI
        // extended with a URI fragment composed of the Anchor string.
        anchor: PackedStr,
    },
    AnyOf {
        any_of: PackedSlice<Schema<A>>,
    },
    Const {
        r#const: Box<serde_json::Value>,
    },
    Contains {
        contains: Box<Schema<A>>,
    },
    Definitions {
        // definitions is the legacy name for the $def keyword,
        // having identical semantics, which we continue to support.
        definitions: PackedSlice<(PackedStr, Schema<A>)>,
    },
    Defs {
        // $def is a keyword which defines a schema playing no direct
        // role in validation, but which may be referenced by other schemas
        // (and is indexed).
        defs: PackedSlice<(PackedStr, Schema<A>)>,
    },
    DependentSchemas {
        dependent_schemas: PackedSlice<(PackedStr, Schema<A>)>,
    },
    DynamicAnchor {
        // $dynamicAnchor does two things:
        // 1) It indexes this schema under an additional anchor-form URI.
        // 2) It advertises that this schema's base URL is eligible for
        //    inclusion in the "dynamic scope" against which $dynamicRef
        //    is evaluated.
        dynamic_anchor: PackedStr,
    },
    DynamicRef {
        dynamic_ref: PackedStr,
    },
    Else {
        r#else: Box<Schema<A>>,
    },
    Enum {
        r#enum: PackedSlice<serde_json::Value>,
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
        curi: PackedStr,
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
        one_of: PackedSlice<Schema<A>>,
    },
    Pattern {
        pattern: Box<regex::Regex>,
    },
    PatternProperties {
        pattern_properties: PackedSlice<(regex::Regex, Schema<A>)>,
    },
    PrefixItems {
        prefix_items: PackedSlice<Schema<A>>,
    },
    Properties {
        properties: PackedSlice<(PackedStr, Schema<A>)>,
    },
    PropertyNames {
        property_names: Box<Schema<A>>,
    },
    Ref {
        r#ref: PackedStr,
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

impl<A: Annotation> Keyword<A> {
    pub fn keyword(&self) -> &str {
        match self {
            Keyword::Annotation { annotation } => annotation.keyword(),
            Keyword::AdditionalProperties { .. } => ADDITIONAL_PROPERTIES,
            Keyword::AllOf { .. } => ALL_OF,
            Keyword::Anchor { .. } => ANCHOR,
            Keyword::AnyOf { .. } => ANY_OF,
            Keyword::Const { .. } => CONST,
            Keyword::Contains { .. } => CONTAINS,
            Keyword::Definitions { .. } => DEFINITIONS,
            Keyword::Defs { .. } => DEFS,
            Keyword::DependentSchemas { .. } => DEPENDENT_SCHEMAS,
            Keyword::DynamicAnchor { .. } => DYNAMIC_ANCHOR,
            Keyword::DynamicRef { .. } => DYNAMIC_REF,
            Keyword::Else { .. } => ELSE,
            Keyword::Enum { .. } => ENUM,
            Keyword::ExclusiveMaximumPosInt { .. } => EXCLUSIVE_MAXIMUM,
            Keyword::ExclusiveMaximumNegInt { .. } => EXCLUSIVE_MAXIMUM,
            Keyword::ExclusiveMaximumFloat { .. } => EXCLUSIVE_MAXIMUM,
            Keyword::ExclusiveMinimumPosInt { .. } => EXCLUSIVE_MINIMUM,
            Keyword::ExclusiveMinimumNegInt { .. } => EXCLUSIVE_MINIMUM,
            Keyword::ExclusiveMinimumFloat { .. } => EXCLUSIVE_MINIMUM,
            Keyword::Format { .. } => FORMAT,
            Keyword::Id { .. } => ID,
            Keyword::If { .. } => IF,
            Keyword::Items { .. } => ITEMS,
            Keyword::MaximumPosInt { .. } => MAXIMUM,
            Keyword::MaximumNegInt { .. } => MAXIMUM,
            Keyword::MaximumFloat { .. } => MAXIMUM,
            Keyword::MaxContains { .. } => MAX_CONTAINS,
            Keyword::MaxItems { .. } => MAX_ITEMS,
            Keyword::MaxLength { .. } => MAX_LENGTH,
            Keyword::MaxProperties { .. } => MAX_PROPERTIES,
            Keyword::MinimumPosInt { .. } => MINIMUM,
            Keyword::MinimumNegInt { .. } => MINIMUM,
            Keyword::MinimumFloat { .. } => MINIMUM,
            Keyword::MinContains { .. } => MIN_CONTAINS,
            Keyword::MinItems { .. } => MIN_ITEMS,
            Keyword::MinLength { .. } => MIN_LENGTH,
            Keyword::MinProperties { .. } => MIN_PROPERTIES,
            Keyword::MultipleOfPosInt { .. } => MULTIPLE_OF,
            Keyword::MultipleOfNegInt { .. } => MULTIPLE_OF,
            Keyword::MultipleOfFloat { .. } => MULTIPLE_OF,
            Keyword::Not { .. } => NOT,
            Keyword::OneOf { .. } => ONE_OF,
            Keyword::Pattern { .. } => PATTERN,
            Keyword::PatternProperties { .. } => PATTERN_PROPERTIES,
            Keyword::PrefixItems { .. } => PREFIX_ITEMS,
            Keyword::Properties { .. } => PROPERTIES,
            Keyword::PropertyNames { .. } => PROPERTY_NAMES,
            Keyword::Ref { .. } => REF,
            Keyword::Then { .. } => THEN,
            Keyword::Type { .. } => TYPE,
            Keyword::UnevaluatedItems { .. } => UNEVALUATED_ITEMS,
            Keyword::UnevaluatedProperties { .. } => UNEVALUATED_PROPERTIES,
            Keyword::UniqueItems { .. } => UNIQUE_ITEMS,

            // Callers should be filtering False before calling keyword(),
            // but return something reasonable if they don't.
            Keyword::False => "'false' is not a keyword",
        }
    }
}

impl Annotation for CoreAnnotation {
    type KeywordError = serde_json::Error;

    fn keyword(&self) -> &'static str {
        match self {
            CoreAnnotation::Comment(_) => COMMENT,
            CoreAnnotation::ContentEncoding(_) => CONTENT_ENCODING,
            CoreAnnotation::ContentMediaType(_) => CONTENT_MEDIA_TYPE,
            CoreAnnotation::ContentSchema(_) => CONTENT_SCHEMA,
            CoreAnnotation::Default(_) => DEFAULT,
            CoreAnnotation::Deprecated(_) => DEPRECATED,
            CoreAnnotation::Description(_) => DESCRIPTION,
            CoreAnnotation::Examples(_) => EXAMPLES,
            CoreAnnotation::Format(_) => FORMAT,
            CoreAnnotation::ReadOnly(_) => READ_ONLY,
            CoreAnnotation::Title(_) => TITLE,
            CoreAnnotation::WriteOnly(_) => WRITE_ONLY,
        }
    }

    fn uses_keyword(kw: &str) -> bool {
        match kw {
            COMMENT | CONTENT_ENCODING | CONTENT_MEDIA_TYPE | CONTENT_SCHEMA | DEFAULT
            | DEPRECATED | DESCRIPTION | EXAMPLE | EXAMPLES | FORMAT | READ_ONLY | TITLE
            | WRITE_ONLY => true,
            _ => false,
        }
    }

    fn from_keyword(kw: &str, v: &serde_json::Value) -> Result<Self, serde_json::Error> {
        let v = v.clone();

        let as_str = |v: serde_json::Value| -> Result<PackedStr, serde_json::Error> {
            Ok(serde_json::from_value::<String>(v)?.into())
        };
        let as_bool = |v: serde_json::Value| -> Result<bool, serde_json::Error> {
            Ok(serde_json::from_value::<bool>(v)?)
        };

        Ok(match kw {
            COMMENT => CoreAnnotation::Comment(as_str(v)?),
            CONTENT_ENCODING => CoreAnnotation::ContentEncoding(as_str(v)?),
            CONTENT_MEDIA_TYPE => CoreAnnotation::ContentMediaType(as_str(v)?),
            CONTENT_SCHEMA => CoreAnnotation::ContentSchema(Box::new(v)),
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

pub const ADDITIONAL_ITEMS: &str = "additionalItems"; // Legacy 2019-09 keyword for `items`.
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
pub const DEFAULT: &str = "default";
pub const DEFINITIONS: &str = "definitions"; // Alternate of $defs. Same semantics.
pub const DEFS: &str = "$defs";
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
pub const RECURSIVE_ANCHOR: &str = "$recursiveAnchor"; // Legacy 2019-09 keyword.
pub const RECURSIVE_REF: &str = "$recursiveRef"; // Legacy 2019-09 keyword.
pub const REF: &str = "$ref";
pub const REQUIRED: &str = "required";
pub const SCHEMA: &str = "$schema";
pub const THEN: &str = "then";
pub const TITLE: &str = "title";
pub const TYPE: &str = "type";
pub const UNEVALUATED_ITEMS: &str = "unevaluatedItems";
pub const UNEVALUATED_PROPERTIES: &str = "unevaluatedProperties";
pub const UNIQUE_ITEMS: &str = "uniqueItems";
pub const URL: &str = "url"; // Appears in draft 2020-12 schema.
pub const VOCABULARY: &str = "$vocabulary";
pub const WRITE_ONLY: &str = "writeOnly";
