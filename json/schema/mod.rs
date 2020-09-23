use crate::Number;
use serde_json as sj;
use std::fmt::Write;

pub mod build;
pub mod index;
pub mod intern;
pub mod keywords;
pub mod types;

pub use build::Error as BuildError;

#[derive(Debug)]
pub struct Schema<A>
where
    A: Annotation,
{
    // Canonical URI of this Schema.
    pub curi: url::Url,
    // Keywords of the Schema.
    pub kw: Vec<Keyword<A>>,
    // Interned property names of this Schema.
    pub tbl: intern::Table,
}

/// Annotation is a parsed JSON-Schema annotation that's associated with a Schema instance.
/// An Annotation may wrap, and is potentially convertible to a CoreAnnotation.
pub trait Annotation: Sized + std::fmt::Debug {
    fn as_core(&self) -> Option<&CoreAnnotation>;
}

/// CoreAnnotation represents annotations of the JSON-Schema validation specification.
/// C.f. https://json-schema.org/draft/2019-09/json-schema-validation.html#rfc.section.9
#[derive(Debug)]
pub enum CoreAnnotation {
    Title(String),
    Description(String),
    Default(sj::Value),
    Deprecated(bool),
    ReadOnly(bool),
    WriteOnly(bool),
    Examples(Vec<sj::Value>),
    ContentEncodingBase64,
    ContentMediaType(String),
    /// The "format" annotation can be simply informational, or it can optionally be an assertion
    /// that a string is valid for the given format. We currently only collect and use it as an
    /// annotation, but we may also use it as an assertion in the future.
    /// https://json-schema.org/draft/2019-09/json-schema-validation.html#format
    Format(String),
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
    // recursiveAnchor indicates that, should this schema appears first in
    // the current *dynamic* scope, then its base URI should be used when
    // resolving a $recursiveRef of a sub-schema of the current scope.
    RecursiveAnchor,
    // $anchor keyword indicates that this Schema should be indexed under
    // an additional canonical URI, which is computed as the base URI
    // extended with a URI fragment composed of the Anchor string.
    Anchor(url::Url),
    // Application of an in-place or child Schema, with respect to this Schema.
    Application(Application, Schema<A>),
    // Validation keyword verified by this Schema.
    Validation(Validation),
    // Annotation collected by a successful application of this Schema.
    Annotation(A),
}

#[derive(Debug)]
pub enum Application {
    // $def is a keyword which defines a schema playing no direct
    // role in validation, but which may be referenced by other schemas
    // (and is indexed).
    Def {
        key: String,
    },

    // In-place applications.
    Ref(url::Url),
    RecursiveRef(String),
    AnyOf {
        index: usize,
    },
    AllOf {
        index: usize,
    },
    OneOf {
        index: usize,
    },
    Not,
    If,
    Then,
    Else,
    DependentSchema {
        if_: String,
        if_interned: intern::Set,
    },

    // Property applications.
    PropertyNames,
    Properties {
        name: String,
        name_interned: intern::Set,
    },
    PatternProperties {
        re: regex::Regex,
    },
    AdditionalProperties,
    UnevaluatedProperties,

    // Item applications.
    Contains,
    Items {
        index: Option<usize>,
    },
    AdditionalItems,
    UnevaluatedItems,
}

impl Application {
    /// Returns a new Location that extends this one with the Application's keyword.
    pub fn push_keyword<'a>(&'a self, parent: &'a super::Location<'a>) -> super::Location<'a> {
        use Application::*;
        match self {
            Def { .. } => parent.push_prop(keywords::DEF),

            // In-place keywords.
            Ref(_) => parent.push_prop(keywords::REF),
            RecursiveRef(_) => parent.push_prop(keywords::RECURSIVE_REF),
            AnyOf { .. } => parent.push_prop(keywords::ANY_OF),
            AllOf { .. } => parent.push_prop(keywords::ALL_OF),
            OneOf { .. } => parent.push_prop(keywords::ONE_OF),
            Not => parent.push_prop(keywords::NOT),
            If => parent.push_prop(keywords::IF),
            Then => parent.push_prop(keywords::THEN),
            Else => parent.push_prop(keywords::ELSE),
            DependentSchema { .. } => parent.push_prop(keywords::DEPENDENT_SCHEMAS),

            // Property keywords.
            PropertyNames => parent.push_prop(keywords::PROPERTY_NAMES),
            Properties { .. } => parent.push_prop(keywords::PROPERTIES),
            PatternProperties { .. } => parent.push_prop(keywords::PATTERN_PROPERTIES),
            AdditionalProperties => parent.push_prop(keywords::ADDITIONAL_PROPERTIES),
            UnevaluatedProperties => parent.push_prop(keywords::UNEVALUATED_PROPERTIES),

            // Item keywords.
            Contains => parent.push_prop(keywords::CONTAINS),
            Items { .. } => parent.push_prop(keywords::ITEMS),
            AdditionalItems => parent.push_prop(keywords::ADDITIONAL_ITEMS),
            UnevaluatedItems => parent.push_prop(keywords::UNEVALUATED_ITEMS),
        }
    }

    /// Returns a new Location that extends this one with the Application's target,
    /// if applicable. If not applicable, a copy of |parent| is returned instead.
    /// The parent should be a Location of this Application's keyword (c.f. push_keyword).
    pub fn push_keyword_target<'a>(
        &'a self,
        parent: &'a super::Location<'a>,
    ) -> super::Location<'a> {
        use Application::*;
        match self {
            Def { key } => parent.push_prop(key),

            // In-place keywords.
            Ref(_) => *parent,
            RecursiveRef(_) => *parent,
            AnyOf { index } => parent.push_item(*index),
            AllOf { index } => parent.push_item(*index),
            OneOf { index } => parent.push_item(*index),
            Not | If | Then | Else => *parent,
            DependentSchema { if_, .. } => parent.push_prop(if_),

            // Property keywords.
            PropertyNames => *parent,
            Properties { name, .. } => parent.push_prop(name),
            PatternProperties { re, .. } => parent.push_prop(re.as_str()),
            AdditionalProperties | UnevaluatedProperties => *parent,

            // Item keywords.
            Contains => *parent,
            Items { index: None } => *parent,
            Items { index: Some(i) } => parent.push_item(*i),
            AdditionalItems | UnevaluatedItems => *parent,
        }
    }

    /// Extend |ptr| with the JSON-Pointer components of this Application.
    pub fn extend_fragment_pointer(&self, mut ptr: String) -> String {
        let l_root = super::Location::Root;
        let l_kw = self.push_keyword(&l_root);
        let l_kwt = self.push_keyword_target(&l_kw);

        write!(&mut ptr, "{}", l_kwt.url_escaped()).unwrap();
        ptr
    }
}

#[derive(Debug)]
pub struct HashedLiteral {
    pub hash: u64,
    pub value: sj::Value,
}

#[derive(Debug)]
pub enum Validation {
    False,
    Type(types::Set),
    Const(HashedLiteral),
    Enum {
        variants: Vec<HashedLiteral>,
    },

    // String-specific validations.
    MaxLength(usize),
    MinLength(usize),
    Pattern(regex::Regex),
    // Format(String),

    // Number-specific validations.
    MultipleOf(Number),
    Maximum(Number),
    ExclusiveMaximum(Number),
    Minimum(Number),
    ExclusiveMinimum(Number),

    // Array-specific validations.
    MaxItems(usize),
    MinItems(usize),
    UniqueItems,
    MaxContains(usize),
    MinContains(usize),

    // Object-specific validations.
    MaxProperties(usize),
    MinProperties(usize),
    Required {
        props: Vec<String>,
        props_interned: intern::Set,
    },
    DependentRequired {
        if_: String,
        if_interned: intern::Set,
        then_: Vec<String>,
        then_interned: intern::Set,
    },
}
