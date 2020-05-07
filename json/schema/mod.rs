use crate::Number;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde_json as sj;
use std::fmt::Write;
use std::ops::Add;

pub mod build;
pub mod index;
pub mod inference;
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

fn encode_frag_ptr(into: &mut String, component: &str) {
    for p in utf8_percent_encode(component, FRAGMENT) {
        for c in p.chars() {
            match c {
                '~' => into.push_str("~0"),
                '/' => into.push_str("~1"),
                _ => into.push(c),
            }
        }
    }
}

impl Application {
    pub fn extend_fragment_pointer(&self, mut ptr: String) -> String {
        ptr.push('/');

        use Application::*;
        match self {
            Def { key } => {
                ptr.push_str(keywords::DEF);
                ptr.push('/');
                encode_frag_ptr(&mut ptr, key);
                ptr
            }

            // In-place keywords.
            Ref(_) => ptr.add(keywords::REF),
            RecursiveRef(_) => ptr.add(keywords::RECURSIVE_REF),
            AnyOf { index } => {
                write!(&mut ptr, "{}/{}", keywords::ANY_OF, index).unwrap();
                ptr
            }
            AllOf { index } => {
                write!(&mut ptr, "{}/{}", keywords::ALL_OF, index).unwrap();
                ptr
            }
            OneOf { index } => {
                write!(&mut ptr, "{}/{}", keywords::ALL_OF, index).unwrap();
                ptr
            }
            Not => ptr.add(keywords::NOT),
            If => ptr.add(keywords::IF),
            Then => ptr.add(keywords::THEN),
            Else => ptr.add(keywords::ELSE),
            DependentSchema { if_, .. } => {
                ptr.push_str(keywords::DEPENDENT_SCHEMAS);
                ptr.push('/');
                encode_frag_ptr(&mut ptr, if_);
                ptr
            }

            // Property keywords.
            PropertyNames => ptr.add(keywords::PROPERTY_NAMES),
            Properties { name, .. } => {
                ptr.push_str(keywords::PROPERTIES);
                ptr.push('/');
                encode_frag_ptr(&mut ptr, name);
                ptr
            }
            PatternProperties { re, .. } => {
                ptr.push_str(keywords::PATTERN_PROPERTIES);
                ptr.push('/');
                encode_frag_ptr(&mut ptr, re.as_str());
                ptr
            }
            AdditionalProperties => ptr.add(keywords::ADDITIONAL_PROPERTIES),
            UnevaluatedProperties => ptr.add(keywords::UNEVALUATED_PROPERTIES),

            // Item keywords.
            Contains => ptr.add(keywords::CONTAINS),
            Items { index: None } => ptr.add(keywords::ITEMS),
            Items { index: Some(i) } => {
                write!(&mut ptr, "{}/{}", keywords::ITEMS, i).unwrap();
                ptr
            }
            AdditionalItems => ptr.add(keywords::ADDITIONAL_ITEMS),
            UnevaluatedItems => ptr.add(keywords::UNEVALUATED_ITEMS),
        }
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
        variants: Vec<HashedLiteral>
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
    Required{
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

/// https://url.spec.whatwg.org/#fragment-percent-encode-set
const FRAGMENT: &AsciiSet = &CONTROLS
    .add(b'%')
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`');
