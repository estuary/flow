use crate::Number;
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use serde_json as sj;
use std::fmt::{self, Write};
use std::ops::Add;

pub mod build;
pub mod index;
pub mod intern;

// Lexical scope / "absoluteKeywordLocation"
//   - the canonical URI of the current schema, plus app / val keyword.
// Dynamic scope / "keywordLocation"
//   - the keyword path of the current context stack, plus app/val keyword.
// Instance path / "instanceLocation"
//   - URI JSON pointer of instance entity under validation.
//
// While evaluating, maintain a current lexicalBase and dynamicBase:
// - lexicalBase is CURI of the *last* encountered schema having $id
//    (if there is no such schema, it's implicitly equal to the CURI of the root schema document).
// - dynamicBase is CURI of the *first* encountered schema having $recursiveAnchor: true
//    (if there is no such schema, it's implicitly equal to lexicalBase).
//
// Then, when you encounter:
//   $ref? - Resolve as URI-reference against lexicalBase CURI.
//   $recursiveRef? - Resolve as URI-reference against dynamicBase CURI.
//
// $anchor - produces an alternative URI under which a schema is indexed.
//
// Non-canonical URI's (URI's from the root base URI and ignoring subsequent $id) are ignored.

/*
#[derive(Debug)]
pub struct Catalog(Vec<Box<Schema>>);

impl Catalog {
    /// `new` returns an empty Catalog.
    pub fn new() -> Catalog {
        Catalog(Vec::new())
    }

    /*
    pub fn add(&mut self, url: url::Url, v: &sj::Value) -> BuildResult<()> {
        if let Some(_) = url.fragment() {
            return Err(BuildError::from_str(
                "root url cannot have a fragment component",
            ));
        }
        let s = Box::new(build_schema(url, &v)?);
        println!("schema: {:?}", s);

        self.0.push(s);
        Ok(())
    }
    */
}
*/

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
pub trait Annotation: Sized + std::fmt::Debug {}

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
}
impl Annotation for CoreAnnotation {}

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
pub struct TypeSet(u32);

pub const TYPE_INVALID: TypeSet = TypeSet(0b0000000);
pub const TYPE_ARRAY: TypeSet = TypeSet(0b0000001);
pub const TYPE_BOOLEAN: TypeSet = TypeSet(0b0000010);
pub const TYPE_INTEGER: TypeSet = TypeSet(0b0000100);
pub const TYPE_NULL: TypeSet = TypeSet(0b0001000);
pub const TYPE_NUMBER: TypeSet = TypeSet(0b0010000);
pub const TYPE_OBJECT: TypeSet = TypeSet(0b0100000);
pub const TYPE_STRING: TypeSet = TypeSet(0b1000000);

impl std::ops::BitOr for TypeSet {
    type Output = Self;

    fn bitor(self, other: Self) -> Self::Output {
        TypeSet(self.0 | other.0)
    }
}

impl std::ops::BitAnd for TypeSet {
    type Output = Self;

    fn bitand(self, other: Self) -> Self::Output {
        TypeSet(self.0 & other.0)
    }
}

impl TypeSet {
    pub fn as_str<'a>(&self, mut s: Vec<&'static str>) -> Vec<&'static str> {
        if self.0 & TYPE_ARRAY.0 != 0 {
            s.push("array")
        }
        if self.0 & TYPE_BOOLEAN.0 != 0 {
            s.push("boolean")
        }
        if self.0 & TYPE_INTEGER.0 != 0 {
            s.push("integer")
        }
        if self.0 & TYPE_NULL.0 != 0 {
            s.push("null")
        }
        if self.0 & TYPE_NUMBER.0 != 0 {
            s.push("number")
        }
        if self.0 & TYPE_OBJECT.0 != 0 {
            s.push("object")
        }
        if self.0 & TYPE_STRING.0 != 0 {
            s.push("string")
        }
        s
    }
}

impl fmt::Debug for TypeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str(Vec::new()).fmt(f)
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

    Application(Application, Schema<A>),
    Validation(Validation),
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
                ptr.push_str(KW_DEF);
                ptr.push('/');
                encode_frag_ptr(&mut ptr, key);
                ptr
            }

            // In-place keywords.
            Ref(_) => ptr.add(KW_REF),
            RecursiveRef(_) => ptr.add(KW_RECURSIVE_REF),
            AnyOf { index } => {
                write!(&mut ptr, "{}/{}", KW_ANY_OF, index).unwrap();
                ptr
            }
            AllOf { index } => {
                write!(&mut ptr, "{}/{}", KW_ALL_OF, index).unwrap();
                ptr
            }
            OneOf { index } => {
                write!(&mut ptr, "{}/{}", KW_ALL_OF, index).unwrap();
                ptr
            }
            Not => ptr.add(KW_NOT),
            If => ptr.add(KW_IF),
            Then => ptr.add(KW_THEN),
            Else => ptr.add(KW_ELSE),
            DependentSchema { if_, .. } => {
                ptr.push_str(KW_DEPENDENT_SCHEMAS);
                ptr.push('/');
                encode_frag_ptr(&mut ptr, if_);
                ptr
            }

            // Property keywords.
            PropertyNames => ptr.add(KW_PROPERTY_NAMES),
            Properties { name, .. } => {
                ptr.push_str(KW_PROPERTIES);
                ptr.push('/');
                encode_frag_ptr(&mut ptr, name);
                ptr
            }
            PatternProperties { re, .. } => {
                ptr.push_str(KW_PATTERN_PROPERTIES);
                ptr.push('/');
                encode_frag_ptr(&mut ptr, re.as_str());
                ptr
            }
            AdditionalProperties => ptr.add(KW_ADDITIONAL_PROPERTIES),
            UnevaluatedProperties => ptr.add(KW_UNEVALUATED_PROPERTIES),

            // Item keywords.
            Contains => ptr.add(KW_CONTAINS),
            Items { index: None } => ptr.add(KW_ITEMS),
            Items { index: Some(i) } => {
                write!(&mut ptr, "{}/{}", KW_ITEMS, i).unwrap();
                ptr
            }
            AdditionalItems => ptr.add(KW_ADDITIONAL_ITEMS),
            UnevaluatedItems => ptr.add(KW_UNEVALUATED_ITEMS),
        }
    }
}

#[derive(Debug)]
pub enum Validation {
    False,
    Type(TypeSet),
    Const {
        hash: u64,
    },
    Enum {
        hashes: Vec<u64>,
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
    Required(intern::Set),
    DependentRequired {
        if_: String,
        if_interned: intern::Set,
        then_: intern::Set,
    },
}

const KW_ADDITIONAL_ITEMS: &str = "additionalItems";
const KW_ADDITIONAL_PROPERTIES: &str = "additionalProperties";
const KW_ALL_OF: &str = "allOf";
const KW_ANCHOR: &str = "$anchor";
const KW_ANY_OF: &str = "anyOf";
const KW_CONST: &str = "const";
const KW_CONTAINS: &str = "contains";
const KW_DEF: &str = "$defs";
const KW_DEFINITIONS: &str = "definitions";
const KW_DEPENDENT_REQUIRED: &str = "dependentRequired";
const KW_DEPENDENT_SCHEMAS: &str = "dependentSchemas";
const KW_ELSE: &str = "else";
const KW_ENUM: &str = "enum";
const KW_EXCLUSIVE_MAXIMUM: &str = "exclusiveMaximum";
const KW_EXCLUSIVE_MINIMUM: &str = "exclusiveMinimum";
const KW_ID: &str = "$id";
const KW_IF: &str = "if";
const KW_ITEMS: &str = "items";
const KW_MAXIMUM: &str = "maximum";
const KW_MAX_CONTAINS: &str = "maxContains";
const KW_MAX_ITEMS: &str = "maxItems";
const KW_MAX_LENGTH: &str = "maxLength";
const KW_MAX_PROPERTIES: &str = "maxProperties";
const KW_MINIMUM: &str = "minimum";
const KW_MIN_CONTAINS: &str = "minContains";
const KW_MIN_ITEMS: &str = "minItems";
const KW_MIN_LENGTH: &str = "minLength";
const KW_MIN_PROPERTIES: &str = "minProperties";
const KW_MULTIPLE_OF: &str = "multipleOf";
const KW_NOT: &str = "not";
const KW_ONE_OF: &str = "oneOf";
const KW_PATTERN: &str = "pattern";
const KW_PATTERN_PROPERTIES: &str = "patternProperties";
const KW_PROPERTIES: &str = "properties";
const KW_PROPERTY_NAMES: &str = "propertyNames";
const KW_RECURSIVE_ANCHOR: &str = "$recursiveAnchor";
const KW_RECURSIVE_REF: &str = "$recursiveRef";
const KW_REF: &str = "$ref";
const KW_REQUIRED: &str = "required";
const KW_THEN: &str = "then";
const KW_TYPE: &str = "type";
const KW_UNEVALUATED_ITEMS: &str = "unevaluatedItems";
const KW_UNEVALUATED_PROPERTIES: &str = "unevaluatedProperties";
const KW_UNIQUE_ITEMS: &str = "uniqueItems";

const KW_SCHEMA: &str = "$schema";
const KW_VOCABULARY: &str = "$vocabulary";
const KW_COMMENT: &str = "$comment";
const KW_FORMAT: &str = "format";

const KW_TITLE: &str = "title";
const KW_DESCRIPTION: &str = "description";
const KW_DEFAULT: &str = "default";
const KW_DEPRECATED: &str = "deprecated";
const KW_READ_ONLY: &str = "readOnly";
const KW_WRITE_ONLY: &str = "writeOnly";
const KW_EXAMPLES: &str = "examples";

/// https://url.spec.whatwg.org/#fragment-percent-encode-set
const FRAGMENT: &AsciiSet = &CONTROLS
    .add(b'%')
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'`');
