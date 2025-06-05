use crate::schema::{
    intern, keywords, types, Annotation, Application, CoreAnnotation, HashedLiteral, Keyword,
    Schema, Validation,
};
use crate::{de, NoopWalker, Number};
use itertools::Itertools;
use serde::Deserialize;
use serde_json as sj;
use thiserror;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("expected a boolean")]
    ExpectedBool,
    #[error("expected a string")]
    ExpectedString,
    #[error("expected an object")]
    ExpectedObject,
    #[error("expected an array")]
    ExpectedArray,
    #[error("expected a schema or array of schemas")]
    ExpectedSchemaOrArrayOfSchemas,
    #[error("expected a schema")]
    ExpectedSchema,
    #[error("unexpected fragment component '{0}' of $id keyword")]
    UnexpectedFragment(String),
    #[error("expected a type or array of types: {0}")]
    ExpectedType(sj::Error),
    #[error("expected an unsigned integer")]
    ExpectedUnsigned,
    #[error("expected a number")]
    ExpectedNumber,
    #[error("expected an array of strings")]
    ExpectedStringArray,
    #[error("expected '{0}' to be a base URI")]
    ExpectedBaseURI(url::Url),
    #[error("unexpected keyword '{0}'")]
    UnknownKeyword(String),
    #[error("failed to intern property: {0}")]
    InternErr(#[from] intern::Error),
    #[error("failed to parse URL: {0}")]
    URLErr(#[from] url::ParseError),
    #[error("failed to parse regex: {0}")]
    RegexErr(#[from] regex::Error),
    #[error("failed to build annotation: {0}")]
    AnnotationErr(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error(transparent)]
    FormatErr(#[from] serde_json::Error),

    #[error("at schema '{curi}': {detail}")]
    AtSchema { curi: url::Url, detail: Box<Error> },
    #[error("at keyword '{keyword}' of schema '{curi}': {detail}")]
    AtKeyword {
        curi: url::Url,
        detail: Box<Error>,
        keyword: String,
    },
}
use Error::*;

pub trait AnnotationBuilder: Annotation {
    /// uses_keyword returns true if the builder knows how to extract
    /// an Annotation from the given keyword.
    fn uses_keyword(keyword: &str) -> bool;
    /// from_keyword builds an Annotation from the given keyword & value,
    /// which MUST be a keyword for which uses_keyword returns true.
    fn from_keyword(keyword: &str, value: &sj::Value) -> Result<Self, Error>;
}

struct Builder<A>
where
    A: AnnotationBuilder,
{
    curi: url::Url,
    kw: Vec<Keyword<A>>,
    tbl: intern::Table,

    // "nullable" support for OpenAPI schemas prior to version 3.1,
    // which are still prevelant as of Sept 2021.
    nullable: bool,
}

impl<A> Builder<A>
where
    A: AnnotationBuilder,
{
    fn build(mut self) -> Schema<A> {
        // Special-case: the presence of a "contains" application implies the
        // semantics of {"minContains": 1}, if a MinContains validation is not
        // otherwise specified.
        let (has_contains, has_min) = self.kw.iter().fold((false, false), |(c, m), kw| match kw {
            Keyword::Application(Application::Contains, _) => (true, m),
            Keyword::Validation(Validation::MinContains(_)) => (c, true),
            _ => (c, m),
        });
        if has_contains && !has_min {
            self.kw
                .push(Keyword::Validation(Validation::MinContains(1)));
        } else if !has_contains {
            // The spec explicitly says to ignore minContains and maxContains if the schema
            // does not include the "contains" keyword, so we remove those here if that's the case
            self.kw.retain(|kw| match kw {
                Keyword::Validation(Validation::MinContains(_)) => false,
                Keyword::Validation(Validation::MaxContains(_)) => false,
                _ => true,
            })
        }

        self.tbl.freeze();

        self.kw.sort_unstable_by_key(|kw| -> u32 {
            use Application as A;
            use Keyword as K;

            match kw {
                // $recursiveAnchor updates the current dynamic base URI before other keywords apply.
                K::RecursiveAnchor => 0,

                // Properties / PatternProperties conditions whether AdditionalProperties applies.
                K::Application(A::Properties { .. }, _) => 2,
                K::Application(A::PatternProperties { .. }, _) => 3,
                // AdditionalProperties also conditions whether UnevaluatedProperties applies.
                K::Application(A::AdditionalProperties, _) => 4,
                // UnevaluatedProperties is evaluated last.

                // Contains is always applied. Items conditions whether AdditionalItems applies.
                K::Application(A::Contains, _) => 5,
                K::Application(A::Items { .. }, _) => 6,
                // AdditionalItems also conditions whether UnevaluatedItems applies.
                K::Application(A::AdditionalItems, _) => 7,
                // UnevaluatedItems is evaluated last.

                // When unwinding applications, we want to know which branch was taken before
                // we examine branch results.
                K::Application(A::Else, _) => 8,
                K::Application(A::Then, _) => 9,
                K::Application(A::If, _) => 10,

                _ => 100,
            }
        });

        Schema {
            curi: self.curi,
            kw: self.kw,
            tbl: self.tbl,
        }
    }

    fn process_keyword(&mut self, keyword: &str, v: &sj::Value) -> Result<(), Error> {
        use Application as App;
        use Validation as Val;

        let true_placeholder = sj::Value::Bool(true);

        let mut unknown = false;
        match keyword {
            // Already handled outside of this match.
            keywords::ID => (),
            keywords::NULLABLE => (),

            // Meta keywords.
            keywords::RECURSIVE_ANCHOR => match v {
                sj::Value::Bool(b) if *b => self.kw.push(Keyword::RecursiveAnchor),
                sj::Value::Bool(b) if !*b => (), // Ignore.
                _ => return Err(ExpectedBool),
            },
            keywords::ANCHOR => match v {
                sj::Value::String(anchor) => {
                    let anchor = self.curi.join(&format!("#{}", anchor))?;
                    self.kw.push(Keyword::Anchor(anchor))
                }
                _ => return Err(ExpectedString),
            },
            keywords::DEF => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        self.add_application(App::Def { key: prop.clone() }, child)?;
                    }
                }
                _ => return Err(ExpectedObject),
            },
            keywords::DEFINITIONS => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        self.add_application(App::Definition { key: prop.clone() }, child)?;
                    }
                }
                _ => return Err(ExpectedObject),
            },

            // In-place application keywords.
            keywords::REF => match v {
                sj::Value::String(ref_uri) => {
                    let mut ref_uri = self.curi.join(ref_uri)?;
                    if let Some("") = ref_uri.fragment() {
                        ref_uri.set_fragment(None);
                    }
                    self.add_application(App::Ref(ref_uri), &true_placeholder)?;
                }
                _ => return Err(ExpectedString),
            },
            keywords::RECURSIVE_REF => match v {
                sj::Value::String(ref_uri) => {
                    // Assert |ref_uri| parses correctly when joined with a base URL.
                    url::Url::parse("http://example")?.join(ref_uri)?;
                    self.add_application(App::RecursiveRef(ref_uri.clone()), &true_placeholder)?;
                }
                _ => return Err(ExpectedString),
            },
            keywords::ANY_OF => match v {
                sj::Value::Array(children) => {
                    for (i, child) in children.iter().enumerate() {
                        self.add_application(App::AnyOf { index: i }, child)?;
                    }
                }
                _ => return Err(ExpectedArray),
            },
            keywords::ALL_OF => match v {
                sj::Value::Array(children) => {
                    for (i, child) in children.iter().enumerate() {
                        self.add_application(App::AllOf { index: i }, child)?;
                    }
                }
                _ => return Err(ExpectedArray),
            },
            keywords::ONE_OF => match v {
                sj::Value::Array(children) => {
                    for (i, child) in children.iter().enumerate() {
                        self.add_application(App::OneOf { index: i }, child)?;
                    }
                }
                _ => return Err(ExpectedArray),
            },
            keywords::NOT => self.add_application(App::Not, v)?,
            keywords::IF => self.add_application(App::If, v)?,
            keywords::THEN => self.add_application(App::Then, v)?,
            keywords::ELSE => self.add_application(App::Else, v)?,
            keywords::DEPENDENT_SCHEMAS => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        let app = App::DependentSchema {
                            if_: prop.clone(),
                            if_interned: self.tbl.intern(prop)?,
                        };
                        self.add_application(app, child)?;
                    }
                }
                _ => return Err(ExpectedObject),
            },

            // Property application keywords.
            keywords::PROPERTY_NAMES => self.add_application(App::PropertyNames, v)?,
            keywords::PROPERTIES => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        let app = App::Properties { name: prop.clone() };
                        self.add_application(app, child)?;
                    }
                }
                _ => return Err(ExpectedObject),
            },
            keywords::PATTERN_PROPERTIES => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        self.add_application(
                            App::PatternProperties {
                                re: regex::Regex::new(prop)?,
                            },
                            child,
                        )?;
                    }
                }
                _ => return Err(ExpectedObject),
            },
            keywords::ADDITIONAL_PROPERTIES => {
                self.add_application(App::AdditionalProperties, v)?
            }
            keywords::UNEVALUATED_PROPERTIES => {
                self.add_application(App::UnevaluatedProperties, v)?
            }

            // Item application keywords.
            keywords::CONTAINS => self.add_application(App::Contains, v)?,
            keywords::ITEMS => match v {
                sj::Value::Object(_) | sj::Value::Bool(_) => {
                    self.add_application(App::Items { index: None }, v)?
                }
                sj::Value::Array(vec) => {
                    for (i, child) in vec.iter().enumerate() {
                        self.add_application(App::Items { index: Some(i) }, child)?;
                    }
                }
                _ => return Err(ExpectedSchemaOrArrayOfSchemas),
            },
            keywords::ADDITIONAL_ITEMS => self.add_application(App::AdditionalItems, v)?,
            keywords::UNEVALUATED_ITEMS => self.add_application(App::UnevaluatedItems, v)?,

            // Common validation keywords.
            keywords::TYPE => {
                // As a support crutch for OpenAPI versions prior to 3.1,
                // merge a "nullable" keyword into the "type" keyword.
                let nullable = if self.nullable {
                    types::NULL
                } else {
                    types::INVALID
                };
                let actual = types::Set::deserialize(v).map_err(|e| ExpectedType(e))?;

                self.add_validation(Val::Type(nullable | actual))
            }
            keywords::CONST => self.add_validation(Val::Const(extract_hash(v))),
            keywords::ENUM => self.add_validation(Val::Enum {
                variants: extract_hashes(v)?,
            }),

            // String-specific validation keywords.
            keywords::MAX_LENGTH => self.add_validation(Val::MaxLength(extract_usize(v)?)),
            keywords::MIN_LENGTH => self.add_validation(Val::MinLength(extract_usize(v)?)),
            keywords::PATTERN => {
                self.add_validation(Val::Pattern(regex::Regex::new(extract_str(v)?)?))
            }

            // Number-specific validation keywords.
            keywords::MULTIPLE_OF => self.add_validation(Val::MultipleOf(extract_number(v)?)),
            keywords::MAXIMUM => self.add_validation(Val::Maximum(extract_number(v)?)),
            keywords::EXCLUSIVE_MAXIMUM => {
                self.add_validation(Val::ExclusiveMaximum(extract_number(v)?))
            }
            keywords::MINIMUM => self.add_validation(Val::Minimum(extract_number(v)?)),
            keywords::EXCLUSIVE_MINIMUM => {
                self.add_validation(Val::ExclusiveMinimum(extract_number(v)?))
            }

            // Array-specific validation keywords.
            keywords::MAX_ITEMS => self.add_validation(Val::MaxItems(extract_usize(v)?)),
            keywords::MIN_ITEMS => self.add_validation(Val::MinItems(extract_usize(v)?)),
            keywords::UNIQUE_ITEMS => match v {
                sj::Value::Bool(true) => self.add_validation(Val::UniqueItems),
                sj::Value::Bool(false) => (),
                _ => return Err(ExpectedBool),
            },
            keywords::MAX_CONTAINS => self.add_validation(Val::MaxContains(extract_usize(v)?)),
            keywords::MIN_CONTAINS => self.add_validation(Val::MinContains(extract_usize(v)?)),

            // Object-specific validation keywords.
            keywords::MAX_PROPERTIES => self.add_validation(Val::MaxProperties(extract_usize(v)?)),
            keywords::MIN_PROPERTIES => self.add_validation(Val::MinProperties(extract_usize(v)?)),
            keywords::REQUIRED => panic!("`required` keyword should use process_required"),
            keywords::DEPENDENT_REQUIRED => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        let (then_set, then_props) = match child {
                            sj::Value::Array(vec) => extract_intern_set(&mut self.tbl, vec.iter())?,
                            _ => return Err(ExpectedStringArray),
                        };

                        let dr = Val::DependentRequired {
                            if_: prop.clone(),
                            if_interned: self.tbl.intern(prop)?,
                            then_: then_props,
                            then_interned: then_set,
                        };
                        self.add_validation(dr);
                    }
                }
                _ => return Err(ExpectedObject),
            },
            keywords::FORMAT => self.add_validation(Val::Format(
                serde_json::from_value(v.clone()).map_err(|err| Error::FormatErr(err))?,
            )),

            keywords::SCHEMA | keywords::VOCABULARY | keywords::COMMENT => (), // Ignored.

            // This is not a core validation keyword. Does the AnnotationBuilder consume it?
            _ => {
                unknown = true;
            }
        };

        if A::uses_keyword(keyword) {
            self.kw
                .push(Keyword::Annotation(A::from_keyword(keyword, v)?));
            unknown = false;
        }

        if unknown {
            Err(UnknownKeyword(keyword.to_owned()).into())
        } else {
            Ok(())
        }
    }

    fn process_required(&mut self, child: &sj::Value) -> Result<(), Error> {
        let vec = match child {
            sj::Value::Array(vec) => vec,
            _ => return Err(ExpectedStringArray),
        };

        // Split |vec| into a |head| which will fit within the remaining intern
        // table space, and a |tail| which is chunked by the maximum table size
        // and pushed down into inline schemas.
        let (head, tail) = vec.split_at(vec.len().min(self.tbl.remaining()));

        let (set, props) = extract_intern_set(&mut self.tbl, head.iter())?;
        self.add_validation(Validation::Required {
            props,
            props_interned: set,
        });

        for chunk in tail.iter().chunks(intern::MAX_TABLE_SIZE).into_iter() {
            let vec: Vec<_> = chunk.map(|v| v.clone()).collect();
            self.add_application(Application::Inline, &sj::json!({ "required": vec }))?;
        }
        Ok(())
    }

    fn add_validation(&mut self, val: Validation) {
        self.kw.push(Keyword::Validation(val))
    }

    // build_app builds a child of the current Builder schema,
    // wrapped in an a Keyword::Application.
    fn add_application(&mut self, app: Application, child: &sj::Value) -> Result<(), Error> {
        // Init a fragment pointer for the schema of this application.
        let mut ptr = "#".to_string();
        // Extend with path of this *this* schema, the application's parent.
        if let Some(f) = self.curi.fragment() {
            ptr.push_str(f);
        }
        // Then add pointer components from the application itself.
        let ptr = app.extend_fragment_pointer(ptr);
        // Finally build the complete lexical URI of the child.
        // Note that it could still override with it's own $id keyword.
        let child_uri = self.curi.join(ptr.as_str()).unwrap();

        let child = build_schema(child_uri, child)?;
        self.kw.push(Keyword::Application(app, child));

        Ok(())
    }
}

/// `build_schema` builds a Schema instance from a JSON-Schema document.
pub fn build_schema<A>(curi: url::Url, v: &sj::Value) -> Result<Schema<A>, Error>
where
    A: AnnotationBuilder,
{
    let mut kw = Vec::new();
    let tbl = intern::Table::new();

    let obj = match v {
        // Hoist map to outer scope if schema is a JSON object.
        sj::Value::Object(m) => m,

        // If schema is a JSON bool, early-return an empty Schema (if true)
        // or a schema with a lone False validation (if false).
        sj::Value::Bool(b) => {
            if !b {
                kw.push(Keyword::Validation(Validation::False));
            }
            return Ok(Schema { curi, tbl, kw });
        }
        _ => {
            return Err(AtSchema {
                detail: Box::new(ExpectedSchema),
                curi,
            })
        }
    };

    // This is a schema object. We'll walk its properties and JSON values
    // to extract its applications and validations.

    let mut builder = Builder {
        curi: build_curi(curi, obj.get(keywords::ID))?,
        kw,
        tbl,
        nullable: obj
            .get(keywords::NULLABLE)
            .and_then(|n| n.as_bool())
            .unwrap_or_default(),
    };

    let mapped_err = |err: Error, curi: &url::Url, keyword: &str| match err {
        // Pass through errors that have already been located.
        AtSchema { .. } | AtKeyword { .. } => err,
        // Otherwise, wrap error with its keyword location.
        _ => {
            return AtKeyword {
                detail: Box::new(err),
                curi: curi.clone(),
                keyword: keyword.to_string(),
            }
        }
    };

    let mut required = None;
    for (k, v) in obj {
        if k == keywords::REQUIRED {
            required = Some(v);
            continue;
        }
        builder
            .process_keyword(k, v)
            .map_err(|e| mapped_err(e, &builder.curi, k))?;
    }

    // Process `required` last, once we know how much intern table space remains.
    if let Some(required) = required {
        builder
            .process_required(required)
            .map_err(|e| mapped_err(e, &builder.curi, keywords::REQUIRED))?;
    }

    Ok(builder.build())
}

fn build_curi(curi: url::Url, id: Option<&sj::Value>) -> Result<url::Url, Error> {
    let curi = match id {
        Some(sj::Value::String(id)) => {
            let curi = curi.join(id)?;

            if let Some(f) = curi.fragment() {
                return Err(UnexpectedFragment(f.to_owned()));
            }
            curi
        }
        None => curi,
        Some(_) => return Err(ExpectedString),
    };
    if curi.cannot_be_a_base() {
        return Err(ExpectedBaseURI(curi));
    }
    Ok(curi)
}

fn extract_hash(v: &sj::Value) -> HashedLiteral {
    let mut walker = NoopWalker;
    let span = de::walk(v, &mut walker).unwrap();
    HashedLiteral {
        hash: span.hashed,
        value: v.clone(),
    }
}

fn extract_hashes(v: &sj::Value) -> Result<Vec<HashedLiteral>, Error> {
    let arr = match v {
        sj::Value::Array(arr) => arr,
        _ => return Err(ExpectedArray),
    };
    Ok(arr.iter().map(|v| extract_hash(v)).collect())
}

fn extract_usize(v: &sj::Value) -> Result<usize, Error> {
    match v {
        sj::Value::Number(num) if num.is_u64() => Ok(num.as_u64().unwrap() as usize),
        _ => return Err(ExpectedUnsigned),
    }
}

fn extract_str(v: &sj::Value) -> Result<&str, Error> {
    match v {
        sj::Value::String(s) => Ok(s),
        _ => return Err(ExpectedString),
    }
}

fn extract_bool(v: &sj::Value) -> Result<bool, Error> {
    match v {
        sj::Value::Bool(b) => Ok(*b),
        _ => return Err(ExpectedBool),
    }
}

fn extract_number(v: &sj::Value) -> Result<Number, Error> {
    match v {
        sj::Value::Number(num) if num.is_u64() => Ok(Number::Unsigned(num.as_u64().unwrap())),
        sj::Value::Number(num) if num.is_i64() => Ok(Number::Signed(num.as_i64().unwrap())),
        sj::Value::Number(num) => Ok(Number::Float(num.as_f64().unwrap())),
        _ => return Err(ExpectedNumber),
    }
}

fn extract_intern_set<'a>(
    tbl: &mut intern::Table,
    vec: impl Iterator<Item = &'a sj::Value>,
) -> Result<(intern::Set, Vec<String>), Error> {
    let mut set: intern::Set = 0;
    let mut props = Vec::new();

    for item in vec {
        let prop = extract_str(item)?;
        set |= tbl.intern(extract_str(item)?)?;
        props.push(prop.to_owned());
    }
    Ok((set, props))
}

impl AnnotationBuilder for CoreAnnotation {
    fn uses_keyword(kw: &str) -> bool {
        match kw {
            keywords::CONTENT_ENCODING
            | keywords::CONTENT_MEDIA_TYPE
            | keywords::FORMAT
            | keywords::DEFAULT
            | keywords::DEPRECATED
            | keywords::DESCRIPTION
            | keywords::EXAMPLE
            | keywords::EXAMPLES
            | keywords::READ_ONLY
            | keywords::TITLE
            | keywords::WRITE_ONLY => true,
            _ => false,
        }
    }

    fn from_keyword(kw: &str, v: &sj::Value) -> Result<Self, Error> {
        Ok(match kw {
            keywords::CONTENT_ENCODING => {
                CoreAnnotation::ContentEncoding(extract_str(v)?.to_owned())
            }
            keywords::CONTENT_MEDIA_TYPE => {
                CoreAnnotation::ContentMediaType(extract_str(v)?.to_owned())
            }
            keywords::FORMAT => CoreAnnotation::Format(
                serde_json::from_value(v.clone()).map_err(|err| Error::FormatErr(err))?,
            ),
            keywords::DEFAULT => CoreAnnotation::Default(v.clone()),
            keywords::DEPRECATED => CoreAnnotation::Deprecated(extract_bool(v)?),
            keywords::DESCRIPTION => CoreAnnotation::Description(extract_str(v)?.to_owned()),
            keywords::EXAMPLE => CoreAnnotation::Examples(vec![v.clone()]),
            keywords::EXAMPLES => CoreAnnotation::Examples(
                match v {
                    sj::Value::Array(v) => v,
                    _ => return Err(ExpectedArray),
                }
                .clone(),
            ),
            keywords::READ_ONLY => CoreAnnotation::ReadOnly(extract_bool(v)?),
            keywords::TITLE => CoreAnnotation::Title(extract_str(v)?.to_owned()),
            keywords::WRITE_ONLY => CoreAnnotation::WriteOnly(extract_bool(v)?),
            _ => panic!("unexpected keyword: '{}'", kw),
        })
    }
}

#[cfg(test)]
mod test {
    use super::{super::build::build_schema, super::CoreAnnotation};
    use crate::schema::{intern, Application, Keyword, Validation};

    #[test]
    fn test_required_splits_into_inline_schemas() {
        let do_test = |fill_to: usize| {
            // Our fixture under test uses seven properties other than those of `required`.
            let required: Vec<_> = (0..fill_to).map(|i| i.to_string()).collect();

            let schema = serde_json::json!({
                "type": "object",
                "dependentRequired": {
                    "foo": ["bar", "baz"],
                    "bar": ["baz"],
                },
                "dependentSchemas": {
                    "bing": true,
                    "quark": {},
                    "baz": {},
                },
                "properties": {
                    "these-props": true,
                    "dont-get-interned": false,
                },
                "required": required,
            });

            let curi = url::Url::parse("http://example/schema").unwrap();
            let schema = build_schema::<CoreAnnotation>(curi, &schema).unwrap();

            let mut saw_required = false;
            let mut total_inline = 0;
            let mut total_required = 0;

            for kw in &schema.kw {
                match kw {
                    Keyword::Application(Application::Inline, schema) => {
                        assert_eq!(schema.kw.len(), 1, "{:?}", &schema.kw);
                        match &schema.kw[0] {
                            Keyword::Validation(Validation::Required { props, .. }) => {
                                total_required += props.len();
                            }
                            kw => panic!("unexpected inline keyword: {:?}", kw),
                        }
                        total_inline += 1;
                    }
                    Keyword::Validation(Validation::Required { props, .. }) => {
                        total_required += props.len();
                        saw_required = true;
                    }
                    _ => {}
                }
            }
            assert!(saw_required);
            assert_eq!(total_required, fill_to);

            (schema.tbl.len(), total_inline)
        };

        assert_eq!(do_test(0), (5, 0));
        assert_eq!(do_test(3), (8, 0));
        assert_eq!(
            do_test(intern::MAX_TABLE_SIZE - 5),
            (intern::MAX_TABLE_SIZE, 0)
        );
        assert_eq!(
            do_test(intern::MAX_TABLE_SIZE * 3 - 5),
            (intern::MAX_TABLE_SIZE, 2)
        );
        assert_eq!(
            do_test(intern::MAX_TABLE_SIZE * 3 - 4),
            (intern::MAX_TABLE_SIZE, 3)
        );
    }
}
