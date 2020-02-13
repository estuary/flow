use crate::schema::*;
use crate::{de, NoopWalker, Number};
use error_chain::{bail, error_chain};
use regex;
use serde_json::Value;
use url;

error_chain! {
    links {
        Intern(intern::Error, intern::ErrorKind);
    }
    foreign_links {
        Regex(::regex::Error);
        UrlParse(::url::ParseError);
    }
    errors {
        UnknownKeyword(kw: String) {
            description("unknown keyword")
            display("unknown keyword '{}'", kw)
        }
        At(curi: url::Url, kw: String) {
            description("at")
            display("at {}/{}", curi, kw)
        }
    }
}

pub trait AnnotationBuilder: Annotation {
    /// uses_keyword returns true if the builder knows how to extract
    /// an Annotation from the given keyword.
    fn uses_keyword(keyword: &str) -> bool;
    /// from_keyword builds an Annotation from the given keyword & value,
    /// which MUST be a keyword for which uses_keyword returns true.
    fn from_keyword(keyword: &str, value: &sj::Value) -> Result<Self>;
}

struct Builder<A>
where
    A: AnnotationBuilder,
{
    curi: url::Url,
    kw: Vec<Keyword<A>>,
    tbl: intern::Table,
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

    fn process_keyword(&mut self, keyword: &str, v: &sj::Value) -> Result<()> {
        let true_placeholder = sj::Value::Bool(true);
        use Application as App;
        use Validation as Val;

        Ok(match keyword {
            // Meta keywords.
            KW_ID => (), // Already handled.
            KW_RECURSIVE_ANCHOR => match v {
                sj::Value::Bool(b) if *b => self.kw.push(Keyword::RecursiveAnchor),
                sj::Value::Bool(b) if !*b => (), // Ignore.
                _ => bail!("expected a bool"),
            },
            KW_ANCHOR => match v {
                sj::Value::String(anchor) => {
                    let anchor = self.curi.join(&format!("#{}", anchor))?;
                    self.kw.push(Keyword::Anchor(anchor))
                }
                _ => bail!("expected a string"),
            },
            KW_DEF | KW_DEFINITIONS => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        self.add_application(App::Def { key: prop.clone() }, child)?;
                    }
                }
                _ => bail!("expected an object"),
            },

            // In-place application keywords.
            KW_REF => match v {
                sj::Value::String(ref_uri) => {
                    let mut ref_uri = self.curi.join(ref_uri)?;
                    if let Some("") = ref_uri.fragment() {
                        ref_uri.set_fragment(None);
                    }
                    self.add_application(App::Ref(ref_uri), &true_placeholder)?;
                }
                _ => bail!("expected a string"),
            },
            KW_RECURSIVE_REF => match v {
                sj::Value::String(ref_uri) => {
                    // Assert |ref_uri| parses correctly when joined with a base URL.
                    url::Url::parse("http://example")?.join(ref_uri)?;
                    self.add_application(App::RecursiveRef(ref_uri.clone()), &true_placeholder)?;
                }
                _ => bail!("expected a string"),
            },
            KW_ANY_OF => match v {
                sj::Value::Array(children) => {
                    for (i, child) in children.iter().enumerate() {
                        self.add_application(App::AnyOf { index: i }, child)?;
                    }
                }
                _ => bail!("expected an array"),
            },
            KW_ALL_OF => match v {
                sj::Value::Array(children) => {
                    for (i, child) in children.iter().enumerate() {
                        self.add_application(App::AllOf { index: i }, child)?;
                    }
                }
                _ => bail!("expected an array"),
            },
            KW_ONE_OF => match v {
                sj::Value::Array(children) => {
                    for (i, child) in children.iter().enumerate() {
                        self.add_application(App::OneOf { index: i }, child)?;
                    }
                }
                _ => bail!("expected an array"),
            },
            KW_NOT => self.add_application(App::Not, v)?,
            KW_IF => self.add_application(App::If, v)?,
            KW_THEN => self.add_application(App::Then, v)?,
            KW_ELSE => self.add_application(App::Else, v)?,
            KW_DEPENDENT_SCHEMAS => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        let app = App::DependentSchema {
                            if_: prop.clone(),
                            if_interned: self.tbl.intern(prop)?,
                        };
                        self.add_application(app, child)?;
                    }
                }
                _ => bail!("expected an object"),
            },

            // Property application keywords.
            KW_PROPERTY_NAMES => self.add_application(App::PropertyNames, v)?,
            KW_PROPERTIES => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        let app = App::Properties {
                            name: prop.clone(),
                            name_interned: self.tbl.intern(prop)?,
                        };
                        self.add_application(app, child)?;
                    }
                }
                _ => bail!("expected an object"),
            },
            KW_PATTERN_PROPERTIES => match v {
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
                _ => bail!("expected an object"),
            },
            KW_ADDITIONAL_PROPERTIES => self.add_application(App::AdditionalProperties, v)?,
            KW_UNEVALUATED_PROPERTIES => self.add_application(App::UnevaluatedProperties, v)?,

            // Item application keywords.
            KW_CONTAINS => self.add_application(App::Contains, v)?,
            KW_ITEMS => match v {
                sj::Value::Object(_) | sj::Value::Bool(_) => {
                    self.add_application(App::Items { index: None }, v)?
                }
                sj::Value::Array(vec) => {
                    for (i, child) in vec.iter().enumerate() {
                        self.add_application(App::Items { index: Some(i) }, child)?;
                    }
                }
                _ => bail!("expected a schema or array"),
            },
            KW_ADDITIONAL_ITEMS => self.add_application(App::AdditionalItems, v)?,
            KW_UNEVALUATED_ITEMS => self.add_application(App::UnevaluatedItems, v)?,

            // Common validation keywords.
            KW_TYPE => self.add_validation(Val::Type(extract_type_mask(v)?)),
            KW_CONST => self.add_validation(Val::Const {
                hash: extract_hash(v),
            }),
            KW_ENUM => self.add_validation(Val::Enum {
                hashes: extract_hashes(v)?,
            }),

            // String-specific validation keywords.
            KW_MAX_LENGTH => self.add_validation(Val::MaxLength(extract_usize(v)?)),
            KW_MIN_LENGTH => self.add_validation(Val::MinLength(extract_usize(v)?)),
            KW_PATTERN => self.add_validation(Val::Pattern(regex::Regex::new(extract_str(v)?)?)),
            // "format" => vals.push(Str(Format(extract_str(v)?))),

            // Number-specific validation keywords.
            KW_MULTIPLE_OF => self.add_validation(Val::MultipleOf(extract_number(v)?)),
            KW_MAXIMUM => self.add_validation(Val::Maximum(extract_number(v)?)),
            KW_EXCLUSIVE_MAXIMUM => self.add_validation(Val::ExclusiveMaximum(extract_number(v)?)),
            KW_MINIMUM => self.add_validation(Val::Minimum(extract_number(v)?)),
            KW_EXCLUSIVE_MINIMUM => self.add_validation(Val::ExclusiveMinimum(extract_number(v)?)),

            // Array-specific validation keywords.
            KW_MAX_ITEMS => self.add_validation(Val::MaxItems(extract_usize(v)?)),
            KW_MIN_ITEMS => self.add_validation(Val::MinItems(extract_usize(v)?)),
            KW_UNIQUE_ITEMS => match v {
                sj::Value::Bool(true) => self.add_validation(Val::UniqueItems),
                sj::Value::Bool(false) => (),
                _ => bail!("expected a bool"),
            },
            KW_MAX_CONTAINS => self.add_validation(Val::MaxContains(extract_usize(v)?)),
            KW_MIN_CONTAINS => self.add_validation(Val::MinContains(extract_usize(v)?)),

            // Object-specific validation keywords.
            KW_MAX_PROPERTIES => self.add_validation(Val::MaxProperties(extract_usize(v)?)),
            KW_MIN_PROPERTIES => self.add_validation(Val::MinProperties(extract_usize(v)?)),
            KW_REQUIRED => {
                let set = extract_intern_set(&mut self.tbl, v)?;
                self.add_validation(Val::Required(set))
            }
            KW_DEPENDENT_REQUIRED => match v {
                sj::Value::Object(m) => {
                    for (prop, child) in m {
                        let dr = Val::DependentRequired {
                            if_: prop.clone(),
                            if_interned: self.tbl.intern(prop)?,
                            then_: extract_intern_set(&mut self.tbl, child)?,
                        };
                        self.add_validation(dr);
                    }
                }
                _ => bail!("expected an object"),
            },

            KW_SCHEMA | KW_VOCABULARY | KW_COMMENT => (), // Ignored.
            KW_FORMAT => (),                              // Ignored.

            // This is not a core validation keyword. Does the AnnotationBuilder consume it?
            _ => {
                if A::uses_keyword(keyword) {
                    self.add_annotation(A::from_keyword(keyword, v)?);
                } else {
                    return Err(ErrorKind::UnknownKeyword(keyword.to_owned()).into());
                }
            }
        })
    }

    fn add_annotation(&mut self, annot: A) {
        self.kw.push(Keyword::Annotation(annot))
    }
    fn add_validation(&mut self, val: Validation) {
        self.kw.push(Keyword::Validation(val))
    }

    // build_app builds a child of the current Builder schema,
    // wrapped in an a Keyword::Application.
    fn add_application(&mut self, app: Application, child: &sj::Value) -> Result<()> {
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
pub fn build_schema<A>(curi: url::Url, v: &sj::Value) -> Result<Schema<A>>
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
        _ => bail!("expected a schema (object or bool)"),
    };

    // This is a schema object. We'll walk its properties and JSON values
    // to extract its applications and validations.

    let curi = build_curi(curi, obj.get(KW_ID))?;
    let mut builder = Builder { curi, kw, tbl };

    for (k, v) in obj {
        if let Err(e) = builder.process_keyword(k, v) {
            match e.kind() {
                ErrorKind::At(_, _) => return Err(e),
                _ => return Err(e).chain_err(|| ErrorKind::At(builder.curi.clone(), k.to_owned())),
            }
        }
    }

    Ok(builder.build())
}

fn build_curi(curi: url::Url, id: Option<&sj::Value>) -> Result<url::Url> {
    let curi = match id {
        Some(sj::Value::String(id)) => {
            let curi = curi.join(id)?;

            if let Some(f) = curi.fragment() {
                bail!("unexpected fragment component '{}' of $id", f);
            }
            curi
        }
        None => curi,
        Some(_) => bail!("expected a string"),
    };
    if curi.cannot_be_a_base() {
        bail!("expected a base URI");
    }
    Ok(curi)
}

fn extract_type_mask(v: &sj::Value) -> Result<TypeSet> {
    let mut set = TYPE_INVALID;

    let mut fold = |vv: &sj::Value| -> Result<()> {
        use sj::Value::String as S;

        set = set
            | match vv {
                S(s) if s == "array" => TYPE_ARRAY,
                S(s) if s == "boolean" => TYPE_BOOLEAN,
                S(s) if s == "integer" => TYPE_INTEGER,
                S(s) if s == "null" => TYPE_NULL,
                S(s) if s == "number" => TYPE_NUMBER,
                S(s) if s == "object" => TYPE_OBJECT,
                S(s) if s == "string" => TYPE_STRING,
                _ => bail!("expected type string"),
            };
        Ok(())
    };

    match v {
        sj::Value::Array(vec) => {
            for vv in vec {
                fold(vv)?
            }
        }
        sj::Value::String(_) => fold(v)?,
        _ => bail!("expected type string, or array of type strings"),
    }
    Ok(set)
}

fn extract_hash(v: &sj::Value) -> u64 {
    let mut walker = NoopWalker;
    let span = de::walk(v, &mut walker).unwrap();
    span.hashed
}

fn extract_hashes(v: &sj::Value) -> Result<Vec<u64>> {
    let arr = match v {
        sj::Value::Array(arr) => arr,
        _ => bail!("expected array"),
    };
    Ok(arr.iter().map(|v| extract_hash(v)).collect())
}

fn extract_usize(v: &sj::Value) -> Result<usize> {
    match v {
        sj::Value::Number(num) if num.is_u64() => Ok(num.as_u64().unwrap() as usize),
        _ => bail!("expected unsigned integer"),
    }
}

fn extract_str(v: &sj::Value) -> Result<&str> {
    match v {
        sj::Value::String(s) => Ok(s),
        _ => bail!("expected string"),
    }
}

fn extract_bool(v: &sj::Value) -> Result<bool> {
    match v {
        sj::Value::Bool(b) => Ok(*b),
        _ => bail!("expected bool"),
    }
}

fn extract_number(v: &sj::Value) -> Result<Number> {
    match v {
        sj::Value::Number(num) if num.is_u64() => Ok(Number::Unsigned(num.as_u64().unwrap())),
        sj::Value::Number(num) if num.is_i64() => Ok(Number::Signed(num.as_i64().unwrap())),
        sj::Value::Number(num) => Ok(Number::Float(num.as_f64().unwrap())),
        _ => bail!("expected number"),
    }
}

fn extract_intern_set(tbl: &mut intern::Table, v: &sj::Value) -> Result<intern::Set> {
    match v {
        sj::Value::Array(vec) => {
            let mut out: intern::Set = 0;

            for item in vec {
                out |= tbl.intern(extract_str(item)?)?;
            }
            Ok(out)
        }
        _ => bail!("expected array of strings"),
    }
}

impl AnnotationBuilder for CoreAnnotation {
    fn uses_keyword(kw: &str) -> bool {
        match kw {
            KW_TITLE | KW_DESCRIPTION | KW_DEFAULT | KW_DEPRECATED | KW_READ_ONLY
            | KW_WRITE_ONLY | KW_EXAMPLES => true,
            _ => false,
        }
    }

    fn from_keyword(kw: &str, v: &Value) -> Result<Self> {
        Ok(match kw {
            KW_TITLE => CoreAnnotation::Title(extract_str(v)?.to_owned()),
            KW_DESCRIPTION => CoreAnnotation::Description(extract_str(v)?.to_owned()),
            KW_DEFAULT => CoreAnnotation::Default(v.clone()),
            KW_DEPRECATED => CoreAnnotation::Deprecated(extract_bool(v)?),
            KW_READ_ONLY => CoreAnnotation::ReadOnly(extract_bool(v)?),
            KW_WRITE_ONLY => CoreAnnotation::WriteOnly(extract_bool(v)?),
            KW_EXAMPLES => CoreAnnotation::Examples(
                match v {
                    sj::Value::Array(v) => v,
                    _ => bail!("expected array"),
                }
                .clone(),
            ),
            _ => panic!("unexpected keyword: '{}'", kw),
        })
    }
}
