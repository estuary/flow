use lazy_static::lazy_static;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json, Value};
use validator::{Validate, ValidationError, ValidationErrors};

// This module contains types which are references to other entities
// within the catalog. They use the newtype pattern for strong type safety.

// TOKEN is a string production which allows Unicode letters and digits,
// and a *very* restricted set of other allowed punctuation symbols.
// Compare to Gazette's ValidateToken and TokenSymbols:
// https://github.com/gazette/core/blob/master/broker/protocol/validator.go#L52
const TOKEN: &'static str = r"[\p{Letter}\p{Digit}\-_\.]+";

lazy_static! {
    // TOKEN_RE is a single TOKEN component.
    static ref TOKEN_RE: Regex = Regex::new(TOKEN).unwrap();
    // CATALOG_NAME_RE is one or more TOKEN components joined by '/'.
    // It may not begin or end in a '/'.
    static ref CATALOG_NAME_RE: Regex = Regex::new(&[TOKEN, "(/", TOKEN, ")*"].concat()).unwrap();
    // CATALOG_PREFIX_RE is TOKEN components joined by '/'.
    // It may not begin with '/', but unlike CATALOG_NAME_RE it _must_ end in '/'.
    static ref CATALOG_PREFIX_RE: Regex = Regex::new( &["(", TOKEN, "/)*"].concat()).unwrap();
    // JSON_POINTER_RE matches a JSON pointer.
    static ref JSON_POINTER_RE: Regex = Regex::new("(/([^/~]|(~[01]))+)*").unwrap();
    // FREETEXT_RE allows anything except for Zl: Separator:line,
    // Zp: Separator:paragraph, or Other. Note Zs: Separator:space is allowed.
    static ref FREETEXT_RE: Regex = Regex::new(r"[^\p{Other}\p{Zl}\p{Zp}]+").unwrap();
}

macro_rules! string_reference_types {
    (
        $(#[$outer:meta])*
        $vis:vis struct $Wrapper:ident($WrapperStr:literal, pattern = $Regex:ident, example = $Example:literal);

        $($rest:tt)*
    ) => {

        $(#[$outer])*
        #[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, JsonSchema, Eq, PartialOrd, Ord, Hash)]
        #[schemars(example = "Self::example")]
        pub struct $Wrapper(#[schemars(schema_with = $WrapperStr)] String);

        impl $Wrapper {
            pub fn new(s: impl Into<String>) -> Self {
                Self(s.into())
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
            pub fn example() -> Self {
                Self($Example.into())
            }

            fn schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
                from_value(json!({
                    "type": "string",
                    "pattern": &["^", $Regex.as_str(), "$"].concat(),
                }))
                .unwrap()
            }
        }

        impl std::ops::Deref for $Wrapper {
            type Target = str;

            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $Wrapper {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Validate for $Wrapper {
            fn validate(&self) -> Result<(), ValidationErrors> {
                let s = self.0.as_ref();

                let unmatched = match $Regex.find(s) {
                    Some(m) if m.start() == 0 && m.end() == s.len() => None, // Full match.
                    Some(m) => Some([&s[..m.start()], &s[m.end()..]].concat()), // Partial match.
                    None => Some(s.to_string()), // No match.
                };

                if let Some(unmatched) = unmatched {
                    let mut errors = ValidationErrors::new();
                    errors.add(
                        "",
                        ValidationError {
                            code: "regex mismatch".into(),
                            message: None,
                            params: vec![
                                ("pattern".into(), json!($Regex.to_string())),
                                ("value".into(), json!(s)),
                                ("unmatched".into(), json!(unmatched)),
                            ]
                            .into_iter()
                            .collect(),
                        },
                    );
                    Err(errors)
                } else {
                    Ok(())
                }
            }
        }

        string_reference_types! {
            $($rest)*
        }
    };

    () => {};
}

// TODO(johnny): The "Collection::schema" literals are super-hacky,
// but were the only way I could figure to implement this using macro_rules!
// without having to resort to proc macros. The specific problem is that we
// need to pass the schema-generating function into a schemars derivation
// proc macro which expects a literal. However, use of the concat! and
// stringify! macros produce expressions: an incompatible AST token.
// There's no way with macro_rules! to literalize the production for purposes
// of passing it into another macro.

string_reference_types! {
    /// Collection names are paths of Unicode letters, numbers, '-', '_', or '.'.
    /// Each path component is separated by a slash '/',
    /// and a name may not begin or end in a '/'.
    pub struct Collection("Collection::schema", pattern = CATALOG_NAME_RE, example = "acmeCo/collection");

    /// Capture names are paths of Unicode letters, numbers, '-', '_', or '.'.
    /// Each path component is separated by a slash '/',
    /// and a name may not begin or end in a '/'.
    pub struct Capture("Capture::schema", pattern = CATALOG_NAME_RE, example = "acmeCo/capture");

    /// Materialization names are paths of Unicode letters, numbers, '-', '_', or '.'.
    /// Each path component is separated by a slash '/',
    /// and a name may not begin or end in a '/'.
    pub struct Materialization("Materialization::schema", pattern = CATALOG_NAME_RE, example = "acmeCo/materialization");

    /// Prefixes are paths of Unicode letters, numbers, '-', '_', or '.'.
    /// Each path component is separated by a slash '/'.
    /// Prefixes may not begin in a '/', but must end in one.
    pub struct Prefix("Prefix::schema", pattern = CATALOG_PREFIX_RE, example = "acmeCo/");

    /// Transform names are paths of Unicode letters, numbers, '-', '_', or '.'.
    pub struct Transform("Transform::schema", pattern = TOKEN_RE, example = "myTransform");

    /// Test names are meaningful descriptions of the test's behavior.
    pub struct Test("Test::schema", pattern = FREETEXT_RE, example = "My Test");

    /// Rules are deprecated and will be removed.
    pub struct Rule("Rule::schema", pattern = FREETEXT_RE, example = "00: Rule");

    /// JSON Pointer which identifies a location in a document.
    pub struct JsonPointer("JsonPointer::schema", pattern = JSON_POINTER_RE, example = "/json/ptr");

    /// Field names a projection of a document location.
    pub struct Field("Field::schema", pattern = TOKEN_RE, example = "my_field");
}

/// Ordered JSON-Pointers which define how a composite key may be extracted from
/// a collection document.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
#[schemars(example = "CompositeKey::example")]
pub struct CompositeKey(Vec<JsonPointer>);

impl CompositeKey {
    pub fn new(parts: impl Into<Vec<JsonPointer>>) -> Self {
        Self(parts.into())
    }
    pub fn example() -> Self {
        CompositeKey(vec![JsonPointer::example()])
    }
}

impl std::ops::Deref for CompositeKey {
    type Target = Vec<JsonPointer>;

    fn deref(&self) -> &Vec<JsonPointer> {
        &self.0
    }
}

impl Validate for CompositeKey {
    fn validate(&self) -> Result<(), ValidationErrors> {
        ValidationErrors::merge_all(
            Ok(()),
            "composite key",
            self.0.iter().map(JsonPointer::validate).collect(),
        )
    }
}

/// Object is an alias for a JSON object.
pub type Object = serde_json::Map<String, Value>;

#[cfg(test)]
mod test {
    use super::{Collection, JsonPointer, Prefix, Transform, Validate};

    #[test]
    fn test_catalog_name_re() {
        for (case, expect) in [
            ("valid", true),
            ("valid/1", true),
            ("valid/one/va_lid", true),
            ("valid-1/valid/2/th.ree", true),
            ("Приключения/Foo", true),
            ("/bad/leading/slash", false),
            ("bad/trailing/slash/", false),
            ("bad-middle//slash", false),
            ("", false),
            ("a-bad/sp ace", false),
            ("/", false),
        ] {
            let out = Collection::new(case).validate();
            if expect {
                out.unwrap();
            } else {
                out.unwrap_err();
            }
        }
    }

    #[test]
    fn test_catalog_prefix_re() {
        for (case, expect) in [
            ("valid/", true),
            ("valid/1/", true),
            ("valid/one/va_lid/", true),
            ("valid-1/valid/2/th.ree/", true),
            ("Приключения/Foo/", true),
            ("/bad/leading/slash", false),
            ("bad-middle//slash", false),
            ("", true),
            ("a-bad/sp ace/", false),
            ("/", false),
        ] {
            let out = Prefix::new(case).validate();
            if expect {
                out.unwrap();
            } else {
                out.unwrap_err();
            }
        }
    }

    #[test]
    fn test_catalog_token_re() {
        for (case, expect) in [
            ("valid", true),
            ("no/slashes", false),
            ("Прик.0_люче-ния", true),
            ("no spaces", false),
            ("", false),
            ("/", false),
        ] {
            let out = Transform::new(case).validate();
            if expect {
                out.unwrap();
            } else {
                out.unwrap_err();
            }
        }
    }

    #[test]
    fn test_json_pointer_re() {
        for (case, expect) in [
            ("/a/json/pointer", true),
            ("", true), // Document root.
            ("missing/leading/slash", false),
            ("/double//slash", false),
            ("/Прик/0/люче-ния", true),
            ("/with/esc~0ape", true),
            ("/bad/esc~ape", false),
            ("/", false),
        ] {
            let out = JsonPointer::new(case).validate();
            if expect {
                out.unwrap();
            } else {
                out.unwrap_err();
            }
        }
    }
}
