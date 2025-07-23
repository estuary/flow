use lazy_static::lazy_static;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, json};
use std::fmt;
use validator::{Validate, ValidationError, ValidationErrors};

// This module contains types which are references to other entities
// within the catalog. They use the newtype pattern for strong type safety.

// TOKEN_CHAR is a string production which allows Unicode letters and numbers,
// and a *very* restricted set of other allowed punctuation symbols.
// Compare to Gazette's ValidateToken and TokenSymbols:
// https://github.com/gazette/core/blob/master/broker/protocol/validator.go#L52
const TOKEN_CHAR: &'static str = r"\p{Letter}\p{Number}\-_\.";
// SPACE_CHAR is a space character.
// TODO(johnny): this ought to be \p{Z} rather than ' '.
const SPACE_CHAR: &'static str = r" ";
// JSON_POINTER_CHAR are characters allowed to participate in
// JSON pointers, subject to its escaping rules.
const JSON_POINTER_CHAR: &'static str = r"([^/~]|(~[01]))";

lazy_static! {
    // TOKEN is one or more TOKEN_CHARs.
    static ref TOKEN: String = ["[", TOKEN_CHAR, "]+"].concat();
    // TOKEN_RE is a single TOKEN component.
    pub static ref TOKEN_RE: Regex = Regex::new(&TOKEN).unwrap();
    // CATALOG_NAME_RE is one or more TOKEN components joined by '/'.
    // It may not begin or end in a '/'.
    static ref CATALOG_NAME_RE: Regex = Regex::new(&[&TOKEN, "(/", &TOKEN, ")*"].concat()).unwrap();
    // CATALOG_PREFIX_RE is TOKEN components joined by '/'.
    // It may not begin with '/', but unlike CATALOG_NAME_RE it _must_ end in '/'.
    pub static ref CATALOG_PREFIX_RE: Regex = Regex::new( &["(", &TOKEN, "/)*"].concat()).unwrap();
    // JSON_POINTER_RE matches a JSON pointer.
    static ref JSON_POINTER_RE: Regex = Regex::new(&["(/", &JSON_POINTER_CHAR, "+)*"].concat()).unwrap();
    // FIELD_RE is like a JSON_POINTER_RE, but doesn't require a leading '/'.
    static ref FIELD_RE: Regex = Regex::new(&[&JSON_POINTER_CHAR, "+(/", &JSON_POINTER_CHAR, "+)*"].concat()).unwrap();
    // RELATIVE_URL_RE matches a relative or absolute URL. It's quite permissive, prohibiting only a space.
    static ref RELATIVE_URL_RE: Regex = Regex::new(&["[^", &SPACE_CHAR, "]+"].concat()).unwrap();
    static ref ENDPOINT_RE: Regex = Regex::new(r#"^(http://|https://)?[a-z0-9]+[a-z0-9\.:-]*[a-z0-9]+"#).unwrap();
}

macro_rules! string_reference_types {
    (
        $(#[$outer:meta])*
        $vis:vis struct $Wrapper:ident($WrapperStr:literal, pattern = $Regex:ident, example = $Example:literal);

        $($rest:tt)*
    ) => {

        $(#[$outer])*
        #[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, JsonSchema, Eq, PartialOrd, Ord, Hash)]
        #[cfg_attr(feature = "sqlx-support", derive(sqlx::Decode, sqlx::Encode))]
        #[schemars(example = Self::example())]
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
            pub fn schema_pattern() -> String {
                ["^", $Regex.as_str(), "$"].concat()
            }
            pub fn regex() -> &'static Regex {
                &$Regex
            }

            pub fn as_mut_string(&mut self) -> &mut String {
                &mut self.0
            }

            fn schema(_: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
                from_value(json!({
                    "type": "string",
                    "pattern": Self::schema_pattern(),
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

        impl Into<String> for $Wrapper {
            fn into(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $Wrapper {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(self.as_ref())
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
                            message: Some(format!(
                                "{} doesn't match pattern {} (unmatched portion is: {})",
                                s, $Regex.to_string(), unmatched,
                            ).into()),
                            params: std::collections::HashMap::new(),
                        },
                    );
                    Err(errors)
                } else {
                    Ok(())
                }
            }
        }

        #[cfg(feature = "sqlx-support")]
        impl sqlx::Type<sqlx::Postgres> for $Wrapper {
            fn type_info() -> <sqlx::Postgres as sqlx::Database>::TypeInfo {
                <String as sqlx::Type<sqlx::Postgres>>::type_info()
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
    /// Token is Unicode letters, numbers, '-', '_', or '.'.
    pub struct Token("Token::schema", pattern = TOKEN_RE, example = "token");

    /// Catalog Name is a series of tokens separated by a forward slash.
    pub struct Name("Name::schema", pattern = CATALOG_NAME_RE, example = "acmeCo/name");

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
    pub struct Prefix("Prefix::schema", pattern = CATALOG_PREFIX_RE, example = "acmeCo/widgets/");

    /// Transform names are Unicode letters, numbers, '-', '_', or '.'.
    pub struct Transform("Transform::schema", pattern = TOKEN_RE, example = "myTransform");

    /// Test names are paths of Unicode letters, numbers, '-', '_', or '.'.
    /// Each path component is separated by a slash '/',
    /// and a name may not begin or end in a '/'.
    pub struct Test("Test::schema", pattern = CATALOG_NAME_RE, example = "acmeCo/conversions/test");

    /// JSON Pointer which identifies a location in a document.
    pub struct JsonPointer("JsonPointer::schema", pattern = JSON_POINTER_RE, example = "/json/ptr");

    /// Field names a projection of a document location. They may include '/',
    /// but cannot begin or end with one.
    /// Many Fields are automatically inferred by Flow from a collection JSON Schema,
    /// and are the JSON Pointer of the document location with the leading '/' removed.
    /// User-provided Fields which act as a logical partitions are restricted to
    /// Unicode letters, numbers, '-', '_', or '.'
    pub struct Field("Field::schema", pattern = FIELD_RE, example = "my_field");

    /// PartitionField is a Field which names a logically partitioned projection of a document location,
    /// and is restricted to Unicode letters, numbers, '-', '_', or '.'
    pub struct PartitionField("PartitionField::schema", pattern = TOKEN_RE, example = "my_field");

    /// A URL identifying a resource, which may be a relative local path
    /// with respect to the current resource (i.e, ../path/to/flow.yaml),
    /// or may be an external absolute URL (i.e., http://example/flow.yaml).
    pub struct RelativeUrl("RelativeUrl::schema", pattern = RELATIVE_URL_RE, example = "https://example/resource");

    /// An address for a custom storage endpoint
    pub struct StorageEndpoint("StorageEndpoint::schema", pattern = ENDPOINT_RE, example = "storage.example.com");
}

impl RelativeUrl {
    pub fn example_relative() -> Self {
        Self("../path/to/local.yaml".to_owned())
    }
    pub fn example_absolute() -> Self {
        Self::example()
    }
}

/// Ordered JSON-Pointers which define how a composite key may be extracted from
/// a collection document.
#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema, PartialEq)]
#[schemars(example = CompositeKey::example())]
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

#[cfg(test)]
mod test {
    use super::{
        Collection, Field, JsonPointer, Prefix, RelativeUrl, StorageEndpoint, Transform, Validate,
    };

    #[test]
    fn test_token_re() {
        for (case, expect) in [
            ("valid", true),
            ("no/slashes", false),
            ("Прик.0੫_люче-ния", true),
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
    fn test_catalog_name_re() {
        for (case, expect) in [
            ("valid", true),
            ("valid/1", true),
            ("valid/one/va_lid", true),
            ("valid-1/valid/2/th.ree", true),
            ("Приключения/੫൬/Foo", true),
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
            ("Приключе႘ния/Foo/", true),
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

    #[test]
    fn test_field_re() {
        for (case, expect) in [
            ("valid", true),
            ("/a/json/pointer/with/leading/slash", false),
            ("a/json/pointer/without/leading/slash", true),
            ("may have space", true),
            ("", false),
            ("/", false),
            ("with/esc~0ape", true),
            ("bad/esc~ape", false),
        ] {
            let out = Field::new(case).validate();
            if expect {
                out.unwrap();
            } else {
                out.unwrap_err();
            }
        }
    }

    #[test]
    fn test_relative_url_re() {
        for (case, expect) in [
            ("https://github.com/a/path?query#/and/stuff", true),
            ("../../a/path?query#/and/stuff", true),
            ("", false), // Cannot be empty
            ("cannot have a space", false),
        ] {
            let out = RelativeUrl::new(case).validate();
            if expect {
                out.unwrap();
            } else {
                out.unwrap_err();
            }
        }
    }

    #[test]
    fn test_custom_storage_endpoint() {
        for (case, expect) in [
            ("https://github.com/a/path?query#/and/stuff", false),
            ("", false), // Cannot be empty
            ("foo.bar:123", true),
            ("cannot have a space", false),
            ("http://test.test:345", true),
            ("https://test.test:456", true),
            ("123.45.67.8:567", true),
        ] {
            let out = StorageEndpoint::new(case).validate();
            if expect {
                out.unwrap();
            } else {
                out.unwrap_err();
            }
        }
    }
}
