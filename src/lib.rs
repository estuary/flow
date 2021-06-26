mod decorate;
mod format;

use schemars::{gen, schema as schemagen};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;

pub use self::format::{parse, Input, Output, ParseError, Parser};

#[derive(
    Eq, PartialEq, Hash, PartialOrd, Ord, Clone, Debug, schemars::JsonSchema, Serialize, Deserialize,
)]
#[schemars(example = "JsonPointer::example")]
pub struct JsonPointer(#[schemars(schema_with = "JsonPointer::schema")] pub String);

impl AsRef<str> for JsonPointer {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl<T: Into<String>> From<T> for JsonPointer {
    fn from(s: T) -> Self {
        JsonPointer(s.into())
    }
}

impl JsonPointer {
    pub fn example() -> Self {
        JsonPointer("/json/pointer".to_string())
    }
    fn schema(_: &mut gen::SchemaGenerator) -> schemagen::Schema {
        serde_json::from_value(serde_json::json!({
            "type": "string",
            "pattern": "^(/[^/]+)*$",
        }))
        .unwrap()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Format {
    Json,
}

impl std::convert::TryFrom<String> for Format {
    type Error = String;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl Into<String> for Format {
    fn into(self) -> String {
        self.id().to_string()
    }
}

impl schemars::JsonSchema for Format {
    fn schema_name() -> String {
        "format".to_string()
    }
    fn json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let possible_values = Format::all().iter().map(Format::id).collect::<Vec<_>>();
        serde_json::from_value(serde_json::json!({
            "type": "string",
            "enum": possible_values,
            "title": "format",
            "description": "Specifies the format to use for parsing, which overrides the default behavior of infering the format.",
        }))
        .unwrap()
    }
}

impl Format {
    pub fn id(&self) -> &'static str {
        match *self {
            Format::Json => "json",
        }
    }

    pub fn all() -> &'static [Format] {
        &[Format::Json]
    }
}
impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.id())
    }
}

impl std::str::FromStr for Format {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for format in Format::all() {
            if format.id() == s {
                return Ok(*format);
            }
        }
        Err(format!("invalid format id: '{}'", s))
    }
}

#[derive(Clone, PartialEq, Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ParseConfig {
    /// format forces the use of the given parser and disables automatic format detection. If
    /// unspecified, then the format will be inferred from the filename, content-type, or file
    /// contents.
    #[serde(default)]
    pub format: Option<Format>,

    /// filename is used for format inference. It will be ignored if `format` is specified.
    #[serde(default)]
    pub filename: Option<String>,

    /// The mime type of the data, if known. This will be used for format inference, or ignored if
    /// `format` is specified.
    #[serde(default)]
    pub content_type: Option<String>,

    #[serde(default)]
    /// Add the record offset as a property of each object at the location given. The offset is a
    /// monotonic counter that starts at 0 and increases by 1 for each output document.
    pub add_record_offset: Option<JsonPointer>,

    /// Static data to add to each output JSON document.
    #[serde(default)]
    pub add_values: BTreeMap<JsonPointer, Value>,

    /// Projections control how tabular data like CSV gets transformed into potentially nested JSON
    /// structures. The keys are field names, which may match column names in the source data, and
    /// the values are json pointers indicating where to place the values within the output JSON
    /// document.
    #[serde(default)]
    pub projections: BTreeMap<String, JsonPointer>,

    /// JSON schema describing the desired shape of the output JSON documents. Output documents
    /// will not be validated against this schema, but it can be used to automatically infer
    /// projections for mapping tabular data to nested JSON structures.
    #[serde(default)]
    pub schema: Value,

    /// Mappings from file extensions to format identifiers.
    #[serde(default)]
    pub file_extension_mappings: BTreeMap<String, Format>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config: {0}")]
    Io(#[from] io::Error),

    #[error("failed to parse config: {0}")]
    Parse(#[from] serde_json::Error),
}

impl ParseConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<ParseConfig, ConfigError> {
        let file = fs::File::open(path)?;
        let conf = serde_json::from_reader(io::BufReader::new(file))?;
        Ok(conf)
    }

    pub fn override_from(mut self, other: &ParseConfig) -> Self {
        if other.format.is_some() {
            self.format = other.format.clone();
        }
        if other.filename.is_some() {
            self.filename = other.filename.clone();
        }
        if other.content_type.is_some() {
            self.content_type = other.content_type.clone();
        }
        if other.add_record_offset.is_some() {
            self.add_record_offset = other.add_record_offset.clone();
        }
        self.add_values.extend(
            other
                .add_values
                .iter()
                .map(|kv| (kv.0.clone(), kv.1.clone())),
        );
        self.projections.extend(
            other
                .projections
                .iter()
                .map(|kv| (kv.0.clone(), kv.1.clone())),
        );
        if other.schema != Value::Null {
            self.schema = other.schema.clone();
        }
        self.file_extension_mappings.extend(
            other
                .file_extension_mappings
                .iter()
                .map(|kv| (kv.0.clone(), kv.1.clone())),
        );
        self
    }

    pub fn with_format(mut self, format: Format) -> Self {
        self.format = Some(format);
        self
    }
}

impl Default for ParseConfig {
    fn default() -> Self {
        ParseConfig {
            format: None,
            filename: None,
            content_type: None,
            add_record_offset: None,
            add_values: BTreeMap::new(),
            projections: BTreeMap::new(),
            schema: Value::Bool(true),
            file_extension_mappings: default_file_extension_mappings(),
        }
    }
}

fn default_file_extension_mappings() -> BTreeMap<String, Format> {
    (&[("jsonl", Format::Json), ("json", Format::Json)])
        .iter()
        .map(|(k, v)| (k.to_string(), *v))
        .collect()
}
