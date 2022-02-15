pub mod csv;

use encoding_rs::Encoding;
use schemars::{gen, schema as schemagen};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;

use self::csv::CharacterSeparatedConfig;

/// References an encoding by WHATWG label.
#[derive(Debug, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct EncodingRef(&'static Encoding);

impl schemars::JsonSchema for EncodingRef {
    fn schema_name() -> String {
        "encoding".to_string()
    }
    fn json_schema(_gen: &mut gen::SchemaGenerator) -> schemagen::Schema {
        serde_json::from_value(serde_json::json!({
            "title": "encoding",
            "description": "An encoding scheme, identified by WHATWG label. The list of allowable values is available at: https://encoding.spec.whatwg.org/#names-and-labels",
            "type": "string",
            "pattern": "^[a-z0-9_\\-:]{2,20}$"
        })).unwrap()
    }
}

impl EncodingRef {
    /// Returns the actual encoding_rs struct reference. This function signature may change
    /// if we want to add support for other encodings beyond what's provided by encoding_rs.
    pub(crate) fn encoding(&self) -> &'static Encoding {
        self.0
    }

    pub fn is_utf8(&self) -> bool {
        self.encoding() == encoding_rs::UTF_8
    }
}

impl From<&'static Encoding> for EncodingRef {
    fn from(e: &'static Encoding) -> EncodingRef {
        EncodingRef(e)
    }
}

impl<'a> TryFrom<&'a str> for EncodingRef {
    type Error = String;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        if let Some(encoding) = Encoding::for_label_no_replacement(s.as_bytes()) {
            Ok(EncodingRef(encoding))
        } else {
            Err(format!("no such WHATWG encoding label: '{}'", s))
        }
    }
}

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
    /// JSON objects separated by whitespace, typically a single newline. This format works for
    /// JSONL (a.k.a. JSON-newline), but also for any stream of JSON objects, as long as they have
    /// at least one character of whitespace between them.
    Json,
    /// Comma-separated values
    Csv,
    /// Tab-separated values
    Tsv,
    /// A W3C Extended Log file, as defined by the working group draft at:
    /// https://www.w3.org/TR/WD-logfile.html
    W3cExtendedLog,
    /// Avro object container files, as defined by the [avro spec](https://avro.apache.org/docs/current/spec.html#Object+Container+Files)
    Avro,
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
            Format::Csv => "csv",
            Format::Tsv => "tsv",
            Format::W3cExtendedLog => "w3cExtendedLog",
            Format::Avro => "avro",
        }
    }

    pub fn all() -> &'static [Format] {
        &[
            Format::Json,
            Format::Csv,
            Format::Tsv,
            Format::W3cExtendedLog,
            Format::Avro,
        ]
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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Compression {
    Gzip,
    ZipArchive,
}

impl Compression {
    pub const ALL: &'static [Compression] = &[Compression::Gzip, Compression::ZipArchive];

    pub fn id(&self) -> &'static str {
        match *self {
            Compression::Gzip => "gzip",
            Compression::ZipArchive => "zip",
        }
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.id())
    }
}

impl Into<String> for Compression {
    fn into(self) -> String {
        self.id().to_owned()
    }
}

impl std::convert::TryFrom<String> for Compression {
    type Error = String;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        std::str::FromStr::from_str(&s)
    }
}

impl std::str::FromStr for Compression {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for compression in Compression::ALL {
            if s == compression.id() {
                return Ok(*compression);
            }
        }
        Err(format!("no supported compress called: {:?}", s))
    }
}

impl schemars::JsonSchema for Compression {
    fn schema_name() -> String {
        "compression".to_string()
    }
    fn json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let possible_values = Compression::ALL
            .iter()
            .map(|c| Value::String(c.id().to_owned()))
            .collect::<Vec<_>>();
        serde_json::from_value(serde_json::json!({
            "type": "string",
            "enum": possible_values,
            "title": "compression",
            "description": "Specifies the compression format to use to decompress contents. If left undefined, then the compression will be determined automatically, which is probably what you want.",
        }))
        .unwrap()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ErrorThreshold {
    pub max_percent: u8,
}

impl ErrorThreshold {
    pub fn new(percent: u64) -> Result<Self, ConfigError> {
        if percent <= 100 {
            Ok(Self {
                max_percent: percent as u8,
            })
        } else {
            Err(ConfigError::InvalidErrorThreshold(percent))
        }
    }

    pub fn exceeded(&self, test_percent: u8) -> bool {
        test_percent >= self.max_percent
    }
}

impl Default for ErrorThreshold {
    fn default() -> Self {
        Self { max_percent: 0 }
    }
}

impl<'de> Deserialize<'de> for ErrorThreshold {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'v> serde::de::Visitor<'v> for Visitor {
            type Value = ErrorThreshold;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("expected a percentage value between 0 and 100")
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                ErrorThreshold::new(v).map_err(|e| E::custom(e))
            }
        }

        deserializer.deserialize_u64(Visitor)
    }
}

impl schemars::JsonSchema for ErrorThreshold {
    fn schema_name() -> String {
        "errorThreshold".to_string()
    }
    fn json_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        serde_json::from_value(serde_json::json!({
            "type": ["integer"],
            "title": "errorThreshold",
            "description": "The percentage of malformed rows which can be encountered without halting the parsing process. Only the most recent 1000 rows are used to calculate the error rate.",
            "minimum": 0,
            "maximum": 100,
        }))
        .unwrap()
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

    /// compression forces the use of the given compression format to decompress the contents.
    /// If unspecified, then the compression (or lack thereof) will be inferred from the filename,
    /// content-encoding, content-type, or file contents.
    #[serde(default)]
    pub compression: Option<Compression>,

    /// filename is used for format inference. It will be ignored if `format` is specified.
    #[serde(default)]
    pub filename: Option<String>,

    /// The mime type of the data, if known. This will be used for format inference, or ignored if
    /// `format` is specified.
    #[serde(default)]
    pub content_type: Option<String>,

    /// The content-encoding of the data, if known. This is used in determining how to decompress
    /// files. If your file contents came from a web server that sets the `Content-Encoding`
    /// header, then that header value can be used directly here.
    #[serde(default)]
    pub content_encoding: Option<String>,

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

    /// Mappings from content types to format identifiers.
    #[serde(default)]
    pub content_type_mappings: BTreeMap<String, Format>,

    // Below are format-specific configurations, which are used by the parsers.
    /// Configures handling of comma-separated values (CSV) format.
    #[serde(default)]
    pub csv: Option<CharacterSeparatedConfig>,

    /// Configures handling of tab-separated values (TSV) format.
    #[serde(default)]
    pub tsv: Option<CharacterSeparatedConfig>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config: {0}")]
    Io(#[from] io::Error),

    #[error("failed to parse config: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("ErrorThreshold cannot be greater than 100%: {0}%")]
    InvalidErrorThreshold(u64),
}

impl ParseConfig {
    /// Returns the generated json schema for the configuration file.
    pub fn json_schema() -> schemars::schema::RootSchema {
        let mut settings = schemars::gen::SchemaSettings::draft07();
        settings.option_add_null_type = false;
        let generator = schemars::gen::SchemaGenerator::new(settings);
        generator.into_root_schema_for::<ParseConfig>()
    }

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
        self.content_type_mappings.extend(
            other
                .content_type_mappings
                .iter()
                .map(|kv| (kv.0.clone(), kv.1.clone())),
        );
        if let Some(other_csv) = other.csv.as_ref() {
            if let Some(self_csv) = self.csv.as_mut() {
                self_csv.merge(other_csv);
            } else {
                self.csv = Some(other_csv.clone());
            }
        }
        if let Some(other_tsv) = other.tsv.as_ref() {
            if let Some(self_tsv) = self.tsv.as_mut() {
                self_tsv.merge(other_tsv);
            } else {
                self.tsv = Some(other_tsv.clone());
            }
        }
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
            content_encoding: None,
            compression: None,
            add_record_offset: None,
            add_values: BTreeMap::new(),
            projections: BTreeMap::new(),
            schema: Value::Null,
            file_extension_mappings: default_file_extension_mappings(),
            content_type_mappings: default_content_type_mappings(),
            csv: None,
            tsv: None,
        }
    }
}

fn default_content_type_mappings() -> BTreeMap<String, Format> {
    (&[
        ("application/json", Format::Json),
        ("text/json", Format::Json),
        ("text/csv", Format::Csv),
        ("text/tab-separated-values", Format::Tsv),
    ])
        .iter()
        .map(|(k, v)| (k.to_string(), *v))
        .collect()
}

fn default_file_extension_mappings() -> BTreeMap<String, Format> {
    (&[
        ("jsonl", Format::Json),
        ("json", Format::Json),
        ("csv", Format::Csv),
        ("tsv", Format::Tsv),
        ("avro", Format::Avro),
    ])
        .iter()
        .map(|(k, v)| (k.to_string(), *v))
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn config_schema_is_generated() {
        let schema = ParseConfig::json_schema();
        insta::assert_json_snapshot!(schema);
    }

    #[test]
    fn test_override_from() {
        let base: ParseConfig = serde_json::from_str(
            r#"{
            "format": "json",
            "contentType": "nooooo",
            "addValues": {
                "/foo": "bar",
                "/baz": 2
            },
            "projections": {
                "weee": "wooo"
            },
            "csv": {"quote": "a"}
        }"#,
        )
        .unwrap();
        let actual = base.override_from(
            &serde_json::from_str(
                r#"{
                    "contentType": "application/json",
                    "addValues": {
                        "/foo": "newFoo",
                        "/new": "new"
                    },
                    "schema": true,
                    "addRecordOffset": "/offset",
                    "projections": {
                        "fee": "fi"
                    },
                    "csv": {"quote": "\""},
                    "tsv": {"escape": "\\"}
                }"#,
            )
            .unwrap(),
        );

        let expected: ParseConfig = serde_json::from_str(
            r#"{
                "format": "json",
                "contentType": "application/json",
                "addValues": {
                    "/baz": 2,
                    "/foo": "newFoo",
                    "/new": "new"
                },
                "schema": true,
                "addRecordOffset": "/offset",
                "projections": {
                    "weee": "wooo",
                    "fee": "fi"
                },
                "csv": {"quote": "\""},
                "tsv": {"escape": "\\"}
            }"#,
        )
        .unwrap();
        assert_eq!(expected, actual);
    }
}
