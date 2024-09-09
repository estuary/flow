pub mod character_separated;
pub mod protobuf;

use encoding_rs::Encoding;
use schemars::{
    gen,
    schema::{self as schemagen, InstanceType},
    JsonSchema,
};
use serde::{
    de::{self, DeserializeOwned},
    Deserialize, Serialize,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::Path;
use strum::{EnumIter, IntoEnumIterator};

/// A helper trait for configuration values that are represented as enums, to allow
/// for easy and consistent generation of JSON schemas for them.
pub trait EnumSelection: Sized + Clone + DeserializeOwned + Serialize {
    fn string_title(&self) -> &'static str;

    fn possible_values() -> Vec<Self>;

    fn schema_title() -> &'static str;
}

const AUTO: &str = "Auto";

/// A wrapper type for configuration values that the parser can attempt to determine automatically.
/// An example is the delimiter character for a CSV file. If the value is `null` or `undefined`,
/// then the parser will attempt to determine it automatically, either based on the file content or
/// by choosing some reasonable default.
///
/// The use of `null` to indicate "Automatically determined" has some consequences. For one, it
/// obviously precludes the use of `null` or `undefined` for indicating that something should not
/// be set. This means that if there's a need to represent the lack of a value, then the wrapped
/// type must define a specific variant for it. For example, a `QuoteCharacter` type might need to
/// define `QuoteCharacter::Disabled`, which would need to have a non-null value associated with
/// it. The rationale here is that many fields will want to define an automatic option, but few
/// will need an option to disable it completely. Also, it seems less surprising and more robust to
/// interpret `null` and `undefined` in the same way. For types that need to have an option to
/// disable it, an empty string is typically used to represent the 'Disabled' variant.
#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DefaultNullIsAutomatic<T>(Option<T>);

impl<T> DefaultNullIsAutomatic<T> {
    pub fn as_option(&self) -> Option<T>
    where
        T: Copy,
    {
        self.0
    }

    pub fn as_ref(&self) -> Option<&T> {
        self.0.as_ref()
    }
}

impl<T> From<T> for DefaultNullIsAutomatic<T> {
    fn from(t: T) -> Self {
        DefaultNullIsAutomatic(Some(t))
    }
}

impl<T> Default for DefaultNullIsAutomatic<T> {
    fn default() -> Self {
        DefaultNullIsAutomatic(None)
    }
}

impl<T: EnumSelection> JsonSchema for DefaultNullIsAutomatic<T> {
    fn schema_name() -> String {
        Self::schema_title().to_string()
    }

    fn json_schema(_gen: &mut gen::SchemaGenerator) -> schemagen::Schema {
        let mut variants: Vec<serde_json::Value> = T::possible_values()
            .into_iter()
            .map(|variant| {
                serde_json::json!({
                    "title": variant.string_title(),
                    "const": variant,
                })
            })
            .collect();

        variants.push(serde_json::json!({
            "title": AUTO,
            "const": null,
        }));

        serde_json::from_value(serde_json::json!({
            "title": T::schema_title(),
            "oneOf": variants,
            "default": null,
        }))
        .unwrap()
    }
}

impl<T> EnumSelection for DefaultNullIsAutomatic<T>
where
    T: EnumSelection,
{
    fn string_title(&self) -> &'static str {
        if let Some(t) = self.0.as_ref() {
            t.string_title()
        } else {
            AUTO
        }
    }

    fn possible_values() -> Vec<Self> {
        let mut all: Vec<Self> = T::possible_values()
            .into_iter()
            .map(|t| DefaultNullIsAutomatic(Some(t)))
            .collect();
        all.push(DefaultNullIsAutomatic(None));
        all
    }

    fn schema_title() -> &'static str {
        T::schema_title()
    }
}

/// References an encoding by WHATWG name or label. Labels, according to WHATWG, are just aliases
/// for the canonical name, which is always a valid way to refer to an encoding. If configuration
/// is provided with a label that does not match the name, then it will be normalized during
/// deserialization. For example, the value `latin1` will get normalized to `windows-1252`.
#[derive(Debug, PartialEq, Copy, Clone, Serialize)]
pub struct EncodingRef(&'static str);

impl<'de> Deserialize<'de> for EncodingRef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let name: String = Deserialize::deserialize(deserializer)?;

        if let Some(e) = Encoding::for_label_no_replacement(name.as_bytes()) {
            Ok(EncodingRef(e.name()))
        } else {
            Err(de::Error::custom(format!("invalid encoding label: '{}', must be a WHATWG name or label as described by: https://encoding.spec.whatwg.org/#names-and-labels", name)))
        }
    }
}

impl fmt::Display for EncodingRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

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

impl EnumSelection for EncodingRef {
    fn string_title(&self) -> &'static str {
        self.0
    }

    fn possible_values() -> Vec<Self> {
        // These names are taken from:
        // https://encoding.spec.whatwg.org/#names-and-labels
        // The * indicates encodings that are more commonly used. If we desire to shorten the list
        // of possible encodings in the UI, then we could leave off anything that isn't starred.
        vec![
            EncodingRef("UTF-8"),    // *
            EncodingRef("UTF-16LE"), // *
            EncodingRef("UTF-16BE"), // *
            EncodingRef("IBM866"),
            EncodingRef("ISO-8859-2"),
            EncodingRef("ISO-8859-3"),
            EncodingRef("ISO-8859-4"),
            EncodingRef("ISO-8859-5"),
            EncodingRef("ISO-8859-6"),
            EncodingRef("ISO-8859-7"),
            EncodingRef("ISO-8859-8"),
            EncodingRef("ISO-8859-8-I"),
            EncodingRef("ISO-8859-10"),
            EncodingRef("ISO-8859-13"),
            EncodingRef("ISO-8859-14"),
            EncodingRef("ISO-8859-15"),
            EncodingRef("ISO-8859-16"),
            EncodingRef("KOI8-R"),
            EncodingRef("KOI8-U"),
            EncodingRef("macintosh"),
            EncodingRef("windows-874"),
            EncodingRef("windows-1250"),
            EncodingRef("windows-1251"),
            EncodingRef("windows-1252"), // *
            EncodingRef("windows-1253"),
            EncodingRef("windows-1254"),
            EncodingRef("windows-1255"),
            EncodingRef("windows-1256"),
            EncodingRef("windows-1257"),
            EncodingRef("windows-1258"),
            EncodingRef("x-mac-cyrillic"),
            EncodingRef("GBK"), // *
            EncodingRef("gb18030"),
            EncodingRef("Big5"),   // *
            EncodingRef("EUC-JP"), // *
            EncodingRef("ISO-2022-JP"),
            EncodingRef("Shift_JIS"), // *
            EncodingRef("EUC-KR"),    // *
        ]
    }

    fn schema_title() -> &'static str {
        "Encoding"
    }
}

impl EncodingRef {
    /// Returns the actual encoding_rs struct reference. This function signature may change
    /// if we want to add support for other encodings beyond what's provided by encoding_rs.
    pub(crate) fn encoding(&self) -> &'static Encoding {
        // for_label will return the 'replacement' encoding if the label doesn't map to a known
        // encoding. Using `for_label_no_replacement` is just a sanity check that the name we store
        // in the EncodingRef is actually valid.
        Encoding::for_label_no_replacement(self.0.as_bytes())
            .expect("invalid EncodingRef does not map to a supported encoding")
    }

    pub fn is_utf8(&self) -> bool {
        self.encoding() == encoding_rs::UTF_8
    }
}

impl From<&'static Encoding> for EncodingRef {
    fn from(e: &'static Encoding) -> EncodingRef {
        EncodingRef(e.name())
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

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config")]
pub enum Format {
    /// Attempt to determine the format automatically, based on the file extension or associated
    /// content-type.
    #[serde(rename = "auto")]
    #[schemars(title = "Auto")]
    Auto,

    /// Avro object container files, as defined by the [avro spec](https://avro.apache.org/docs/current/spec.html#Object+Container+Files)
    #[serde(rename = "avro")]
    #[schemars(title = "Avro")]
    Avro,

    /// JSON objects separated by whitespace, typically a single newline. This format works for
    /// JSONL (a.k.a. JSON-newline), but also for any stream of JSON objects, as long as they have
    /// at least one character of whitespace between them.
    #[serde(rename = "json")]
    #[schemars(title = "JSON")]
    Json,

    /// Character Separated Values, such as comma-separated, tab-separated, etc.
    #[serde(rename = "csv")]
    #[schemars(title = "CSV")]
    Csv(character_separated::AdvancedCsvConfig),

    /// Parses a single protobuf message, using the given .proto file in the configuration.
    #[serde(rename = "protobuf")]
    #[schemars(title = "Protobuf")]
    Protobuf(protobuf::ProtobufConfig),

    /// A W3C Extended Log file, as defined by the working group draft at:
    /// https://www.w3.org/TR/WD-logfile.html
    #[serde(rename = "w3cExtendedLog")]
    #[schemars(title = "W3C Extended Log")]
    W3cExtendedLog,

    #[serde(rename="parquet")]
    #[schemars(title = "Parquet")]
    Parquet,

    /// Placeholders for files types that are unsupported at this time.
    #[schemars(skip)]
    Excel,
    #[schemars(skip)]
    Xml,
    #[schemars(skip)]
    Ods, // Open-document spreadsheet, for LibreOffice, etc.
}

impl fmt::Display for Format {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Format::Auto => "auto",
            Format::Avro => "avro",
            Format::Json => "json",
            Format::Csv(_) => "csv",
            Format::Protobuf(_) => "protobuf",
            Format::W3cExtendedLog => "w3cExtendedLog",
            Format::Excel => "excel",
            Format::Parquet => "parquet",
            Format::Xml => "xml",
            Format::Ods => "ods",
        };
        f.write_str(s)
    }
}

impl Default for Format {
    fn default() -> Format {
        Format::Auto
    }
}

impl Format {
    pub fn is_auto(&self) -> bool {
        match self {
            Format::Auto => true,
            _ => false,
        }
    }
    pub fn non_auto(&self) -> Option<Format> {
        match self {
            Format::Auto => None,
            _ => Some(self.clone()),
        }
    }

    /// Customizes the generated schema for Format to make it work better in the UI.
    /// Format still derives `JsonSchema`, and this function delegates to that impl.
    /// This function is used by setting `schema_with` on the property in ParseConfig.
    fn schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        let mut schema = gen.subschema_for::<Format>().into_object();
        schema.object().required.insert("type".to_string());
        schema.extensions.insert(
            "discriminator".to_string(),
            serde_json::json!({"propertyName": "type"}),
        );
        schema.instance_type = Some(InstanceType::Object.into());

        schema.into()
    }
}

/// This value is always an empty JSON object.
#[derive(Default, PartialEq, Clone, Debug)]
pub struct EmptyConfig;
impl JsonSchema for EmptyConfig {
    fn schema_name() -> String {
        String::from("empty object")
    }

    fn json_schema(_gen: &mut gen::SchemaGenerator) -> schemagen::Schema {
        serde_json::from_value(serde_json::json!({"type": "object", "default": {}})).unwrap()
    }
}

impl<'de> de::Deserialize<'de> for EmptyConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // ignore any properties that happen to be there.
        let _ = serde_json::value::Map::<String, serde_json::Value>::deserialize(deserializer)?;
        Ok(EmptyConfig)
    }
}

impl Serialize for EmptyConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let s = serializer.serialize_map(Some(0))?;
        s.end()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, EnumIter)]
pub enum Compression {
    /// Corresponds to the .gz file extension.
    #[serde(rename = "gzip")]
    Gzip,
    /// Corresponds to the .zip file extension.
    #[serde(rename = "zip")]
    ZipArchive,
    /// Zstandard compression, corresponds to the .zst file extension
    #[serde(rename = "zstd")]
    Zstd,
    /// Do not try to decompress, even if the file has an extension that indicates that it's
    /// compressed.
    #[serde(rename = "none")]
    None,
}

impl Compression {
    pub fn is_none(&self) -> bool {
        *self == Compression::None
    }
}

impl fmt::Display for Compression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.string_title())
    }
}

impl EnumSelection for Compression {
    fn string_title(&self) -> &'static str {
        match *self {
            Compression::Gzip => "GZip",
            Compression::ZipArchive => "Zip Archive",
            Compression::Zstd => "Zstandard",
            Compression::None => "None",
        }
    }

    fn possible_values() -> Vec<Self> {
        Compression::iter().collect()
    }

    fn schema_title() -> &'static str {
        "Compression"
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

    pub fn is_zero(&self) -> bool {
        self.max_percent == 0
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
            "type": "integer",
            "title": "Error Threshold",
            "default": 0,
            "minimum": 0,
            "maximum": 100,
        }))
        .unwrap()
    }
}

fn default_offset_string() -> String {
    "+00:00".to_string()
}

// Fields annotated with `schemars(skip)` will not appear in the JSON schema, and thus won't be
// shown in the UI. These are things that connectors set programatically when it generates the
// config. We could consider moving these fields to be CLI arguments if we want a clearer
// separation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[schemars(
    title = "Parser Configuration",
    description = "Configures how files are parsed"
)]
#[serde(rename_all = "camelCase")]
pub struct ParseConfig {
    /// Determines how to parse the contents. The default, 'Auto', will try to determine the format
    /// automatically based on the file extension or MIME type, if available.
    #[serde(default)]
    #[schemars(schema_with = "Format::schema")]
    pub format: Format,

    /// Determines how to decompress the contents. The default, 'Auto', will try to determine the
    /// compression automatically.
    #[serde(default)]
    pub compression: DefaultNullIsAutomatic<Compression>,

    /// The default timezone to use when parsing timestamps that do not have a timezone. Timezones
    /// must be specified as an +/-HH:MM offset, defaults to +00:00.
    #[serde(default = "default_offset_string")]
    pub default_offset: String,

    /// filename is used for format inference. It will be ignored if `format` is specified.
    #[serde(default)]
    #[schemars(skip)]
    pub filename: Option<String>,

    #[serde(default)]
    #[schemars(skip)]
    /// Add the record offset as a property of each object at the location given. The offset is a
    /// monotonic counter that starts at 0 and increases by 1 for each output document.
    pub add_record_offset: Option<JsonPointer>,

    /// Static data to add to each output JSON document. This _could_ be exposed in the UI, but
    /// we'd need to change the schema to represent it as an array of key/value instead of an
    /// object, and there's no motivating use case for it right now.
    #[serde(default)]
    #[schemars(skip)]
    pub add_values: BTreeMap<JsonPointer, Value>,

    /// The mime type of the data, if known. This will be used for format inference, or ignored if
    /// `format` is specified.
    #[serde(default)]
    #[schemars(skip)]
    pub content_type: Option<String>,

    /// The content-encoding of the data, if known. This is used in determining how to decompress
    /// files. If your file contents came from a web server that sets the `Content-Encoding`
    /// header, then that header value can be used directly here.
    #[serde(default)]
    #[schemars(skip)]
    pub content_encoding: Option<String>,
}

impl Default for ParseConfig {
    fn default() -> Self {
        ParseConfig {
            format: Default::default(),
            compression: Default::default(),
            default_offset: default_offset_string(),
            filename: Default::default(),
            add_record_offset: Default::default(),
            add_values: Default::default(),
            content_type: Default::default(),
            content_encoding: Default::default(),
        }
    }
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

/// A `schemars::visit::Visitor` that sets a `default` for all values that are `const`. Also
/// normalizes any schemas with `enum: ["singleValue"]` to `const: "singleValue"`, which is needed
/// by the estuary UI in order to render properly.
#[derive(Debug, Clone)]
struct ConstDefaultSchemaVisitor;
impl schemars::visit::Visitor for ConstDefaultSchemaVisitor {
    fn visit_schema_object(&mut self, schema: &mut schemagen::SchemaObject) {
        schemars::visit::visit_schema_object(self, schema);

        if schema.enum_values.as_ref().map(|v| v.len()) == Some(1) {
            let value = schema.enum_values.take().unwrap().pop().unwrap();
            schema.const_value = Some(value);
        }

        if let Some(const_val) = schema.const_value.as_ref() {
            if schema.metadata.is_none() {
                schema.metadata = Some(Default::default());
            }
            if let Some(md) = schema.metadata.as_mut() {
                if md.default.is_none() {
                    md.default = Some(const_val.clone());
                }
            }
        }
    }
}

impl ParseConfig {
    /// Returns the generated json schema for the configuration file.
    pub fn json_schema() -> schemars::schema::RootSchema {
        let mut settings = schemars::gen::SchemaSettings::draft07();
        settings.option_add_null_type = false;
        settings.inline_subschemas = true;
        settings = settings.with_visitor(ConstDefaultSchemaVisitor);
        let generator = schemars::gen::SchemaGenerator::new(settings);
        generator.into_root_schema_for::<ParseConfig>()
    }

    pub fn load(path: impl AsRef<Path>) -> Result<ParseConfig, ConfigError> {
        let file = fs::File::open(path)?;
        let conf = serde_json::from_reader(io::BufReader::new(file))?;
        Ok(conf)
    }

    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format.into();
        self
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::character_separated::{AdvancedCsvConfig, Delimiter, Escape, LineEnding, Quote};
    use serde_json::json;

    #[test]
    fn config_schema_is_generated() {
        let schema = ParseConfig::json_schema();
        insta::assert_json_snapshot!(schema);
    }

    #[test]
    fn config_serde_round_trip() {
        let config = ParseConfig {
            format: Format::Csv(AdvancedCsvConfig::default()),
            add_record_offset: Some("/foo".into()),
            compression: Compression::Gzip.into(),
            filename: Some("a file.json".into()),
            ..Default::default()
        };
        let ser = serde_json::to_value(config.clone()).expect("serialization failed");
        let end: ParseConfig = serde_json::from_value(ser).expect("deser failed");
        assert_eq!(config, end);
    }

    #[test]
    fn config_is_deserialized_using_default_format() {
        let c1 = json!({
            "filename": "tha-file",
            "compression": "none",
        });

        let r1: ParseConfig = serde_json::from_value(c1).expect("deserialize config");

        let expected = ParseConfig {
            format: Format::Auto,
            compression: Compression::None.into(),
            filename: Some("tha-file".to_string()),
            ..Default::default()
        };
        assert_eq!(expected, r1);
    }

    #[test]
    fn basic_configs_are_deserialized_without_format_config() {
        // We use adjacently tagged enums to represent the config. This means that any variant
        // values, like `AdvancedCsvConfig`, cannot use `#[serde(default)]`. If a variant contains
        // a config object, it must be present in the JSON. This means that we cannot add
        // configuration to a format that currently has none, or else old configs that are missing
        // the `config` property will suddently fail to deserialize. This test is here to fail in
        // the case that someone adds a config object to a variant that currently has none.
        // If you're here because this is failing, fear not. There's a way to fix it. Add a custom
        // `Deserialize` impl for `Format` that deserializes the `config` using two passes. On the
        // first pass, deserialize it into a `Value` or `RawValue`. Then, once the target variant
        // (and thus the target type of `config`) is known, deserialize that as the typed value.
        let types = ["json", "auto", "avro", "w3cExtendedLog"];
        for ty in types {
            let config = json!({
                "format": {
                    "type": ty,
                }
            });
            let result = serde_json::from_value::<ParseConfig>(config);
            assert!(
                result.is_ok(),
                "format type '{}' failed to deserialize with result: {:?}",
                ty,
                result
            );
        }
    }

    // Just documenting what might otherwise be surprising behavior.
    #[test]
    fn csv_config_cannot_be_deserialized_without_config_object() {
        let c1 = json!({
            "format": {
                "type": "csv",
            },
            "filename": "tha-file",
            "compression": "zip",
        });

        serde_json::from_value::<ParseConfig>(c1).expect_err("expected deserialization error");
    }

    #[test]
    fn csv_config_is_deserialized() {
        let c1 = json!({
            "format": {
                "type": "csv",
                "config": {
                    "delimiter": ",",
                    "lineEnding": "\n",
                    "quote": "'",
                    "escape": "\\",
                    "encoding": "latin1",
                }
            },
            "filename": "tha-file",
            "compression": "zip"
        });

        let r1: ParseConfig = serde_json::from_value(c1).expect("deserialize config");

        let expected = ParseConfig {
            format: Format::Csv(AdvancedCsvConfig {
                delimiter: Delimiter::Comma.into(),
                line_ending: LineEnding::LF.into(),
                quote: Quote::SingleQuote.into(),
                escape: Escape::Backslash.into(),
                encoding: EncodingRef("windows-1252").into(),
                headers: Vec::new(),
                error_threshold: Default::default(),
                skip_lines: 0,
            })
            .into(),
            compression: Compression::ZipArchive.into(),
            filename: Some("tha-file".to_string()),
            ..Default::default()
        };
        assert_eq!(expected, r1);
    }
}
