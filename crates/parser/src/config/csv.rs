//! Configuration related to the various character-separated formats, like CSV.
use super::{EncodingRef, ErrorThreshold};
use schemars::{
    gen::SchemaGenerator,
    schema::{InstanceType, Schema, SchemaObject},
    JsonSchema,
};
use serde::{de, ser, Deserialize, Serialize};
use std::convert::TryFrom;

/// A single character. More specifically, a single utf-8 code _unit_, which includes any character
/// in the range 0-127.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct Char(pub u8);

impl JsonSchema for Char {
    fn schema_name() -> String {
        "Char".to_string()
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        let mut schema = SchemaObject::default();
        schema.instance_type = Some(InstanceType::String.into());
        schema.string().min_length = Some(1);
        schema.string().max_length = Some(1);
        schema.metadata().description = Some("A single character in the range 0-127".to_string());
        schema.into()
    }
}

impl Into<u8> for Char {
    fn into(self) -> u8 {
        self.0
    }
}

impl From<u8> for Char {
    fn from(c: u8) -> Char {
        Char(c)
    }
}

impl<'a> TryFrom<&'a str> for Char {
    type Error = &'static str;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        if value.len() != 1 {
            Err("expected a single ascii character")
        } else {
            Ok(Char(value.as_bytes()[0]))
        }
    }
}

impl<'de> de::Deserialize<'de> for Char {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = std::borrow::Cow::<'de, str>::deserialize(deserializer)?;
        Char::try_from(s.as_ref()).map_err(|e| de::Error::custom(e))
    }
}

impl ser::Serialize for Char {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = &[self.0];
        let s = std::str::from_utf8(bytes)
            .map_err(|_| ser::Error::custom("value is not valid utf-8"))?;
        serializer.serialize_str(s)
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum LineEnding {
    CRLF,
    Other(Char),
}

impl JsonSchema for LineEnding {
    fn schema_name() -> String {
        "LineEnding".to_string()
    }

    fn json_schema(gen: &mut SchemaGenerator) -> Schema {
        let char_schema = gen.subschema_for::<Char>();
        serde_json::from_value(serde_json::json!({
            "description": "the character(s) that separates lines, which must either be a single character or '\r\n'",
            "oneOf": [
                { "enum": ["\r\n"] },
                char_schema,
            ]
        })).unwrap()
    }
}

impl<'a> TryFrom<&'a str> for LineEnding {
    type Error = &'static str;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        if value == "\r\n" {
            Ok(LineEnding::CRLF)
        } else if value.len() == 1 {
            Char::try_from(value).map(LineEnding::Other)
        } else {
            Err(r#"expected either a single character or the string "\r\n""#)
        }
    }
}

impl<'de> de::Deserialize<'de> for LineEnding {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = std::borrow::Cow::<'de, str>::deserialize(deserializer)?;
        LineEnding::try_from(s.as_ref()).map_err(|e| de::Error::custom(e))
    }
}

impl ser::Serialize for LineEnding {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            LineEnding::CRLF => serializer.serialize_str("\r\n"),
            LineEnding::Other(c) => c.serialize(serializer),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CharacterSeparatedConfig {
    /// Manually specified headers, which can be used in cases where the file itself doesn't
    /// contain a header row. If specified, then the parser will assume that the first row is data,
    /// not column names, and the column names given here will be used. The column names will be
    /// matched with the columns in the file by the order in which they appear here.
    #[serde(default)]
    pub headers: Vec<String>,
    /// The delimiter that separates values within each row. Only single-byte delimiters are
    /// supported.
    #[serde(default)]
    pub delimiter: Option<Char>,
    #[serde(default)]
    /// The value that terminates a line. Only single-byte values are supported, withe the
    /// exception of "\r\n" (CRLF), which will accept lines terminated by
    /// _either_ a carriage return, a newline, or both.
    pub line_ending: Option<LineEnding>,
    /// The character used to quote fields.
    #[serde(default)]
    pub quote: Option<Char>,
    /// The escape character, used to escape quotes within fields.
    #[serde(default)]
    pub escape: Option<Char>,
    /// The character encoding of the source file. If unspecified, then the parser will make a
    /// best-effort guess based on peeking at a small portion of the beginning of the file. If
    /// known, it is best to specify. Encodings are specified by their WHATWG label.
    #[serde(default)]
    pub encoding: Option<EncodingRef>,
    /// Allows a percentage of errors to be ignored without failing the entire
    /// parsing process. When this limit is exceeded, parsing halts.
    #[serde(default)]
    pub error_threshold: Option<ErrorThreshold>,
}

impl CharacterSeparatedConfig {
    pub fn merge(&mut self, other: &CharacterSeparatedConfig) {
        if !other.headers.is_empty() {
            self.headers = other.headers.clone();
        }
        if other.delimiter.is_some() {
            self.delimiter = other.delimiter;
        }
        if other.line_ending.is_some() {
            self.line_ending = other.line_ending;
        }
        if other.quote.is_some() {
            self.quote = other.quote;
        }
        if other.escape.is_some() {
            self.escape = other.escape;
        }
        if other.encoding.is_some() {
            self.encoding = other.encoding;
        }
        if other.error_threshold.is_some() {
            self.error_threshold = other.error_threshold.clone();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn character_separated_config_is_merged() {
        let mut base = CharacterSeparatedConfig {
            delimiter: Some(Char(20)),
            headers: vec![String::from("nope")],
            quote: Some(Char(34)),
            error_threshold: Some(ErrorThreshold::new(33).unwrap()),
            ..Default::default()
        };
        base.merge(&CharacterSeparatedConfig {
            delimiter: Some(Char(44)),
            headers: vec![String::from("foo")],
            escape: Some(Char(22)),
            error_threshold: Some(ErrorThreshold::new(77).unwrap()),
            ..Default::default()
        });

        assert_eq!(Some(Char(44)), base.delimiter);
        assert_eq!(Some(Char(34)), base.quote);
        assert_eq!(Some(Char(22)), base.escape);
        assert_eq!(Some(ErrorThreshold::new(77).unwrap()), base.error_threshold);
        assert_eq!(&[String::from("foo")], base.headers.as_slice());
    }
}
