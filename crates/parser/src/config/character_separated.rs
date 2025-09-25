use crate::config::{DefaultNullIsAutomatic, EncodingRef, EnumSelection, ErrorThreshold};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use strum::{EnumIter, IntoEnumIterator, IntoStaticStr};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct AdvancedCsvConfig {
    /// Manually specified headers, which can be used in cases where the file itself doesn't
    /// contain a header row. If specified, then the parser will assume that the first row is data,
    /// not column names, and the column names given here will be used. The column names will be
    /// matched with the columns in the file by the order in which they appear here.
    #[serde(default)]
    pub headers: Vec<String>,
    /// The delimiter that separates values within each row. Only single-byte delimiters are
    /// supported.
    #[serde(default)]
    pub delimiter: DefaultNullIsAutomatic<Delimiter>,
    #[serde(default)]
    /// The value that terminates a line. Only single-byte values are supported, with the
    /// exception of "\r\n" (CRLF), which will accept lines terminated by
    /// either a carriage return, a newline, or both.
    pub line_ending: DefaultNullIsAutomatic<LineEnding>,
    /// The character used to quote fields.
    #[serde(default)]
    pub quote: DefaultNullIsAutomatic<Quote>,
    /// The escape character, used to escape quotes within fields.
    #[serde(default)]
    pub escape: DefaultNullIsAutomatic<Escape>,
    /// The character encoding of the source file. If unspecified, then the parser will make a
    /// best-effort guess based on peeking at a small portion of the beginning of the file. If
    /// known, it is best to specify. Encodings are specified by their WHATWG label.
    #[serde(default)]
    pub encoding: DefaultNullIsAutomatic<EncodingRef>,
    /// Allows a percentage of errors to be ignored without failing the entire
    /// parsing process. When this limit is exceeded, parsing halts.
    #[serde(default)]
    pub error_threshold: ErrorThreshold,
    /// Skip a number of lines at the beginning of the file before parsing begins.
    /// This is useful for skipping over metadata that is sometimes added to the top of files.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub skip_lines: usize,
}

fn is_zero(i: &usize) -> bool {
    *i == 0
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, EnumIter, IntoStaticStr)]
pub enum LineEnding {
    #[serde(rename = "\r\n")]
    CRLF,
    #[serde(rename = "\r")]
    CR,
    #[serde(rename = "\n")]
    LF,
    #[serde(rename = "\x1E")]
    RecordSeparator,
}

impl EnumSelection for LineEnding {
    fn string_title(&self) -> &'static str {
        match *self {
            LineEnding::CRLF => "CRLF (\\r\\n) (Windows)",
            LineEnding::CR => "CR (\\r)",
            LineEnding::LF => "LF (\\n)",
            LineEnding::RecordSeparator => "Record Separator (0x1E)",
        }
    }

    fn possible_values() -> Vec<Self> {
        LineEnding::iter().collect()
    }

    fn schema_title() -> &'static str {
        "Line Ending"
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, EnumIter)]
pub enum Escape {
    #[serde(rename = "\\")]
    Backslash,
    #[serde(rename = "")]
    None,
}

impl EnumSelection for Escape {
    fn string_title(&self) -> &'static str {
        match *self {
            Escape::Backslash => "Backslash (\\)",
            Escape::None => "Disable Escapes",
        }
    }

    fn possible_values() -> Vec<Self> {
        Escape::iter().collect()
    }

    fn schema_title() -> &'static str {
        "Escape Character"
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize, EnumIter)]
pub enum Quote {
    #[serde(rename = "\"")]
    DoubleQuote,
    #[serde(rename = "'")]
    SingleQuote,
    #[serde(rename = "")]
    None,
}

impl EnumSelection for Quote {
    fn string_title(&self) -> &'static str {
        match *self {
            Quote::DoubleQuote => "Double Quote (\")",
            Quote::SingleQuote => "Single Quote (')",
            Quote::None => "Disable Quoting",
        }
    }

    fn possible_values() -> Vec<Self> {
        Quote::iter().collect()
    }

    fn schema_title() -> &'static str {
        "Quote Character"
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize, EnumIter)]
pub enum Delimiter {
    #[serde(rename = ",")]
    Comma,
    #[serde(rename = "|")]
    Pipe,
    #[serde(rename = " ")]
    Space,
    #[serde(rename = ";")]
    Semicolon,
    #[serde(rename = "\t")]
    Tab,
    #[serde(rename = "~")]
    Tilde,
    #[serde(rename = "\x0B")]
    VerticalTab,
    #[serde(rename = "\x1F")]
    UnitSeparator,
    #[serde(rename = "\x01")]
    SOH,
}

impl fmt::Display for Delimiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.string_title())
    }
}

impl Delimiter {
    pub fn byte_value(&self) -> u8 {
        match *self {
            Delimiter::Comma => b',',
            Delimiter::Pipe => b'|',
            Delimiter::Space => b' ',
            Delimiter::Semicolon => b';',
            Delimiter::Tab => b'\t',
            Delimiter::Tilde => b'~',
            Delimiter::VerticalTab => 0x0B,
            Delimiter::UnitSeparator => 0x1F,
            Delimiter::SOH => 0x01,
        }
    }
}

impl EnumSelection for Delimiter {
    fn string_title(&self) -> &'static str {
        match *self {
            Delimiter::Comma => "Comma (,)",
            Delimiter::Pipe => "Pipe (|)",
            Delimiter::Space => "Space (0x20)",
            Delimiter::Semicolon => "Semicolon (;)",
            Delimiter::Tab => "Tab (0x09)",
            Delimiter::Tilde => "Tilde (~)",
            Delimiter::VerticalTab => "Vertical Tab (0x0B)",
            Delimiter::UnitSeparator => "Unit Separator (0x1F)",
            Delimiter::SOH => "SOH (0x01)",
        }
    }

    fn possible_values() -> Vec<Self> {
        Delimiter::iter().collect()
    }

    fn schema_title() -> &'static str {
        "Delimiter"
    }
}
