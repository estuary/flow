//! Parsers for character-separated formats like csv.

mod detection;
mod error_buffer;
mod w3c_extended_log;

use self::error_buffer::ParseErrorBuffer;
use crate::config::character_separated::{AdvancedCsvConfig, Delimiter, Escape, LineEnding, Quote};
use crate::format::{Output, ParseError, ParseResult, Parser};
use crate::input::{Input, detect_encoding};
use csv::{Reader, StringRecord, Terminator};
use json::schema::types;
use serde_json::Value;
use std::io;

/// Returns a parser for the [W3C extended log format](https://www.w3.org/TR/WD-logfile.html)
pub use self::w3c_extended_log::new_w3c_extended_log_parser;

/// Returns a Parser for the comma-separated values format.
pub fn new_csv_parser(config: AdvancedCsvConfig) -> Box<dyn Parser> {
    Box::new(CsvParser { config })
}

struct CsvParser {
    config: AdvancedCsvConfig,
}

const PEEK_PREFIX_LEN: usize = 1 << 16; // 64KiB

impl Parser for CsvParser {
    fn parse(&self, content: Input) -> Result<Output, ParseError> {
        // Peek at the input so we can detect the encoding.
        let (raw_peek, raw_input) = content.peek(PEEK_PREFIX_LEN)?;

        // Transcode into UTF-8 before attempting to parse the CSV. This simplifies a lot, since
        // our ultimate target is JSON in UTF-8, and we'd otherwise need to transcode every parsed
        // value separately. This also saves us from having to transcode the delimiter, quote char,
        // etc when configuring the CSV parser.
        let input_encoding = self
            .config
            .encoding
            .as_option()
            .unwrap_or_else(|| detect_encoding(raw_peek.as_ref()));
        let mut utf8_input = if input_encoding.is_utf8() {
            raw_input
        } else {
            raw_input.transcode_to_utf8(input_encoding)
        };

        // If the config says to skip lines, do that now, before we try to detect the dialect, so
        // that the skipped bytes don't interfere with the detection.
        if self.config.skip_lines > 0 {
            utf8_input = utf8_input.skip_lines(self.config.skip_lines)?;
        }

        // We're finished transforming the input, and are ready to start dialect detection.
        let (peek, input) = utf8_input.peek(PEEK_PREFIX_LEN)?;

        // line ending _detection_ is not yet implemented, but CRLF is a pretty reasonable default
        // since it also permits lone CR or LF characters.
        let line_ending = self
            .config
            .line_ending
            .as_option()
            .unwrap_or(LineEnding::CRLF);

        // It's not clear to me that escapes are common enough to try to detect, so for now we'll
        // only enable escape sequences if the config explicitly provides a character. The default
        // behavior is for the csv parser to only interpred doubled quotes within a quoted string,
        // but not to process any other escapes like "\n" or "\"".
        let escape = self.config.escape.as_option().unwrap_or(Escape::None);
        let config_quote = self.config.quote.as_option();
        let config_delimiter = self.config.delimiter.as_option();

        // Unless the configuration explicitly provides both a quote and delimiter,
        // we'll try to detect whichever values are missing. Note that we don't currently
        // try to detect line endings or escape characters, though we may wish to in the future.
        let (quote, delimiter) = match config_quote.zip(config_delimiter) {
            Some(qd) => qd,
            None => {
                let dialect = detection::detect_dialect(
                    Some(self.config.headers.len()).filter(|n| *n > 0),
                    line_ending,
                    escape,
                    peek,
                    config_quote,
                    config_delimiter,
                );
                (dialect.quote, dialect.delimiter)
            }
        };

        let mut builder = new_read_builder(line_ending, quote, delimiter, escape);

        // If headers were specified in the config, then we'll use those and tell the parser to
        // interpret the first row as data. Otherwise, we'll try to read headers from the file.
        let mut headers = self.config.headers.clone();
        builder.has_headers(headers.is_empty());

        let mut reader = builder.from_reader(input.into_stream());

        // If headers were not specified in the config, then ask the reader to parse them now.
        if headers.is_empty() {
            headers = reader
                .headers()
                .map_err(box_err)?
                .into_iter()
                .map(|h| h.to_string())
                .collect();
            tracing::debug!(nColumns = headers.len(), "Parsed headers from file");
        }
        let columns = resolve_headers(headers, CSV_NULLS);

        let csv_output = CsvOutput::new(columns, reader);
        let iterator = if !self.config.error_threshold.is_zero() {
            Box::new(ParseErrorBuffer::new(
                csv_output,
                self.config.error_threshold,
            )) as Output
        } else {
            Box::new(csv_output) as Output
        };
        Ok(iterator)
    }
}

/// Returns a new ReaderBuilder that configured with the given options
fn new_read_builder(
    line_ending: LineEnding,
    quote: Quote,
    delimiter: Delimiter,
    escape: Escape,
) -> csv::ReaderBuilder {
    let mut builder = csv::ReaderBuilder::new();
    // Configure the underlying parser to allow rows to have more or fewer columns than the header row.
    // This is needed in order to properly parse files using explicitly configured headers
    // instead of reading the column names from the first row. `CsvOutput` will explicitly check
    // each row to ensure that it has no more columns than there are headers, which will account
    // for explicitly configured headers.
    builder.flexible(true);
    builder.delimiter(delimiter.byte_value());

    let terminator = match line_ending {
        LineEnding::CRLF => Terminator::CRLF,
        LineEnding::CR => Terminator::Any(b'\r'),
        LineEnding::LF => Terminator::Any(b'\n'),
        LineEnding::RecordSeparator => Terminator::Any(0x1E),
    };
    builder.terminator(terminator);

    match quote {
        Quote::DoubleQuote => builder.quote(b'"'),
        Quote::SingleQuote => builder.quote(b'\''),
        Quote::None => builder.quoting(false),
    };

    match escape {
        Escape::Backslash => {
            builder.escape(Some(b'\\'));
        }
        Escape::None => { /* default ignores special escapes, but handles doubled quote chars */ }
    };
    builder
}

/// Associates each column header with projection information. This is needed in order to construct
/// a potentially nested JSON document from the tabular data. If there's no projection information
/// available for a given field, then we'll use a default projection that simply uses the column
/// name as the JSON property name and permits any type of value. This is so that the parser can at
/// least do a basic CSV to JSON conversion without having any prior knowledge about the desired
/// shape of the JSON.
fn resolve_headers(
    column_header_names: Vec<String>,
    null_sentinels: &'static [&'static str],
) -> Vec<Column> {
    let mut columns = Vec::new();
    for name in column_header_names {
        columns.push(Column {
            name,
            allowed_types: types::STRING | types::NULL,
            null_sentinels,
        });
    }
    tracing::info!(headers = ?columns, "resolved column headers");
    columns
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("value {0:?} could not be parsed as type: {1:?}")]
    InvalidType(String, types::Set),

    #[error("failed to parse character-separated content: {0}")]
    InvalidContent(#[from] csv::Error),

    #[error(
        "row {0} has {1} columns, but the headers only define {2} columns. See: https://go.estuary.dev/Pgy3nf for help with configuring the parser"
    )]
    ExtraColumn(u64, usize, usize),
}

fn box_err<E: Into<Error>>(err: E) -> Box<dyn std::error::Error> {
    Box::new(err.into())
}

const PARSE_ORDER: &[TargetType] = &[
    // Try null before string so that fields that allow either null or string will end up as null
    // if a field matches a null sentinel value.
    TargetType::Null,
    // Always attempt integer before fractional.
    TargetType::Integer,
    TargetType::Float,
    TargetType::Boolean,
    TargetType::Array,
    TargetType::Object,
    // Try parsing strings last, since anything is a valid string.
    TargetType::String,
];

#[derive(Debug, PartialEq, Copy, Clone)]
enum TargetType {
    Null,
    Object,
    Array,
    Integer,
    Float,
    Boolean,
    String,
}

impl TargetType {
    fn to_set(&self) -> types::Set {
        match *self {
            TargetType::Null => types::NULL,
            TargetType::Array => types::ARRAY,
            TargetType::Object => types::OBJECT,
            TargetType::Integer => types::INTEGER,
            TargetType::Float => types::FRACTIONAL,
            TargetType::String => types::STRING,
            TargetType::Boolean => types::BOOLEAN,
        }
    }
}

/// Encapsulates a specific named column and associated Projection information.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub allowed_types: types::Set,
    /// The allowable values that will be interpreted as null. This is static because we currently
    /// don't have a use case for them to be dynamic, and it's convenient to just use string
    /// literals.
    pub null_sentinels: &'static [&'static str],
}

pub const CSV_NULLS: &[&str] = &["", "NULL", "null", "nil"];

impl Column {
    fn parse(&self, value: &str) -> Result<Value, Error> {
        for candidate_type in PARSE_ORDER {
            if candidate_type.to_set().overlaps(self.allowed_types) {
                if let Some(parsed) = self.parse_as_type(value, *candidate_type) {
                    return Ok(parsed);
                }
            }
        }
        Err(Error::InvalidType(
            value.to_string(),
            self.allowed_types.clone(),
        ))
    }

    fn parse_as_type(&self, value: &str, target_type: TargetType) -> Option<Value> {
        match target_type {
            TargetType::Null => {
                if self.null_sentinels.contains(&value) {
                    Some(Value::Null)
                } else {
                    None
                }
            }
            TargetType::Array => serde_json::from_str::<Vec<Value>>(value)
                .ok()
                .map(|a| Value::Array(a)),
            TargetType::Object => serde_json::from_str::<serde_json::Map<String, Value>>(value)
                .ok()
                .map(|o| Value::Object(o)),
            TargetType::Integer => serde_json::from_str::<serde_json::Number>(value)
                .ok()
                .and_then(|n| {
                    if n.is_i64() || n.is_u64() {
                        Some(Value::Number(n))
                    } else {
                        None
                    }
                }),
            TargetType::Float => serde_json::from_str::<serde_json::Number>(value)
                .ok()
                .and_then(|n| {
                    if n.is_f64() {
                        Some(Value::Number(n))
                    } else {
                        None
                    }
                }),
            TargetType::Boolean => serde_json::from_str::<bool>(value)
                .ok()
                .map(|b| Value::Bool(b)),
            TargetType::String => Some(Value::String(value.to_string())),
        }
    }
}

pub struct CsvOutput {
    headers: Vec<Column>,
    reader: Reader<Box<dyn io::Read>>,
    current_row: StringRecord,
    row_num: u64,
}

impl CsvOutput {
    pub fn new(headers: Vec<Column>, reader: Reader<Box<dyn io::Read>>) -> CsvOutput {
        CsvOutput {
            headers,
            reader,
            current_row: StringRecord::new(),
            row_num: 0,
        }
    }

    fn parse_current_row(&mut self) -> Result<Value, ParseError> {
        let CsvOutput {
            headers,
            current_row,
            row_num,
            ..
        } = self;
        let mut result = serde_json::Map::with_capacity(current_row.len());
        for (i, header) in headers.iter().enumerate() {
            if let Some(value) = current_row.get(i) {
                let parsed = header.parse(value).map_err(box_err)?;
                result.insert(header.name.clone(), parsed);
            }
        }
        if current_row.len() > headers.len() {
            return Err(box_err(Error::ExtraColumn(
                *row_num,
                current_row.len(),
                headers.len(),
            ))
            .into());
        }
        Ok(Value::Object(result))
    }
}

impl Iterator for CsvOutput {
    type Item = Result<Value, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let has_next = {
            let CsvOutput {
                reader,
                current_row,
                ..
            } = self;
            match reader.read_record(current_row) {
                Ok(more) => more,
                Err(err) => return Some(Err(box_err(err).into())),
            }
        };
        if has_next {
            self.row_num += 1;
            Some(self.parse_current_row())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use serde_json::json;

    fn test_input(content: impl Into<Vec<u8>>) -> Input {
        use std::io::Cursor;
        Input::Stream(Box::new(Cursor::new(content.into())))
    }

    // Test for: https://github.com/estuary/connectors/issues/97
    #[test]
    fn fails_when_there_are_more_columns_than_headers() {
        // CSV has 2 column headers, but row 3 has 3 columns :boom:
        let csv = test_input("a,b\n1\n2,3\n4,5,6");
        let mut iter = new_csv_parser(Default::default())
            .parse(csv)
            .expect("parse failed");

        // First two rows should parse successfully
        let one = iter
            .next()
            .expect("first row should exist")
            .expect("first row should succeed");
        assert_eq!(json!({"a": "1"}), one);
        let two = iter
            .next()
            .expect("second row should exist")
            .expect("second row should succeed");
        assert_eq!(json!({"a": "2", "b": "3"}), two);

        let err_message = iter
            .next()
            .expect("third row should exist")
            .expect_err("third row should be an error")
            .to_string();
        assert!(
            err_message.contains("row 3 has 3 columns, but the headers only define 2 columns"),
            "unexpected error message: {}",
            err_message
        );
    }

    #[test]
    fn parses_when_there_are_more_configured_headers_than_columns() {
        let conf = AdvancedCsvConfig {
            headers: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            ..Default::default()
        };

        let csv = test_input("1\n2,3\n4,5,6");
        let results = new_csv_parser(conf)
            .parse(csv)
            .expect("parse failed")
            .collect::<Result<Vec<_>, ParseError>>()
            .expect("output fail");
        assert_eq!(
            vec![
                json!({"a": "1"}),
                json!({"a": "2", "b": "3"}),
                json!({"a": "4", "b": "5", "c": "6"}),
            ],
            results
        );
    }

    #[test]
    fn single_column_csv_is_parsed() {
        // When a CSV contains only a single column, it will not contain any delimiter characters,
        // and detect_delimiter will need to gracefully handle this.
        let csv = test_input("h\na\nb\nc\nd");
        let results = new_csv_parser(Default::default())
            .parse(csv)
            .expect("parse failed")
            .collect::<Result<Vec<_>, ParseError>>()
            .expect("output fail");
        let expected = vec![
            json!({"h": "a"}),
            json!({"h": "b"}),
            json!({"h": "c"}),
            json!({"h": "d"}),
        ];
        assert_eq!(expected, results);
    }

    #[test]
    fn quotes_can_be_disabled() {
        let csv = test_input("a,b,c\n\"foo\",\"bar\",\"b''az\"");

        let config = AdvancedCsvConfig {
            quote: Quote::None.into(),
            ..Default::default()
        };
        let results = new_csv_parser(config)
            .parse(csv)
            .expect("parse failed")
            .collect::<Result<Vec<_>, ParseError>>()
            .expect("output fail");
        assert_eq!(
            vec![json!({"a": "\"foo\"", "b": "\"bar\"", "c": "\"b''az\""}),],
            results
        );
    }

    #[test]
    fn values_parsed_as_null_when_sentinel_matches() {
        let csv = test_input("foo\nnuul\nNULL\nnil\nNul\nnull\nnullll\n0\n");
        let results = new_csv_parser(Default::default())
            .parse(csv)
            .expect("parse failed")
            .collect::<Result<Vec<_>, ParseError>>()
            .expect("output fail");
        assert_eq!(
            vec![
                json!({"foo": "nuul"}),
                json!({ "foo": null }),
                json!({ "foo": null }),
                json!({"foo": "Nul"}),
                json!({ "foo": null }),
                json!({"foo": "nullll"}),
                json!({"foo": "0"}),
            ],
            results
        );
    }
}
