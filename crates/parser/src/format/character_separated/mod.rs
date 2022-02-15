//! Parsers for character-separated formats like csv.

mod error_buffer;
mod w3c_extended_log;

use self::error_buffer::ParseErrorBuffer;
use crate::config::{
    csv::{CharacterSeparatedConfig, LineEnding},
    ParseConfig,
};
use crate::format::projection::{build_projections, Projection, Projections};
use crate::format::{Format, Output, ParseError, ParseResult, Parser};
use crate::input::{detect_encoding, Input};
use csv::{Reader, StringRecord, Terminator};
use json::schema::types;
use serde_json::Value;
use std::io;

/// Returns a parser for the [W3C extended log format](https://www.w3.org/TR/WD-logfile.html)
pub use self::w3c_extended_log::new_w3c_extended_log_parser;

/// Returns a Parser for the comma-separated values format.
pub fn new_csv_parser() -> Box<dyn Parser> {
    Box::new(CsvParser {
        format: Format::Csv,
        default_delimiter: b',',
    })
}

/// Returns a Parser for the tab-separated values format.
pub fn new_tsv_parser() -> Box<dyn Parser> {
    Box::new(CsvParser {
        format: Format::Tsv,
        default_delimiter: b'\t',
    })
}

struct CsvParser {
    /// The specific format associated with this parser. Used to lookup the correct configuration
    /// section.
    format: Format,
    /// The default value used to separate values in a row.
    default_delimiter: u8,
}

// There's definitely some room for improvement in this function, but it doesn't seem worth the
// time right now. This detection is only to provide a best-effort guess of the quote character in
// the case that the config doesn't specifiy one.
fn detect_quote_char(delimiter: u8, peeked: &[u8]) -> Option<u8> {
    let mut n_double = 0;
    let mut n_single = 0;

    for subslice in peeked.split(|&b| b == delimiter) {
        match subslice.first() {
            Some(b'"') => n_double += 1,
            Some(b'\'') => n_single += 1,
            _ => {}
        }
        match subslice.last() {
            Some(b'"') => n_double += 1,
            Some(b'\'') => n_single += 1,
            _ => {}
        }
    }

    if n_double < 2 && n_single < 2 {
        None
    } else if n_double >= n_single {
        // If there's a tie, then I guess double quotes win?
        Some(b'"')
    } else {
        Some(b'\'')
    }
}

impl Parser for CsvParser {
    fn parse(&self, config: &ParseConfig, content: Input) -> Result<Output, ParseError> {
        let projections = build_projections(config)?;
        let user_provided_config = get_config(self.format, config).cloned().unwrap_or_default();
        // Transcode into UTF-8 before attempting to parse the CSV. This simplifies a lot, since
        // our ultimate target is JSON in UTF-8, and since the configuration is also provided as
        // JSON in UTF-8.
        let (content_peek, input) = content.peek(2048)?;
        let encoding = detect_encoding(&content_peek);
        let input = if encoding.is_utf8() {
            input
        } else {
            input.transcode_non_utf8(Some(encoding), 0)?
        };

        let delim = user_provided_config
            .delimiter
            .map(Into::into)
            .unwrap_or(self.default_delimiter);

        let mut builder = csv::ReaderBuilder::new();
        // Configure the underlying parser to allow rows to have more columns than the header row.
        // This is needed in order to properly parse files using explicitly configured headers
        // instead of reading the column names from the first row. `CsvOutput` will explicitly check
        // each row to ensure that it has no more columns than there are headers, which will account
        // for explicitly configured headers.
        builder.delimiter(delim).flexible(true);

        // The default line ending is CRLF, and we'll stick with that unless the user specifies
        // something different.
        if let Some(ending) = user_provided_config.line_ending {
            let terminator = match ending {
                LineEnding::CRLF => Terminator::CRLF,
                LineEnding::Other(c) => Terminator::Any(c.0),
            };
            builder.terminator(terminator);
        }

        // If the user hasn't specified a quote character, then we'll try to detect it.
        let quote = user_provided_config.quote.map(Into::into).or_else(|| {
            let detected = detect_quote_char(delim, &content_peek);
            tracing::debug!("detected quote char: {:?}", detected);
            detected
        });
        if let Some(c) = quote {
            builder.quote(c);
        } else {
            // The config didn't specify a quote character and we didn't see any, so disable
            // special handling of quote characters. This will help avoid issues with mismatched
            // quotes if a value happens to contain a quote character.
            builder.quoting(false);
        }

        // It's not clear to me that escapes are common enough to try to detect, so for now we'll
        // only enable escape sequences if the config explicitly provides a character. The default
        // behavior is for the csv parser to only interpred doubled quotes within a quoted string,
        // but not to process any other escapes like "\n" or "\"".
        builder.escape(user_provided_config.escape.map(Into::into));

        // If headers were specified in the config, then we'll use those and tell the parser to
        // interpret the first row as data. Otherwise, we'll try to read headers from the file.
        let mut headers = user_provided_config.headers.clone();
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
        let columns = resolve_headers(headers, projections, CSV_NULLS);

        let csv_output = CsvOutput::new(columns, reader);
        let iterator = if let Some(threshold) = user_provided_config.error_threshold {
            Box::new(ParseErrorBuffer::new(csv_output, threshold)) as Output
        } else {
            Box::new(csv_output) as Output
        };
        Ok(iterator)
    }
}

/// Associates each column header with projection information. This is needed in order to construct
/// a potentially nested JSON document from the tabular data. If there's no projection information
/// available for a given field, then we'll use a default projection that simply uses the column
/// name as the JSON property name and permits any type of value. This is so that the parser can at
/// least do a basic CSV to JSON conversion without having any prior knowledge about the desired
/// shape of the JSON.
fn resolve_headers(
    column_header_names: Vec<String>,
    projections: Projections,
    null_sentinels: &'static [&'static str],
) -> Vec<Header> {
    let mut columns = Vec::new();
    for name in column_header_names {
        let projection = projections.lookup(&name);
        columns.push(Header {
            name,
            projection,
            null_sentinels,
        });
    }
    tracing::info!(headers = ?columns, "resolved column headers");
    columns
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("value {0:?} is not valid for: {1:?}")]
    InvalidType(String, Projection),

    #[error("failed to parse character-separated content: {0}")]
    InvalidContent(#[from] csv::Error),

    #[error("row {0} is missing required column: {1:?}")]
    MissingColumn(u64, String),

    #[error("cannot construct a JSON object from row {0} because it's impossible to create the location {2:?} within the document: {1}")]
    InvalidStructure(u64, Value, String),

    #[error("row {0} has {1} columns, but the headers only define {2} columns. See: https://go.estuary.dev/QRKf3x for help with configuring the parser")]
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
pub struct Header {
    pub name: String,
    pub projection: Projection,
    /// The allowable values that will be interpreted as null. Ignored if the projection
    /// information doesn't allow nulls. This is static because we currently don't have a use case
    /// for them to be dynamic, and it's convenient to just use string literals.
    pub null_sentinels: &'static [&'static str],
}

pub const CSV_NULLS: &[&str] = &["", "NULL", "null", "nil"];

impl Header {
    fn parse(&self, value: &str) -> Result<Value, Error> {
        if let Some(possible_types) = self.projection.possible_types {
            // Since we have type information about this field, try to parse it as one of the
            // allowable types.
            for possible_type in PARSE_ORDER {
                if possible_type.to_set().overlaps(possible_types) {
                    if let Some(parsed) = self.parse_as_type(value, *possible_type) {
                        return Ok(parsed);
                    }
                }
            }
            Err(Error::InvalidType(
                value.to_string(),
                self.projection.clone(),
            ))
        } else {
            // If we don't know any actual type information about this field, then always treat it
            // as a string.
            Ok(Value::String(value.to_string()))
        }
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

fn get_config(format: Format, conf: &ParseConfig) -> Option<&CharacterSeparatedConfig> {
    match format {
        Format::Csv => conf.csv.as_ref(),
        Format::Tsv => conf.tsv.as_ref(),
        other => panic!("called csv::get_config with invalid format: {:?}", other),
    }
}

pub struct CsvOutput {
    headers: Vec<Header>,
    reader: Reader<Box<dyn io::Read>>,
    current_row: StringRecord,
    row_num: u64,
}

impl CsvOutput {
    pub fn new(headers: Vec<Header>, reader: Reader<Box<dyn io::Read>>) -> CsvOutput {
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
        let mut result = Value::Object(serde_json::Map::with_capacity(current_row.len()));
        for (i, header) in headers.iter().enumerate() {
            if let Some(value) = current_row.get(i) {
                let parsed = header.parse(value).map_err(box_err)?;
                if let Some(target) = header.projection.target_location.create(&mut result) {
                    // Success! We've now placed the parsed value into it's home.
                    *target = parsed;
                } else {
                    return Err(box_err(Error::InvalidStructure(
                        *row_num,
                        result.clone(),
                        format!("{:?}", header.projection.target_location),
                    ))
                    .into());
                }
            } else {
                if header.projection.must_exist {
                    return Err(box_err(Error::MissingColumn(*row_num, header.name.clone())).into());
                }
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
        Ok(result)
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
    use serde_json::{json, Value};

    fn test_input(content: impl Into<Vec<u8>>) -> Input {
        use std::io::Cursor;
        Input::Stream(Box::new(Cursor::new(content.into())))
    }

    // Test for: https://github.com/estuary/connectors/issues/97
    #[test]
    fn fails_when_there_are_more_columns_than_headers() {
        let conf = ParseConfig::default();

        // CSV has 2 column headers, but row 3 has 3 columns :boom:
        let csv = test_input("a,b\n1\n2,3\n4,5,6");
        let mut iter = new_csv_parser().parse(&conf, csv).expect("parse failed");

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
        let conf = ParseConfig {
            format: Some(Format::Csv),
            csv: Some(CharacterSeparatedConfig {
                headers: vec!["a".to_string(), "b".to_string(), "c".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };

        let csv = test_input("1\n2,3\n4,5,6");
        let results = new_csv_parser()
            .parse(&conf, csv)
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
    fn values_parsed_as_null_when_sentinel_matches() {
        let conf = ParseConfig {
            format: Some(Format::Csv),
            schema: json!({
                "type": "object",
                "properties": {
                    "foo": {
                        "type": ["string", "integer", "null"]
                    }
                }
            }),
            ..Default::default()
        };
        let csv = test_input("foo\nnuul\nNULL\nnil\nNul\nnull\nnullll\n0\n");
        let results = new_csv_parser()
            .parse(&conf, csv)
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
                json!({"foo": 0}),
            ],
            results
        );
    }

    #[test]
    fn values_parsed_as_strings_when_numbers_would_overflow() {
        let conf = ParseConfig {
            format: Some(Format::Csv),
            schema: json!({
                "type": "object",
                "properties": {
                    "ride_id": {
                        "type": ["number", "string"]
                    }
                }
            }),
            ..Default::default()
        };

        // This file was created by pulling out some of the naughty rows from: '202102-citibike-tripdata.csv.zip' in the publicly available
        // 'tripdata' bucket. The ride_id column there contains some ids that just so happen to be
        // all numeric digits with a single 'E' character in them, and so serde will parse them as
        // numbers, since the `arbitrary_precision` feature flag is enabled. This test is asserting
        // that the numbers that would overflow are parsed as strings.
        let file =
            std::fs::File::open("tests/examples/valid-big-nums.csv").expect("failed to open file");
        let input = Input::File(file);
        let mut result_iter = new_csv_parser()
            .parse(&conf, input)
            .expect("failed to init parser");
        for i in 0..3 {
            let parsed = result_iter
                .next()
                .unwrap()
                .expect(&format!("failed to parse row: {}", i));
            assert_is_string(&parsed, "/ride_id");
        }
    }

    #[test]
    fn values_parsed_as_strings_when_missing_type_info() {
        let conf = ParseConfig {
            format: Some(Format::Csv),
            ..Default::default()
        };

        // This file was created by pulling out some of the naughty rows from: '202102-citibike-tripdata.csv.zip' in the publicly available
        // 'tripdata' bucket. The ride_id column there contains some ids that just so happen to be
        // all numeric digits with a single 'E' character in them, and so serde will parse them as
        // numbers, since the `arbitrary_precision` feature flag is enabled. This test is asserting
        // that the numbers that would overflow are parsed as strings.
        let file = std::fs::File::open("tests/examples/valid-mixed-types.csv")
            .expect("failed to open file");
        let input = Input::File(file);
        let mut result_iter = new_csv_parser()
            .parse(&conf, input)
            .expect("failed to init parser");
        for i in 0..4 {
            let parsed = result_iter
                .next()
                .unwrap()
                .expect(&format!("failed to parse row: {}", i));
            assert_is_string(&parsed, "/int_or_string");
            assert_is_string(&parsed, "/bool_or_string");
        }
    }

    fn assert_is_string(value: &Value, pointer: &str) {
        let actual = value
            .pointer(pointer)
            .expect(&format!("missing: {} in: {}", pointer, value));
        assert!(actual.is_string(), "expected a string, got: {:?}", actual);
    }
}
