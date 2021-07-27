//! Parsers for character-separated formats like csv.
use super::{Format, Output, ParseError, Parser};
use crate::config::{
    csv::{CharacterSeparatedConfig, LineEnding},
    ParseConfig,
};
use crate::format::projection::{build_projections, TypeInfo};
use crate::input::{detect_encoding, Input};
use csv::{Reader, StringRecord, Terminator};
use doc::Pointer;
use json::schema::types;
use serde_json::Value;
use std::io;

struct CsvParser {
    /// The specific format associated with this parser. Used to lookup the correct configuration
    /// section.
    format: Format,
    /// The default value used to separate values in a row.
    default_delimiter: u8,
}

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
        let mut projections = build_projections(config)?;
        let user_provided_config = get_config(self.format, config).cloned().unwrap_or_default();
        // Transcode into UTF-8 before attempting to parse the CSV. This simplifies a lot, since
        // our ultimate target is JSON in UTF-8, and since the configuration is also provided as
        // JSON in UTF-8.
        let (content_peek, input) = content.peek(2148)?;
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
        builder.delimiter(delim);

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

        // Associate each header with projection information. This is needed in order to construct
        // a potentially nested JSON document from the tabular data. If there's no projection
        // information available for a given field, then we'll use a default projection that simply
        // uses the column name as the JSON property name and permits any type of value. This is so
        // that the parser can at least do a basic CSV to JSON conversion without having any prior
        // knowledge about the desired shape of the JSON.
        let mut columns = Vec::new();
        for name in headers {
            let projection = projections.remove(&name).unwrap_or_else(|| {
                let location = String::from("/") + name.as_str();
                TypeInfo {
                    possible_types: types::ANY,
                    must_exist: false,
                    target_location: Pointer::from_str(&location),
                }
            });
            columns.push(Header { name, projection });
        }
        tracing::info!("Resolved column headers: {:?}", columns);

        Ok(Box::new(CsvOutput {
            headers: columns,
            reader,
            current_row: csv::StringRecord::new(),
            row_num: 0,
        }))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("value {0:?} is not valid for: {1:?}")]
    InvalidType(String, TypeInfo),

    #[error("failed to parse character-separated content: {0}")]
    InvalidContent(#[from] csv::Error),

    #[error("row {0} is missing required column: {1:?}")]
    MissingColumn(u64, String),

    #[error("cannot construct a JSON object from row {0} because it's impossible to create the location {2:?} within the document: {1}")]
    InvalidStructure(u64, Value, String),
}

fn box_err<E: Into<Error>>(err: E) -> Box<dyn std::error::Error> {
    Box::new(err.into())
}

const PARSE_ORDER: &[TargetType] = &[
    // Try null before string so that fields that allow either null or string will end up as null
    // if a field is empty.
    TargetType::Null,
    // Always attempt integer before fractional.
    TargetType::Integer,
    TargetType::Number,
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
    Number,
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
            TargetType::Number => types::INT_OR_FRAC,
            TargetType::String => types::STRING,
            TargetType::Boolean => types::BOOLEAN,
        }
    }
}

/// Encapsulates a specific named column and associated Projection information.
#[derive(Debug, Clone)]
struct Header {
    name: String,
    projection: TypeInfo,
}

impl Header {
    fn parse(&self, value: &str) -> Result<Value, Error> {
        for possible_type in PARSE_ORDER {
            if possible_type
                .to_set()
                .overlaps(self.projection.possible_types)
            {
                if let Ok(parsed) = self.parse_as_type(value, *possible_type) {
                    return Ok(parsed);
                }
            }
        }
        Err(Error::InvalidType(
            value.to_string(),
            self.projection.clone(),
        ))
    }

    fn parse_as_type(
        &self,
        value: &str,
        target_type: TargetType,
    ) -> Result<Value, serde_json::Error> {
        match target_type {
            TargetType::Null => {
                if value.is_empty() {
                    Ok(Value::Null)
                } else {
                    Err(serde::de::Error::custom("expected empty value"))
                }
            }
            TargetType::Array => serde_json::from_str::<Vec<Value>>(value).map(|a| Value::Array(a)),
            TargetType::Object => serde_json::from_str::<serde_json::Map<String, Value>>(value)
                .map(|o| Value::Object(o)),
            TargetType::Integer | TargetType::Number => {
                serde_json::from_str::<serde_json::Number>(value).map(|n| Value::Number(n))
            }
            TargetType::Boolean => serde_json::from_str::<bool>(value).map(|b| Value::Bool(b)),
            TargetType::String => Ok(Value::String(value.to_string())),
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
