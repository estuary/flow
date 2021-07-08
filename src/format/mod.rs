mod json;

use crate::decorate::{AddFieldError, Decorator};
use crate::input::Input;
use crate::{Format, ParseConfig};
use serde_json::Value;
use std::io::{self, Write};
use std::path::Path;

/// Error type returned by all parse operations.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("format is empty")]
    MissingFormat,

    #[error(
        "unable to automatically determine the format. explicit format configuration required"
    )]
    CannotInferFormat,

    #[error("unsupported format: '{0}'")]
    UnsupportedFormat(String),

    #[error("failed to read stream: {0}")]
    Io(#[from] io::Error),

    #[error("failed to parse JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("adding fields to json: {0}")]
    AddFields(#[from] AddFieldError),
}

/// Runs format inference if the config does not specify a `format`. The expectation is that more
/// complex formats will also need to inspect the content in order to determine a recommended
/// parser configuration, and that this function will also drive that process. For example, the CSV
/// parser may inspect the content to determine the separator character, and return a base
/// ParseConfig including the inferred separator, which the user-provided config will be merged
/// onto.
#[tracing::instrument(level = "debug", skip(_content))]
pub fn resolve_config(config: &ParseConfig, _content: Input) -> Result<ParseConfig, ParseError> {
    let format = match config.format {
        Some(f) => f,
        None => {
            let tmp_config = ParseConfig::default().override_from(config);
            let resolved =
                determine_format(&tmp_config).ok_or_else(|| ParseError::CannotInferFormat)?;
            tracing::info!("inferred format: {}", resolved);
            resolved
        }
    };

    // TODO: lookup parser and ask it for a recommended ParseConfig based on the current config and
    // the content.
    // let recommended_config = parser_for(format).resolve_config(config, content)?;
    // ParseConfig::default().override_from(&recommended_config).with_format(format).override_from(config)
    Ok(ParseConfig::default()
        .with_format(format)
        .override_from(config))
}

/// Drives the parsing process using the given configuration, input, and output streams. The
/// `content` will be parsed according to the `config` and written in JSONL format to `dest`.
/// The given `config` will be used to override any default or recommended values.
pub fn parse(
    config: &ParseConfig,
    content: Input,
    dest: &mut impl io::Write,
) -> Result<(), ParseError> {
    // TODO: peek at the content and remove this empty placeholder
    let config = resolve_config(config, Input::Stream(Box::new(io::empty())))?;
    tracing::debug!(action = "resolved config", result = ?config);
    let format = config.format.ok_or(ParseError::MissingFormat)?;
    let parser = parser_for(format);
    let output = parser.parse(&config, content)?;
    format_output(&config, output, dest)
}

fn parser_for(format: Format) -> Box<dyn Parser> {
    match format {
        Format::Json => json::new_parser(),
    }
}

/// Type of output returned by a parser, which will lazily return parsed JSON or an error. Once an
/// error is returned, the iterator will not be polled again.
pub type Output = Box<dyn Iterator<Item = Result<Value, ParseError>>>;

/// Parser is an object-safe trait for parsing a particular format, such as CSV or JSONL.
/// Implementations live in the various sub-modules.
pub trait Parser {
    //fn resolve_config<I>(config: &ParseConfig, content: Input) -> Result<ParseConfig, ParseError>;

    /// Parse the given `content` using the `config`, which will already have been fully resolved.
    fn parse(&self, config: &ParseConfig, content: Input) -> Result<Output, ParseError>;
}

/// Takes the output of a parser and writes it to the given destination, generally stdout.
fn format_output(
    config: &ParseConfig,
    output: Output,
    dest: &mut impl io::Write,
) -> Result<(), ParseError> {
    let decorator = Decorator::from_config(config);
    let mut buffer = io::BufWriter::new(dest);
    let mut record_count = 0u64;
    for result in output {
        let mut value = result.map_err(|e| {
            tracing::warn!(
                record_count = record_count,
                "parsing failed after {} records",
                record_count
            );
            e
        })?;

        decorator.add_fields(record_count, &mut value)?;
        serde_json::to_writer(&mut buffer, &value)?;
        buffer.write(&[b'\n'])?;
        record_count += 1;
    }
    buffer.flush()?;
    tracing::info!(record_count = record_count, "successfully finished parsing");
    Ok(())
}

/// Attempts to reoslve a Format using the the fields in the config.
fn determine_format(config: &ParseConfig) -> Option<Format> {
    config
        .format // If format is set, then use whatever it says
        .clone()
        .or_else(|| {
            // Next try to lookup based on file extension. This will need to get a little more
            // sophisticated in order to handle things like foo.json.gz, but that's being ignored
            // for the moment since we don't handle decompression yet anyway.
            config
                .filename
                .as_deref()
                .and_then(|filename| {
                    AsRef::<Path>::as_ref(filename)
                        .extension()
                        .map(|e| e.to_str().unwrap())
                })
                .and_then(|ext| config.file_extension_mappings.get(ext).cloned())
        })
        .or_else(|| {
            config
                .content_type
                .as_deref()
                .and_then(|content_type| config.content_type_mappings.get(content_type).cloned())
        })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn format_is_determined_from_file_extension() {
        let mut conf = ParseConfig {
            filename: Some("whatever.json".to_string()),
            content_type: Some("xml or something lol".to_string()),
            ..Default::default()
        };
        assert_format_eq(Some(Format::Json), &conf);
        conf.filename = Some("nope.jason".to_string());
        assert_format_eq(None, &conf);
    }

    #[test]
    fn format_is_determined_from_content_type_when_it_cannot_be_determined_by_extension() {
        let mut conf = ParseConfig {
            filename: Some("whatever.whatever".to_string()),
            content_type: Some("application/json".to_string()),
            ..Default::default()
        };
        assert_format_eq(Some(Format::Json), &conf);
        conf.content_type = Some("text/json".to_string());
        assert_format_eq(Some(Format::Json), &conf);
        conf.content_type = Some("wat".to_string());
        assert_format_eq(None, &conf);
    }

    fn assert_format_eq(expected: Option<Format>, config: &ParseConfig) {
        let actual = determine_format(config);
        assert_eq!(
            expected, actual,
            "incorrect format determined from config: {:?}",
            config
        );
    }
}
