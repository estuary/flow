mod jsonl;

use crate::decorate::AddFieldError;
use crate::{Format, ParseConfig};
use serde_json::Value;
use std::io;
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

    #[error("at line number: {0}: {1}")]
    AtLine(u64, Box<ParseError>),
    #[error("failed to read stream: {0}")]
    Io(#[from] io::Error),

    #[error("failed to parse JSON: {0}")]
    Json(#[from] serde_json::Error),

    #[error("adding fields to json: {0}")]
    AddFields(#[from] AddFieldError),
}

impl ParseError {
    fn locate_line(self, line: u64) -> Self {
        match self {
            ParseError::AtLine(_, err) => ParseError::AtLine(line, err),
            other => ParseError::AtLine(line, Box::new(other)),
        }
    }
}

/// Runs format inference if the config does not specify a `format`. The expectation is that more
/// complex formats will also need to inspect the content in order to determine a recommended
/// parser configuration, and that this function will also drive that process. For example, the CSV
/// parser may inspect the content to determine the separator character, and return a base
/// ParseConfig including the inferred separator, which the user-provided config will be merged
/// onto.
pub fn resolve_config(config: &ParseConfig, _content: Input) -> Result<ParseConfig, ParseError> {
    let format = match config.format {
        Some(f) => f,
        None => {
            let tmp_config = ParseConfig::default().override_from(config);
            let resolved =
                determine_format(&tmp_config).ok_or_else(|| ParseError::CannotInferFormat)?;
            log::info!("inferred format: {}", resolved);
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
    dest: Box<dyn io::Write>,
) -> Result<(), ParseError> {
    log::debug!("using base config: {:?}", config);
    // TODO: peek at the content and remove this empty placeholder
    let config = resolve_config(config, Box::new(io::empty()))?;
    log::debug!("resolved config: {:?}", config);
    let format = config.format.ok_or(ParseError::MissingFormat)?;
    let parser = parser_for(format);
    let output = parser.parse(&config, content)?;
    format_output(output, dest)
}

fn parser_for(format: Format) -> Box<dyn Parser> {
    match format {
        Format::Jsonl => jsonl::new_parser(),
    }
}

/// Type of content input provided to parsers. We use a trait object here so that we have
/// flexibility to read from different implementations without needing parsers to be generic over
/// the type of input.
pub type Input = Box<dyn io::Read>;

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
fn format_output(output: Output, mut dest: Box<dyn io::Write>) -> Result<(), ParseError> {
    for result in output {
        let value = result?;
        serde_json::to_writer(&mut dest, &value)?;
        dest.write_all(&[b'\n'])?;
        // This flush is necessary in cases where the caller of the parser needs to associate lines
        // of input and ouput. For example, the kinesis connector may expect that each kinesis
        // records maps 1-1 with a single output line, so we need to ensure that every line is
        // flushed, or else the caller may wait indefinitely on an output that's sitting in the
        // stdout buffer. Eventually it will probably make sense to make this behavior configurable
        // so we can avoid unnecessary calls to flush when possible.
        dest.flush()?;
    }
    Ok(())
}

/// Attempts to reoslve a Format using the remainder of the fields in the config.
fn determine_format(config: &ParseConfig) -> Option<Format> {
    let from_ext = config
        .filename
        .as_deref()
        .and_then(|filename| {
            AsRef::<Path>::as_ref(filename)
                .extension()
                .map(|e| e.to_str().unwrap())
        })
        .and_then(|ext| config.file_extension_mappings.get(ext).cloned());
    if from_ext.is_some() {
        return from_ext;
    }

    None
}
