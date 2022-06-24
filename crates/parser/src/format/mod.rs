pub mod avro;
pub mod character_separated;
pub mod json;

use crate::config::ErrorThreshold;
use crate::decorate::{AddFieldError, Decorator};
use crate::input::{detect_compression, CompressionError, Input};
use crate::{Compression, Format, ParseConfig};

use serde_json::Value;
use std::io::{self, Write};

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

    #[error("failed to parse content: {0}")]
    Parse(#[from] Box<dyn std::error::Error>),

    #[error("unable to decompress input: {0}")]
    Decompression(#[from] CompressionError),

    #[error("error limit exceeded")]
    ErrorLimitExceeded(ErrorThreshold),
}

/// Runs format inference if the config does not specify a `format`. The expectation is that more
/// complex formats will also need to inspect the content in order to determine a recommended
/// parser configuration, and that this function will also drive that process. For example, the CSV
/// parser may inspect the content to determine the separator character, and return a base
/// ParseConfig including the inferred separator, which the user-provided config will be merged
/// onto.
#[tracing::instrument(level = "debug", skip(content))]
pub fn resolve_config(
    config: &ParseConfig,
    content: Input,
) -> Result<(Format, Compression, Input), ParseError> {
    let resolved_format = if let Some(f) = config.format.non_auto() {
        tracing::debug!("using user-provided format: {:?}", f);
        f.clone()
    } else {
        let inferred = determine_format(&config).ok_or_else(|| ParseError::CannotInferFormat)?;
        tracing::info!(format = %inferred, "inferred format");
        inferred
    };

    let (resolved_compression, input) = match determine_compression(&config) {
        Some(from_conf) => {
            tracing::debug!(compression = %from_conf, "determined compression from configuration");
            (from_conf, content)
        }
        None => {
            let (bytes, new_input) = content.peek(32)?;
            if let Some(from_file) = detect_compression(&bytes) {
                tracing::debug!(compression = %from_file, "determined compression from file contents");
                (from_file, new_input)
            } else {
                tracing::debug!("assuming content is uncompressed");
                (Compression::None, new_input)
            }
        }
    };

    Ok((resolved_format, resolved_compression, input))
}

/// Drives the parsing process using the given configuration, input, and output streams. The
/// `content` will be parsed according to the `config` and written in JSONL format to `dest`.
/// The given `config` will be used to override any default or recommended values.
pub fn parse(
    config: &ParseConfig,
    content: Input,
    dest: &mut impl io::Write,
) -> Result<(), ParseError> {
    let (resolved_format, resolved_compression, content) = resolve_config(config, content)?;
    tracing::debug!(format = ?resolved_format, compression = %resolved_compression, "resolved config");

    let parser = parser_for(resolved_format);

    // Do we need to decompress the input before sending it to the parser?
    let input = if parser.decompress() {
        content.decompressed(resolved_compression)?
    } else {
        content
    };

    let output = parser.parse(input)?;
    format_output(&config, output, dest)
}

fn parser_for(format: Format) -> Box<dyn Parser> {
    match format {
        Format::Auto(_) => character_separated::new_csv_parser(Default::default()),
        Format::Json(_) => json::new_parser(),
        Format::Csv(csv_config) => character_separated::new_csv_parser(csv_config),
        Format::W3cExtendedLog(_) => character_separated::new_w3c_extended_log_parser(),
        Format::Avro(_) => avro::new_parser(),
    }
}

/// A parser will produce a valid json document or a parser error.
type ParseResult = Result<Value, ParseError>;

/// Type of output returned by a parser, which will lazily return parsed JSON or an error. Once an
/// error is returned, the iterator will not be polled again.
pub type Output = Box<dyn Iterator<Item = ParseResult>>;

/// Parser is an object-safe trait for parsing a particular format, such as CSV or JSONL.
/// Implementations live in the various sub-modules.
pub trait Parser {
    //fn resolve_config<I>(config: &ParseConfig, content: Input) -> Result<ParseConfig, ParseError>;

    /// Returns true if the contents should be decompressed before being passed to the parser.
    /// Parsers that work directly with compressed file formats should implement this function to
    /// return `false`, so that files like .xlsx don't get automatically decompressed prior to
    /// being given to the parser.
    fn decompress(&self) -> bool {
        true
    }

    /// Parse the given `content`
    fn parse(&self, content: Input) -> Result<Output, ParseError>;
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
        let mut value = result?;

        decorator.add_fields(record_count, &mut value)?;
        serde_json::to_writer(&mut buffer, &value)?;
        buffer.write_all(&[b'\n'])?;
        record_count += 1;
    }
    buffer.flush()?;
    tracing::info!(record_count = record_count, "successfully finished parsing");
    Ok(())
}

/// Attempts to reoslve a Format using the the fields in the config.
fn determine_format(config: &ParseConfig) -> Option<Format> {
    Some(
        config
            .format // If format is set, then use whatever it says
            .clone(),
    )
    .filter(|f| !f.is_auto())
    .or_else(|| {
        // Try to determine based on file extension
        config.filename.as_deref().and_then(|filename| {
            extensions(filename).find_map(|ext| format_for_file_extension(ext))
        })
    })
    .or_else(|| {
        // Try to determine based on content-type
        config
            .content_type
            .as_deref()
            .and_then(|content_type| format_for_content_type(content_type))
    })
}

fn format_for_content_type(content_type: &str) -> Option<Format> {
    match content_type {
        "application/json" => Some(Format::Json(Default::default())),
        "text/json" => Some(Format::Json(Default::default())),
        "text/csv" => Some(Format::Csv(Default::default())),
        "text/tab-separated-values" => Some(Format::Csv(Default::default())),
        _ => None,
    }
}

fn format_for_file_extension(extension: &str) -> Option<Format> {
    match extension {
        "jsonl" | "json" => Some(Format::Json(Default::default())),
        "csv" => Some(Format::Csv(Default::default())),
        "tsv" => Some(Format::Csv(Default::default())),
        "avro" => Some(Format::Avro(Default::default())),
        _ => None,
    }
}

fn extensions(filename: &str) -> impl Iterator<Item = &str> {
    let start = filename
        .char_indices()
        .skip(1)
        .next()
        .map(|(i, _)| i)
        .unwrap_or_default();
    (&filename[start..]).split('.').rev()
}

fn determine_compression(config: &ParseConfig) -> Option<Compression> {
    if config.compression.as_ref().is_some() {
        return config.compression.as_ref().cloned();
    }
    config
        .compression
        .as_ref()
        .cloned()
        .or_else(|| {
            config
                .filename
                .as_deref()
                .and_then(compression_from_filename)
        })
        .or_else(|| {
            config
                .content_encoding
                .as_deref()
                .and_then(compression_from_content_encoding)
        })
        .or_else(|| {
            config
                .content_type
                .as_deref()
                .and_then(compression_from_content_type)
        })
}

fn compression_from_content_encoding(content_encoding: &str) -> Option<Compression> {
    match content_encoding.trim() {
        "gzip" => Some(Compression::Gzip),
        // TODO: Add support for deflate, br, and compress
        // deflate, confusingly, actually maps to zlib (rfc 1950)
        other => {
            if !other.is_empty() {
                tracing::warn!("ignoring unsupported content-encoding: {:?}", other);
            }
            None
        }
    }
}

fn compression_from_content_type(content_type: &str) -> Option<Compression> {
    // TODO: hoist the parsed Mime into the ParseConfig, so we can error when deserializing if
    // the content type string is invalid.
    content_type
        .parse::<mime::Mime>()
        .ok()
        .and_then(|ct| match ct.essence_str() {
            "application/gzip" => Some(Compression::Gzip),
            "application/zip" => Some(Compression::ZipArchive),
            _ => None,
        })
}

fn compression_from_filename(filename: &str) -> Option<Compression> {
    extensions(filename).find_map(|ext| match ext {
        "gz" => Some(Compression::Gzip),
        "zip" => Some(Compression::ZipArchive),
        _ => None,
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn compression_is_determined_from_filename() {
        let conf = ParseConfig {
            filename: "some.csv.zip".to_string().into(),
            // content encoding disagrees, but we ignore it
            content_encoding: "gzip".to_string().into(),
            // content_type disagrees, but we ignore it
            content_type: "application/gzip".to_string().into(),
            ..Default::default()
        };
        let result = determine_compression(&conf).expect("failed to determine compression");
        assert_eq!(Compression::ZipArchive, result);
    }

    #[test]
    fn compression_is_determined_from_content_encoding() {
        let conf = ParseConfig {
            // filename has no compression extension, so we look at content encoding
            filename: Some("some.csv".to_string()),
            content_encoding: Some("gzip".to_string()),
            // content type disagrees, but we ignore it
            content_type: Some("application/zip".to_string()),
            ..Default::default()
        };
        let result = determine_compression(&conf).expect("failed to determine compression");
        assert_eq!(Compression::Gzip, result);
    }

    #[test]
    fn compression_is_determined_from_content_type() {
        let conf = ParseConfig {
            filename: Some("some.csv".to_string()),
            content_encoding: Some("not-a-real-encoding".to_string()),
            content_type: Some("application/zip".to_string()),
            ..Default::default()
        };
        let result = determine_compression(&conf).expect("failed to determine compression");
        assert_eq!(Compression::ZipArchive, result);
    }

    #[test]
    fn format_is_determined_from_file_extension() {
        let mut conf = ParseConfig {
            filename: Some("whatever.json".to_string()),
            content_type: Some("xml or something lol".to_string()),
            ..Default::default()
        };
        assert_format_eq(Some(Format::Json(Default::default())), &conf);
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
        assert_format_eq(Some(Format::Json(Default::default())), &conf);
        conf.content_type = Some("text/json".to_string());
        assert_format_eq(Some(Format::Json(Default::default())), &conf);
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
