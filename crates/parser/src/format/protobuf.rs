use crate::config::protobuf::ProtobufConfig;
use crate::format::{Output, ParseError, Parser};
use crate::input::Input;
use protobuf::{CodedInputStream, MessageDyn};
use protobuf_json_mapping::{PrintError, PrintOptions};
use serde_json::Value;

use std::fs;

use protobuf::reflect::FileDescriptor;

#[derive(Debug)]
pub struct ProtobufParser {
    config: ProtobufConfig,
}

pub fn new_protobuf_parser(config: ProtobufConfig) -> Box<dyn Parser> {
    Box::new(ProtobufParser { config })
}

impl Parser for ProtobufParser {
    fn parse(&self, input: Input) -> Result<Output, ParseError> {
        let tmp = tempfile::tempdir()?;
        let tempfile = tmp.path().join("parser.proto");
        fs::write(&tempfile, self.config.proto_file_content.as_bytes())?;

        let parsed_proto = protobuf_parse::Parser::new()
            // use the pure-rust parser instead of shelling out to protoc
            .pure()
            // must set an include dir that is a parent of the input
            .includes(&[tmp.path().to_path_buf()])
            .input(&tempfile)
            .parse_and_typecheck()
            .map_err(|e| ProtobufParseError::ProtoParseFailed(e.into()))?;

        tracing::debug!(
            ?parsed_proto.file_descriptors,
            "successfully parsed proto definitions from config"
        );

        let message_descriptor =
            FileDescriptor::new_dynamic_fds(parsed_proto.file_descriptors, &[])
                .map_err(|err| ParseError::Parse(Box::new(err)))?
                .into_iter()
                .flat_map(|file_descriptor| {
                    file_descriptor.message_by_package_relative_name(self.config.message.as_str())
                })
                .next()
                .ok_or_else(|| ProtobufParseError::NoSuchMessage(self.config.message.clone()))?;

        let mut buffered_stream = input.into_stream();
        let mut cis: CodedInputStream<'_> = CodedInputStream::new(&mut buffered_stream);

        let mut message = message_descriptor.new_instance();
        message
            .merge_from_dyn(&mut cis)
            .map_err(ProtobufParseError::ProtobufParse)?;

        Ok(Box::new(ProtobufJsonIter { msg: Some(message) }))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtobufParseError {
    #[error("failed to parse payload as a protobuf message: {0}")]
    ProtobufParse(protobuf::Error),
    #[error("could not parse protobuf definitions: {0}")]
    ProtoParseFailed(Box<dyn std::error::Error>),
    #[error("no such message '{0}' found in proto file")]
    NoSuchMessage(String),
    #[error("cannot serialize protobuf message to JSON: {0}")]
    PrintError(PrintError),
}

impl From<ProtobufParseError> for ParseError {
    fn from(proto_err: ProtobufParseError) -> ParseError {
        ParseError::Parse(Box::new(proto_err))
    }
}

struct ProtobufJsonIter {
    msg: Option<Box<dyn MessageDyn>>,
}

impl Iterator for ProtobufJsonIter {
    type Item = Result<Value, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let message = self.msg.take()?;

        let print_options = PrintOptions {
            enum_values_int: false,
            proto_field_name: false,
            always_output_default_values: false,
            ..Default::default()
        };
        let result = protobuf_json_mapping::print_to_string_with_options(&*message, &print_options)
            .map_err(ProtobufParseError::PrintError);

        match result {
            Ok(json_str) => {
                let value: Value = serde_json::from_str(&json_str)
                    .expect("internal error re-parsing json-encoded proto message");
                Some(Ok(value))
            }
            Err(err) => Some(Err(err.into())),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn proto_file_is_parsed() {
        let proto_file =
            fs::read_to_string(crate::test::path("tests/examples/gtfs-realtime.proto")).unwrap();

        let config = ProtobufConfig {
            proto_file_content: proto_file,
            message: "FeedMessage".to_string(),
        };
        let input = Input::File(
            fs::File::open(crate::test::path("tests/examples/vehicle-positions.pb")).unwrap(),
        );
        let parser = new_protobuf_parser(config);
        let mut output = parser.parse(input).expect("parse failed");
        let json = output
            .next()
            .expect("first output must be Some")
            .expect("first output erred");
        insta::assert_json_snapshot!(json);
        assert!(output.next().is_none());
    }
}
