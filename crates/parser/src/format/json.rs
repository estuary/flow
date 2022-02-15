//! Parser for the json format. This will accept any stream of JSON values separated by whitespace.
//! It allows any amount of whitespace (including newlines) within and in between records.
use super::{Input, Output, ParseError, Parser};
use crate::ParseConfig;

struct JsonParser;

pub fn new_parser() -> Box<dyn Parser> {
    Box::new(JsonParser)
}

impl Parser for JsonParser {
    fn parse(&self, _config: &ParseConfig, content: Input) -> Result<Output, ParseError> {
        // The JSON RFC (RFC 4627) specifies that JSON content is "unicode", but explicitly allows
        // for UTF-16 and 32 encoding schemes in addition to the default of UTF-8. Technically,
        // we're being too permissive here since `transcode_non_utf8` will accept basically any
        // encoding, but I don't see the value of restricting the possible source encodings.
        // Unfortunately, encoding_rs doesn't actually handle utf-32, though
        let input = content
            // only look at the first 32 bytes to determine encoding, since it's most likely only
            // going to look for a BOM.
            .transcode_non_utf8(None, 32)?
            .into_buffered_stream(8192);
        let deser = serde_json::de::Deserializer::from_reader(input).into_iter();
        let wrapped = deser.map(|r| r.map_err(Into::into));
        Ok(Box::new(wrapped))
    }
}
