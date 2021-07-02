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
        let deser = serde_json::de::Deserializer::from_reader(content).into_iter();
        let wrapped = deser.map(|r| r.map_err(Into::into));
        Ok(Box::new(wrapped))
    }
}
