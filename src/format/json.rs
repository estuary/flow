//! Parser for the json format. This is similar to jsonl, but more lax because it allows any amount
//! of whitespace (including newlines) within and in between records. The source offset for this
//! parser is in terms of bytes, not lines, unlike jsonl.
use super::{Input, Output, ParseError, Parser};
use crate::decorate::Decorator;
use crate::ParseConfig;
use serde_json::Value;

struct JsonParser;

pub fn new_parser() -> Box<dyn Parser> {
    Box::new(JsonParser)
}

impl Parser for JsonParser {
    fn parse(&self, config: &ParseConfig, content: Input) -> Result<Output, ParseError> {
        let deser = serde_json::de::Deserializer::from_reader(content).into_iter();
        let decorator = Decorator::from_config(config);
        Ok(Box::new(JsonIter {
            prev_byte_offset: 0,
            decorator,
            deser,
        }))
    }
}

struct JsonIter {
    prev_byte_offset: u64,
    decorator: Decorator,
    deser: serde_json::de::StreamDeserializer<'static, serde_json::de::IoRead<Input>, Value>,
}

impl Iterator for JsonIter {
    type Item = Result<Value, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        let offset = self.prev_byte_offset;
        let result = self
            .deser
            .next()? // return if deser returns None
            .map_err(Into::into)
            .and_then(|mut value| {
                self.decorator
                    .add_fields(Some(offset), &mut value)
                    .map_err(Into::into)
                    .map(|_| value)
            });
        // update the offset to just after the end of the record we just deserialized
        self.prev_byte_offset = self.deser.byte_offset() as u64;
        Some(result)
    }
}
