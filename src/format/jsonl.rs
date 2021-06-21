use super::{Input, Output, ParseError, Parser};
use crate::decorate::Decorator;
use crate::ParseConfig;
use serde_json::Value;
use std::io::{self, BufRead};

struct JsonlParser;

pub fn new_parser() -> Box<dyn Parser> {
    Box::new(JsonlParser)
}

impl Parser for JsonlParser {
    fn parse(&self, config: &ParseConfig, content: Input) -> Result<Output, ParseError> {
        let lines = io::BufReader::new(content);
        Ok(Box::new(JsonlIter {
            lines,
            decorator: Decorator::from_config(config),
            line_number: 0,
            line_buf: Vec::with_capacity(512),
        }))
    }
}

struct JsonlIter {
    decorator: Decorator,
    lines: std::io::BufReader<Input>,
    line_number: u64,
    line_buf: Vec<u8>,
}

impl Iterator for JsonlIter {
    type Item = Result<Value, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_line()? {
            Ok(()) => Some(self.process_line()),
            Err(e) => return Some(Err(e)),
        }
    }
}

impl JsonlIter {
    fn process_line(&self) -> Result<Value, ParseError> {
        let mut value: Value = serde_json::from_slice(self.line_buf.as_slice())?;
        self.decorator
            .add_fields(Some(self.line_number), &mut value)?;
        Ok(value)
    }

    fn read_line(&mut self) -> Option<Result<(), ParseError>> {
        let JsonlIter {
            ref mut lines,
            ref mut line_buf,
            ref mut line_number,
            ..
        } = self;
        *line_number += 1;
        line_buf.clear();
        match lines.read_until(b'\n', line_buf) {
            Ok(0) => None,
            Ok(_) => {
                if line_buf
                    .iter()
                    .rposition(|&b| b != b'\n' && b != b'\r')
                    .is_some()
                {
                    Some(Ok(()))
                } else {
                    None
                }
            }
            Err(e) => Some(Err(ParseError::from(e).locate_line(*line_number))),
        }
    }
}
