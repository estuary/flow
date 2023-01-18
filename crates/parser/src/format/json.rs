//! Parser for the json format. This will accept any stream of JSON values separated by whitespace.
//! It allows any amount of whitespace (including newlines) within and in between records.
use super::{Input, Output, ParseError, Parser};
use serde_json::Value;

struct JsonParser;

pub fn new_parser() -> Box<dyn Parser> {
    Box::new(JsonParser)
}

impl Parser for JsonParser {
    fn parse(&self, content: Input) -> Result<Output, ParseError> {
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
        let wrapped = JsonIter {
            inner: Box::new(deser),
            current_array: None,
        };
        Ok(Box::new(wrapped))
    }
}

struct JsonIter {
    inner: Box<dyn Iterator<Item = Result<Value, serde_json::Error>>>,
    current_array: Option<std::vec::IntoIter<Value>>,
}

impl Iterator for JsonIter {
    type Item = Result<Value, ParseError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(array) = self.current_array.as_mut() {
            if let Some(elem) = array.next() {
                if elem.is_object() {
                    return Some(Ok(elem));
                } else {
                    return Some(Err(ParseError::Parse(InvalidJsonType(elem).into())));
                }
            }
        }
        let next_elem = self.inner.next()?;
        match next_elem {
            Ok(Value::Array(array)) => {
                self.current_array = Some(array.into_iter());
                self.next()
            }
            Ok(other) if other.is_object() => Some(Ok(other)),
            Ok(other) => Some(Err(ParseError::Parse(InvalidJsonType(other).into()))),
            Err(e) => Some(Err(e.into())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("The JSON parser only supports objects or arrays of objects, found value: '{0}'")]
struct InvalidJsonType(Value);

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    fn test_input(content: impl Into<Vec<u8>>) -> Input {
        use std::io::Cursor;
        Input::Stream(Box::new(Cursor::new(content.into())))
    }

    #[test]
    fn top_level_array_is_unrolled() {
        let input = test_input(r#"[{"a": "b"}][{"c": "d"}]"#);
        let mut output = JsonParser
            .parse(input)
            .expect("must return output iterator");

        let first = output
            .next()
            .expect("expected a result")
            .expect("must parse object Ok");
        assert_eq!(json!({"a": "b"}), first);
        let second = output
            .next()
            .expect("expected a result")
            .expect("must parse object Ok");
        assert_eq!(json!({"c": "d"}), second);

        assert!(output.next().is_none());
    }

    #[test]
    fn error_is_returned_when_array_items_are_scalar() {
        let input = test_input(r#"[{"a": "b"}, true]"#);
        let mut output = JsonParser
            .parse(input)
            .expect("must return output iterator");
        // first item should be ok because it's an object
        output
            .next()
            .expect("expected a result")
            .expect("must parse object Ok");

        // Second item should error because it's scalar
        let err = output
            .next()
            .expect("expected a result")
            .expect_err("expected result to be an error");
        if let ParseError::Parse(e) = err {
            e.downcast_ref::<InvalidJsonType>()
                .expect("expected error to be InvalidJsonType");
        } else {
            panic!("expected a parse error, got a different variant: {:?}", err);
        }
    }

    #[test]
    fn error_is_returned_when_top_level_value_is_scalar() {
        let input = test_input("true");
        let mut output = JsonParser
            .parse(input)
            .expect("must return output iterator");
        let err = output
            .next()
            .expect("expected a result")
            .expect_err("expected result to be an error");
        if let ParseError::Parse(e) = err {
            e.downcast_ref::<InvalidJsonType>()
                .expect("expected error to be InvalidJsonType");
        } else {
            panic!("expected a parse error, got a different variant: {:?}", err);
        }
    }
}
