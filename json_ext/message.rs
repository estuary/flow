use serde_json as sj;
use thiserror;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to parse JSON: {0}")]
    JsonErr(#[from] sj::Error),
}

/// Builder builds instances of Message.
pub struct Builder {
    uuid_ptr: String,
}

pub struct Message<'b> {
    pub builder: &'b Builder,
    value: sj::Value,
}

impl Builder {
    pub fn new_acknowledgement<'b>(&self) -> Message {
        Message {
            builder: self,
            value: sj::Value::Null,
        }
    }

    pub fn from_json_slice<'b>(&self, b: &[u8]) -> Result<Message, Error> {
        Ok(Message {
            builder: self,
            value: sj::from_slice(b)?,
        })
    }
}
