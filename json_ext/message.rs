use super::ptr;
use serde_json as sj;
use thiserror;
use uuid;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to parse JSON: {0}")]
    JsonErr(#[from] sj::Error),
    #[error("message UUID location is invalid within JSON document structure")]
    UuidBadLocation,
    #[error("message UUID location is not a string or null")]
    UuidNotAString,
    #[error("failed to parse UUID: {0}")]
    UuidErr(#[from] uuid::Error),
}

/// Builder builds instances of Message.
pub struct Builder {
    uuid_ptr: ptr::Pointer,
}

/// Message is a JSON document which carries a validated UUID
/// at a document location specified by the Message Builder.
pub struct Message<'b> {
    pub builder: &'b Builder,
    pub doc: sj::Value,
}

impl Builder {
    /// Builds and returns a Builder which constructs Message instances
    /// having the given document UUID location.
    pub fn new(uuid_ptr: ptr::Pointer) -> Builder {
        Builder { uuid_ptr }
    }

    /// Returns a Message with a Null document root.
    pub fn build(&self) -> Message {
        let mut doc = sj::Value::Null;

        // Initialize UUID location with Null.
        *self.uuid_ptr.create(&mut doc).unwrap() = sj::Value::Null;

        Message {
            builder: self,
            doc: doc,
        }
    }

    /// Returns a Message parsed from the given JSON slice.
    pub fn from_json_slice(&self, b: &[u8]) -> Result<Message, Error> {
        let mut doc = sj::from_slice(b)?;

        // Verify existing UUID, or initialize with Null.
        let uuid_loc = self.uuid_ptr.create(&mut doc);

        match uuid_loc {
            Some(sj::Value::String(uuid_str)) => {
                uuid::Uuid::parse_str(uuid_str)?;
            }
            Some(sj::Value::Null) => {
                // No-op.
            }
            Some(_) => return Err(Error::UuidNotAString),
            None => return Err(Error::UuidBadLocation),
        }

        Ok(Message {
            builder: self,
            doc: doc,
        })
    }
}

impl<'b> Message<'b> {
    /// Returns the UUID of the Message. If a UUID does not exist at the
    /// expected location, a "Nil" zero-valued UUID is returned.
    pub fn get_uuid(&self) -> uuid::Uuid {
        return match self.builder.uuid_ptr.query(&self.doc) {
            Some(sj::Value::String(uuid_str)) => uuid::Uuid::parse_str(uuid_str).unwrap(),
            Some(sj::Value::Null) => uuid::Uuid::nil(),
            Some(_) | None => panic!("Message should only hold validated UUIDs"),
        };
    }

    /// Sets or replaces the UUID of the Message.
    /// UUIDs are encoded in lower-case, hyphenated form.
    pub fn set_uuid(&mut self, to: uuid::Uuid) {
        let uuid_loc = self.builder.uuid_ptr.create(&mut self.doc).unwrap();
        *uuid_loc = sj::Value::String(
            to.to_hyphenated()
                .encode_lower(&mut uuid::Uuid::encode_buffer())
                .to_owned(),
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use sj::Value as sjv;
    use std::convert::TryInto;

    static A_UUID: &str = "936da01f-9abd-4d9d-80c7-02af85c822a8";
    type Ret = Result<(), Box<dyn std::error::Error>>;

    #[test]
    fn test_build_empty_msg() -> Ret {
        let b = Builder::new("/_hdr/uuid".try_into()?);
        let mut msg = b.build();

        // Expect UUID location is initialized with Null.
        assert_eq!(msg.doc, sj::json!({"_hdr": {"uuid": sjv::Null}}));

        // Expect a set UUID is get-able and updates the document.
        msg.set_uuid(uuid::Uuid::parse_str(A_UUID)?);
        assert_eq!(msg.get_uuid(), uuid::Uuid::parse_str(A_UUID)?);
        assert_eq!(msg.doc, sj::json!({"_hdr": {"uuid": A_UUID}}));

        Ok(())
    }

    #[test]
    fn test_parse_msg_has_uuid() -> Ret {
        let raw = r#"
        {
            "_hdr": {"uuid": "936da01f-9abd-4d9d-80c7-02af85c822a8"},
            "name": "John Doe",
            "age": 43
        }"#;
        let b = Builder::new("/_hdr/uuid".try_into()?);
        let msg = b.from_json_slice(raw.as_bytes())?;

        assert_eq!(msg.get_uuid(), uuid::Uuid::parse_str(A_UUID)?);
        assert_eq!(msg.doc.pointer("/name").unwrap(), &sj::json!("John Doe"));
        Ok(())
    }

    #[test]
    fn test_parse_no_uuid() -> Ret {
        let raw = r#"
        {
            "name": "John Doe",
            "age": 43
        }"#;
        let b = Builder::new("/_hdr/uuid".try_into()?);
        let msg = b.from_json_slice(raw.as_bytes())?;

        // UUID is interpreted as zero-valued.
        assert_eq!(msg.get_uuid(), uuid::Uuid::nil());

        // A Null JSON value is explictly set.
        assert_eq!(
            msg.doc.pointer("/_hdr").unwrap(),
            &sj::json!({ "uuid": sjv::Null })
        );
        assert_eq!(msg.doc.pointer("/name").unwrap(), &sj::json!("John Doe"));

        // An explicit Null UUID is also permitted.
        let msg = b.from_json_slice(r#"{"_hdr": {"uuid": null}}"#.as_bytes())?;
        assert_eq!(&msg.doc, &sj::json!({"_hdr": {"uuid": sjv::Null}}));

        Ok(())
    }

    #[test]
    fn test_parse_bad_uuid_structure() {
        let b = Builder::new("/_hdr/uuid".try_into().unwrap());

        match b.from_json_slice(r#"{"_hdr": []}"#.as_bytes()) {
            Err(Error::UuidBadLocation) => (),
            _ => panic!("expected bad UUID location"),
        };
        match b.from_json_slice(r#"{"_hdr": {"uuid": 123}}"#.as_bytes()) {
            Err(Error::UuidNotAString) => (),
            _ => panic!("expected UUID not a string"),
        };
    }

    #[test]
    fn test_parse_bad_json() {
        let b = Builder::new("/uuid".try_into().unwrap());
        match b.from_json_slice("{invalid json".as_bytes()) {
            Err(Error::JsonErr(_)) => (),
            _ => panic!("expected JSON error"),
        };
    }

    #[test]
    fn test_parse_bad_uuid() {
        let b = Builder::new("/uuid".try_into().unwrap());
        match b.from_json_slice(r#"{"uuid": "invalid"}"#.as_bytes()) {
            Err(Error::UuidErr(_)) => (),
            _ => panic!("expected UUID error"),
        };
    }
}
