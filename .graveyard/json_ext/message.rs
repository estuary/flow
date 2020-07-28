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

/// Message is a JSON document which carries a Gazette UUID
/// at a specified document location.
pub struct Message {
    pub doc: sj::Value,
    pub uuid_ptr: ptr::Pointer,
}

impl Message {
    /// Builds a new Message with an empty document.
    pub fn new(uuid_ptr: ptr::Pointer) -> Message {
        Message {
            doc: sj::Value::Null,
            uuid_ptr,
        }
    }

    /// Builds a new Message parsed from the given JSON slice,
    /// with a validated UUID.
    pub fn from_json_slice(uuid_ptr: ptr::Pointer, b: &[u8]) -> Result<Message, Error> {
        let mut doc = sj::from_slice(b)?;

        // Verify existing UUID, or initialize with Null.
        let uuid_loc = uuid_ptr.create(&mut doc);

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

        Ok(Message { uuid_ptr, doc })
    }

    /// Returns the UUID of the Message. If a UUID does not exist at the
    /// expected location, a "Nil" zero-valued UUID is returned.
    pub fn get_uuid(&self) -> uuid::Uuid {
        return match self.uuid_ptr.query(&self.doc) {
            Some(sj::Value::String(uuid_str)) => uuid::Uuid::parse_str(uuid_str).unwrap(),
            None | Some(sj::Value::Null) => uuid::Uuid::nil(),
            Some(vv @ _) => panic!("UUID value is not a string: {:?}", vv),
        };
    }

    /// Sets or replaces the UUID of the Message.
    /// UUIDs are encoded in lower-case, hyphenated form.
    pub fn set_uuid(&mut self, to: uuid::Uuid) {
        let uuid_loc = self.uuid_ptr.create(&mut self.doc).unwrap();
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
    fn test_new_empty_msg() -> Ret {
        let mut msg = Message::new("/_hdr/uuid".try_into()?);

        // Message is initially null.
        assert_eq!(msg.doc, sjv::Null);
        assert_eq!(msg.get_uuid(), uuid::Uuid::nil());

        // Expect a set UUID is get-able and updates the document.
        msg.set_uuid(uuid::Uuid::parse_str(A_UUID)?);
        assert_eq!(msg.get_uuid(), uuid::Uuid::parse_str(A_UUID)?);
        assert_eq!(msg.doc, sj::json!({"_hdr": {"uuid": A_UUID}}));

        Ok(())
    }

    #[test]
    fn test_parsed_msg_has_uuid() -> Ret {
        let raw = r#"
        {
            "_hdr": {"uuid": "936da01f-9abd-4d9d-80c7-02af85c822a8"},
            "name": "John Doe",
            "age": 43
        }"#;
        let msg = Message::from_json_slice("/_hdr/uuid".try_into()?, raw.as_bytes())?;

        assert_eq!(msg.get_uuid(), uuid::Uuid::parse_str(A_UUID)?);
        assert_eq!(msg.doc.pointer("/name").unwrap(), &sj::json!("John Doe"));
        Ok(())
    }

    #[test]
    fn test_parsed_msg_has_no_uuid() -> Ret {
        let raw = r#"
        {
            "name": "John Doe",
            "age": 43
        }"#;
        let msg = Message::from_json_slice("/_hdr/uuid".try_into()?, raw.as_bytes())?;

        // UUID is interpreted as zero-valued.
        assert_eq!(msg.get_uuid(), uuid::Uuid::nil());

        // A Null JSON value is explictly set.
        assert_eq!(
            msg.doc.pointer("/_hdr").unwrap(),
            &sj::json!({ "uuid": sjv::Null })
        );
        assert_eq!(msg.doc.pointer("/name").unwrap(), &sj::json!("John Doe"));

        // An explicit Null UUID is also permitted.
        let msg = Message::from_json_slice(msg.uuid_ptr, r#"{"_hdr": {"uuid": null}}"#.as_bytes())?;
        assert_eq!(&msg.doc, &sj::json!({"_hdr": {"uuid": sjv::Null}}));

        Ok(())
    }

    #[test]
    fn test_parse_with_bad_uuid_structure() {
        let u_p: ptr::Pointer = "/_hdr/uuid".try_into().unwrap();

        match Message::from_json_slice(u_p.clone(), r#"{"_hdr": []}"#.as_bytes()) {
            Err(Error::UuidBadLocation) => (),
            _ => panic!("expected bad UUID location"),
        };
        match Message::from_json_slice(u_p, r#"{"_hdr": {"uuid": 123}}"#.as_bytes()) {
            Err(Error::UuidNotAString) => (),
            _ => panic!("expected UUID not a string"),
        };
    }

    #[test]
    fn test_parse_with_bad_json() {
        let u_p = "/uuid".try_into().unwrap();
        match Message::from_json_slice(u_p, "{invalid json".as_bytes()) {
            Err(Error::JsonErr(_)) => (),
            _ => panic!("expected JSON error"),
        };
    }

    #[test]
    fn test_parse_bad_uuid() {
        let u_p = "/uuid".try_into().unwrap();
        match Message::from_json_slice(u_p, r#"{"uuid": "invalid"}"#.as_bytes()) {
            Err(Error::UuidErr(_)) => (),
            _ => panic!("expected UUID error"),
        };
    }
}
