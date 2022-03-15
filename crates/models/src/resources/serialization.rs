use bytes::Bytes;
use serde::Serialize;

use crate::resources::{ContentFormat, ContentType, ResourceDef};

pub fn content_schema(_: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    serde_json::from_value(serde_json::json!({
        "type": "string",
    }))
    .unwrap()
}

impl Serialize for ResourceDef {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("resourceDef", 2)?;
        state.serialize_field("contentType", &self.content_type)?;

        match self.content_type {
            ContentType::Catalog(ContentFormat::Json)
            | ContentType::JsonSchema(ContentFormat::Json)
            | ContentType::Config(ContentFormat::Json) => {
                // Content that is already formatted as json should not be
                // escaped. This will allow clients to immediately parse it as
                // json along with the rest of the surrounding document.
                state.serialize_field("content", &JsonBytes::new(&self.content))?
            }
            ContentType::Catalog(ContentFormat::Yaml)
            | ContentType::JsonSchema(ContentFormat::Yaml)
            | ContentType::Config(ContentFormat::Yaml)
            | ContentType::TypescriptModule
            | ContentType::DocumentsFixture => {
                // Content that is not json but is guaranteed to be valid utf-8
                // encoded strings can be written as a string. Escape sequences
                // will be inserted to produce a valid json payload, but
                // otherwise the content will be human readable.
                state.serialize_field("content", &StringBytes::new(&self.content))?
            }
            ContentType::NpmPackage => {
                // Content that cannot be guaranteed to be valid utf-8 should be
                // base64 encoded before being serialized. This makes the
                // content opaque to humans reading it, but will always produce
                // valid json payloads.
                state.serialize_field("content", &Base64Bytes::new(&self.content))?
            }
        };

        state.end()
    }
}

#[derive(Serialize)]
#[serde(transparent)]
struct Base64Bytes {
    #[serde(serialize_with = "as_base64")]
    bytes: Bytes,
}

impl Base64Bytes {
    pub fn new(bytes: &Bytes) -> Self {
        Self {
            bytes: bytes.clone(),
        }
    }
}

fn as_base64<T, S>(bytes: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: AsRef<[u8]>,
    S: serde::Serializer,
{
    serializer.serialize_str(&base64::encode(bytes.as_ref()))
}

#[derive(Serialize)]
#[serde(transparent)]
struct StringBytes {
    #[serde(serialize_with = "as_str")]
    bytes: Bytes,
}

impl StringBytes {
    pub fn new(bytes: &Bytes) -> Self {
        Self {
            bytes: bytes.clone(),
        }
    }
}

fn as_str<T, S>(bytes: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: AsRef<[u8]>,
    S: serde::Serializer,
{
    serializer.serialize_str(std::str::from_utf8(bytes.as_ref()).map_err(|err| {
        serde::ser::Error::custom(format!("could not encode bytes as utf-8 string: {}", err))
    })?)
}

#[derive(Serialize)]
#[serde(transparent)]
struct JsonBytes {
    #[serde(serialize_with = "as_json_str")]
    bytes: Bytes,
}

impl JsonBytes {
    pub fn new(bytes: &Bytes) -> Self {
        Self {
            bytes: bytes.clone(),
        }
    }
}

fn as_json_str<T, S>(bytes: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: AsRef<[u8]>,
    S: serde::Serializer,
{
    let string = String::from_utf8(bytes.as_ref().to_vec()).map_err(|err| {
        serde::ser::Error::custom(format!("could not encode bytes as utf-8 string: {}", err))
    })?;
    let raw_value = serde_json::value::RawValue::from_string(string)
        .map_err(|err| serde::ser::Error::custom(format!("bytes were not valid json: {}", err)))?;
    raw_value.serialize(serializer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_json_resource() {
        let input = r#"{
    "foo": "bar",
    "baz": [1, 2]
}"#;
        let resource = ResourceDef {
            content_type: ContentType::Config(ContentFormat::Json),
            content: Bytes::from_static(input.as_bytes()),
        };

        insta::assert_json_snapshot!(&resource);
    }

    #[test]
    fn serialize_yaml_resource() {
        let input = r#"# yaml content
foo: "bar",
"baz":
  - 1
  - 2
"#;
        let resource = ResourceDef {
            content_type: ContentType::Config(ContentFormat::Yaml),
            content: Bytes::from_static(input.as_bytes()),
        };

        insta::assert_json_snapshot!(&resource);
    }

    #[test]
    fn serialize_base64_resource() {
        let input = b"binary content of a zip archive
            with invalid utf8 sequences like \xFF or \xC2\xC0 which
            cannot be embedded directly in utf8 strings";

        let resource = ResourceDef {
            content_type: ContentType::NpmPackage,
            content: Bytes::from_static(input),
        };

        insta::assert_json_snapshot!(&resource);
    }
}
