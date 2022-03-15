use bytes::Bytes;
use serde::de::{MapAccess, Visitor};
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::resources::ResourceDef;
use crate::{ContentFormat, ContentType};

static RESOURCE_DEF_FIELDS: &[&str] = &["contentType", "content"];

impl<'de> Deserialize<'de> for ResourceDef {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_struct("ResourceDef", RESOURCE_DEF_FIELDS, ResourceDefVisitor)
    }
}

struct ResourceDefVisitor;

impl<'de> Visitor<'de> for ResourceDefVisitor {
    type Value = ResourceDef;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "struct ResourceDef")
    }

    fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        let mut content_type = None;
        let mut content: Option<Box<RawValue>> = None;

        while let Some(key) = map.next_key()? {
            match key {
                ResourceDefField::ContentType => match &content_type {
                    Some(_) => return Err(serde::de::Error::duplicate_field("contentType")),
                    None => content_type = Some(map.next_value()?),
                },
                ResourceDefField::Content => match &content {
                    Some(_) => return Err(serde::de::Error::duplicate_field("content")),
                    None => content = Some(map.next_value()?),
                },
            }
        }

        let content_type =
            content_type.ok_or_else(|| serde::de::Error::missing_field("content_type"))?;
        let content = content.ok_or_else(|| serde::de::Error::missing_field("content"))?;

        let content = match content_type {
            ContentType::Catalog(ContentFormat::Json)
            | ContentType::JsonSchema(ContentFormat::Json)
            | ContentType::Config(ContentFormat::Json) => {
                // This content is raw, unescaped json. We can use the bytes
                // directly without passing it through serde_json again.
                Bytes::copy_from_slice(content.get().as_bytes())
            }
            ContentType::Catalog(ContentFormat::Yaml)
            | ContentType::JsonSchema(ContentFormat::Yaml)
            | ContentType::Config(ContentFormat::Yaml)
            | ContentType::TypescriptModule
            | ContentType::DocumentsFixture => {
                // This content is a utf8 string. It isn't json, so it is full
                // of escape sequences. We don't want these in the final output,
                // so we'll parse it from a `serde_json::String` to a Rust
                // `String` and use those (now un-escaped) bytes.
                let s = from_json_string(content.get())?;
                Bytes::from(s)
            }
            ContentType::NpmPackage => {
                // This content is base64 encoded. We need to unescape the json
                // string and then decode the bytes.
                let s = from_json_string(content.get())?;
                let decoded = from_base64_string(&s)?;
                Bytes::from(decoded)
            }
        };

        Ok(ResourceDef {
            content_type,
            content,
        })
    }
}

fn from_json_string<E: serde::de::Error>(input: &str) -> Result<String, E> {
    serde_json::from_str::<String>(input).map_err(|_err| {
        serde::de::Error::invalid_value(
            serde::de::Unexpected::Bytes(input.as_bytes()),
            &"a json string",
        )
    })
}

fn from_base64_string<E: serde::de::Error>(input: &str) -> Result<Vec<u8>, E> {
    base64::decode(input).map_err(|_err| {
        serde::de::Error::invalid_value(serde::de::Unexpected::Str(input), &"base64 encoded string")
    })
}

enum ResourceDefField {
    ContentType,
    Content,
}

struct ResourceDefFieldVisitor;

impl<'de> Visitor<'de> for ResourceDefFieldVisitor {
    type Value = ResourceDefField;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "`contentType` or `content`")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        match value {
            "contentType" => Ok(ResourceDefField::ContentType),
            "content" => Ok(ResourceDefField::Content),
            _ => Err(serde::de::Error::unknown_field(value, RESOURCE_DEF_FIELDS)),
        }
    }
}

impl<'de> Deserialize<'de> for ResourceDefField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_identifier(ResourceDefFieldVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_json_resource() {
        let resource = ResourceDef {
            content_type: ContentType::Config(ContentFormat::Json),
            content: Bytes::from_static(b"{\"foo\": \"bar\"}"),
        };

        let serialized = serde_json::to_string(&resource).unwrap();
        let deserialized = serde_json::from_str::<ResourceDef>(&serialized).unwrap();

        assert_eq!(resource.content_type, deserialized.content_type);
        assert_eq!(resource.content, deserialized.content);
    }

    #[test]
    fn deserialize_yaml_resource() {
        let resource = ResourceDef {
            content_type: ContentType::Config(ContentFormat::Yaml),
            content: Bytes::from_static(b"foo: \"bar\"\nbaz:\n  - 1\n  - 2"),
        };

        let serialized = serde_json::to_string(&resource).unwrap();
        let deserialized = serde_json::from_str::<ResourceDef>(&serialized).unwrap();

        assert_eq!(resource.content_type, deserialized.content_type);
        assert_eq!(resource.content, deserialized.content);
    }

    #[test]
    fn deserialize_base64_resource() {
        let resource = ResourceDef {
            content_type: ContentType::NpmPackage,
            content: Bytes::from_static(
                b"binary content of a zip archive with invalid utf8 sequences like \xFF or \xC2\xC0",
            ),
        };

        let serialized = serde_json::to_string(&resource).unwrap();
        let deserialized = serde_json::from_str::<ResourceDef>(&serialized).unwrap();

        assert_eq!(resource.content_type, deserialized.content_type);
        assert_eq!(resource.content, deserialized.content);
    }
}
