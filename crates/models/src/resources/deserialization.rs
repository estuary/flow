use bytes::Bytes;
use schemars::JsonSchema;
use serde::de::{MapAccess, Visitor};
use serde::Deserialize;

use crate::resources::ResourceDef;
use crate::{ContentFormat, ContentType, Object};

#[derive(Clone, Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase", untagged)]
pub enum Content {
    String(String),
    Json(Object),
}

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
        let mut content: Option<Content> = None;

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
                match content {
                    Content::String(bytestring) => {
                        // The content wasn't actually parsed as json. If we can't
                        // parse it now, there's no way we'll be able to parse it
                        // later.
                        return Err(serde::de::Error::invalid_value(
                            serde::de::Unexpected::Bytes(bytestring.as_bytes()),
                            &"a valid json object",
                        ));
                    }
                    // The content was already parsed as valid json by serde. We
                    // don't actually want that though, since we want to stash
                    // the resource content as bytes. So we re-serialize it
                    // here.
                    Content::Json(object) => {
                        let json_str = serde_json::to_string(&object)
                            .expect("a json object should be serializable");
                        Bytes::from(json_str)
                    }
                }
            }
            ContentType::Catalog(ContentFormat::Yaml)
            | ContentType::JsonSchema(ContentFormat::Yaml)
            | ContentType::Config(ContentFormat::Yaml)
            | ContentType::TypescriptModule
            | ContentType::DocumentsFixture => {
                match content {
                    Content::String(string) => {
                        // This content is a utf8 string. It isn't json, so it is full
                        // of escape sequences. We don't want these in the final output,
                        // so we'll parse it from a `serde_json::String` to a Rust
                        // `String` and use those (now un-escaped) bytes.
                        Bytes::from(string)
                    }
                    Content::Json(object) => {
                        // Interestingly, this content got parsed as json when
                        // we expected it to be yaml/etc. This isn't a problem,
                        // it means we had a yaml document that was actually a
                        // json document.
                        Bytes::from(
                            serde_json::to_string(&object)
                                .expect("a json object should be serializable"),
                        )
                    }
                }
            }
            ContentType::NpmPackage => {
                let bytes = match content {
                    Content::String(bytestring) => {
                        // This content is base64 encoded. We need to unescape the json
                        // string and then decode the bytes.
                        from_base64_string(&bytestring)?
                    }
                    Content::Json(object) => {
                        // Now things are getting weird. We expected a zipped
                        // npm package, but got valid json? Something is wrong.
                        return Err(serde::de::Error::invalid_value(
                            serde::de::Unexpected::Bytes(
                                serde_json::to_string(&object)
                                    .expect("a json object should be serializable")
                                    .as_bytes(),
                            ),
                            &"a base64 encoded npm package",
                        ));
                    }
                };
                Bytes::from(bytes)
            }
        };

        Ok(ResourceDef {
            content_type,
            content,
        })
    }
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
            content: Bytes::from_static(b"{\"foo\":\"bar\"}"),
        };

        let serialized = serde_json::to_string(&resource).unwrap();
        dbg!(&serialized);
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
