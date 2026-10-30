use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize};

/// Configuration for parsing XML documents into JSON.
///
/// Elements are converted using the following rules:
/// - Each document holds its element under the element's own name, such as `{"Order": {...}}`.
/// - An element with only text content becomes a JSON string.
/// - An element with attributes or child elements becomes a JSON object. Attributes are
///   prefixed with `@`, and any text content is placed under `#text`.
/// - Child elements that occur more than once become JSON arrays. Elements named in
///   `arrayElements` always become arrays, even if they only occur once.
/// - All values are strings. Namespace prefixes are removed from names.
#[derive(Debug, Clone, PartialEq, Default, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[schemars(title = "XML")]
pub struct XmlConfig {
    /// The path of the element to emit as a document, for example `/Orders/Order`.
    /// Each matching element becomes a separate document. If empty, the root element of
    /// each file is emitted as a single document.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(title = "Record Path", example = "/Orders/Order")]
    pub record_path: Option<String>,

    /// Names of elements that should always be represented as arrays, even when an element
    /// contains only a single occurrence of them. Use this for repeated elements, so that the
    /// shape of documents does not change depending on how many occurrences a file contains.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(title = "Array Elements")]
    pub array_elements: Vec<String>,
}

/// Unlike the configuration of other formats, the XML configuration may be omitted entirely,
/// since all of its fields are optional. `Format` is an adjacently tagged enum, which would
/// otherwise require a `config` property to be present. Deserializing through an `Option`
/// allows both a missing and a `null` config to resolve to the default.
impl<'de> Deserialize<'de> for XmlConfig {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Fields {
            #[serde(default)]
            record_path: Option<String>,
            #[serde(default)]
            array_elements: Vec<String>,
        }

        Ok(Option::<Fields>::deserialize(deserializer)?
            .map(|fields| XmlConfig {
                record_path: fields.record_path,
                array_elements: fields.array_elements,
            })
            .unwrap_or_default())
    }
}
