use super::errors::*;
use indexmap::IndexMap;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};

// The basic elastic search data types to represent data in Flow.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ESBasicType {
    Boolean,
    Date {
        format: String,
    }, // refer to the comments of DateSpec in ../run.go for details.
    Double,
    GeoPoint, // refer to https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-point.html to see the format of geo_point data.
    GeoShape, // refer to https://www.elastic.co/guide/en/elasticsearch/reference/current/geo-shape.html to see the format of geo_shape data.
    Keyword {
        ignore_above: u16,
    }, // refer to the comments of KeywordSpec in ../run.go for details.
    Long,
    Null,
    #[serde(serialize_with = "serialize_text")]
    Text {
        dual_keyword: bool,
        keyword_ignore_above: u16, // effective if dual_keyword is true.
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ESFieldType {
    Basic(ESBasicType),
    Object {
        properties: IndexMap<String, ESFieldType>,
    },
}

// The type of a elastic search field is allowed to be overridden, so that
// more elastic-search-specific features, such as dual-text-keyword and date format,
// be specified.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ESTypeOverride {
    // The json pointer delimitated by '/'.
    pub pointer: String,
    // The overriding elastic search data type of the field.
    pub es_type: ESBasicType,
}

impl ESFieldType {
    pub fn apply_type_override(mut self, es_override: &ESTypeOverride) -> Result<Self, Error> {
        let pointer = &es_override.pointer;
        if pointer.is_empty() {
            return Err(Error::OverridePointerError {
                message: POINTER_EMPTY,
                overriding_schema: serde_json::to_value(&self)?,
                pointer: pointer.clone(),
            });
        }

        let mut cur_field = &mut self;
        let mut prop_itr = pointer
            .split('/')
            .skip(if pointer.starts_with('/') { 1 } else { 0 })
            .map(|fld| fld.to_string())
            .peekable();

        while let Some(prop) = prop_itr.next() {
            cur_field = match cur_field {
                Self::Object { properties } => {
                    if !properties.contains_key(&prop) {
                        return Err(Error::OverridePointerError {
                            message: POINTER_MISSING_FIELD,
                            overriding_schema: serde_json::to_value(&self)?,
                            pointer: pointer.clone(),
                        });
                    } else if prop_itr.peek() == None {
                        properties.insert(prop, ESFieldType::Basic(es_override.es_type.clone()));
                        return Ok(self);
                    } else {
                        // prop is ensured to exist in properties.
                        properties.get_mut(&prop).unwrap()
                    }
                }
                _ => {
                    return Err(Error::OverridePointerError {
                        message: POINTER_WRONG_FIELD_TYPE,
                        overriding_schema: serde_json::to_value(&self)?,
                        pointer: pointer.clone(),
                    })
                }
            }
        }
        Ok(self)
    }
}

fn serialize_text<S>(
    dual_keyword: &bool,
    keyword_ignore_above: &u16,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if *dual_keyword {
        let mut fields: IndexMap<String, ESBasicType> = IndexMap::new();
        fields
            .entry("keyword".to_string())
            .or_insert(ESBasicType::Keyword {
                ignore_above: *keyword_ignore_above,
            });

        let mut serialized = serializer.serialize_map(Some(1))?;
        serialized.serialize_entry("fields", &fields)?;
        serialized.end()
    } else {
        serializer.serialize_none()
    }
}
