use doc::inference::{ObjProperty, Shape};
use json::schema::types;
use schemars::{gen::SchemaGenerator, schema::RootSchema, schema::*};
use serde_json::Value as JSONValue;
use uuid::Uuid;

mod properties;
mod validations;

#[derive(Debug, Default)]
pub struct JsonSchema {
    metadata: Option<Metadata>,
    root: Shape,
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaParseError {
    #[error("failed to parse the value: {0}")]
    InvalidValueType(serde_json::Value),

    #[error("failed generating a property: #{0}")]
    PropertyError(properties::PropertyError),

    #[error("failed to encode schema: #{0}")]
    EncodeError(serde_json::Error),

    #[error("missing metadata while rendering schema")]
    MissingMetadataError(),
}

impl From<serde_json::Error> for SchemaParseError {
    fn from(err: serde_json::Error) -> SchemaParseError {
        SchemaParseError::EncodeError(err)
    }
}

impl From<properties::PropertyError> for SchemaParseError {
    fn from(err: properties::PropertyError) -> SchemaParseError {
        SchemaParseError::PropertyError(err)
    }
}

// Generate a JSONSchema from a JSON document.
// Metadata is used to build the metadata of the root schema
// so that things like the `title`, `description` and `$id` can be passed to
// the JSONSchema. The current implementation only supports scalar values. If
// non-scalar values are provided as a JSON value, an error will be returned.
//
// The current implementation is also inlining all object validations that are generated
// from the JSON. As more complex validations will be generated, it's possible that some
// validations will be moved into definitions.
pub fn generate(
    mut metadata: Metadata,
    json_value: &JSONValue,
) -> Result<JsonSchema, SchemaParseError> {
    let mut schema = JsonSchema {
        metadata: None,
        root: Shape {
            type_: types::OBJECT,
            ..Shape::default()
        },
        ..JsonSchema::default()
    };

    // This is explicitly checked and only updated if no value is set to help
    // with testing the JSON output. It also gives the nice benefit that if the calling methods wants
    // to set the id to a given value, it can. However, it is expected to not be set by the caller
    // and be dynamically generated here.
    if metadata.id.is_none() {
        metadata.id = Some(Uuid::new_v4().hyphenated().to_string())
    }

    let data = if let JSONValue::Object(data) = json_value {
        data
    } else {
        return Err(SchemaParseError::InvalidValueType(json_value.to_owned()));
    };

    data.iter().try_for_each(|(key, value)| {
        let mut property = ObjProperty {
            name: key.to_string(),
            is_required: true,
            shape: Shape::default(),
        };

        match properties::build(&mut property, &value) {
            Ok(_) => (),
            Err(e) => return Err(e),
        };

        schema.root.object.properties.push(property);

        Ok(())
    })?;

    schema.metadata = Some(metadata);
    Ok(schema)
}

impl JsonSchema {
    pub fn merge(&mut self, other: &JsonSchema) -> Result<&Self, SchemaParseError> {
        let root = Shape::intersect(self.root.clone(), other.root.clone());
        return Ok(self);
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        let schema_obj = SchemaObject {
            instance_type: Some(SingleOrVec::from(InstanceType::Object)),
            metadata: Some(Box::new(self.metadata.clone().unwrap())),
            ..SchemaObject::default()
        };

        let mut root = RootSchema {
            schema: schema_obj,
            meta_schema: SchemaGenerator::default().settings().meta_schema.clone(),
            ..RootSchema::default()
        };

        root.schema.object = validations::object(&self.root);

        serde_json::to_string(&root)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_generation_with_non_acceptable_values() {
        vec![
            json!([{}]),
            json!("JSON String"),
            json!(&123),
            json!(null),
            json!(true),
        ]
        .iter()
        .for_each(|json| {
            let result = generate(Metadata::default(), &json);
            result.expect_err("generate should return error");
        });
    }

    #[test]
    fn test_generator_with_multiple_values() {
        let metadata = Metadata {
            id: Some("342ac041-7e3c-42ca-8311-c248284cd034".to_string()),
            ..Metadata::default()
        };

        let data = json!({"a_null_value": null, "boolean": true, "number": 123, "string": "else"});
        let schema = generate(metadata, &data).unwrap();

        insta::assert_json_snapshot!(schema.to_json().unwrap());
    }

    #[test]
    fn test_merging_json_documents() {
        let metadata = Metadata {
            id: Some("342ac041-7e3c-42ca-8311-c248284cd034".to_string()),
            ..Metadata::default()
        };

        let mut schema = generate(metadata, &json!({"string": "else"})).unwrap();
        schema
            .merge(&generate(schema.metadata.clone().unwrap(), &json!({ "string": null })).unwrap())
            .unwrap();

        println!("{:?}", schema.to_json().unwrap());
    }
}
