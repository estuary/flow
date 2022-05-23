use schemars::{gen::SchemaGenerator, schema::*};
use serde_json::Value as JSONValue;

#[derive(Debug, Default)]
pub struct JSONSchema {
    root: RootSchema,
}

#[derive(Debug, thiserror::Error)]
pub enum SchemaParseError {
    #[error("failed to parse the value: {0}")]
    InvalidValueType(serde_json::Value),
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
    metadata: Metadata,
    json_value: &JSONValue,
) -> Result<JSONSchema, SchemaParseError> {
    let mut schema = JSONSchema::default();
    schema.root.schema = SchemaObject::default();

    schema.root.schema.metadata = Some(Box::new(metadata));
    schema.root.schema.instance_type = Some(SingleOrVec::from(InstanceType::Object));

    // Initializing the SchemaGenerator only to get the meta_schema.
    // I think it's probably overkill, and it's possible hardcoding
    // a static' string instead would be better.
    let sg = SchemaGenerator::default();
    schema.root.meta_schema = sg.settings().meta_schema.clone();

    let data = if let JSONValue::Object(data) = json_value {
        data
    } else {
        return Err(SchemaParseError::InvalidValueType(json_value.to_owned()));
    };

    schema.generate_root_validation_schema(data)?;

    Ok(schema)
}

impl JSONSchema {
    fn generate_root_validation_schema(
        &mut self,
        data: &serde_json::Map<String, JSONValue>,
    ) -> Result<(), SchemaParseError> {
        let mut validation = ObjectValidation::default();

        data.iter().try_for_each(|(key, value)| {
            let schema_obj = match schema_object_for_value(&value) {
                Ok(s) => s.clone(),
                Err(e) => return Err(e),
            };

            validation
                .properties
                .insert(key.to_owned(), Schema::Object(schema_obj));

            Ok(())
        })?;

        // If each pair of key/value was processed into validation without error,
        // the schema can be safely configured with the ObjectValidation built.
        self.root.schema.object = Some(Box::new(validation));
        Ok(())
    }
}

fn schema_object_for_value(value: &JSONValue) -> Result<SchemaObject, SchemaParseError> {
    let mut schema_obj = SchemaObject::default();
    schema_obj.instance_type = match value {
        JSONValue::Bool(_) => Some(SingleOrVec::Single(Box::new(InstanceType::Boolean))),
        JSONValue::Number(_) => Some(SingleOrVec::Single(Box::new(InstanceType::Number))),
        JSONValue::String(_) => Some(SingleOrVec::Single(Box::new(InstanceType::String))),
        JSONValue::Null => Some(SingleOrVec::Single(Box::new(InstanceType::Null))),
        e => {
            return Err(SchemaParseError::InvalidValueType(e.to_owned()));
        }
    };

    return Ok(schema_obj);
}

#[cfg(test)]
mod test {
    use super::*;
    use schemars::gen::SchemaGenerator;
    use serde_json::json;
    use uuid::Uuid;

    macro_rules! enum_value {
        ($value:expr, $pattern:pat => $extracted_value:expr) => {
            match $value {
                $pattern => $extracted_value,
                _ => panic!("Pattern doesn't match!"),
            }
        };
    }

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

            match result {
                Ok(_) => assert!(false),
                Err(_) => {}
            }
        });
    }

    #[test]
    fn test_parsing_deep_nested_error() {
        let iter = vec![json!({"test": {"more": "than 1 level"}})].into_iter();

        iter.for_each(|json| {
            let result = generate(Metadata::default(), &json);

            match result {
                Ok(e) => assert!(
                    false,
                    "expected the document to fail: {}",
                    serde_json::to_string_pretty(&e.root).unwrap()
                ),
                Err(_) => {}
            }
        });
    }

    #[test]
    fn test_generator_with_multiple_values() {
        let data = json!({"a_null_value": null, "boolean": true, "number": 123, "string": "else"});
        let schema = generate(Metadata::default(), &data).unwrap();
        let properties = &schema.root.schema.object.as_ref().unwrap().properties;

        {
            let val =
                enum_value!(properties.get("a_null_value").unwrap(), Schema::Object(so) => so);
            assert_eq!(
                val.instance_type,
                Some(SingleOrVec::Single(Box::new(InstanceType::Null)))
            );
        }

        {
            let val = enum_value!(properties.get("boolean").unwrap(), Schema::Object(so) => so);
            assert_eq!(
                val.instance_type,
                Some(SingleOrVec::Single(Box::new(InstanceType::Boolean)))
            );
        }

        {
            let val = enum_value!(properties.get("number").unwrap(), Schema::Object(so) => so);
            assert_eq!(
                val.instance_type,
                Some(SingleOrVec::Single(Box::new(InstanceType::Number)))
            );
        }

        {
            let val = enum_value!(properties.get("string").unwrap(), Schema::Object(so) => so);
            assert_eq!(
                val.instance_type,
                Some(SingleOrVec::Single(Box::new(InstanceType::String)))
            );
        }
    }

    #[test]
    fn test_metadata() {
        let data = json!({});
        let mut metadata = Metadata::default();
        metadata.title = Some("test".to_string());
        metadata.description = Some("My description".to_string());
        metadata.id = Some(Uuid::new_v4().hyphenated().to_string());

        let schema = generate(metadata, &data).unwrap();

        assert_eq!(
            schema.root.meta_schema,
            SchemaGenerator::default().settings().meta_schema
        );

        // The root schema instance type is required to be an instance type
        // Object as that's what expected in flow.
        match schema.root.schema.instance_type {
            Some(SingleOrVec::Single(x)) if *x == InstanceType::Object => {}
            _ => assert!(false),
        }
    }
}
