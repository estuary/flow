use doc::inference::{ObjProperty, Reduction, Shape};
use json::schema::types;
use schema_inference::schema::*;
use schema_inference::*;
use schemars::schema::Metadata;
use serde_json::Value as JSONValue;
use std::fs::File;
use std::io::BufReader;
use uuid::Uuid;

pub fn file(file: File) {
    let reader = BufReader::new(file);
    let data: JSONValue = serde_json::from_reader(reader).unwrap();
    let schema = generate(Metadata::default(), &data).unwrap();

    println!("{:?}", schema.to_json().unwrap());
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
) -> Result<JsonSchema, SchemaParseError> {
    let mut schema = JsonSchema {
        metadata: metadata,
        root: Shape {
            type_: types::OBJECT,
            reduction: Reduction::Merge,
            ..Shape::default()
        },
        ..JsonSchema::default()
    };

    // This is explicitly checked and only updated if no value is set to help
    // with testing the JSON output. It also gives the nice benefit that if the calling methods wants
    // to set the id to a given value, it can. However, it is expected to not be set by the caller
    // and be dynamically generated here.
    if schema.metadata.id.is_none() {
        schema.metadata.id = Some(Uuid::new_v4().hyphenated().to_string())
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
            shape: Shape {
                reduction: Reduction::Merge,
                ..Shape::default()
            },
        };

        if let Err(err) = properties::build(&mut property, &value) {
            return Err(err);
        }

        schema.root.object.properties.push(property);

        Ok(())
    })?;

    Ok(schema)
}

#[cfg(test)]
mod test {
    use super::*;
    use doc::inference;
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
}
