use crate::errors::Error;
use schemars::{
    schema::{ObjectValidation, RootSchema, Schema, SchemaObject, SubschemaValidation},
    JsonSchema,
};
use serde_json::{value::RawValue, Value};

// Create the RootSchema given datatype T.
pub fn create_root_schema<T: JsonSchema>() -> RootSchema {
    let mut settings = schemars::gen::SchemaSettings::draft07();
    settings.inline_subschemas = true;
    let generator = schemars::gen::SchemaGenerator::new(settings);
    return generator.into_root_schema_for::<T>();
}

// Extract the sub object keyed at `key` from v if v is an object, and returns both the remainder and the removed value.
pub fn remove_subobject(mut v: Value, key: &str) -> (Option<Value>, Value) {
    let mut sub_object = None;

    if let Value::Object(ref mut m) = v {
        sub_object = m.remove(key)
    }

    (sub_object, v)
}

// Extend the endpoint schema (of a connector) with the interceptor schema. The resulting schema allows the connector configuration JSON
// object to accept an additional subobject in the field specified by interceptor_object_field, in addition to all the other configs of the connector.
pub fn extend_endpoint_schema(
    endpoint_spec_schema: Box<RawValue>,
    interceptor_object_field: String,
    interceptor_schema: RootSchema,
) -> Result<Box<RawValue>, Error> {
    let mut interceptor_schema_object_validation = ObjectValidation::default();
    interceptor_schema_object_validation.properties.insert(
        interceptor_object_field,
        Schema::Object(interceptor_schema.schema),
    );

    let mut interceptor_schema_object = SchemaObject::default();
    // interceptor_schema_object validates a JSON object that contains a subobject of
    // { interceptor_object_field: { interceptor config object } }
    interceptor_schema_object.object = Some(Box::new(interceptor_schema_object_validation));

    // If the endpoint schema is already in `all_of` structure, just append the interceptor shcema object into the list.
    let mut extended_schema: RootSchema = serde_json::from_str(endpoint_spec_schema.get())?;
    if let Some(ref mut subschemas) = extended_schema.schema.subschemas {
        if let Some(ref mut all_of) = subschemas.all_of {
            all_of.push(Schema::Object(interceptor_schema_object));
            return RawValue::from_string(serde_json::to_string_pretty(&extended_schema)?)
                .map_err(Into::into);
        }
    }

    // Construct a new schema object with "all_of" structure.
    let connector_schema_object = extended_schema.schema;
    let all_of = vec![
        Schema::Object(connector_schema_object),
        Schema::Object(interceptor_schema_object),
    ];
    let mut subschema_validation = SubschemaValidation::default();
    subschema_validation.all_of = Some(all_of);

    extended_schema.schema = SchemaObject::default();
    extended_schema.schema.subschemas = Some(Box::new(subschema_validation));

    RawValue::from_string(serde_json::to_string_pretty(&extended_schema)?).map_err(Into::into)
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    #[derive(Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct TestInterceptorConfigA {
        #[allow(dead_code)]
        interceptor_name_a: String,
    }

    #[derive(Deserialize, JsonSchema)]
    #[serde(deny_unknown_fields)]
    pub struct TestInterceptorConfigB {
        #[allow(dead_code)]
        interceptor_name_b: String,
    }

    fn verify_extended_endpoint_schema(endpoint_schema: String) {
        let interceptor_schema_a = create_root_schema::<TestInterceptorConfigA>();
        let interceptor_schema_b = create_root_schema::<TestInterceptorConfigB>();

        let valid_config_instance_1 = json!({
            "connector_name": "connector",
            "interceptor_config_a": {
                "interceptor_name_a": "interceptor_a"
            },
            "interceptor_config_b": {
                "interceptor_name_b": "interceptor_b"
            }
        });

        let valid_config_instance_2 = json!({
            "connector_name": "connector"
        });

        let config_instance_bad_connector_config = json!({
            "connector_name_bad": "connector"
        });

        let config_instance_bad_interceptor_config = json!({
            "connector_name": "connector",
            "interceptor_config_a": {
                "interceptor_name_bad": "interceptor"
            }
        });

        let extended_schema_json = extend_endpoint_schema(
            extend_endpoint_schema(
                RawValue::from_string(endpoint_schema).unwrap(),
                "interceptor_config_a".to_string(),
                interceptor_schema_a,
            )
            .unwrap(),
            "interceptor_config_b".to_string(),
            interceptor_schema_b,
        )
        .unwrap()
        .to_string();

        let compiled_schema = jsonschema::JSONSchema::options()
            .compile(&serde_json::from_str(&extended_schema_json).unwrap())
            .unwrap();

        assert!(compiled_schema.validate(&valid_config_instance_1).is_ok());
        assert!(compiled_schema.validate(&valid_config_instance_2).is_ok());
        assert!(compiled_schema
            .validate(&config_instance_bad_connector_config)
            .is_err());
        assert!(compiled_schema
            .validate(&config_instance_bad_interceptor_config)
            .is_err());
    }

    #[test]
    fn test_extend_endpoint_schema() {
        let endpoint_schema_a = json!({
            "type": "object",
            "required": ["connector_name"],
            "properties": {
                "connector_name": {"type": "string"}
            },
        })
        .to_string();

        verify_extended_endpoint_schema(endpoint_schema_a);

        let endpoint_schema_b = json!({
            "$ref": "#/definitions/nested",
            "definitions": {
              "nested": {
                  "type": "object",
                  "required": ["connector_name"],
                  "properties": {
                      "connector_name": {"type": "string"}
                  },
              }
            }
        })
        .to_string();

        verify_extended_endpoint_schema(endpoint_schema_b);
    }
}
