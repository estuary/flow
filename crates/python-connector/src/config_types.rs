//! Generated `EndpointConfig` and `ResourceConfig` types of a connector's
//! declared spec.
//!
//! These are the garden path. User code may instead parse its configuration
//! into types of its own, in which case the user keeps them equivalent to the
//! declared schemas: they're not checked.

use crate::pydantic::Mapper;
use anyhow::Context;
use doc::Shape;
use json::schema::types;

/// Generate Python source of the `EndpointConfig` and `ResourceConfig` types
/// of `spec`. It uses the `datetime`, `typing`, and `pydantic` modules.
/// A derivation's `ResourceConfig` is the `lambda` of a transform, and a spec
/// which doesn't declare one has a class which allows any properties.
pub fn config_types_py(spec: &models::custom_connector::Spec) -> anyhow::Result<String> {
    let mut source = String::new();

    let (mapper, shape) =
        config_shape(spec.config_schema_or_default()).context("invalid `configSchema`")?;
    let mapping = mapper.map_shape(shape, ENDPOINT_CONFIG);
    source.push_str("# Generated for the endpoint configuration of `spec.configSchema`.\n");
    mapping.render(&mut source);

    let resource_config_schema = spec
        .resource_config_schema
        .as_ref()
        .map(|schema| schema.get())
        .unwrap_or("{}");

    let (mapper, shape) =
        config_shape(resource_config_schema).context("invalid `resourceConfigSchema`")?;
    let mapping = mapper.map_shape(shape, RESOURCE_CONFIG);
    source.push_str("# Generated for the resource configuration of `spec.resourceConfigSchema`.\n");
    mapping.render(&mut source);

    Ok(source)
}

/// Build a Mapper of a configuration schema and infer its Shape.
/// A configuration is always an object, so a shape which may be one (such as
/// that of `{}`, or of `type: [object, null]`) is one, and its generated type
/// is a class rather than an alias of a union.
fn config_shape(schema: &str) -> anyhow::Result<(Mapper, Shape)> {
    let mapper = Mapper::for_config(schema.as_bytes())?;
    let mut shape = Shape::infer(mapper.schema(), mapper.index());

    if shape.type_.overlaps(types::OBJECT) {
        shape.type_ = types::OBJECT;
    }
    Ok((mapper, shape))
}

const ENDPOINT_CONFIG: &str = "EndpointConfig";
const RESOURCE_CONFIG: &str = "ResourceConfig";

#[cfg(test)]
mod test {
    use super::config_types_py;
    use models::custom_connector::Spec;

    fn render(spec: serde_json::Value) -> String {
        let spec: Spec = serde_json::from_value(spec).unwrap();
        config_types_py(&spec).unwrap()
    }

    #[test]
    fn defaults_and_formats_of_config_types() {
        let spec = serde_json::json!({
            "configSchema": {
                "type": "object",
                "properties": {
                    "greeting": {"type": "string", "default": "Hello", "description": "Greeting of each document"},
                    "count": {"type": "integer", "default": 10, "minimum": 0},
                    "interval": {"type": "string", "format": "duration", "default": "PT1H"},
                    "start_date": {"type": "string", "format": "date-time"},
                    "tags": {"type": "array", "items": {"type": "string"}, "default": ["a", "b"]},
                    "bad_default": {"type": "integer", "default": "not an integer"},
                    "credentials": {
                        "type": "object",
                        "properties": {
                            "client_id": {"type": "string"},
                            "client_secret": {"type": "string", "secret": true},
                        },
                        "required": ["client_id", "client_secret"],
                    },
                },
                "required": ["credentials"],
            },
            "resourceConfigSchema": {
                "type": "object",
                "properties": {
                    "stream": {"type": "string"},
                    "interval": {"type": "string", "format": "duration", "default": "PT0S"},
                },
                "required": ["stream"],
            },
        });
        insta::assert_snapshot!(render(spec));
    }

    #[test]
    fn derivation_config_types() {
        // A trivial config schema is still a class, which allows any properties.
        let spec = serde_json::json!({
            "configSchema": {},
            "resourceConfigSchema": {
                "type": "object",
                "properties": {"readOnly": {"type": "boolean", "default": false}},
            },
        });
        // An absent config schema is a class which forbids every property.
        let absent = serde_json::json!({});

        insta::assert_snapshot!([render(spec), render(absent)].join("\n=====\n"));
    }

    #[test]
    fn configurations_are_classes() {
        // Schemas which may be objects (no `type`, or nullable) generate a class
        // rather than a union alias. A non-object resource is an alias.
        let spec = serde_json::json!({
            "configSchema": {"type": ["object", "null"], "properties": {"a": {"type": "string"}}},
            "resourceConfigSchema": {"properties": {"name": {"type": "string"}}, "required": ["name"]},
        });
        let non_object = serde_json::json!({"resourceConfigSchema": {"type": "string"}});

        insta::assert_snapshot!([render(spec), render(non_object)].join("\n=====\n"));
    }

    #[test]
    fn user_text_is_escaped_and_names_dont_collide() {
        let spec = serde_json::json!({
            "configSchema": {
                "type": "object",
                "description": "Ends with a quote\"",
                "properties": {
                    "path": {"type": "string", "description": "A Windows path like C:\\Users\\me"},
                    "quoted": {"type": "string", "description": "Has \"\"\" inside, and a \u{0} NUL"},
                    "trailing\"": {"type": "string"},
                    "choice": {"enum": ["a\nb", "c\"d", "e\\f"]},
                    "datetime": {"type": "string", "format": "date-time"},
                    "typing": {"type": "integer", "default": 10.0},
                    "model_config": {"type": "string"},
                    "schema": {"type": "string"},
                    "json": {"type": "boolean"},
                },
            },
        });
        insta::assert_snapshot!(render(spec));
    }

    #[test]
    fn invalid_schemas_are_errors() {
        let spec: Spec = serde_json::from_value(serde_json::json!({
            "configSchema": {"type": "not-a-type"},
        }))
        .unwrap();
        let err = config_types_py(&spec).unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"));
    }
}
