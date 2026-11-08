//! Generated configuration types of a connector's declared spec.
//!
//! These are the garden path. User code may instead parse its configuration
//! into types of its own (such as CDK credential classes), in which case the
//! user keeps them equivalent to the declared schemas: they're not checked.
//!
//! `EndpointConfig` is the same for every connector. A `ResourceConfig` is
//! not: a derivation's is the `lambda` of a transform, and a capture's is a
//! CDK resource. Each connector maps its own from `config_shape`.

use crate::pydantic::Mapper;
use anyhow::Context;
use doc::Shape;
use json::schema::types;

/// Generate Python source of the `EndpointConfig` type of `spec`, which is of
/// `Spec::config_schema_or_default`. It uses the `typing` and `pydantic`
/// modules, and imports those of its `x-python-type` annotations itself.
pub fn endpoint_config_py(spec: &models::custom_connector::Spec) -> anyhow::Result<String> {
    let (mapper, shape) =
        config_shape(spec.config_schema_or_default()).context("invalid `configSchema`")?;

    let mut source =
        String::from("# Generated for the endpoint configuration of `spec.configSchema`.\n");
    mapper
        .map_shape(shape, "EndpointConfig")?
        .render(&mut source);
    Ok(source)
}

/// Build a Mapper of a configuration schema and infer its Shape.
/// A configuration is always an object, so a shape which may be one (such as
/// that of `{}`, or of `type: [object, null]`) is one, and its generated type
/// is a class rather than an alias of a union.
pub fn config_shape(schema: &str) -> anyhow::Result<(Mapper, Shape)> {
    let mapper = Mapper::new(schema.as_bytes(), "")?;
    let mut shape = Shape::infer(mapper.schema(), mapper.index());

    if shape.type_.overlaps(types::OBJECT) {
        shape.type_ = types::OBJECT;
    }
    Ok((mapper, shape))
}

/// Query `ptr` of a `config` having `shape`, as its generated type reads it:
/// an omitted property is its field's default, if it has one. Each token of
/// `ptr` is a property, as a configuration's annotated locations are.
pub fn query_config<'a>(
    shape: &'a Shape,
    ptr: &json::Pointer,
    config: &'a serde_json::Value,
) -> Option<&'a serde_json::Value> {
    let mut shape = Some(shape);
    let mut value = config;

    for token in ptr.iter() {
        let name: std::borrow::Cow<str> = match token {
            json::ptr::Token::Property(name) => name.into(),
            json::ptr::Token::Index(index) => index.to_string().into(),
            json::ptr::Token::NextIndex | json::ptr::Token::NextProperty => return None,
        };
        let property = shape.and_then(|shape| {
            shape
                .object
                .properties
                .iter()
                .find(|property| *property.name == *name)
        });
        shape = property.map(|property| &property.shape);

        value = match value.get(name.as_ref()) {
            Some(value) => value,
            // Only an object's omitted property is its field's default.
            None if value.is_object() => property.and_then(crate::pydantic::field_default)?,
            None => return None,
        };
    }
    Some(value)
}

#[cfg(test)]
mod test {
    use super::endpoint_config_py;
    use models::custom_connector::Spec;

    fn render(config_schema: Option<serde_json::Value>) -> String {
        let spec: Spec =
            serde_json::from_value(serde_json::json!({"configSchema": config_schema})).unwrap();
        endpoint_config_py(&spec).unwrap()
    }

    #[test]
    fn defaults_and_python_types_of_config_types() {
        insta::assert_snapshot!(render(Some(serde_json::json!({
            "type": "object",
            "properties": {
                "greeting": {"type": "string", "default": "Hello", "description": "Greeting of each document"},
                "count": {"type": "integer", "default": 10, "minimum": 0},
                "interval": {"type": "string", "format": "duration", "default": "PT1H", "x-python-type": "datetime.timedelta"},
                "start_date": {"type": "string", "format": "date-time", "x-python-type": "datetime.datetime"},
                "end_date": {"type": "string", "format": "date-time"},
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
        }))));
    }

    #[test]
    fn configurations_are_classes() {
        insta::assert_snapshot!(
            [
                // Schemas which may be objects (no `type`, or nullable) generate
                // a class rather than a union alias.
                render(Some(serde_json::json!({
                    "type": ["object", "null"],
                    "properties": {"a": {"type": "string"}},
                }))),
                // A trivial schema is still a class, which allows any properties.
                render(Some(serde_json::json!({}))),
                // An absent schema is a class which forbids every property.
                render(None),
                // A non-object schema is an alias.
                render(Some(serde_json::json!({"type": "string"}))),
            ]
            .join("\n=====\n")
        );
    }

    #[test]
    fn user_text_is_escaped_and_names_dont_collide() {
        insta::assert_snapshot!(render(Some(serde_json::json!({
            "type": "object",
            "description": "Ends with a quote\"",
            "properties": {
                "path": {"type": "string", "description": "A Windows path like C:\\Users\\me"},
                "quoted": {"type": "string", "description": "Has \"\"\" inside, and a \u{0} NUL"},
                "trailing\"": {"type": "string"},
                "choice": {"enum": ["a\nb", "c\"d", "e\\f"]},
                "datetime": {"type": "string", "x-python-type": "datetime.datetime"},
                "typing": {"type": "integer", "default": 10.0},
                "model_config": {"type": "string"},
                "schema": {"type": "string"},
                "json": {"type": "boolean"},
            },
        }))));
    }

    #[test]
    fn invalid_schemas_are_errors() {
        let spec: Spec = serde_json::from_value(serde_json::json!({
            "configSchema": {"type": "not-a-type"},
        }))
        .unwrap();
        let err = endpoint_config_py(&spec).unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"));
    }
}
