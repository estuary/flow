//! Generated `EndpointConfig` and `ResourceConfig` types of a connector's
//! declared spec, which are shared by Python captures and derivations.
//!
//! These are the garden path. User code may instead parse its configuration
//! into types of its own (such as CDK credential classes), in which case the
//! user keeps them equivalent to the declared schemas: they're not checked.

use crate::pydantic::{Mapper, Mapping, field_name};
use crate::spec::Spec;
use anyhow::Context;
use doc::Shape;
use json::schema::types;

/// Kind of connector whose resource configuration is generated.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Resources {
    /// A capture's `ResourceConfig` extends the CDK's `BaseResourceConfig`,
    /// and is importable as `common` (`estuary_cdk.capture.common`). A spec
    /// which doesn't declare one has the CDK's stock `ResourceConfig`.
    Capture,
    /// A derivation's `ResourceConfig` is the `lambda` of a transform. Its
    /// connector resolves a spec which doesn't declare one to its default.
    Derivation,
}

/// Generated configuration types of a connector.
#[derive(Debug)]
pub struct ConfigTypes {
    /// Python source of the `EndpointConfig` and `ResourceConfig` types.
    /// It uses the `datetime`, `typing`, and `pydantic` modules, and if
    /// `uses_common`, the CDK's `common` module.
    pub source: String,
    /// Does `source` use the CDK's `common` module?
    pub uses_common: bool,
    /// Does a capture's `ResourceConfig` have `PATH_POINTERS` (and `path()`)?
    pub has_path: bool,
}

/// Generate the `EndpointConfig` and `ResourceConfig` types of `spec`.
pub fn config_types_py(spec: &Spec, resources: Resources) -> anyhow::Result<ConfigTypes> {
    let mut source = String::new();

    let (mapper, shape) = config_shape(&spec.config_schema()).context("invalid `configSchema`")?;
    let mapping = mapper.map_shape(shape, ENDPOINT_CONFIG);
    source.push_str("# Generated for the endpoint configuration of `spec.configSchema`.\n");
    mapping.render(&mut source);

    let resource_config_schema = match (resources, &spec.resource_config_schema) {
        (_, Some(schema)) => schema.clone(),
        (Resources::Capture, None) => {
            source.push_str(
                "# `spec.resourceConfigSchema` isn't declared, so each binding's resource\n\
                 # configuration is the CDK's stock `ResourceConfig`.\n\
                 ResourceConfig = common.ResourceConfig\n",
            );
            return Ok(ConfigTypes {
                source,
                uses_common: true,
                has_path: true,
            });
        }
        (Resources::Derivation, None) => serde_json::json!({}),
    };

    let (mapper, shape) =
        config_shape(&resource_config_schema).context("invalid `resourceConfigSchema`")?;
    let path = match resources {
        Resources::Capture => resource_path(&shape),
        Resources::Derivation => None,
    };
    let mut mapping = mapper.map_shape(shape, RESOURCE_CONFIG);

    let (uses_common, has_path) = match resources {
        Resources::Capture => extend_capture_resource(&mut mapping, path.as_ref()),
        Resources::Derivation => (false, false),
    };
    if uses_common && !has_path {
        // The user writes `path()` of a subclass of the generated
        // `ResourceConfig`, or of a resource configuration of their own
        // (the escape hatch).
        source.push_str(
            "# The resource schema doesn't have exactly one `x-collection-name` property\n\
             # (and at most one `x-schema-name`), each a required string, so\n\
             # `ResourceConfig` has no `path()`: declare a subclass having\n\
             # `PATH_POINTERS` and a `path()` of its own.\n",
        );
    }
    source.push_str("# Generated for the resource configuration of `spec.resourceConfigSchema`.\n");
    mapping.render(&mut source);

    Ok(ConfigTypes {
        source,
        uses_common,
        has_path,
    })
}

/// Build a Mapper of a configuration schema and infer its Shape.
/// A configuration is always an object, so a shape which may be one (such as
/// that of `{}`, or of `type: [object, null]`) is one, and its generated type
/// is a class rather than an alias of a union.
fn config_shape(schema: &serde_json::Value) -> anyhow::Result<(Mapper, Shape)> {
    let mapper = Mapper::for_config(schema.to_string().as_bytes())?;
    let mut shape = Shape::infer(mapper.schema(), mapper.index());

    if shape.type_.overlaps(types::OBJECT) {
        shape.type_ = types::OBJECT;
    }
    Ok((mapper, shape))
}

/// Names of the properties of a resource path: an optional `x-schema-name`,
/// followed by its `x-collection-name`.
#[derive(Debug)]
struct ResourcePath {
    schema: Option<String>,
    collection: String,
}

/// The resource path of a capture's resource configuration, if it has exactly
/// one `x-collection-name` property and at most one `x-schema-name`, each a
/// required string, which `path()` can return without further logic.
fn resource_path(shape: &Shape) -> Option<ResourcePath> {
    let annotated = |annotation: &str| -> Option<Vec<String>> {
        shape
            .object
            .properties
            .iter()
            .filter(|prop| prop.shape.annotations.get(annotation) == Some(&serde_json::json!(true)))
            .map(|prop| {
                (prop.is_required && prop.shape.type_ == types::STRING)
                    .then(|| prop.name.to_string())
            })
            .collect()
    };
    let mut collection = annotated("x-collection-name")?;
    let mut schema = annotated("x-schema-name")?;

    if collection.len() != 1 || schema.len() > 1 {
        return None;
    }
    Some(ResourcePath {
        schema: schema.pop(),
        collection: collection.pop().unwrap(),
    })
}

/// Extend the generated `ResourceConfig` class of a capture into a CDK
/// `BaseResourceConfig`, having `PATH_POINTERS` and a `path()` of its
/// resource `path` (if any). Returns whether there was a class to extend,
/// and whether it has a path.
fn extend_capture_resource(mapping: &mut Mapping, path: Option<&ResourcePath>) -> (bool, bool) {
    let Some(class) = mapping
        .classes
        .iter_mut()
        .find(|class| class.name == RESOURCE_CONFIG)
    else {
        return (false, false);
    };
    class.base = Some("common.BaseResourceConfig".to_string());

    let Some(path) = path else {
        return (true, false);
    };

    let names: Vec<&String> = path.schema.iter().chain([&path.collection]).collect();
    let pointers: Vec<String> = names
        .iter()
        .map(|name| json::Pointer(vec![json::ptr::Token::Property(name.to_string())]).to_string())
        .collect();
    let attributes: Vec<String> = names
        .iter()
        .map(|name| format!("self.{}", field_name(name, true).0))
        .collect();

    class.body = vec![
        String::new(),
        format!(
            "PATH_POINTERS: _typing.ClassVar[list[str]] = {}",
            crate::pydantic::python_literal(&serde_json::json!(pointers))
        ),
        String::new(),
        "def path(self) -> list[str]:".to_string(),
        "    \"\"\"Resource path of this binding: its `x-schema-name` (if any),".to_string(),
        "    followed by its `x-collection-name`.\"\"\"".to_string(),
        format!("    return [{}]", attributes.join(", ")),
    ];

    (true, true)
}

const ENDPOINT_CONFIG: &str = "EndpointConfig";
const RESOURCE_CONFIG: &str = "ResourceConfig";

#[cfg(test)]
mod test {
    use super::{Resources, Spec, config_types_py};

    fn render(spec: serde_json::Value, resources: Resources) -> String {
        let spec: Spec = serde_json::from_value(spec).unwrap();
        let types = config_types_py(&spec, resources).unwrap();
        format!(
            "{}\n# uses_common: {:?}, has_path: {:?}\n",
            types.source, types.uses_common, types.has_path
        )
    }

    #[test]
    fn capture_config_types() {
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
                    "namespace": {"type": "string", "x-schema-name": true},
                    "stream": {"type": "string", "x-collection-name": true},
                    "interval": {"type": "string", "format": "duration", "default": "PT0S"},
                },
                "required": ["namespace", "stream"],
            },
        });
        insta::assert_snapshot!(render(spec, Resources::Capture));
    }

    #[test]
    fn capture_resource_paths() {
        let resource = |properties: serde_json::Value, required: serde_json::Value| {
            render(
                serde_json::json!({
                    "resourceConfigSchema": {
                        "type": "object",
                        "properties": properties,
                        "required": required,
                    }
                }),
                Resources::Capture,
            )
        };

        insta::assert_snapshot!(
            [
                // A required schema name precedes the collection name.
                resource(
                    serde_json::json!({
                        "schema": {"type": "string", "x-schema-name": true},
                        "_table": {"type": "string", "x-collection-name": true},
                    }),
                    serde_json::json!(["schema", "_table"]),
                ),
                // Without a single collection name, there's no generated path.
                resource(
                    serde_json::json!({
                        "a": {"type": "string", "x-collection-name": true},
                        "b": {"type": "string", "x-collection-name": true},
                    }),
                    serde_json::json!([]),
                ),
                // Nor if a path property isn't a required string.
                resource(
                    serde_json::json!({
                        "schema": {"type": "string", "x-schema-name": true},
                        "table": {"type": "string", "x-collection-name": true},
                    }),
                    serde_json::json!(["table"]),
                ),
                resource(
                    serde_json::json!({"table": {"type": ["string", "null"], "x-collection-name": true}}),
                    serde_json::json!(["table"]),
                ),
                // Fields of the CDK's stock `ResourceConfig` still extend
                // `BaseResourceConfig`, so an optional `interval` is optional.
                resource(
                    serde_json::json!({
                        "name": {"type": "string", "x-collection-name": true},
                        "interval": {"type": "string", "format": "duration"},
                    }),
                    serde_json::json!(["name"]),
                ),
                // An absent schema is the CDK's stock `ResourceConfig`.
                render(serde_json::json!({}), Resources::Capture),
            ]
            .join("\n=====\n")
        );
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
        insta::assert_snapshot!(render(spec, Resources::Derivation));
    }

    #[test]
    fn configurations_are_classes() {
        // Schemas which may be objects (no `type`, or nullable) generate a class
        // rather than a union alias, and a non-object resource has no path.
        let spec = serde_json::json!({
            "configSchema": {"type": ["object", "null"], "properties": {"a": {"type": "string"}}},
            "resourceConfigSchema": {"properties": {"name": {"type": "string", "x-collection-name": true}}, "required": ["name"]},
        });
        let non_object = serde_json::json!({"resourceConfigSchema": {"type": "string"}});

        insta::assert_snapshot!(
            [
                render(spec, Resources::Capture),
                render(non_object, Resources::Capture),
            ]
            .join("\n=====\n")
        );
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
        insta::assert_snapshot!(render(spec, Resources::Derivation));
    }

    #[test]
    fn invalid_schemas_are_errors() {
        let spec: Spec = serde_json::from_value(serde_json::json!({
            "configSchema": {"type": "not-a-type"},
        }))
        .unwrap();
        let err = config_types_py(&spec, Resources::Capture).unwrap_err();
        insta::assert_snapshot!(format!("{err:#}"));
    }
}
