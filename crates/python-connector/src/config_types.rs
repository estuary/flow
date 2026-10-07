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
    /// and is importable as `common` (`estuary_cdk.capture.common`).
    Capture,
    /// A derivation's `ResourceConfig` is the `lambda` of a transform.
    Derivation,
}

/// Generated configuration types of a connector.
#[derive(Debug)]
pub struct ConfigTypes {
    /// Python source of the `EndpointConfig` and `ResourceConfig` types.
    /// It uses the `datetime`, `typing`, and `pydantic` modules, and for
    /// captures, the CDK's `common` module.
    pub source: String,
    /// Resource path pointers of a capture's `ResourceConfig`, if it has a
    /// generated `path()`.
    pub path_pointers: Option<Vec<String>>,
}

/// Generate the `EndpointConfig` and `ResourceConfig` types of `spec`.
pub fn config_types_py(spec: &Spec, resources: Resources) -> anyhow::Result<ConfigTypes> {
    let mut source = String::new();

    let (mapper, shape) = config_shape(&spec.config_schema).context("invalid `configSchema`")?;
    let mapping = mapper.map_shape(shape, ENDPOINT_CONFIG);
    source.push_str("# Generated for the endpoint configuration of `spec.configSchema`.\n");
    mapping.render(&mut source);

    let (mapper, shape) =
        config_shape(&spec.resource_config_schema).context("invalid `resourceConfigSchema`")?;
    let path = match resources {
        Resources::Capture => resource_path(&shape),
        Resources::Derivation => None,
    };
    let is_stock = is_stock_resource(&shape);
    let mut mapping = mapper.map_shape(shape, RESOURCE_CONFIG);

    let path_pointers = match path {
        Some(path) => {
            extend_capture_resource(&mut mapping, &path, is_stock && path.schema.is_none())
        }
        None => None,
    };
    if resources == Resources::Capture && path_pointers.is_none() {
        // The user writes `path()` of a resource configuration of their own
        // (the escape hatch).
        source.push_str(
            "# The resource schema doesn't have exactly one `x-collection-name` property\n\
             # (and at most one `x-schema-name`), each a required string, so\n\
             # `ResourceConfig` has no `path()`: declare a subclass of the CDK's\n\
             # `BaseResourceConfig` with a `path()` of its own.\n",
        );
    }
    source.push_str("# Generated for the resource configuration of `spec.resourceConfigSchema`.\n");
    mapping.render(&mut source);

    Ok(ConfigTypes {
        source,
        path_pointers,
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

/// Does a resource configuration have the fields of the CDK's stock
/// `ResourceConfig`: a `name` (its path) and an update `interval`?
fn is_stock_resource(shape: &Shape) -> bool {
    let property = |name: &str| shape.object.properties.iter().find(|p| &*p.name == name);

    let name = property("name").is_some_and(|prop| {
        prop.shape.type_ == types::STRING
            && prop.shape.annotations.get("x-collection-name") == Some(&serde_json::json!(true))
    });
    let interval = property("interval").is_some_and(|prop| {
        prop.shape.type_ == types::STRING
            && prop.shape.string.format == Some(json::schema::formats::Format::Duration)
    });
    name && interval
}

/// Extend the generated `ResourceConfig` class of a capture into a CDK
/// `BaseResourceConfig` having a `path()`, returning its path pointers,
/// or None if there's no class to extend.
///
/// A configuration having the fields of the CDK's stock `ResourceConfig`
/// (`is_stock`) extends it instead, because CDK helpers such as
/// `common.open_binding` are typed by it and read its `interval`.
fn extend_capture_resource(
    mapping: &mut Mapping,
    path: &ResourcePath,
    is_stock: bool,
) -> Option<Vec<String>> {
    let class = mapping
        .classes
        .iter_mut()
        .find(|class| class.name == RESOURCE_CONFIG)?;

    let names: Vec<&String> = path.schema.iter().chain([&path.collection]).collect();
    let pointers: Vec<String> = names
        .iter()
        .map(|name| json::Pointer(vec![json::ptr::Token::Property(name.to_string())]).to_string())
        .collect();
    let attributes: Vec<String> = names
        .iter()
        .map(|name| format!("self.{}", field_name(name, true).0))
        .collect();

    class.base = Some(
        if is_stock {
            "common.ResourceConfig"
        } else {
            "common.BaseResourceConfig"
        }
        .to_string(),
    );
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

    Some(pointers)
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
            "{}\n# path_pointers: {:?}\n",
            types.source, types.path_pointers
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
