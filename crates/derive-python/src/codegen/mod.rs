use anyhow::Context;
use itertools::Itertools;
use proto_flow::flow;
use python_connector::Spec;
use python_connector::pydantic::{Mapper, to_pascal_case};
use std::fmt::Write;

/// Generate Pydantic models and protocol types for a Python derivation.
pub fn types_py(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
    spec: &Spec,
) -> anyhow::Result<String> {
    let mut w = String::with_capacity(4096);

    let mapper = Mapper::new(&collection.write_schema_json, "Document")
        .with_context(|| format!("invalid schema of collection {}", collection.name))?;
    writeln!(
        w,
        "# Generated for published documents of derived collection {}",
        collection.name
    )
    .unwrap();
    mapper.map(mapper.schema(), "Document").render(&mut w);

    // Generate Source{Transform} collection types for each transform
    for (name, collection) in transforms {
        let source_name = format!("Source{}", to_pascal_case(name));

        let mapper = if collection.read_schema_json.is_empty() {
            Mapper::new(&collection.write_schema_json, &source_name)
        } else {
            Mapper::new(&collection.read_schema_json, &source_name)
        }
        .with_context(|| format!("invalid schema of collection {}", collection.name))?;

        writeln!(
            w,
            "# Generated for read documents of sourced collection {}",
            collection.name
        )
        .unwrap();
        mapper.map(mapper.schema(), &source_name).render(&mut w);
    }

    // Generate configuration types of the declared spec.
    let config_types =
        python_connector::config_types_py(spec, python_connector::Resources::Derivation)?;
    w.push_str(&config_types.source);

    // Generate protocol message types
    write!(
        w,
        r#"class ConnectorState(pydantic.BaseModel):
    """Mirror of Estuary protocol's flow.ConnectorState."""

    # An updated state, or patch thereof.
    updated: dict[str, typing.Any]
    # When true, `updated` is an RFC 7396 JSON Merge Patch rather than a full replacement.
    merge_patch: bool = pydantic.Field(default=False, serialization_alias='mergePatch')


class Request(pydantic.BaseModel):

    class Open[R = ResourceConfig](pydantic.BaseModel):
        """Opens the derivation with its connector state."""

        state: dict[str, typing.Any]
        # Resource configuration (`lambda`) of each transform, in order.
        resources: list[R] = []

    class Transform[R = ResourceConfig](pydantic.BaseModel):
        """A transform of a derivation which is being validated."""

        name: str
        # Resource configuration of the transform, which is its `lambda`.
        resource_config: R = pydantic.Field(alias='resourceConfig')

    class Validate[R = ResourceConfig](pydantic.BaseModel):
        """Validates the derivation of collection `name` as it's published."""

        name: str
        transforms: "list[Request.Transform[R]]"

    class Flush(pydantic.BaseModel):
        # Aggregated connector state patches contributed by all participating shards
        # in the PREVIOUS Flush iteration of the current transaction; empty on the
        # first iteration.
        state_patches: list[typing.Any] = pydantic.Field(default_factory=list, alias='statePatches')

    class Reset(pydantic.BaseModel):
        pass

    flush: typing.Optional[Flush] = None
    reset: typing.Optional[Reset] = None

"#
    )
    .unwrap();

    // Generate Read{Transform} classes for each transform.
    for (idx, (name, _)) in transforms.iter().enumerate() {
        let name = to_pascal_case(name);

        write!(
            w,
            r#"
    class Read{name}(pydantic.BaseModel):
        doc: Source{name}
        transform: typing.Literal[{idx}]

"#,
        )
        .unwrap();
    }

    // Generate discriminated union over all Read{Transform} types.
    let union_names = transforms
        .iter()
        .map(|(name, _)| format!("Read{}", to_pascal_case(name)))
        .join(" | ");

    write!(
        w,
        "    read : typing.Annotated[{union_names}, pydantic.Field(discriminator='transform')] | None = None"
    )
    .unwrap();

    write!(
        w,
        r#"

    @pydantic.model_validator(mode='before')
    @classmethod
    def inject_default_transform(cls, data: dict[str, typing.Any]) -> dict[str, typing.Any]:
        if 'read' in data and 'transform' not in data['read']:
            data['read']['transform'] = 0 # Make implicit default explicit
        return data


class Response(pydantic.BaseModel):
    class Opened(pydantic.BaseModel):
        pass

    class Published(pydantic.BaseModel):
        doc: Document

    class Flushed(pydantic.BaseModel):
        # Connector state update to contribute this transaction.
        # Aggregated across shards and fed back via Request.Flush.state_patches.
        state: typing.Optional[ConnectorState] = None
        # Request a further Flush iteration this transaction.
        more: bool = False

    class Validated(pydantic.BaseModel):
        """Outcome of validating the derivation."""

        class Transform(pydantic.BaseModel):
            # Does this transform never publish documents? A read-only
            # transform needn't wait for prior transactions to commit.
            read_only: bool = pydantic.Field(default=False, serialization_alias='readOnly')

        # Validated transforms, in the order of `Request.Validate.transforms`.
        transforms: "list[Response.Validated.Transform]"

    opened: typing.Optional[Opened] = None
    published: typing.Optional[Published] = None
    flushed: typing.Optional[Flushed] = None

"#
    )
    .unwrap();

    // Generate IDerivation base class
    write!(
        w,
        r#"class IDerivation[C = EndpointConfig, R = ResourceConfig](ABC):
    """Abstract base class for derivation implementations.

    `C` is the type of the derivation's `config`, and `R` of the resource
    configuration (`lambda`) of each transform. They default to the types
    generated from the derivation's `spec`. A derivation may instead declare
    types of its own, as `class Derivation(IDerivation[MyConfig])`, and is
    then responsible for keeping them equivalent to its declared `spec`."""

    def __init__(self, open: Request.Open[R], config: C):
        """Initialize the derivation with an Open message, and its `config`."""
        pass

    @classmethod
    def validate(cls, validate: Request.Validate[R], config: C) -> Response.Validated:
        """Validate the derivation as it's published, raising to fail.

        The default marks a transform as read-only if its resource
        configuration has a true `readOnly`."""
        return Response.Validated(
            transforms=[
                Response.Validated.Transform(
                    read_only=bool(getattr(transform.resource_config, "readOnly", False))
                )
                for transform in validate.transforms
            ]
        )

"#
    )
    .unwrap();

    // Generate abstract transform methods
    for (name, _) in transforms {
        let method_name = to_snake_case(name);
        let class_name = to_pascal_case(name);

        write!(
            w,
            r#"    @abstractmethod
    async def {method_name}(self, read: Request.Read{class_name}) -> collections.abc.AsyncIterator[Document]:
        """Transform method for '{name}' source."""
        if False:
            yield  # Mark as a generator.

"#,
        )
        .unwrap();
    }

    // Add default lifecycle methods
    write!(
        w,
        r#"    async def flush(self, state_patches: list[typing.Any], flushed: Response.Flushed) -> collections.abc.AsyncIterator[Document]:
        """Complete deferred work for the current transaction, publishing all
        documents derived from prior reads.

        Set `flushed.state` to contribute a connector state update, and/or set
        `flushed.more = True` to request a further Flush iteration (a scatter/gather
        round across the derivation's shards). On each call `state_patches` holds the
        aggregated `state` updates returned by all shards in the previous iteration
        (empty on the first). All Documents MUST be yielded before flush returns
        a terminal (`more = False`) Response.Flushed.
        Override to implement pipelining or stateful derivations."""
        if False:
            yield  # Mark as a generator.

    async def reset(self):
        """Reset internal state for testing. Override if needed."""
        pass
"#
    )
    .unwrap();

    Ok(format!(
        "{}\n\n{w}",
        python_connector::imports_py(&w, PROTOCOL_IMPORTS)
    ))
}

/// Imports of the hand-written protocol types, beside the (aliased) modules
/// which generated types use.
const PROTOCOL_IMPORTS: &[&str] = &[
    "from abc import ABC, abstractmethod",
    "import collections.abc",
    "import typing",
    "import pydantic",
];

/// Generate the main.py runtime wrapper from template.
pub fn main_py(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
    module_name: &str,
) -> String {
    let template = include_str!("main.py.template");

    let read_type = transforms
        .iter()
        .map(|(name, _)| format!("Request.Read{}", to_pascal_case(name)))
        .join(" | ");

    let dispatch = transforms
        .iter()
        .map(|(name, _)| {
            format!(
                "            case Request.Read{}():\n                return derivation.{}(read)",
                to_pascal_case(name),
                to_snake_case(name),
            )
        })
        .join("\n");

    let module_path = python_connector::module_parts(&collection.name).join(".");

    template
        .replace("READ_TYPE", &read_type)
        .replace("DISPATCH", &dispatch)
        .replace("MODULE_PATH", &module_path)
        .replace("MODULE_NAME", module_name)
}

/// Generate a stub implementation for a missing module.
pub fn stub_py(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
) -> String {
    let mut w = String::with_capacity(2048);
    let module_path = python_connector::module_parts(&collection.name).join(".");

    write!(
        w,
        r#""""Derivation implementation for {name}."""
from collections.abc import AsyncIterator
from {module_path} import IDerivation, Document, EndpointConfig, Request


# Implementation for derivation {name}.
# `EndpointConfig` is generated from the derivation's `spec.configSchema`.
class Derivation(IDerivation):
    def __init__(self, open: Request.Open, config: EndpointConfig):
        super().__init__(open, config)
        self.config = config

"#,
        name = &collection.name,
    )
    .unwrap();

    for (name, _) in transforms {
        let method_name = to_snake_case(name);
        let class_name = to_pascal_case(name);

        write!(
            w,
            r#"    async def {method_name}(self, read: Request.Read{class_name}) -> AsyncIterator[Document]:
        raise NotImplementedError("{method_name} not implemented")
        if False:
            yield  # Mark as a generator.

"#,
        )
        .unwrap();
    }

    w
}

fn to_snake_case(name: &str) -> String {
    lazy_static::lazy_static! {
        static ref CAMEL_BOUNDARY: regex::Regex = regex::Regex::new(r"([a-z0-9])([A-Z])").unwrap();
    }

    let with_boundaries = CAMEL_BOUNDARY.replace_all(name, "${1}_${2}");
    with_boundaries
        .chars()
        .map(|c| {
            if c.is_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>()
        .split('_')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("_")
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn codegen() {
        // Comprehensive fixture covering:
        // - Derived collection with regular object schema (no anchor)
        // - Source collection with anchor reference
        // - Source collection with regular schema
        // - Multiple transforms with different naming conventions (camelCase, kebab-case)
        let fixture = serde_json::json!({
            "test://example/catalog.yaml": {
                "collections": {
                    "patterns/sums": {
                        "schema": "test://example/sums.json",
                        "key": ["/Key"]
                    },
                    "patterns/ints": {
                        "schema": "test://example/ints.json#IntValue",
                        "key": ["/Key"]
                    },
                    "patterns/strings": {
                        "schema": "test://example/strings.json",
                        "key": ["/id"]
                    }
                }
            },
            "test://example/sums.json": {
                "type": "object",
                "properties": {
                    "Key": {"type": "string"},
                    "Sum": {"type": "integer"},
                    "Count": {"type": "integer"}
                },
                "required": ["Key", "Sum"]
            },
            "test://example/ints.json": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"}
                },
                "required": ["field"],
                "$defs": {
                    "intValue": {
                        "$anchor": "IntValue",
                        "type": "object",
                        "properties": {
                            "Key": {"type": "string"},
                            "Int": {"type": "integer"}
                        },
                        "required": ["Key", "Int"]
                    }
                }
            },
            "test://example/strings.json": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                    "text": {"type": "string"},
                    "metadata": {
                        "type": "object",
                        "additionalProperties": true
                    }
                },
                "required": ["id", "text"]
            }
        });

        let mut sources = sources::scenarios::evaluate_fixtures(Default::default(), &fixture);
        sources::inline_draft_catalog(&mut sources);

        let tables::DraftCatalog {
            collections,
            errors,
            ..
        } = sources;
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");

        // Extract collections by name
        let sums = collections
            .iter()
            .find(|c| c.collection.as_str() == "patterns/sums")
            .unwrap();
        let ints = collections
            .iter()
            .find(|c| c.collection.as_str() == "patterns/ints")
            .unwrap();
        let strings = collections
            .iter()
            .find(|c| c.collection.as_str() == "patterns/strings")
            .unwrap();

        let pluck_schema = |c: &tables::DraftCollection| -> bytes::Bytes {
            c.model
                .as_ref()
                .unwrap()
                .schema
                .as_ref()
                .unwrap()
                .get()
                .as_bytes()
                .to_vec()
                .into()
        };

        let sums_spec = proto_flow::flow::CollectionSpec {
            name: sums.collection.to_string(),
            write_schema_json: pluck_schema(&sums),
            read_schema_json: vec![].into(),
            ..Default::default()
        };

        let ints_spec = proto_flow::flow::CollectionSpec {
            name: ints.collection.to_string(),
            write_schema_json: pluck_schema(&ints),
            read_schema_json: vec![].into(),
            ..Default::default()
        };

        let strings_spec = proto_flow::flow::CollectionSpec {
            name: strings.collection.to_string(),
            write_schema_json: pluck_schema(&strings),
            read_schema_json: vec![].into(),
            ..Default::default()
        };

        // Define transforms with different naming conventions to test case conversion
        let transforms = vec![("fromInts", &ints_spec), ("process-strings", &strings_spec)];

        // A declared spec, whose types are generated alongside the documents.
        let spec: python_connector::Spec = serde_json::from_value(serde_json::json!({
            "configSchema": {
                "type": "object",
                "properties": {
                    "multiplier": {"type": "integer", "default": 2},
                    "apiKey": {"type": "string", "secret": true},
                },
                "required": ["apiKey"],
            },
            "resourceConfigSchema": {
                "type": "object",
                "properties": {"readOnly": {"type": "boolean", "default": false}},
            },
        }))
        .unwrap();

        // Test types_py generation
        let types_output = types_py(&sums_spec, &transforms, &spec).unwrap();
        insta::assert_snapshot!("types_py", types_output);

        // Test stub_py generation
        let stub_output = stub_py(&sums_spec, &transforms);
        insta::assert_snapshot!("stub_py", stub_output);
    }

    #[test]
    fn test_main_py() {
        let sums_spec = proto_flow::flow::CollectionSpec {
            name: "patterns/sums".to_string(),
            write_schema_json: vec![].into(),
            read_schema_json: vec![].into(),
            ..Default::default()
        };

        let ints_spec = proto_flow::flow::CollectionSpec {
            name: "patterns/ints".to_string(),
            write_schema_json: vec![].into(),
            read_schema_json: vec![].into(),
            ..Default::default()
        };

        let strings_spec = proto_flow::flow::CollectionSpec {
            name: "patterns/strings".to_string(),
            write_schema_json: vec![].into(),
            read_schema_json: vec![].into(),
            ..Default::default()
        };

        let transforms = vec![("fromInts", &ints_spec), ("process-strings", &strings_spec)];

        let output = main_py(&sums_spec, &transforms, "my_module");
        insta::assert_snapshot!(output);
    }
}
