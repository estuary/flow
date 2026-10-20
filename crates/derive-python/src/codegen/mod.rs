use anyhow::Context;
use itertools::Itertools;
use proto_flow::flow;
use python_connector::pydantic::{Mapper, to_pascal_case};
use std::fmt::Write;

/// Generate Pydantic models and protocol types for a Python derivation.
pub fn types_py(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
    spec: &models::custom_connector::Spec,
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
    w.push_str(&python_connector::config_types_py(spec)?);

    // Generate namespaces of the protocol's messages, which are those of the
    // CDK. Each transform's read is specialized with its source documents.
    w.push_str(
        r#"class Request:
    """Request messages of the derive protocol (`estuary_cdk.derive.request`)."""

    Open = _request.Open
    Validate = _request.Validate
    Flush = _request.Flush
    Reset = _request.Reset
"#,
    );
    for (name, _) in transforms {
        let name = to_pascal_case(name);
        write!(
            w,
            r#"
    class Read{name}(_request.Read[Source{name}]):
        """A document read by transform `{name}`."""
"#,
        )
        .unwrap();
    }
    w.push_str(
        r#"

class Response:
    """Response messages of the derive protocol (`estuary_cdk.derive.response`)."""

    Opened = _response.Opened
    Published = _response.Published
    Flushed = _response.Flushed
    Validated = _response.Validated
    ValidatedTransform = _response.ValidatedTransform


"#,
    );

    // Generate IDerivation base class
    w.push_str(
        r#"class IDerivation[C = EndpointConfig, R: pydantic.BaseModel = ResourceConfig, S = dict[str, typing.Any]](ABC):
    """Abstract base class for derivation implementations.

    `C` is the type of the derivation's `config`, `R` of the resource
    configuration (`lambda`) of each transform, and `S` of its connector state.
    `C` and `R` default to the types generated from the derivation's `spec`.
    A derivation may instead declare types of its own, as
    `class Derivation(IDerivation[MyConfig, MyResourceConfig, MyState])`, and
    is then responsible for keeping them equivalent to its declared `spec`.

    `Derivation` must directly subclass `IDerivation`, from which its types
    are resolved: a validation of the derivation fails otherwise."""

    def __init__(self, open: Request.Open[C, R, S]):
        """Initialize the derivation with its Open message. Its `config` is
        `open.collection.derivation.config`, and its state is `open.state`."""
        pass

    @classmethod
    def validate(cls, validate: Request.Validate[C, R]) -> Response.Validated:
        """Validate the derivation as it's published, raising to fail.

        The default marks a transform as read-only if its resource
        configuration has a true `readOnly`."""
        return Response.Validated(
            transforms=[
                Response.ValidatedTransform(
                    readOnly=bool(getattr(transform.lambdaConfig, "readOnly", False))
                )
                for transform in validate.transforms
            ]
        )

"#,
    );

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
        r#"    async def flush(self, flush: Request.Flush, flushed: Response.Flushed[S]) -> collections.abc.AsyncIterator[Document]:
        """Complete deferred work for the current transaction, publishing all
        documents derived from prior reads.

        Set `flushed.state` to contribute a connector state update (an
        `estuary_cdk.flow.ConnectorStateUpdate`), and/or set `flushed.more = True`
        to request a further Flush iteration (a scatter/gather round across the
        derivation's shards). On each call `flush.statePatches` holds the
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
    "from estuary_cdk.derive import request as _request, response as _response",
];

/// Generate the entry point of a derivation from template, which runs the
/// `Derivation` exported by its directory `dir`.
pub fn main_py(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
    dir: &str,
) -> String {
    let template = include_str!("main.py.template");

    let read_types = transforms
        .iter()
        .map(|(name, _)| format!("    Request.Read{},", to_pascal_case(name)))
        .join("\n");

    let transform_methods = transforms
        .iter()
        .map(|(name, _)| format!("        derivation.{},", to_snake_case(name)))
        .join("\n");

    let module_path = python_connector::module_parts(&collection.name).join(".");

    template
        .replace("READ_TYPES", &read_types)
        .replace("TRANSFORM_METHODS", &transform_methods)
        .replace("MODULE_PATH", &module_path)
        .replace("LOAD_TASK", &python_connector::load_task_py(dir))
}

/// Generate the starter entry of a derivation: a working derivation whose
/// transforms publish nothing, as a starting point.
pub fn starter_py(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec)],
) -> String {
    let mut w = String::with_capacity(2048);
    let module_path = python_connector::module_parts(&collection.name).join(".");

    write!(
        w,
        r#""""Derivation of collection {name}.

The platform runs the `Derivation` class which this directory exports. It must
directly subclass the generated `IDerivation`: as `class Derivation(IDerivation)`,
or `class Derivation(IDerivation[MyConfig, MyResourceConfig, MyState])` to parse
its configurations and state into types of its own. Modules of this directory
import one another relatively (`from .helpers import ...`).
"""
import typing
from collections.abc import AsyncIterator
from {module_path} import IDerivation, Document, EndpointConfig, Request, ResourceConfig


class Derivation(IDerivation):
    def __init__(self, open: Request.Open[EndpointConfig, ResourceConfig, dict[str, typing.Any]]):
        super().__init__(open)
        # `EndpointConfig` is generated from the derivation's `spec.configSchema`.
        self.config = open.collection.derivation.config

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
        # Yield each `Document` derived from `read.doc`, such as
        # `yield Document(...)`. This starter publishes nothing.
        if False:
            yield

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
        let spec: models::custom_connector::Spec = serde_json::from_value(serde_json::json!({
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

        // Test starter_py generation
        let starter_output = starter_py(&sums_spec, &transforms);
        insta::assert_snapshot!("starter_py", starter_output);
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

        let output = main_py(&sums_spec, &transforms, "sums");
        insta::assert_snapshot!(output);
    }
}
