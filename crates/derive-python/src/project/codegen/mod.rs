use anyhow::Context;
use itertools::Itertools;
use proto_flow::flow;
use python_connector::pydantic::{Mapper, to_pascal_case, to_snake_case};
use std::fmt::Write;

/// A transform of a derivation, as its generated types see it.
#[derive(Debug)]
pub struct Transform<'a> {
    /// Identifier of the transform's method (`transform_{snake}`): its name,
    /// in snake case. Its prefix keeps it clear of keywords and of
    /// `IDerivation`'s own methods.
    pub snake: String,
    /// Identifier of the transform's types (`Source{Pascal}`).
    pub pascal: String,
    pub name: &'a str,
    /// Source collection of the transform.
    pub collection: &'a flow::CollectionSpec,
}

impl<'a> Transform<'a> {
    /// Build the transforms of a derivation from their names and source
    /// collections. Their names must be unique.
    pub fn of(
        transforms: impl IntoIterator<Item = (&'a str, &'a flow::CollectionSpec)>,
    ) -> anyhow::Result<Vec<Self>> {
        let transforms: Vec<Self> = transforms
            .into_iter()
            .map(|(name, collection)| {
                let snake = to_snake_case(name);
                if snake.is_empty() {
                    anyhow::bail!(
                        "transform {name:?} has no ASCII letters or digits, which name its method"
                    );
                }
                Ok(Self {
                    pascal: to_pascal_case(&snake),
                    snake,
                    name,
                    collection,
                })
            })
            .try_collect()?;

        for (index, transform) in transforms.iter().enumerate() {
            if let Some(prior) = transforms[..index]
                .iter()
                .find(|prior| prior.snake == transform.snake || prior.pascal == transform.pascal)
            {
                anyhow::bail!(
                    "transforms {:?} and {:?} have names `{}` and `{}`, \
                     whose generated methods or types collide: rename one",
                    prior.name,
                    transform.name,
                    prior.snake,
                    transform.snake,
                );
            }
        }
        Ok(transforms)
    }
}

/// Generate the module of a derivation of `collection`: Pydantic models of its
/// documents and of each transform's source documents, its configuration
/// types, the protocol's messages, and the generic `IDerivation` base class.
pub fn types_py(
    collection: &flow::CollectionSpec,
    spec: &models::custom_connector::Spec,
    transforms: &[Transform],
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
    mapper.map(mapper.schema(), "Document")?.render(&mut w);

    for Transform {
        name,
        pascal,
        collection,
        ..
    } in transforms
    {
        let source = format!("Source{pascal}");

        let mapper = if collection.read_schema_json.is_empty() {
            Mapper::new(&collection.write_schema_json, &source)
        } else {
            Mapper::new(&collection.read_schema_json, &source)
        }
        .with_context(|| format!("invalid schema of collection {}", collection.name))?;

        writeln!(
            w,
            "# Generated for documents of transform `{name}`, of collection {}",
            collection.name
        )
        .unwrap();
        mapper
            .map(mapper.schema(), &source)
            .with_context(|| format!("invalid schema of collection {}", collection.name))?
            .render(&mut w);
    }

    // A transform's `lambda` is its `ResourceConfig`.
    w.push_str(&python_connector::endpoint_config_py(spec)?);

    let (mapper, shape) = python_connector::config_shape(
        spec.resource_config_schema
            .as_ref()
            .expect("resolved by resolve_spec")
            .get(),
    )
    .context("invalid `resourceConfigSchema`")?;
    w.push_str("# Generated for the resource configuration of `spec.resourceConfigSchema`.\n");
    mapper
        .map_shape(shape, "ResourceConfig")
        .context("invalid `resourceConfigSchema`")?
        .render(&mut w);

    // Generate namespaces of the protocol's messages, which are those of the
    // CDK. Each transform's Read is specialized with its source documents.
    w.push_str(
        r#"class Request:
    """Request messages of the derive protocol (`estuary_cdk.derive.request`)."""

    class Validate[C = EndpointConfig, R: pydantic.BaseModel = ResourceConfig](_request.Validate[C, R]):
        """Validate with the same configuration defaults as IDerivation."""

    class Open[C = EndpointConfig, R: pydantic.BaseModel = ResourceConfig, S = dict[str, typing.Any]](_request.Open[C, R, S]):
        """Open with the same configuration and state defaults as IDerivation."""

    Read = _request.Read
    Flush = _request.Flush
    Reset = _request.Reset
"#,
    );
    for Transform { name, pascal, .. } in transforms {
        write!(
            w,
            r#"
    class Read{pascal}(_request.Read[Source{pascal}]):
        """A document read by transform `{name}`."""
"#,
        )
        .unwrap();
    }
    w.push_str(
        r#"

class Response:
    """Response messages of the derive protocol (`estuary_cdk.derive.response`)."""

    Validated = _response.Validated
    ValidatedTransform = _response.ValidatedTransform
    Opened = _response.Opened
    Published = _response.Published
    Flushed = _response.Flushed


class IDerivation[C = EndpointConfig, R: pydantic.BaseModel = ResourceConfig, S = dict[str, typing.Any]](ABC):
    """Abstract base class of derivation implementations.

    `C` is the type of the derivation's `config`, `R` of the resource
    configuration (`lambda`) of each transform, and `S` of its connector state.
    `C` and `R` default to the types generated from the derivation's `spec`.
    A derivation may instead declare types of its own, as
    `class Derivation(IDerivation[MyConfig, MyResourceConfig, MyState])`, and
    is then responsible for keeping them equivalent to its declared `spec`.

    `Derivation` must directly subclass `IDerivation`, from which its types
    are resolved: a validation of the derivation fails otherwise.

    The platform constructs a derivation only through `open`, and its
    `__init__` is the derivation's own.

    Each transform has a `transform_{name}` method, which yields the documents
    it derives from each read. Its name is the transform's, in snake case."""

    @classmethod
    async def open(cls, open: Request.Open[C, R, S]) -> tuple[typing.Self, Response.Opened]:
        """Construct the derivation from its Open message. Its `config` is
        `open.collection.derivation.config`, and its state is `open.state`.

        The default is `cls()`, for a derivation which needs neither. Override
        it to build the derivation's components, which may require awaiting,
        and pass them to an `__init__` of your own."""
        return cls(), Response.Opened()

    @classmethod
    async def validate(cls, validate: Request.Validate[C, R], validated: Response.Validated) -> None:
        """Validate the derivation as it's published, raising to fail.

        `validated` holds a default of each transform, in the order of
        `validate.transforms`. A transform is read-only if its resource
        configuration has a true `readOnly`. Modify it in place.
        The default does nothing."""
        pass

"#,
    );

    for Transform {
        snake,
        pascal,
        name,
        ..
    } in transforms
    {
        write!(
            w,
            r#"    @abstractmethod
    async def transform_{snake}(self, read: Request.Read{pascal}) -> collections.abc.AsyncIterator[Document]:
        """Derive documents from a document read by transform `{name}`."""
        if False:
            yield  # Mark as a generator.

"#,
        )
        .unwrap();
    }

    w.push_str(
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
        The default does nothing."""
        if False:
            yield  # Mark as a generator.

    async def reset(self) -> None:
        """Reset internal state for testing. The default does nothing."""
        pass
"#,
    );

    Ok(format!(
        "\"\"\"Generated types of derivation {}, from its declared `spec` and transforms.\"\"\"\n\
         {}\n\n\n{w}",
        collection.name,
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
pub fn main_py(task: &str, transforms: &[Transform], dir: &str) -> String {
    let template = include_str!("main.py.template");

    let read_types = transforms
        .iter()
        .map(|transform| format!("    Request.Read{},", transform.pascal))
        .join("\n");
    let transform_methods = transforms
        .iter()
        .map(|transform| format!("        derivation.transform_{},", transform.snake))
        .join("\n");

    template
        .replace("READ_TYPES", &read_types)
        .replace("TRANSFORM_METHODS", &transform_methods)
        .replace(
            "MODULE_PATH",
            &python_connector::module_parts(task).join("."),
        )
        .replace("LOAD_TASK", &python_connector::load_task_py(dir))
}

/// Generate the starter entry of a derivation: a working derivation whose
/// transforms publish nothing.
pub fn starter_py(task: &str, transforms: &[Transform]) -> String {
    let mut w = String::with_capacity(4096);
    let module_path = python_connector::module_parts(task).join(".");

    write!(
        w,
        r#""""Derivation {task}.

The platform runs the `Derivation` class which this directory exports. It must
directly subclass the generated `IDerivation`: as `class Derivation(IDerivation)`,
or `class Derivation(IDerivation[MyConfig, MyResourceConfig, MyState])` to parse
its configurations and state into types of its own. Modules of this directory
import one another relatively (`from .helpers import ...`).

Each transform has a `transform_{{name}}` method, named for the transform,
which yields the documents it derives from each read. This starter publishes
nothing.
"""
import typing
from collections.abc import AsyncIterator
from {module_path} import Document, EndpointConfig, IDerivation, Request, ResourceConfig, Response


class Derivation(IDerivation):
    def __init__(self, config: EndpointConfig):
        self.config = config

    @classmethod
    async def open(
        cls, open: Request.Open[EndpointConfig, ResourceConfig, dict[str, typing.Any]]
    ) -> tuple[typing.Self, Response.Opened]:
        # `EndpointConfig` is generated from the derivation's `spec.configSchema`,
        # and `ResourceConfig` from its `spec.resourceConfigSchema`.
        return cls(open.collection.derivation.config), Response.Opened()

"#,
    )
    .unwrap();

    for Transform { snake, pascal, .. } in transforms {
        write!(
            w,
            r#"    async def transform_{snake}(self, read: Request.Read{pascal}) -> AsyncIterator[Document]:
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
        let transforms =
            Transform::of([("fromInts", &ints_spec), ("process-strings", &strings_spec)]).unwrap();

        // A declared spec, whose types are generated alongside the documents.
        let spec: models::custom_connector::Spec = serde_json::from_value(serde_json::json!({
            "configSchema": {
                "type": "object",
                "properties": {
                    "multiplier": {"type": "integer", "default": 2},
                    // Its module is imported by the header, ahead of `Document`.
                    "apiKey": {"type": "string", "secret": true, "x-python-type": "pydantic.SecretStr"},
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
        let types_output = types_py(&sums_spec, &spec, &transforms).unwrap();
        insta::assert_snapshot!("types_py", types_output);

        // Test starter_py generation
        let starter_output = starter_py(&sums_spec.name, &transforms);
        insta::assert_snapshot!("starter_py", starter_output);
    }

    #[test]
    fn names_of_transforms() {
        let collection = flow::CollectionSpec::default();
        let names = [
            "fromInts",
            "process-strings",
            "2024",
            "class",
            "flush",
            "über",
        ];
        let transforms = Transform::of(names.iter().map(|name| (*name, &collection))).unwrap();

        let names: Vec<_> = transforms
            .iter()
            .map(|transform| (transform.snake.as_str(), transform.pascal.as_str()))
            .collect();
        insta::assert_debug_snapshot!(names, @r#"
        [
            (
                "from_ints",
                "FromInts",
            ),
            (
                "process_strings",
                "ProcessStrings",
            ),
            (
                "2024",
                "2024",
            ),
            (
                "class",
                "Class",
            ),
            (
                "flush",
                "Flush",
            ),
            (
                "ber",
                "Ber",
            ),
        ]
        "#);

        let of = |names: &[&str]| {
            Transform::of(names.iter().map(|name| (*name, &collection)))
                .map(|_| ())
                .map_err(|err| format!("{err:#}"))
        };
        insta::assert_debug_snapshot!([
            of(&["fromInts", "from_ints"]),
            of(&["a_1", "a1"]),
            of(&["-"]),
        ], @r#"
        [
            Err(
                "transforms \"fromInts\" and \"from_ints\" have names `from_ints` and `from_ints`, whose generated methods or types collide: rename one",
            ),
            Err(
                "transforms \"a_1\" and \"a1\" have names `a_1` and `a1`, whose generated methods or types collide: rename one",
            ),
            Err(
                "transform \"-\" has no ASCII letters or digits, which name its method",
            ),
        ]
        "#);
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

        let transforms =
            Transform::of([("fromInts", &ints_spec), ("process-strings", &strings_spec)]).unwrap();

        let output = main_py(&sums_spec.name, &transforms, "sums");
        insta::assert_snapshot!(output);
    }
}
