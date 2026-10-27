use anyhow::Context;
use itertools::Itertools;
use proto_flow::flow;
use python_connector::pydantic::{
    Mapper, python_literal, sanitize_python_identifier, to_pascal_case, to_snake_case,
};
use std::fmt::Write;

/// A binding of a materialization, as its generated types see it.
#[derive(Debug)]
pub struct Binding<'a> {
    /// Identifier of the binding's methods (`store_{snake}`): its resource
    /// path, in snake case. Its prefixes keep it clear of keywords and of
    /// `IMaterialization`'s own methods.
    pub snake: String,
    /// Identifier of the binding's types (`Source{Pascal}`).
    pub pascal: String,
    pub resource_path: Vec<String>,
    pub delta_updates: bool,
    pub collection: &'a flow::CollectionSpec,
    /// Group-by fields of the binding, which are its key.
    pub keys: &'a [String],
}

impl<'a> Binding<'a> {
    /// Build the bindings of a materialization from their resource paths,
    /// delta-updates modes, collections, and keys. Their names must be unique.
    pub fn of(
        bindings: impl IntoIterator<Item = (Vec<String>, bool, &'a flow::CollectionSpec, &'a [String])>,
    ) -> anyhow::Result<Vec<Self>> {
        let bindings: Vec<Self> = bindings
            .into_iter()
            .map(|(resource_path, delta_updates, collection, keys)| {
                let snake = to_snake_case(&resource_path.join("_"));
                if snake.is_empty() {
                    anyhow::bail!(
                        "resource path {resource_path:?} has no ASCII letters or digits, which name its binding's methods"
                    );
                }
                Ok(Self {
                    pascal: to_pascal_case(&snake),
                    snake,
                    resource_path,
                    delta_updates,
                    collection,
                    keys,
                })
            })
            .try_collect()?;

        for (index, binding) in bindings.iter().enumerate() {
            if let Some(prior) = bindings[..index]
                .iter()
                .find(|prior| prior.snake == binding.snake || prior.pascal == binding.pascal)
            {
                anyhow::bail!(
                    "bindings of resource paths {:?} and {:?} have names `{}` and `{}`, \
                     whose generated methods or types collide: give one a distinct resource path",
                    prior.resource_path,
                    binding.resource_path,
                    prior.snake,
                    binding.snake,
                );
            }
        }
        Ok(bindings)
    }
}

/// Generate the module of a materialization `task`: Pydantic models of each
/// binding's documents and key, its configuration types, the protocol's
/// messages, and the generic `IMaterialization` base class.
pub fn types_py(
    task: &str,
    spec: &models::custom_connector::Spec,
    bindings: &[Binding],
) -> anyhow::Result<String> {
    let mut w = String::with_capacity(4096);

    for binding in bindings {
        let Binding {
            snake,
            pascal,
            collection,
            ..
        } = binding;
        let source = format!("Source{pascal}");

        let mapper = if collection.read_schema_json.is_empty() {
            Mapper::new(&collection.write_schema_json, &source)
        } else {
            Mapper::new(&collection.read_schema_json, &source)
        }
        .with_context(|| format!("invalid schema of collection {}", collection.name))?;

        writeln!(
            w,
            "# Generated for documents of binding `{snake}`, of collection {}",
            collection.name
        )
        .unwrap();
        mapper
            .map(mapper.schema(), &source)
            .with_context(|| format!("invalid schema of collection {}", collection.name))?
            .render(&mut w);
        key_py(binding, &mut w)?;
    }

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
    // CDK. Each binding's Load and Store are specialized with its key and documents.
    w.push_str(
        r#"class Request:
    """Request messages of the materialize protocol (`estuary_cdk.materialize.request`)."""

    class Validate[C = EndpointConfig, R: pydantic.BaseModel = ResourceConfig](_request.Validate[C, R]):
        """Validate with the same configuration defaults as IMaterialization."""

    class Apply[C = EndpointConfig, R: pydantic.BaseModel = ResourceConfig, S = dict[str, typing.Any]](_request.Apply[C, R, S]):
        """Apply with the same configuration and state defaults as IMaterialization."""

    class Open[C = EndpointConfig, R: pydantic.BaseModel = ResourceConfig, S = dict[str, typing.Any]](_request.Open[C, R, S]):
        """Open with the same configuration and state defaults as IMaterialization."""

    Load = _request.Load
    Store = _request.Store
    Flush = _request.Flush
    StartCommit = _request.StartCommit
    Acknowledge = _request.Acknowledge
"#,
    );
    for Binding {
        snake,
        pascal,
        delta_updates,
        ..
    } in bindings
    {
        if !delta_updates {
            write!(
                w,
                r#"
    class Load{pascal}(_request.Load[Key{pascal}]):
        """A key of binding `{snake}` to load."""
"#,
            )
            .unwrap();
        }
        write!(
            w,
            r#"
    class Store{pascal}(_request.Store[Key{pascal}, Source{pascal}]):
        """A document of binding `{snake}` to store."""
"#,
        )
        .unwrap();
    }
    w.push_str(
        r#"

class Response:
    """Response messages of the materialize protocol (`estuary_cdk.materialize.response`)."""

    Validated = _response.Validated
    ValidatedBinding = _response.ValidatedBinding
    ProjectionConstraint = _response.ProjectionConstraint
    Constraint = _response.Constraint
    Applied = _response.Applied
    Opened = _response.Opened
    Loaded = _response.Loaded
    Flushed = _response.Flushed
    StartedCommit = _response.StartedCommit
    Acknowledged = _response.Acknowledged


class IMaterialization[C = EndpointConfig, R: pydantic.BaseModel = ResourceConfig, S = dict[str, typing.Any]](ABC):
    """Abstract base class of materialization implementations.

    `C` is the type of the materialization's `config`, `R` of the resource
    configuration of each binding, and `S` of its connector state. `C` and
    `R` default to the types generated from the materialization's `spec`.
    A materialization may instead declare types of its own, as
    `class Materialization(IMaterialization[MyConfig, MyResourceConfig, MyState])`,
    and is then responsible for keeping them equivalent to its declared `spec`.

    `Materialization` must directly subclass `IMaterialization`, from which its
    types are resolved: a validation of the materialization fails otherwise.

    The platform constructs a materialization only through `open`, and its
    `__init__` is the materialization's own.

    Each binding has a `store_{name}` method, and a `load_{name}` method if it
    uses standard (not delta) updates. Its name is its resource path, in snake
    case. Each calls the generic `store` or `load` unless it's overridden.

    Loads and stores run as concurrent tasks, at most `concurrent_loads` and
    `concurrent_stores` at once, which begin in the order of their requests.
    A task runs without interruption until its first `await`, and yielding a
    loaded document doesn't interrupt it. So a task may check and swap a buffer
    which it shares with other tasks before it awaits. When every task is
    busy, the materialization reads no further requests."""

    concurrent_loads: int = 32
    """Maximum number of loads which run at once. An instance may set its own."""

    concurrent_stores: int = 32
    """Maximum number of stores which run at once. An instance may set its own."""

    @classmethod
    async def open(cls, open: Request.Open[C, R, S]) -> tuple[typing.Self, Response.Opened]:
        """Construct the materialization from its Open message. Its `config` is
        `open.materialization.config`, its bindings are
        `open.materialization.bindings`, and its state is `open.state`.

        The default is `cls()`, for a materialization which needs none of
        them. Override it to build the materialization's components, which may
        require awaiting, and pass them to an `__init__` of your own.

        An endpoint which commits the runtime checkpoint of each transaction
        (see `start_commit`) returns it as `runtimeCheckpoint` of `Opened`."""
        return cls(), Response.Opened()

    @classmethod
    async def validate(cls, validate: Request.Validate[C, R], validated: Response.Validated) -> None:
        """Validate the materialization as it's published, raising to fail.

        `validated` holds a default of each binding, in the order of
        `validate.bindings`. Its constraints require the root document, allow
        the fields of its key, and forbid other fields. Modify it in place, such
        as to mark a field `INCOMPATIBLE` with an existing resource. A binding's
        `resourcePath` and `deltaUpdates` are those of its resource
        configuration, and must not change. The default does nothing."""
        pass

    @classmethod
    async def apply(cls, apply: Request.Apply[C, R, S], applied: Response.Applied[S]) -> None:
        """Apply the materialization to its endpoint before it's opened, such as
        by creating the resources of new bindings.

        Apply may run more than once for a version, so it must be idempotent.
        `apply.lastMaterialization` is the last-applied materialization, whose
        configurations aren't modeled. Set `applied.actionDescription` to
        describe the actions taken. The default does nothing."""
        pass

    @abstractmethod
    async def load(self, load: Request.Load[typing.Any]) -> collections.abc.AsyncIterator[Response.Loaded[typing.Any]]:
        """Load a key of a binding whose `load_{name}` isn't overridden.

        Yield `Response.Loaded(binding=..., doc=...)` of each stored document
        which is found: of this key, or of others which were buffered,
        of this binding or any other."""
        if False:
            yield  # Mark as a generator.

    @abstractmethod
    async def store(self, store: Request.Store[typing.Any, typing.Any]) -> None:
        """Store a document of a binding whose `store_{name}` isn't overridden.

        `store.doc` is the binding's `Source{Name}`, which
        `store.doc.model_dump(mode="json", by_alias=True, exclude_unset=True)`
        maps into its JSON document. `store.exists` if its key was loaded or
        stored before, and `store.delete` if the document is a deletion."""
        pass

"#,
    );

    for Binding {
        snake,
        pascal,
        delta_updates,
        ..
    } in bindings
    {
        if !delta_updates {
            write!(
                w,
                r#"    async def load_{snake}(self, load: Request.Load{pascal}) -> collections.abc.AsyncIterator[Response.Loaded[typing.Any]]:
        """Load a key of binding `{snake}`. The default is the generic `load`."""
        async for loaded in self.load(load):
            yield loaded

"#,
            )
            .unwrap();
        }
        write!(
            w,
            r#"    async def store_{snake}(self, store: Request.Store{pascal}) -> None:
        """Store a document of binding `{snake}`. The default is the generic `store`."""
        await self.store(store)

"#,
        )
        .unwrap();
    }

    w.push_str(
        r#"    async def flush(self, flush: Request.Flush, flushed: Response.Flushed[S]) -> collections.abc.AsyncIterator[Response.Loaded[typing.Any]]:
        """End the loads of the transaction, after every load has finished.

        Yield `Response.Loaded` of each found document of keys which are still
        buffered. Set `flushed.state` to update connector state before the
        transaction's stores. The binding of each of `flush.backfillCompletes`
        may delete its documents which were published before its `timestamp`.
        The default does nothing."""
        if False:
            yield  # Mark as a generator.

    async def start_commit(self, start_commit: Request.StartCommit, started: Response.StartedCommit[S]) -> None:
        """End the stores of the transaction, after every store has finished,
        such as by writing a buffered batch.

        Every document must be durable before this returns, unless it's staged
        and applied by `acknowledge`. Set `started.state` to update connector
        state with the transaction's checkpoint. An endpoint may instead commit
        `start_commit.runtimeCheckpoint` with its own transaction (see `open`).
        The default does nothing."""
        pass

    async def acknowledge(self, acknowledge: Request.Acknowledge, acknowledged: Response.Acknowledged[S]) -> None:
        """Acknowledge that the transaction has committed, such as by applying
        the work which `start_commit` staged.

        It runs concurrently with the loads of the next transaction. A
        materialization which applies work here must keep those loads from
        reading the endpoint until it's applied, such as by waiting in `flush`.
        Set `acknowledged.state` to update connector state, which isn't
        transactional: a repeated Acknowledge must be tolerated.
        The default does nothing."""
        pass
"#,
    );

    Ok(format!(
        "\"\"\"Generated types of materialization {task}, from its declared `spec` and bindings.\"\"\"\n\
         {}\n\n\n{w}",
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
    "from estuary_cdk.materialize import request as _request, response as _response",
];

/// Write the `Key{Pascal}` NamedTuple of `binding`: a field of each of its
/// group-by fields, in order, typed by its projection's inference.
fn key_py(binding: &Binding, w: &mut String) -> anyhow::Result<()> {
    let Binding {
        snake,
        pascal,
        collection,
        keys,
        ..
    } = binding;

    writeln!(
        w,
        "class Key{pascal}(_typing.NamedTuple):\n    \"\"\"Key of binding `{snake}`: its group-by fields, in order.\"\"\"\n"
    )
    .unwrap();

    let mut identifiers: Vec<String> = Vec::new();
    for field in keys.iter() {
        let projection = collection
            .projections
            .iter()
            .find(|projection| &projection.field == field)
            .with_context(|| {
                format!(
                    "group-by field {field:?} isn't a projection of collection {}",
                    collection.name
                )
            })?;

        let mut identifier = key_identifier(field);
        while identifiers.contains(&identifier) {
            identifier.push('_');
        }
        let type_ = key_type(projection.inference.as_ref());

        if &identifier == field {
            writeln!(w, "    {identifier}: {type_}").unwrap();
        } else {
            writeln!(
                w,
                "    {identifier}: {type_}  # Field {}.",
                python_literal(&serde_json::json!(field))
            )
            .unwrap();
        }
        identifiers.push(identifier);
    }
    w.push_str("\n\n");

    Ok(())
}

/// Identifier of a key field within its NamedTuple: its snake case, which
/// isn't empty, doesn't begin with a digit or underscore, and doesn't shadow a
/// keyword or a method of `tuple`.
fn key_identifier(field: &str) -> String {
    let identifier = to_snake_case(field);

    if identifier.is_empty() || identifier.starts_with(|c: char| c.is_ascii_digit()) {
        format!("f_{identifier}")
    } else if matches!(identifier.as_str(), "count" | "index")
        || sanitize_python_identifier(&identifier) != identifier
    {
        format!("{identifier}_")
    } else {
        identifier
    }
}

/// Python type of a key component, from its inferred JSON types. Keys hold
/// the wire values of their locations, and a location which may not exist
/// has a null.
fn key_type(inference: Option<&flow::Inference>) -> String {
    let Some(inference) = inference else {
        return "_typing.Any".to_string();
    };
    let mut types: Vec<&str> = Vec::new();

    for type_ in &inference.types {
        types.push(match type_.as_str() {
            "string" => "str",
            "integer" => "int",
            "number" => "float",
            "boolean" => "bool",
            "null" => continue,
            _ => return "_typing.Any".to_string(),
        });
    }
    if types.is_empty() {
        return "_typing.Any".to_string();
    }
    let may_be_null = inference.types.iter().any(|type_| type_ == "null")
        || inference.exists != flow::inference::Exists::Must as i32;
    if may_be_null {
        types.push("None");
    }
    types.join(" | ")
}

/// Generate the entry point of a materialization from template, which runs
/// the `Materialization` exported by its directory `dir`.
pub fn main_py(task: &str, bindings: &[Binding], dir: &str) -> String {
    let template = include_str!("main.py.template");

    let load_types = bindings
        .iter()
        .map(|binding| match binding.delta_updates {
            true => "    Request.Load,".to_string(),
            false => format!("    Request.Load{},", binding.pascal),
        })
        .join("\n");
    let store_types = bindings
        .iter()
        .map(|binding| format!("    Request.Store{},", binding.pascal))
        .join("\n");
    // Delta-updates bindings have no `load_{name}`, and are never loaded.
    let load_methods = bindings
        .iter()
        .map(|binding| match binding.delta_updates {
            true => "        materialization.load,".to_string(),
            false => format!("        materialization.load_{},", binding.snake),
        })
        .join("\n");
    let store_methods = bindings
        .iter()
        .map(|binding| format!("        materialization.store_{},", binding.snake))
        .join("\n");

    template
        .replace("LOAD_TYPES", &load_types)
        .replace("STORE_TYPES", &store_types)
        .replace("LOAD_METHODS", &load_methods)
        .replace("STORE_METHODS", &store_methods)
        .replace(
            "MODULE_PATH",
            &python_connector::module_parts(task).join("."),
        )
        .replace("LOAD_TASK", &python_connector::load_task_py(dir))
}

/// Generate the starter entry of a materialization: a working materialization
/// which discards each document of its present bindings, and fails upon a
/// document of a binding which it doesn't handle.
pub fn starter_py(task: &str, bindings: &[Binding]) -> String {
    let mut w = String::with_capacity(4096);
    let module_path = python_connector::module_parts(task).join(".");

    write!(
        w,
        r#""""Materialization {task}.

The platform runs the `Materialization` class which this directory exports. It
must directly subclass the generated `IMaterialization`: as
`class Materialization(IMaterialization)`, or
`class Materialization(IMaterialization[MyConfig, MyResourceConfig, MyState])`
to parse its configurations and state into types of its own. Modules of this
directory import one another relatively (`from .helpers import ...`).

Each binding has a `store_{{name}}` method, and a `load_{{name}}` method if it
uses standard updates, named for its resource path. This starter discards
the documents of its bindings, and loads none. A binding whose methods aren't
overridden is handled by the generic `store` and `load`, which raise here, so
that a binding which is added without code to handle it fails.

Loads run concurrently, up to `concurrent_loads` at once (and stores, up to
`concurrent_stores`). A load may instead buffer its key, and yield the loaded
documents of a whole batch, or leave the batch to `flush`. A store may likewise
buffer its document, and write the batch in `start_commit`.
"""
import typing
from collections.abc import AsyncIterator
from estuary_cdk.flow import MaterializationBinding
from {module_path} import EndpointConfig, IMaterialization, Request, ResourceConfig, Response


class Materialization(IMaterialization):
    def __init__(self, config: EndpointConfig, bindings: list[MaterializationBinding[ResourceConfig]]):
        self.config = config
        self.bindings = bindings

    @classmethod
    async def open(
        cls, open: Request.Open[EndpointConfig, ResourceConfig, dict[str, typing.Any]]
    ) -> tuple[typing.Self, Response.Opened]:
        # `EndpointConfig` is generated from the materialization's `spec.configSchema`,
        # and `ResourceConfig` from its `spec.resourceConfigSchema`.
        materialization = open.materialization
        return cls(materialization.config, materialization.bindings), Response.Opened()

"#,
    )
    .unwrap();

    for Binding {
        snake,
        pascal,
        delta_updates,
        ..
    } in bindings
    {
        write!(
            w,
            r#"    async def store_{snake}(self, store: Request.Store{pascal}) -> None:
        # Write `store.doc` to the endpoint under `store.key`, or delete it if
        # `store.delete`. This starter discards it.
        pass

"#,
        )
        .unwrap();

        if !delta_updates {
            write!(
                w,
                r#"    async def load_{snake}(self, load: Request.Load{pascal}) -> AsyncIterator[Response.Loaded[typing.Any]]:
        # If `load.key` was stored, yield `Response.Loaded(binding=load.binding, doc=...)`.
        # This starter finds no key, so every document is new.
        if False:
            yield

"#,
            )
            .unwrap();
        }
    }

    w.push_str(
        r#"    async def store(self, store: Request.Store[typing.Any, typing.Any]) -> None:
        raise RuntimeError(f"unhandled store of binding {self.bindings[store.binding].resourcePath}")

    async def load(self, load: Request.Load[typing.Any]) -> AsyncIterator[Response.Loaded[typing.Any]]:
        if False:
            yield
        raise RuntimeError(f"unhandled load of binding {self.bindings[load.binding].resourcePath}")
"#,
    );

    w
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn names_of_resource_paths() {
        let collection = flow::CollectionSpec::default();
        let paths: Vec<Vec<&str>> = vec![
            vec!["contacts"],
            vec!["crm", "Contacts"],
            vec!["myTable"],
            vec!["2024-orders"],
            vec!["über table"],
        ];
        let bindings = Binding::of(paths.iter().map(|path| {
            (
                path.iter().map(|s| s.to_string()).collect(),
                false,
                &collection,
                &[][..],
            )
        }))
        .unwrap();

        let names: Vec<_> = bindings
            .iter()
            .map(|binding| (binding.snake.as_str(), binding.pascal.as_str()))
            .collect();
        insta::assert_debug_snapshot!(names, @r#"
        [
            (
                "contacts",
                "Contacts",
            ),
            (
                "crm_contacts",
                "CrmContacts",
            ),
            (
                "my_table",
                "MyTable",
            ),
            (
                "2024_orders",
                "2024Orders",
            ),
            (
                "ber_table",
                "BerTable",
            ),
        ]
        "#);

        let of = |paths: &[&[&str]]| {
            Binding::of(paths.iter().map(|path| {
                (
                    path.iter().map(|s| s.to_string()).collect(),
                    false,
                    &collection,
                    &[][..],
                )
            }))
            .map(|_| ())
            .map_err(|err| format!("{err:#}"))
        };
        insta::assert_debug_snapshot!([
            of(&[&["my-table"], &["my_table"]]),
            of(&[&["a_1"], &["a1"]]),
            of(&[&["!!"]]),
        ], @r#"
        [
            Err(
                "bindings of resource paths [\"my-table\"] and [\"my_table\"] have names `my_table` and `my_table`, whose generated methods or types collide: give one a distinct resource path",
            ),
            Err(
                "bindings of resource paths [\"a_1\"] and [\"a1\"] have names `a_1` and `a1`, whose generated methods or types collide: give one a distinct resource path",
            ),
            Err(
                "resource path [\"!!\"] has no ASCII letters or digits, which name its binding's methods",
            ),
        ]
        "#);
    }
}
