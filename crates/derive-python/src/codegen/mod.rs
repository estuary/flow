use itertools::Itertools;
use proto_flow::flow;
use std::fmt::Write;

mod ast;
mod mapper;

use super::LambdaConfig;
use mapper::Mapper;

/// Generate Pydantic models and protocol types for a Python derivation.
pub fn types_py(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec, LambdaConfig)],
) -> String {
    let mut w = String::with_capacity(4096);

    // Add imports
    write!(
        w,
        r#"from abc import ABC, abstractmethod
import typing
import collections.abc
import pydantic


"#
    )
    .unwrap();

    let mapper = Mapper::new(&collection.write_schema_json, "Document");
    writeln!(
        w,
        "# Generated for published documents of derived collection {}",
        collection.name
    )
    .unwrap();
    mapper.map(mapper.schema(), "Document").render(&mut w);

    // Generate Source{Transform} collection types for each transform
    for (name, collection, _config) in transforms {
        let source_name = format!("Source{}", to_pascal_case(name));

        let mapper = if collection.read_schema_json.is_empty() {
            Mapper::new(&collection.write_schema_json, &source_name)
        } else {
            Mapper::new(&collection.read_schema_json, &source_name)
        };

        writeln!(
            w,
            "# Generated for read documents of sourced collection {}",
            collection.name
        )
        .unwrap();
        mapper.map(mapper.schema(), &source_name).render(&mut w);
    }

    // Generate protocol message types
    write!(
        w,
        r#"class Request(pydantic.BaseModel):

    class Open(pydantic.BaseModel):
        state: dict[str, typing.Any]

    class Flush(pydantic.BaseModel):
        pass

    class Reset(pydantic.BaseModel):
        pass

    class StartCommit(pydantic.BaseModel):
        runtime_checkpoint: typing.Any = pydantic.Field(default=None, alias='runtimeCheckpoint')

    open: typing.Optional[Open] = None
    flush: typing.Optional[Flush] = None
    reset: typing.Optional[Reset] = None
    start_commit: typing.Optional[StartCommit] = pydantic.Field(default=None, alias='startCommit')

"#
    )
    .unwrap();

    // Generate Read{Transform} classes for each transform.
    for (idx, (name, _, _)) in transforms.iter().enumerate() {
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
        .map(|(name, _, _)| format!("Read{}", to_pascal_case(name)))
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
        pass

    class StartedCommit(pydantic.BaseModel):

        class State(pydantic.BaseModel):
            updated: dict[str, typing.Any]
            merge_patch: bool = False

        state: typing.Optional[State] = None

    opened: typing.Optional[Opened] = None
    published: typing.Optional[Published] = None
    flushed: typing.Optional[Flushed] = None
    started_commit: typing.Optional[StartedCommit] = pydantic.Field(default=None, alias='startedCommit')

"#
    )
    .unwrap();

    // Generate IDerivation base class
    write!(
        w,
        r#"class IDerivation(ABC):
    """Abstract base class for derivation implementations."""

    def __init__(self, open: Request.Open):
        """Initialize the derivation with an Open message."""
        pass

"#
    )
    .unwrap();

    // Generate abstract transform methods
    for (name, _, _) in transforms {
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
        r#"    async def flush(self) -> collections.abc.AsyncIterator[Document]:
        """Flush any buffered documents. Override to implement pipelining."""
        if False:
            yield  # Mark as a generator.

    def start_commit(self, start_commit: Request.StartCommit) -> Response.StartedCommit:
        """Return state updates to persist. Override to implement stateful derivations."""
        return Response.StartedCommit()

    async def reset(self):
        """Reset internal state for testing. Override if needed."""
        pass
"#
    )
    .unwrap();

    w
}

/// Generate the main.py runtime wrapper from template.
pub fn main_py(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec, LambdaConfig)],
    module_name: &str,
) -> String {
    let template = include_str!("main.py.template");

    let transform_methods = transforms
        .iter()
        .map(|(name, _, _)| {
            let method_name = to_snake_case(name);
            format!("derivation.{method_name}")
        })
        .join(", ");

    let module_path = module_path_parts(&collection.name).join(".");

    template
        .replace("TRANSFORMS", &transform_methods)
        .replace("MODULE_PATH", &module_path)
        .replace("MODULE_NAME", module_name)
}

/// Generate __init__.py package files for a collection's directory hierarchy.
/// Returns a map of file paths to their contents (empty for __init__.py files).
///
/// For example, "patterns/sums" generates:
/// - "flow_generated/python/patterns/__init__.py" -> ""
pub fn package_init_files(collection_name: &str, project_root: &str) -> Vec<(String, String)> {
    let parts: Vec<&str> = collection_name.split('/').collect();

    // Need at least one level of nesting to generate any __init__.py files
    if parts.len() < 2 {
        return vec![];
    }

    let mut files = Vec::new();

    // Generate __init__.py for each parent directory
    // For "a/b/c", generate: a/__init__.py, a/b/__init__.py
    for i in 1..parts.len() {
        let init_path = format!(
            "{project_root}/{}/{}/__init__.py",
            super::GENERATED_PREFIX,
            parts[..i].join("/")
        );
        files.push((init_path, String::new())); // Empty __init__.py
    }

    files
}

/// Generate a stub implementation for a missing module.
pub fn stub_py(
    collection: &flow::CollectionSpec,
    transforms: &[(&str, &flow::CollectionSpec, LambdaConfig)],
) -> String {
    let mut w = String::with_capacity(2048);
    let module_path = module_path_parts(&collection.name).join(".");

    write!(
        w,
        r#""""Derivation implementation for {name}."""
from collections.abc import AsyncIterator
from {module_path} import IDerivation, Document, Request

# Implementation for derivation {name}.
class Derivation(IDerivation):
"#,
        name = &collection.name,
    )
    .unwrap();

    for (name, _, _) in transforms {
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

fn to_pascal_case(name: &str) -> String {
    let mut result = String::new();
    let mut uppercase_next = true;

    for c in name.chars() {
        if !c.is_alphanumeric() {
            uppercase_next = true;
        } else if uppercase_next {
            result.extend(c.to_uppercase());
            uppercase_next = false;
        } else {
            result.push(c);
        }
    }
    result
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

/// Sanitize a collection name to the components of its valid Python module path.
/// Maps `/` to module hierarchy, and sanitizes each component to be a valid Python identifier.
pub fn module_path_parts(collection_name: &str) -> impl Iterator<Item = String> {
    collection_name
        .split('/')
        .map(mapper::sanitize_python_identifier)
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
        let transforms = vec![
            ("fromInts", &ints_spec, LambdaConfig { read_only: true }),
            (
                "process-strings",
                &strings_spec,
                LambdaConfig { read_only: false },
            ),
        ];

        // Test types_py generation
        let types_output = types_py(&sums_spec, &transforms);
        insta::assert_snapshot!("types_py", types_output);

        // Test stub_py generation
        let stub_output = stub_py(&sums_spec, &transforms);
        insta::assert_snapshot!("stub_py", stub_output);
    }

    #[test]
    fn test_package_files() {
        let cases = vec![
            ("patterns/sums", "/tmp/test"),
            ("a/b/c/d", "/project"),
            ("simple", "/tmp"),
            ("dir/file", "/root"),
        ];

        let results: Vec<_> = cases
            .iter()
            .map(|(collection, root)| (*collection, *root, package_init_files(collection, root)))
            .collect();

        insta::assert_debug_snapshot!(results);
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

        let transforms = vec![
            ("fromInts", &ints_spec, LambdaConfig { read_only: true }),
            (
                "process-strings",
                &strings_spec,
                LambdaConfig { read_only: false },
            ),
        ];

        let output = main_py(&sums_spec, &transforms, "my_module");
        insta::assert_snapshot!(output);
    }
}
