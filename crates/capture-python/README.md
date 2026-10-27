# capture-python

Runs a user-authored Python capture: a project written with the Estuary CDK.
Shared machinery (staging, uv, Pydantic codegen, the entry point's loader)
lives in `python-connector`.

`validation::custom` rewrites a capture's `endpoint.python` into an image
connector (`ghcr.io/estuary/capture-python`). Its config is a
`python_connector::ProjectConfig`: the user's `config`, plus a `_python`
sentinel holding the project's `files` and its declared `spec`.

## The project

A project is its listed `files`. The capture's directory is the final component
of its name, verbatim (`acmeCo/source-acme` has `source-acme/`). Its
`__init__.py` exports a `Connector`. A `pyproject.toml` declares its
dependencies, including the CDK.

The entry point loads the directory by path, as a package of a fixed name
(`python_connector::load_task_py`). So the directory needn't be a Python
identifier, and its modules import one another relatively.

`examples/capture-python` is a multi-file reference project.

## Entry points

- [`run`](src/lib.rs) — the protocol loop of `proto_flow::capture` messages.
  It answers Spec, Discover, Validate, and Apply, and breaks upon an Open.
  The Open **hands off** (`python_connector::Staged::hand_off`): the user's
  program inherits stdout, and is piped the Open and the remainder of stdin.
  Its exit status is the session's, so a failed capture fails its shard.
- `spec_response` — answers Spec from the declared `spec`, without staging or
  running user code.
- `unary` — answers a Discover, Validate, or Apply by invoking the user's
  program with the one request (`invoke`). The program must write exactly one
  response line, then exit. A Validate first type-checks the
  project and runs its tests (`Staged::check`), and its Validated response gains the generated
  module.
- `starter_validated` — answers the Validate of a project which lists files
  that don't exist yet (`null` in the sentinel), without staging or running
  user code. It returns starters of those files
  (`python_connector::starter_files`), and the generated module. The entry's
  starter is a working single-file capture. Each binding's resource path is a
  placeholder (`binding-{index}`).
- `stage` — renders the generated module and the generated entry point
  (`flow_generated/main.py`, which runs `Connector().serve()`), and stages and
  installs them with the project (`python_connector::Staged::of_project`). Each request
  which runs user code stages and installs anew. Only Validate installs the
  `dev` group.
- `strip_sentinels` — removes `_python` from each of a request's
  configurations, including `lastCapture` and `sealedConfig`, before user code
  sees it.
- `generated_module_py` — the module generated from the declared `spec`. For
  `acmeCo/source-acme` it's `acmeCo.source_acme`, under `flow_generated/python/`,
  which is on the PYTHONPATH. It has `EndpointConfig`
  (`python_connector::endpoint_config_py`), `ResourceConfig`
  (`resource_config_py`), and `SPEC`, a CDK `ConnectorSpec` of the declared
  spec.

## Non-obvious details

- **The user's program inherits stderr**, so its logs and its final error reach
  the runtime as it emits them. `main` exits with a failed program's code, and
  logs no error of its own, which would mask the program's.
- **Spec has no resource path pointers.** The connector returns each binding's
  path instead, in Validated and Discovered responses.
- **An undeclared `resourceConfigSchema` is the CDK's stock `ResourceConfig`.**
  Spec answers its schema, and the generated `ResourceConfig` is
  `common.ResourceConfig`. CDK helpers such as `open_binding` are typed by it.
- **A declared `resourceConfigSchema` must be an object.** It generates a
  `common.BaseResourceConfig` subclass. The subclass has a `path()` only if the
  schema has exactly one `x-collection-name` property and at most one
  `x-schema-name`, each a required string (`resource_path`). Otherwise the user
  declares a subclass having a `path()` of its own. A field which would shadow
  `path`, `PATH_POINTERS`, or `meta_` is prefixed `m_` and aliased.
- **`SPEC` is never served.** Spec is answered from the declared `spec`. The
  starter's `spec()` returns `SPEC` only so that its `Connector` is complete.
- **Pyright needs `extraPaths = ["flow_generated/python"]`** to resolve the
  generated module. The starter sets it.
- **The starter pins the CDK** at `python_connector::CDK_DEPENDENCY`, the CDK
  against which this connector is tested.
- **The starter `pyproject.toml` type-checks in pyright's `standard` mode**,
  not the `strict` of every other starter. This is transitional, until the CDK
  is clean under `strict`. `starter_validated` marks the override to delete.
- **No webhooks yet**: the image declares no public port.
