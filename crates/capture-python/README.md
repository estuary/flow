# capture-python

Runs a user-authored Python capture: a directory of code written with the
Estuary CDK, which is lifted into a first-party connector once demand is proven.

`validation::builtin` rewrites a capture's `endpoint.python` into an image
connector (`ghcr.io/estuary/capture-python`) whose config is the user's `config`
plus a `_python` sentinel: `capture` (its name), `files` (the project, with a
baked `uv.lock`), and the `spec` its model declares. An absent schema stays
absent, and this connector resolves it to its own default.

## The project

A project is its listed `files`. The capture's directory is the final component
of its name, verbatim (`acmeCo/source-acme` has `source-acme/`), and its
`__init__.py` exports a `Connector`. A `pyproject.toml` declares its
dependencies, including the CDK. Its modules import one another relatively:
the directory is loaded by path as a package of a fixed name
(`python_connector::TASK_PACKAGE`), so it needn't be a Python identifier.

## Entry points

- [`run`](src/lib.rs) — the session. Spec is answered from the sentinel
  (`spec_response`) without staging anything. The first other request stages
  the project with its generated module and generated entry point
  (`flow_generated/main.py`, which runs `Connector().serve()`). Then:
  - Discover, Validate, and Apply are each a **unary** invocation (`unary`): the
    user's program is spawned, given the one request (sentinel stripped), and
    must write exactly one response line before exiting. A Validated response
    gains the generated module and the session's resolved `uv.lock`.
  - Open **hands off** (`hand_off`): stdout is inherited, and the Open and the
    remainder of stdin are pumped to the child. Its exit status is the
    session's, so a failed capture fails its shard.
- Installs are chosen per request (`python_connector::Project::prepare`):
  Validate installs the `dev` group to run the project's pyright, and an Open
  which follows it re-syncs without it.
- `starter_validated` — a Validate of a project having listed files which don't
  exist yet (`null` in the sentinel) is answered without running user code:
  starters of those files (a working single-file capture as the entry, a
  `pyproject.toml` which depends on the CDK at `CDK_URL`, or else empty) and the
  generated module.
- `generated_module_py` — the module generated for the capture's name (for
  `acmeCo/source-acme`, `acmeCo.source_acme` under `flow_generated/python/`, on
  the PYTHONPATH): `EndpointConfig` and `ResourceConfig`
  (`python_connector::config_types_py`) and `SPEC`, a CDK `ConnectorSpec` of
  the declared spec. Its CDK imports follow what was generated.

## Non-obvious details

- **Spec never runs user code**, and has no resource path pointers: the
  connector returns each binding's path (in Validated and Discovered).
- **An absent `resourceConfigSchema` is the CDK's stock `ResourceConfig`**: Spec
  answers its schema, and the generated `ResourceConfig` is
  `common.ResourceConfig`. A declared schema always generates a
  `common.BaseResourceConfig` subclass, with a `path()` of its `x-schema-name`
  (if any) and `x-collection-name` properties where they're unambiguous, and
  otherwise none (the escape hatch). CDK helpers typed by the stock
  `ResourceConfig` (such as `open_binding`) want the default.
- **Generated types are the garden path.** User code may use its own types
  instead, which aren't checked against the declared schemas. A connector's
  `spec()` returns `SPEC`, which is dead code under this shim, but makes a
  lifted connector complete.
- **Validate** type-checks with the project's own pyright (from its `dev`
  dependency group and configuration). Projects need
  `extraPaths = ["flow_generated/python"]` for pyright to resolve the generated
  module, as the starter sets.
- **The CDK** is a dependency of the user's `pyproject.toml`. The starter pins
  `CDK_URL`, the archive of the CDK commit against which this connector is
  tested. `examples/capture-python` is the multi-file reference project.
- **No webhooks yet**: the image declares no public port.
