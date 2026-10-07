# capture-python

Runs a user-authored Python capture: a package written with the Estuary CDK,
which is lifted into a first-party connector once demand is proven.

`validation::builtin` rewrites a capture's `endpoint.python` into an image
connector (`ghcr.io/estuary/capture-python`) whose config is the user's `config`
plus a `_python` sentinel (`capture`, `package`, `files`, the resolved `spec`,
and a baked `uv.lock`).

## Entry points

- [`run`](src/lib.rs) — answers each Spec from the sentinel's declared `spec`
  (`spec_response`) without staging anything. The first other request stages
  the project with its generated module, installs it under the
  `python_connector::Install` mode of that request, and runs
  `python -m <package>`. Requests are forwarded with the sentinel removed, and
  responses are copied back unparsed, except that a Validated response gains the
  generated module and the session's resolved `uv.lock` as generated files.
- `generated_module_py` — the module generated for the capture's name (for
  `acmeCo/source-acme`, `acmeCo.source_acme` under `flow_generated/python/`, on
  the PYTHONPATH): `EndpointConfig` and `ResourceConfig`
  (`python_connector::config_types_py`) and `SPEC`, a CDK `ConnectorSpec` of
  the declared spec.
- `scaffold` — answers a project which lacks its required files (as when
  `flowctl generate` starts from `endpoint: { python: {} }`) with the starter
  [`templates`](src/templates), a working hello-world capture, and its generated
  module. `starter_config_schema` is the `spec.configSchema` they read, which
  `flowctl generate` adds to a scaffolded capture which declares none.

## Non-obvious details

- **Spec never runs user code**, and has no resource path pointers: the
  connector returns each binding's path (in Validated, and from the CDK's
  Discovered once it sets `resourcePath`).
- **Generated types are the garden path.** A generated `ResourceConfig` extends
  the CDK's `BaseResourceConfig` with a `path()` of its `x-schema-name` (if any)
  and `x-collection-name` properties, or the CDK's stock `ResourceConfig` if it
  has the stock `name` and `interval` (CDK helpers like `open_binding` are typed
  by it). User code may use its own types instead, which aren't checked against
  the declared schemas. The template's `spec()` returns `SPEC`, which is dead
  code under this shim, but makes a lifted connector complete.
- **Validate** type-checks with the project's own pyright (from its `dev`
  dependency group and configuration) before forwarding the request. Projects
  need `extraPaths = ["flow_generated/python"]` for pyright to resolve the
  generated module, as the templates set.
- **The CDK** is a dependency of the user's `pyproject.toml`. Templates pin
  `CDK_URL`, the archive of the CDK commit against which this connector is tested.
- **No webhooks yet**: the image declares no public port.
