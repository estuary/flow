# materialize-python

Runs a user-authored Python materialization: a directory of code which
implements a callback of each binding, such as to map the documents of a
collection into the requests of an external API (reverse ETL).

`validation::custom` rewrites a materialization's `endpoint.python` into an
image connector (`ghcr.io/estuary/materialize-python`) whose config is a
`python_connector::ProjectConfig`: the user's `config` plus a `_python`
sentinel holding `files` (the project, and its `uv.lock` if it lists one) and
the `spec` its model declares. Python materializations run only on runtime v2,
which sends the JSON `key` of each Load and Store.

## The project

A project is its listed `files`. The materialization's directory is the final
component of its name, verbatim (`acmeCo/materialize-acme` has
`materialize-acme/`), and its `__init__.py` exports a `Materialization`. A
`pyproject.toml` declares its dependencies, including the CDK, whose
`estuary_cdk.materialize` models the protocol's messages. Its modules import
one another relatively (`python_connector::load_task_py`).

## Entry points

- [`run`](src/lib.rs) — the protocol loop. Spec is answered from the declared
  `spec` (`spec_response`) without staging anything. Validate and Apply are
  each a unary invocation of `main.py validate` or `main.py apply`, in a
  project staged and installed anew. The loop breaks upon an Open, which hands
  off to `main.py` (`python_connector::Staged::hand_off`): its exit status is
  the session's. The `_python` sentinel is stripped from each configuration
  before user code sees it.
- `validate_bindings` and `spec_bindings` — a binding's resource path is its
  resource configuration's `x-schema-name` (if non-empty) and
  `x-collection-name`, at the locations which `tables::utils::pointer_for_schema`
  finds (as the control plane does), and its delta updates are its
  `x-delta-updates` (or else false). An omitted property is its schema default.
  [`codegen::Binding`](src/codegen/mod.rs) names each binding for its path in
  snake case, which must be unique.
- `default_validated` — a binding's default constraints require the root
  document, allow its key's fields, and forbid all others. `main.py validate`
  hands them to `Materialization.validate`, which modifies them in place.
  `check_validated` then requires that each binding's path and delta updates
  are unchanged: they're its resource configuration's alone.
- [`codegen`](src/codegen/mod.rs) — the generated module (for
  `acmeCo/materialize-acme`, `acmeCo.materialize_acme` under
  `flow_generated/python/`), the entry point (`main.py`), and the entry starter.
  A project having listed files which don't exist yet is answered with
  starters of them and the defaults, without running user code.

## The user's interface

- Each binding has `Source{Name}` (its collection's documents) and
  `Key{Name}`, a `NamedTuple` of its group-by fields typed by their
  inference. `Request.Store{Name}` has its `key`, `doc`, `exists`, and
  `delete`. A standard-updates binding also has `Request.Load{Name}`.
- `class Materialization(IMaterialization)` (or
  `IMaterialization[MyConfig, MyResource, MyState]`, which must be its direct
  base) implements the abstract generic `store` and `load`. Each binding's
  generated `store_{name}` (and `load_{name}`, if it's standard) calls them
  unless it's overridden. The starter's generic methods raise, so that a binding
  which is added without code to handle it fails.
- `open`, `validate`, and `apply` are async class methods. `open` is the only
  constructor, returning the instance and its `Opened`, and `__init__` is the
  user's own. `validate` and `apply` modify a given `Validated` or `Applied`.
- `load_*` and `flush` are async generators of `Response.Loaded(binding, doc)`,
  of any binding. `flush`, `start_commit`, and `acknowledge` set the state of a
  given `Flushed`, `StartedCommit`, or `Acknowledged`.

## The session

- Loads and stores run as tasks of a `TaskGroup` (`Executor`), at most
  `concurrent_loads` and `concurrent_stores` at once (32 by default). Tasks
  begin in the order of their requests, and each runs until its first `await`,
  so it may check and swap a shared buffer before awaiting. A yielded
  `Loaded` is written without awaiting. When every task is busy, `main.py`
  stops reading stdin: the runtime keeps reading responses meanwhile, so the
  back-pressure can't deadlock.
- A Flush waits for every load task before calling `flush`, and a StartCommit
  waits for every store task before calling `start_commit`.
- `acknowledge` runs as its own task, concurrently with the next
  transaction's loads, and `Acknowledged` is written when it returns. The
  runtime sends no Flush until then. A materialization which applies work in
  `acknowledge` fences its own loads.
- A failed task cancels the session, and its error is the program's.
- Responses omit fields which were never set, so a loaded document parsed from
  the endpoint round-trips as it was stored.

## Non-obvious details

- **Spec never runs user code.** An absent `resourceConfigSchema` is the common
  shape of SQL materializations: `table`, an optional `schema`, and
  `delta_updates` (false). A declared schema without an `x-delta-updates`
  location has only standard-updates bindings; a `const: true` property
  makes them all delta.
- **Delta-updates bindings are never loaded**, and have no `load_{name}`.
- **Validate** type-checks and tests with the project's own pyright and pytest
  (its `dev` dependency group), as derive-python does. Projects need
  `extraPaths = ["flow_generated/python"]`, as the starter sets.
- **Generated types are the garden path.** Configuration and documents are
  parsed with the types a materialization declares, which aren't checked
  against its `spec`.
