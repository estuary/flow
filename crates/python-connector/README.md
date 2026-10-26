# python-connector

Shared machinery of the connectors which run user-authored Python projects:
`capture-python`, `derive-python`, and `materialize-python`. It defines a
Python task's built configuration, the layout of its project, and how the
project is staged and run.

`validation` also depends on it, to build Python tasks. The user-facing model
of a task (`Spec`, `Files`) is `models::custom_connector`.

## Key types and entry points

- [`ProjectConfig`](src/config.rs) — the built configuration of a task. It's
  the user's own `config`, plus a `_python` (`SENTINEL`) property. That
  property holds the task's [`Project`](src/config.rs): its `files` and its
  declared `spec`.
- [`ModuleConfig`](src/config.rs) — the frozen built configuration of a
  derivation having a legacy `module`. Validation still builds it, so that
  images which predate `ProjectConfig` keep working.
- Project layout, in [`config.rs`](src/config.rs): `MANIFEST`, `LOCK`, the
  `RESERVED` directories, and a task directory's `entry`. `starter_manifest`
  renders [`src/pyproject.toml`](src/pyproject.toml).
- [`Staged`](src/staged.rs) — a project staged into a temporary directory,
  from its `files` and its generated files (`Staged::of_project`). It installs
  the project, checks it with its pyright and pytest, and runs user code
  within its environment.
- [`connector.rs`](src/connector.rs) — what each connector shares: its process
  `main`, the `declared_spec` and `oauth2` of its Spec responses, and the
  `starter_files` of a project's missing files.
- [`ENTRY`](src/generated.rs) — the generated entry point
  (`flow_generated/main.py`), which each connector renders and runs.
- [`module_files`](src/generated.rs) — the generated Python module of a
  catalog name, under `GENERATED_PREFIX`, which is on the `PYTHONPATH`.
- [`endpoint_config_py`](src/config_types.rs) — the generated `EndpointConfig`
  of a `Spec`. A `ResourceConfig` differs by connector, so each maps its own
  from `config_shape`.
- [`pydantic`](src/pydantic/mod.rs) — maps JSON schemas, of documents and
  configurations alike, into Pydantic models.

## Installing a project

`Install::of_request` picks a mode from the request and the project's lock:

| | Validate | Other requests |
|-|-|-|
| `uv.lock` listed | `Check`: `uv lock --check`, then sync with `dev` | `Frozen`: sync without `dev` |
| No `uv.lock` | `Resolve`: fresh lock, sync with `dev` | `Resolve`: fresh lock, sync without `dev` |

A fresh lock is never kept. Validate installs the `dev` group because it
provides pyright and pytest. A listed lock is checked because `uv sync --frozen` with a
stale lock succeeds, and silently omits newly added dependencies.

## Generated Python

- **Generated types are the garden path.** User code may parse into its own
  types instead. The platform doesn't check that they match the declared
  schemas.
- A literal `default` of an optional property is its field's default.
  Generated entry points serialize with `exclude_unset`, so a default isn't
  written out. A default which the property's shape doesn't admit is dropped
  (`field_default`), as when a read schema forbids a location that its write
  schema defaults, such as a CDK capture's `_meta/op`.
- Strings are `str`, whatever their `format`. An `x-python-type` annotation
  (`PYTHON_TYPE`) names a location's type instead, such as `datetime.datetime`.
- A connector names a binding or transform's methods and types by its
  `to_snake_case` and `to_pascal_case`. Distinct names may collide, which the
  connector rejects.

## Non-obvious details

- **The task's directory is loaded by path**, as the package `__flow_task__`
  (`load_task_py`). So the directory needn't be an identifier (`source-acme/`),
  and can't collide with a dependency.
- **Tests are project configuration too.** Validate runs `pytest` under the
  project's own `[tool.pytest.ini_options]`. The starter selects the
  `importlib` import mode, as a test module of a task's directory imports its
  modules relatively, and pytest's default mode requires an identifier
  directory. A project having no tests passes (pytest's exit status 5).
- **`Staged` and `connector.rs` are behind the default `staged` feature.**
  `validation` is built for WASM, and depends on this crate without it.
- **The cooldown is project configuration**, not a CLI flag. The starter
  `pyproject.toml` sets `[tool.uv] exclude-newer = "1 day"`. A lock records
  its `exclude-newer`, so overriding it with a flag would make
  `uv lock --check` report the lock as stale.
- **A user program's failure is its own to report.** Its stderr is inherited,
  and its unsuccessful exit is an [`Exited`](src/staged.rs) error. A
  connector's `main` exits with its status, without logging.
- **`ProjectConfig` and `Project` allow unknown fields**, so an older image
  parses the configuration built by a newer control plane. `ModuleConfig`
  denies them, as its shape is frozen.
- **A task's network ports come from its model.** Its `spec.networkPorts`
  become its built spec's ports (`validation::custom::network_ports`).
  User code binds the ports which it declared.
