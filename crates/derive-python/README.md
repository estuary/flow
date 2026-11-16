# derive-python

Runs a user-authored Python derivation: a project written against types
generated from its collections and declared `spec`. The crate is both the
Spec / Validate connector and the runtime connector. Shared machinery (staging,
uv, Pydantic and config codegen, the entry point's loader) lives in
`python-connector`.

`validation::custom` rewrites a derivation's `using.python` into an image
connector (`ghcr.io/estuary/derive-python`). Its config is a
`python_connector::ProjectConfig`: the user's `config`, plus a `_python`
sentinel holding the project's `files` and its declared `spec`. User code sees
only the user's `config`.

A model having a legacy `module` is instead built as a
`python_connector::ModuleConfig`. This crate serves the two through separate
paths:

- [`src/project/`](src/project/mod.rs) serves a project, and is described below.
- [`src/lib.rs`](src/lib.rs) and [`src/codegen/`](src/codegen/mod.rs) serve a
  legacy module. They're frozen: a module keeps the interface it was written
  against, including `Derivation(open)` and `flush(state_patches, flushed)`.
  The crate's `run` hands each request of a project (`project::is_project`)
  to `project::run`.

The legacy path is to be deleted once no task has a `module`. Then
`src/project/` moves to the crate root.

## The project

A project is its listed `files`. The derivation's directory is the final
component of the derived collection's name, verbatim (`acmeCo/2024-orders` has
`2024-orders/`). Its `__init__.py` exports a `Derivation`. A `pyproject.toml`
declares its dependencies, including the CDK, which generated types import.

The entry point loads the directory by path, as a package of a fixed name
(`python_connector::load_task_py`). So the directory needn't be a Python
identifier, and its modules import one another relatively.

## Entry points

- [`project::run`](src/project/mod.rs) — the protocol loop of
  `proto_flow::derive` messages. It answers Spec and Validate, and breaks upon
  an Open. The Open **hands off** (`python_connector::Staged::hand_off`): the
  user's program inherits stdout, and is piped the Open and the remainder of
  stdin. Its exit status is the session's.
- `spec_response` — answers Spec from the declared `spec`, without staging or
  running user code. A schema the spec omits is this connector's default
  (`resolve_spec`). The default `config` allows no properties. The default
  transform `lambda` is `{readOnly: boolean}`. An invalid `ProjectConfig` has
  these defaults too, and Validate reports its error (`project_of`).
- `validate_derivation` — answers Validate:
  1. Generate the typed module (`codegen::types_py`).
  2. Stage the project and install its dependencies (`stage`).
  3. Type-check it with its own pyright, and run its tests with its own pytest
     (`Staged::check`).
  4. Run `main.py validate`, piped the Validate request with the user's own
     `config`, and the default `Validated` (`default_validated`). It calls
     the derivation's `validate`, which modifies the default.
  5. Check that it kept each transform, and return it with the typed module
     as its `generated_files`.
- Listed files which don't exist yet (`null` in the sentinel) are given
  starters (`python_connector::starter_files`). `validate_derivation` answers
  such a project with its defaults, starters, and the typed module, without
  staging or running user code. The entry's starter is a working derivation
  whose transforms publish nothing (`codegen::starter_py`).
- `stage` — renders the typed module and the generated entry point, and
  stages and installs them with the project
  (`python_connector::Staged::of_project`). Each request which runs user code
  stages and installs anew.

## The staged project

`stage` lays out `acmeCo/2024-orders` as:

```
<temp>/
├── pyproject.toml              # the user's
├── uv.lock                     # the user's, or resolved by this request
├── .venv/
├── 2024-orders/
│   ├── __init__.py             # exports `Derivation`
│   └── helpers.py              # imported relatively
└── flow_generated/
    ├── main.py                 # entry point (`codegen::main_py`)
    └── python/acmeCo/          # namespace package
        └── _2024_orders/
            └── __init__.py     # typed module (`codegen::types_py`)
```

`flow_generated/python/` is on the PYTHONPATH, and each component of the
collection's name is sanitized into an identifier
(`python_connector::module_parts`).

- `codegen::main_py` — the entry point, of `main.py.template`. It loads the
  `Derivation` and resolves its types. It then answers `validate`, or else
  serves the session's protocol loop.
- `codegen::types_py` — the typed module. It has the document types of the
  derived collection (`Document`) and of each transform's source
  (`Source{Pascal}`). It has `EndpointConfig` and `ResourceConfig`, the
  `Request` and `Response` namespaces, and the `IDerivation[C, R, S]` base
  class.

## The user's interface

- **`class Derivation(IDerivation)`** parses its configurations into the
  generated `EndpointConfig` (of `spec.configSchema`) and `ResourceConfig` (of
  `spec.resourceConfigSchema`). Its connector state is a `dict`.
  `class Derivation(IDerivation[MyConfig, MyResource, MyState])` parses them
  into types of its own instead.
- **`Derivation` must directly subclass `IDerivation`.** `main.py` resolves
  `C`, `R`, and `S` with `types.get_original_bases`, and raises otherwise, so
  Validate fails. Production derivations all subclass directly, so deeper
  hierarchies aren't supported.
- **Each transform is an abstract method**, an async generator of `Document`s:
  `async def transform_{snake}(self, read: Request.Read{Pascal})`. `main.py`
  dispatches each Read to the method of its `transform`.
- **[`codegen::Transform`](src/project/codegen/mod.rs) names each transform.**
  The `transform_` prefix keeps a method clear of keywords and of
  `IDerivation`'s own methods (`transform_2024`, `transform_flush`). A legacy
  module's methods are unprefixed. Validation fails for transforms whose
  methods or types collide (`fromInts` and `from_ints`), or which have no
  ASCII letters or digits.
- `open(cls, open: Request.Open[C, R, S]) -> tuple[Self, Response.Opened]` is
  an async class method, and the only constructor the platform calls. The
  default is `cls()`. A derivation overrides it to build its components (which
  may be awaited) and pass them to its own `__init__`.
- `validate(cls, validate: Request.Validate[C, R], validated:
  Response.Validated)` is an async class method, as no instance exists before
  Open. `validated` holds a default of each transform (`default_validated`).
  Its `readOnly` is that of the transform's `lambda`. The derivation
  modifies it in place, and raises to fail validation.
- `flush(self, flush: Request.Flush, flushed: Response.Flushed[S])` yields
  documents, and sets `flushed.state` and `flushed.more`. An async generator
  can't also return a value, so `flushed` is an out-parameter.

Messages are the CDK's. The `Request` and `Response` namespaces expose
`estuary_cdk.derive.request` and `.response`:

- `Request.Open` subclasses the CDK's `Open` with `IDerivation`'s type
  defaults. A bare `Request.Open` is
  `Request.Open[EndpointConfig, ResourceConfig, dict[str, typing.Any]]`.
- `Request.Read{Pascal}` specializes the CDK's `Read` with that transform's
  source documents.
- `C` types `open.collection.derivation.config` and `validate.config`. `R`
  types each transform's `lambdaConfig`.
- A message carries each transform's `collection` inline, or as a
  `collectionIndex` into its `linkedCollections`. The CDK sets `collection`
  either way (`estuary_cdk.flow.link_bindings`).

## Non-obvious details

- **The user's program inherits stderr**, so its logs and its final error reach
  the runtime as it emits them. `main` exits with a failed program's code
  (`python_connector::Exited`), and logs no error of its own, which would mask
  the program's.
- **Spec never runs user code.** It needs neither a collection nor a staged
  project, so the session doesn't stage until Validate or Open.
- **The connector doesn't validate `config` against its schema.** The
  derivation's own types parse it. Parse errors omit input values, as configs
  carry merged secrets.
- **Computed shuffles aren't supported.** Validate fails a transform which has
  a shuffle `lambda`.
- **Generated files are rooted at Validate's `project_root`**, the
  specification's directory. So the typed module sits beside the user's
  `pyproject.toml`, as it does when staged.
