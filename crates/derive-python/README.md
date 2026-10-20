# derive-python

Runs a Python derivation: a `derive` task whose `using.python` block carries
its project `files`, its `config`, and a declared `spec`. The crate is both
the Spec / Validate connector and the runtime connector (which execs the user's
code under `uv`). Shared machinery (project staging, uv locking, Pydantic and
config codegen, the entry point's loader) lives in `python-connector`.

`validation::custom` rewrites this block into an image connector whose config
is the user's `config` plus a `_python` sentinel, a
`models::custom_connector::Sentinel` (the derived collection as its `task`,
`files` with a baked `uv.lock`, and the declared `spec`). A model having a
legacy `module` is stored as authored, and built as that module beside the
starter `pyproject.toml` (`models::custom_connector::Language::legacy_files`). The `:dev` tag is frozen, and receives only the
legacy `{module}` shape, filled from the entry file.

## The project

A project is its listed `files`. The derivation's directory is the final
component of the derived collection's name, verbatim (`acmeCo/2024-orders` has
`2024-orders/`), and its `__init__.py` exports a `Derivation`. A
`pyproject.toml` declares its dependencies. Its modules import one another
relatively: the platform loads the directory by path (see
`python_connector::load_task_py`).

## Entry points

- [`run`](src/lib.rs) — the protocol loop. Spec and Validate are answered in
  process; an Open hands off to `uv run flow_generated/main.py` and then only
  proxies stdin.
- `spec_response` — answers Spec from the sentinel's `spec`, without staging
  or running user code. A schema it omits is this connector's default: a
  `config` which allows no properties, and a transform `lambda` of
  `{readOnly: boolean}` (`models::custom_connector::Spec::resolve_derivation`).
- `validate_derivation` — generates types, stages and installs the project,
  runs the project's own pyright (if its `dev` group has one) over its files
  and the generated entry, then `main.py validate`, which is handed the
  Validate request (with the user's own `config`) and calls the derivation's
  `validate` class method. It returns that `Validated` with the
  types and a resolved `uv.lock` (unless the user lists one) as
  `generated_files`. A project having listed files which don't exist yet is
  instead answered with starters of them (a working derivation as the entry,
  a `pyproject.toml`, or else empty), without running user code. An empty
  `uv.lock` starter is reported as out of date by the next Validate.
- [`codegen`](src/codegen/mod.rs) — the entry point (`main.py`), the typed
  module (documents, `EndpointConfig` / `ResourceConfig`, the `Request` and
  `Response` namespaces of protocol messages, and the generic
  `IDerivation[C, R, S]` base class), and the entry starter.

## The user's interface

- **Messages are the protocol's**, as modeled by the CDK's
  `estuary_cdk.derive.request` / `.response` and `estuary_cdk.flow`. Projects
  depend on `models::custom_connector::CDK_DEPENDENCY`. The
  generated `Request` and `Response` namespaces alias the CDK's messages, and
  `Request.Read{Transform}` specializes `Read` with each transform's source
  documents. Configurations are typed in place: `C` is
  `open.collection.derivation.config` (and `validate.config`), and `R` is each
  transform's `lambdaConfig`. The CDK links each transform's `collection`,
  whether the message is inline or indirect.
- `class Derivation(IDerivation)` receives the generated `EndpointConfig` and
  `ResourceConfig` (types of `spec.configSchema` and `spec.resourceConfigSchema`),
  and a `dict` connector state. `class Derivation(IDerivation[MyConfig,
  MyResource, MyState])` declares its own types instead (the escape hatch),
  whose equivalence to the declared schemas isn't checked.
- **`Derivation` must directly subclass `IDerivation`**: `main.py` resolves `C`,
  `R`, and `S` with `types.get_original_bases`, and a Validate fails otherwise.
  Production derivations all do, so this narrow contract is kept rather than
  supporting deeper hierarchies.
- `__init__(self, open: Request.Open[C, R, S])`.
- `validate(cls, validate: Request.Validate[C, R]) -> Response.Validated` is a
  class method (no instance exists before Open). The default maps each
  transform's `readOnly`. Raise to fail validation.
- `flush(self, flush: Request.Flush, flushed: Response.Flushed[S])` yields
  documents, and sets `flushed.state` and `flushed.more`. An async generator
  can't also return a value, so `flushed` is an out-parameter.

## The temp project

```
<temp>/
├── pyproject.toml              # a `files` entry
├── uv.lock                     # the user's, baked, or resolved by Validate
├── 2024-orders/
│   ├── __init__.py             # exports `Derivation`
│   └── helpers.py              # a `files` entry, imported relatively
└── flow_generated/
    ├── main.py                 # generated entry; `Derivation(open)`, or `validate`
    └── python/<collection path>/__init__.py
```

## Non-obvious details

- **Spec never runs user code.** It needs neither a collection nor a staged
  project, so the session doesn't stage until Validate or Open.
- **Shims don't validate `config` against its schema.** The user's declared
  types are what parse it. Parse errors are reported without input values,
  since configs carry merged secrets.
- **Locks.** `python_connector::Install::of_request` picks the install mode:
  Validate resolves a lock (or checks the user's own), and Open installs the
  baked lock frozen.
- **Project root.** Validation roots a derivation's project at its
  specification's directory, so generated types sit beside its `pyproject.toml`.
- **Legacy configs.** Built specs predating `_python` (a top-level `module`)
  are still accepted until they're re-published, as images roll out before the
  control plane. They're staged as the files which validation builds a legacy
  model with (`Language::legacy_files`): the module as the derivation's
  `__init__.py`, beside the starter `pyproject.toml`. Their `dependencies` and `environment` are no longer
  supported, and are errors if non-empty. They declare no spec, so `C` and `R`
  are the generated defaults and `config` is empty.
- **No `deny_unknown_fields`.** A newer control plane must be able to talk to an
  older image.
