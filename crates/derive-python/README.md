# derive-python

Runs a Python derivation: a `derive` task whose `using.python` block carries
its project `files`, its `config`, and a declared `spec`. The crate is both
the Spec / Validate connector and the runtime connector (which execs the user's
code under `uv`). Shared machinery (project staging, uv locking, Pydantic and
config codegen, the entry point's loader, the sentinel `spec`) lives in
`python-connector`.

`validation::builtin` rewrites this block into an image connector whose config
is the user's `config` plus a `_python` sentinel (`collection`, `files` with a
baked `uv.lock`, and the declared `spec`). The `:dev` tag is frozen, and
receives only the legacy `{module}` shape, filled from the entry file.

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
  or running user code. A schema it omits is this connector's default: any
  `config`, and a transform `lambda` of `{readOnly: boolean}` (`resolve_spec`).
- `validate_derivation` — generates types, stages and installs the project,
  runs the project's own pyright (if its `dev` group has one) over its files
  and the generated entry, then `main.py validate`, which calls the
  derivation's `validate` class method. It returns that `Validated` with the
  types and a resolved `uv.lock` (unless the user lists one) as
  `generated_files`. A project having listed files which don't exist yet is
  instead answered with starters of them (a working derivation as the entry,
  a `pyproject.toml`, or else empty), without running user code.
- [`codegen`](src/codegen/mod.rs) — the entry point (`main.py`), the typed
  module (documents, `EndpointConfig` / `ResourceConfig`, protocol messages,
  and the generic `IDerivation[C, R]` base class), and the entry starter.

## The user's interface

- `class Derivation(IDerivation)` receives the generated `EndpointConfig` and
  `ResourceConfig` (types of `spec.configSchema` and `spec.resourceConfigSchema`).
  `class Derivation(IDerivation[MyConfig, MyResource])` declares its own types
  instead (the escape hatch), whose equivalence to the declared schemas isn't
  checked.
- **`Derivation` must directly subclass `IDerivation`**: `main.py` resolves `C`
  and `R` with `types.get_original_bases`, and a Validate fails otherwise.
  Production derivations all do, so this narrow contract is kept rather than
  supporting deeper hierarchies.
- `__init__(self, open: Request.Open[R], config: C)`; `open.resources` holds
  each transform's parsed resource configuration (its `lambda`).
- `validate(cls, validate: Request.Validate[R], config: C) -> Response.Validated`
  is a class method (no instance exists before Open). The default maps each
  transform's `readOnly`. Raise to fail validation.

## The temp project

```
<temp>/
├── pyproject.toml              # a `files` entry
├── uv.lock                     # the user's, baked, or resolved by Validate
├── 2024-orders/
│   ├── __init__.py             # exports `Derivation`
│   └── helpers.py              # a `files` entry, imported relatively
└── flow_generated/
    ├── main.py                 # generated entry; `Derivation(open, config)`, or `validate`
    └── python/<collection path>/__init__.py
```

## Non-obvious details

- **Spec never runs user code.** It needs neither a collection nor a staged
  project, so the session doesn't stage until Validate or Open.
- **Shims don't validate `config` against its schema.** The runtime warns of a
  mismatch, and the user's declared types are what parse it. Parse errors are
  reported without input values, since configs carry merged secrets.
- **Locks.** `python_connector::Install::of_request` picks the install mode:
  Validate resolves a lock (or checks the user's own), and Open installs the
  baked lock frozen.
- **Project root.** Validation roots a derivation's project at its
  specification's directory, so generated types sit beside its `pyproject.toml`.
- **Legacy configs.** Built specs predating `_python` (a top-level `module`,
  `dependencies`) are still accepted until they're re-published, as images roll
  out before the control plane. They're staged as a project of the same shape:
  the module as `module/__init__.py`, beside a `pyproject.toml` of their
  `dependencies`. They declare no spec, so `C` and `R` are the generated
  defaults and `config` is empty.
- **No `deny_unknown_fields`.** A newer control plane must be able to talk to an
  older image.
