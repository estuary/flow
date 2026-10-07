# derive-python

Runs a Python derivation: a `derive` task whose `using.python` block carries a
`module`, its `files`, its `config`, and a declared `spec`. The crate is both
the Spec / Validate connector and the runtime connector (which execs the user's
code under `uv`). Shared machinery (project staging, uv locking, Pydantic and
config codegen, the sentinel `spec`) lives in `python-connector`.

`validation::builtin` rewrites this block into an image connector whose config
is the user's `config` plus a `_python` sentinel (`collection`, `module`,
`files`, the resolved `spec`, and a baked `uv.lock`). The `:dev` tag is frozen,
and receives only the legacy `{module}` shape.

## Entry points

- [`run`](src/lib.rs) — the protocol loop. An Open hands off to
  `uv run main.py` and then only proxies stdin.
- `spec_response` — answers Spec from the sentinel's `spec`, without staging
  or running user code. Legacy configs (and a bare image Spec) answer `{}`.
- `validate_derivation` — generates types, stages and installs the project,
  runs the project's own pyright (if its `dev` group has one), then
  `main.py validate`, which calls the derivation's `validate` class method.
  It returns that `Validated` with the types, a default `pyproject.toml`, and a
  resolved `uv.lock` (unless the user lists one) as `generated_files`.
- [`codegen`](src/codegen/mod.rs) — `main.py`, the typed module (documents,
  `EndpointConfig` / `ResourceConfig`, protocol messages, and the generic
  `IDerivation[C, R]` base class), and the `module` stub.

## The user's interface

- `class Derivation(IDerivation)` receives the generated `EndpointConfig` and
  `ResourceConfig` (types of `spec.configSchema` and `spec.resourceConfigSchema`).
  `class Derivation(IDerivation[MyConfig, MyResource])` declares its own types
  instead (the escape hatch), whose equivalence to the declared schemas isn't
  checked. `main.py` resolves `C` and `R` with `types.get_original_bases`, so
  `Derivation` must subclass `IDerivation` directly.
- `__init__(self, open: Request.Open[R], config: C)`; `open.resources` holds
  each transform's parsed resource configuration (its `lambda`).
- `validate(cls, validate: Request.Validate[R], config: C) -> Response.Validated`
  is a class method (no instance exists before Open). The default maps each
  transform's `readOnly`. Raise to fail validation.

## The temp project

```
<temp>/
├── main.py                  # generated; `Derivation(open, config)`, or `validate`
├── module.py                # the user's `module`
├── lib/geo.py               # a `files` entry, at its key
├── pyproject.toml           # a `files` entry, or a generated default
├── uv.lock                  # the user's, baked, or resolved by Validate
└── flow_generated/python/
    └── <collection path>/__init__.py
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
- **Project root.** Validation roots a Python derivation's project at its
  specification's directory, so generated types sit beside its `pyproject.toml`.
- **Legacy configs.** Built specs predating `_python` (a top-level `module`,
  `dependencies`) are still accepted until they're re-published: they declare
  no spec, so `C` and `R` are the generated defaults and `config` is empty.
- **No `deny_unknown_fields`.** A newer control plane must be able to talk to an
  older image.
