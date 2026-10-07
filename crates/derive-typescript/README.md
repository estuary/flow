# derive-typescript

Runs a TypeScript derivation: a `derive` task whose `using.typescript` block
carries its project `files`, a user `config`, and a declared `spec`. The crate is both
the Spec and Validate connector (type-checks the user's module under Deno, runs
its `validate`, and returns generated files) and the runtime connector (execs
the module under `deno run`). It builds the
`ghcr.io/estuary/derive-typescript:stable` image
(`docker/derive-typescript.Dockerfile`).

`validation::builtin::derive_typescript_connector` rewrites the block into an
image connector, so the image tag a task resolves to is chosen there, not here.

## Configuration

The connector config is the user's `config` at the top level, plus a
`_typescript: {collection, files, spec}` sentinel
([`parse_config`](src/lib.rs)). The sentinel is stripped before user code sees
the config. A schema the `spec` omits is this connector's default
(`Spec::resolve`): any `config`, and a transform `lambda` of
`{readOnly: boolean}`.

A project is its listed `files`. The derivation's directory is the final
component of the derived collection's name, verbatim (`acmeCo/orders` has
`orders/`), and its `mod.ts` exports a `Derivation`. A `deno.json` maps
`flow/` to `./flow_generated/typescript/`, through which modules import their
generated types. Modules of the directory import one another relatively.

Built specs which predate the sentinel carry the legacy `{module}` shape, which
is still accepted with an empty user config and no declared spec, and is
staged as a project of the same shape (`module/mod.ts` beside a `deno.json`).
Its `environment` is no longer supported and is an error if non-empty: secrets
are delivered through `config` and a `secrets` stanza instead.

## The module's interface

- `Derivation` (required) extends the generated
  `IDerivation<C = EndpointConfig, R = ResourceConfig>`, whose constructor is
  `(open, config?: C)`. Its `open` is a structural supertype of `Open<R>`
  (`resources` is optional), so that master-era constructors taking
  `open: { state, range }` and calling `super(open)` still type-check. `open.resources` holds each transform's resource
  configuration (its `lambda`). `EndpointConfig` and `ResourceConfig` are
  generated from `spec.configSchema` and `spec.resourceConfigSchema`; fields
  having a `default` are optional. Configurations are typed, not validated:
  the runtime only warns of a config which doesn't match its declared schema.
- `static validate(validate: Validate, config): Validated` may be overridden
  to check the derivation as it's published (throw to fail) or to decide
  `readOnly` from the derivation's own resource convention. The default reads
  each transform's `readOnly`.

## Entry points

- [`run`](src/lib.rs) — the protocol loop. Spec and Validate are answered in
  process. An Open replaces its derivation config with the user config, then
  hands off to `deno run flow_generated/main.ts`, and only proxies stdin after
  that.
- [`spec_response`](src/lib.rs) — Spec, answered from the sentinel's `spec`
  without loading the module. It's `{}` without a sentinel, as with a legacy
  config or a bare image Spec.
- [`validate`](src/lib.rs) — generates types, runs `deno check` of the entry
  point, then `main.ts validate`. It returns the derivation's `Validated` with
  its generated types. A project having listed files which don't exist yet is
  instead answered with starters of them (a working derivation as the entry,
  a `deno.json`, or else empty), without running user code.
- [`codegen`](src/codegen/mod.rs) — the typed `IDerivation` module (including
  the JSON-schema → TypeScript mapper, which isn't shared with Python),
  `main.ts`, and the entry starter.

## The temp project

```
<temp>/
├── deno.json                # a `files` entry, mapping `flow/`
├── orders/
│   ├── mod.ts               # exports `Derivation`
│   └── helpers.ts           # a `files` entry, imported relatively
└── flow_generated/
    ├── main.ts              # generated; imports ../orders/mod.ts by path
    └── typescript/<collection>.ts
```

## Non-obvious details

- **Spec never loads the module**, so a session's Spec is cheap and the module
  needs no Spec-time stand-in types.
- **Modules are imported dynamically**, after `console.log` is redirected to
  stderr, so a module's top-level logging can't corrupt stdout.
- **Every `deno run` gets the same permissions** (`--allow-net=api.openai.com`),
  so the module loads the same way for Validate and Open.
- **Deno failures are reported with temp-project paths** rewritten to be
  relative ([`rewrite_deno_stderr`](src/lib.rs)).
- **Deno tests are skipped** when `deno` isn't on PATH, as in CI. Run them with
  `mise exec deno@2 -- cargo test -p derive-typescript`.
