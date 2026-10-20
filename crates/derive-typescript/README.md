# derive-typescript

Runs a TypeScript derivation: a `derive` task whose `using.typescript` block
carries its project `files`, a user `config`, and a declared `spec`. The crate is both
the Spec and Validate connector (type-checks the user's module under Deno, runs
its `validate`, and returns generated files) and the runtime connector (execs
the module under `deno run`). It builds the
`ghcr.io/estuary/derive-typescript:stable` image
(`docker/derive-typescript.Dockerfile`). Machinery shared with other
TypeScript connectors, such as the staged Deno `Project`, is in
`typescript-connector`.

`validation::custom::derive_typescript_connector` rewrites the block into an
image connector, so the image tag a task resolves to is chosen there, not here.

## Configuration

The connector config is the user's `config` at the top level, plus a
`_typescript` sentinel, a `models::custom_connector::Sentinel` of the derived
collection as its `task`, its `files`, and its declared `spec`
([`parse_config`](src/lib.rs)). The sentinel is stripped before user code sees
the config. A schema the `spec` omits is the default of a derivation
(`models::custom_connector::Spec::resolve_derivation`): a `config` which allows
no properties, and a transform `lambda` of `{readOnly: boolean}`.

A project is its listed `files`. The derivation's directory is the final
component of the derived collection's name, verbatim (`acmeCo/orders` has
`orders/`), and its `mod.ts` exports a `Derivation`. A `deno.json` maps
`flow/` to `./flow_generated/typescript/`, through which modules import their
generated types. Modules of the directory import one another relatively.

A listed `deno.lock` pins the project's dependencies, and is otherwise
resolved by Validate and baked into the built spec by
`validation::custom::bake_generated_files`. Open runs `--frozen` under
either, so a running task never re-resolves. A listed lock which doesn't
exist yet gets an empty starter, which the next Validate reports as out of
date with the command that resolves it. The starter `deno.json`
(`models::custom_connector::Language::starter_manifest`) has a one-day
`minimumDependencyAge`, which shapes only a fresh resolution.

Built specs which predate the sentinel carry the legacy `{module}` shape, which
is still accepted with an empty user config and no declared spec. It's staged
as the files which validation builds a legacy model with
(`Language::legacy_files`): the module as the derivation's `mod.ts`, beside the
starter `deno.json`. It has no lock, and runs unfrozen.
Its `environment` is no longer supported and is an error if non-empty: secrets
are delivered through `config` and a `secrets` stanza instead.

## The module's interface

- **Messages are the protocol's.** [`derive.ts`](src/derive.ts) is a static
  file written to `flow_generated/typescript/derive.ts` of every project (and
  returned among Validate's generated files). It imports the platform's types
  from `estuary.ts` (`typescript_connector::ESTUARY_TS`), which is written
  beside it. It mirrors the CDK's `estuary_cdk.derive` models of Python. `main.ts`
  links each transform's `collection` (`linkTransforms`), whether the message
  is inline or indirect, and an omitted `lambdaConfig` is `{}`.
- `Derivation` (required) extends the generated
  `IDerivation<C = EndpointConfig, R = ResourceConfig, S = unknown>`, whose
  constructor takes `open: Open<C, R, S>`. Its `config` is
  `open.collection.derivation.config`, and `R` is each transform's
  `lambdaConfig`. It also accepts the master-era `{ state, range }`, so that
  constructors which take that shape and call `super(open)` still type-check.
  `EndpointConfig` and `ResourceConfig` are generated from `spec.configSchema`
  and `spec.resourceConfigSchema`; fields having a `default` are optional.
  Configurations are typed, not validated against their declared schema.
- `static validate(validate: Validate<C, R>): Validated` may be overridden
  to check the derivation as it's published (throw to fail) or to decide
  `readOnly` from the derivation's own resource convention. The default reads
  each transform's `readOnly`.
- Transform methods take `read: Read<Source...>`, which a master-era
  `{ doc }` parameter still accepts.
- `flush(flush: Flush): Promise<FlushResponse<S> | Document[]>`, where
  `FlushResponse<S>` is the protocol's `Flushed<S>` plus `published`
  documents. A bare `Document[]` publishes and ends the derivation's
  participation in the transaction.

## Entry points

- [`run`](src/lib.rs) — the protocol loop. Spec and Validate are answered in
  process. An Open replaces its derivation config with the user config, then
  hands off to `deno run flow_generated/main.ts`, and only proxies stdin after
  that.
- [`spec_response`](src/lib.rs) — Spec, answered from the sentinel's `spec`
  without loading the module. A legacy config or a bare image Spec has no
  sentinel, and is answered with this connector's defaults.
- [`validate_derivation`](src/lib.rs) — generates types, checks the project
  (`typescript_connector::Project::check`: a listed lock must pin every
  import, and the project must type-check), then runs `main.ts validate`, which is handed the Validate request (with the user's
  own `config`). It returns the derivation's `Validated` with its
  generated types, and the lock Deno resolved if the project lists none.
  A project having listed files which don't exist yet is instead answered
  with starters of them (a working derivation as the entry, a `deno.json`,
  or else empty), without running user code.
- [`codegen`](src/codegen/mod.rs) — the typed `IDerivation` module (whose
  configuration types are `typescript_connector::config_types_ts`), `main.ts`,
  and the entry starter.

## The temp project

```
<temp>/
├── deno.json                # a `files` entry, mapping `flow/`
├── orders/
│   ├── mod.ts               # exports `Derivation`
│   └── helpers.ts           # a `files` entry, imported relatively
└── flow_generated/
    ├── main.ts              # generated; imports ../orders/mod.ts by path
    └── typescript/
        ├── estuary.ts       # platform types, imported by derive.ts
        ├── derive.ts        # protocol types, imported by <collection>.ts
        └── <collection>.ts
```

## Non-obvious details

- **Spec never loads the module**, so a session's Spec is cheap and the module
  needs no Spec-time stand-in types.
- **Modules are imported dynamically**, after `console.log` is redirected to
  stderr, so a module's top-level logging can't corrupt stdout.
- **`main.ts` never imports a remote module.** A lock pins the whole module
  graph, including `main.ts`. A remote import added by a newer image would
  be missing from every existing lock, and `--frozen` would fail those tasks.
- **Every `deno run` gets the same permissions** (`DENO_PERMISSIONS`, which
  is `--allow-net=api.openai.com`), so the module loads the same way for
  Validate and Open.
- **Deno tests are skipped** when `deno` isn't on PATH, as in CI. Run them with
  `mise exec deno@2 -- cargo test -p derive-typescript`. Lock tests also fetch
  a package from JSR.
