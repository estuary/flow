# typescript-connector

Shared machinery of connectors which run user-authored TypeScript projects
under Deno: `derive-typescript`, and (in time) capture-typescript and
materialize-typescript. Behavior shared with connectors of other languages,
such as the sentinel of a built configuration and starter manifests, is in
`models::custom_connector`.

## Key types and entry points

- [`Project`](src/project.rs) — a project staged into a temporary directory,
  from the sentinel's `files` plus generated files. A project which has a lock
  (listed or baked) is **frozen**: every `deno` invocation uses that lock as-is.
  Otherwise Deno resolves a lock as it runs, which `Project::lock` returns for
  baking. `Project::check` runs Validate's checks: a frozen lock must pin every
  import (`deno install --frozen`), then `deno check` of the entry point.
  `Project::command` and `Project::run` run the generated entry point `ENTRY`
  (`flow_generated/main.ts`) under the connector's Deno permissions, and errors
  have paths relative to the project.
- [`estuary.ts`](src/estuary.ts) (`ESTUARY_TS`) — platform types of every
  connector protocol, such as `CollectionSpec` and `RangeSpec`. A connector
  writes it at `ESTUARY_PATH` of every project, and its own protocol messages
  (such as derive-typescript's `derive.ts`) import it.
- [`config_types_ts`](src/config_types.rs) — generated `EndpointConfig` and
  `ResourceConfig` types of a `Spec`, whose schemas are resolved with the
  connector's defaults. A field having a `default` is optional, as applying
  it is the user code's job.
- [`ts_types`](src/ts_types/mod.rs) — maps JSON schemas into TypeScript types.
  `Mapper::for_config` maps configurations, and `Mapper::new` maps documents,
  hoisting each `$anchor` whose name begins with a capital into a named type.

## Non-obvious details

- **Deno has no install step.** Unlike `python-connector`, there's no virtual
  environment or `dev` group, and `deno check` is built in. A project's mode
  is decided once, when it's staged, by whether it has a lock.
- **A connector chooses its own Deno permissions**, and gives every run the
  same ones, so that its modules load alike for each request.
- **Generated types are the garden path.** User code may parse into its own
  types instead; the platform doesn't check that they match the declared schemas.
