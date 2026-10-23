# typescript-connector

Shared machinery of connectors which run user-authored TypeScript projects
under Deno: `derive-typescript`, and (in time) capture-typescript and
materialize-typescript.

## Key types and entry points

- [`ts_types`](src/ts_types/mod.rs) — maps JSON schemas into TypeScript types.
