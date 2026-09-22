# Models

Shared catalog models, identifiers, and authorization types used by the control
plane, clients, and catalog compilation. Start with `src/lib.rs` for exports and
`src/references.rs` for catalog-name wrappers and validation.

`src/authz.rs` defines fine-grained capability bits and their bundles; legacy
capabilities map to those bundles through `bits_for_legacy`. Sandbox creation
requires `CreateSandbox` on its catalog name, included in the Admin bundle.
Optional `async-graphql` and `sqlx-support` features expose the shared types to
the API and database layers.
