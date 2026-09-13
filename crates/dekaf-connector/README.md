# Dekaf connector

The portions of Dekaf which are needed to perform build-time validations, plus
the configuration types which both the build and the Dekaf server parse. It's
separate from the main `dekaf` crate because `dekaf` depends on `flow-client`,
which prevented `flow-client` from depending on `control-plane-api`. This crate
does _not_ depend on `flow-client`, and thus can be linked into
`control-plane-api` without creating a cycle.

## Key types

- `connector()` - Serves the materialization protocol: `Spec`, and a `Validate`
  which checks the configuration and derives per-projection constraints. Its
  `variant` argument names the Dekaf flavor and carries no behavior; it appears
  only in error context.
- `DekafConfig` - A Dekaf task's endpoint configuration: its `token`, deletion
  mode, and topic-naming strictness. This is the *inner* half of the
  `models::DekafConfig { variant, config }` wrapper which a built spec persists.
- `DekafResourceConfig` - Per-binding configuration: the exposed topic name.

