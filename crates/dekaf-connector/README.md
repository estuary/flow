# Dekaf connector

This crate includes the portions of dekaf that are needed in order to perform build-time validations. It's separate from the main `dekaf` crate because `dekaf` depends on `flow-client`, which prevented `flow-client` from depending on `control-plane-api`. This crate does _not_ depend on `flow-client`, and thus can be linked to `control-plane-api` (through the `runtime` crate) without creating a cycle.
