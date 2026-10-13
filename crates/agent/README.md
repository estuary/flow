# Control plane agent

The control plane agent is a binary that serves:

- The HTTP (GraphQL and REST) APIs
- Controllers for live specs
- All other background `automations` executors, which today handle interactive publications, discovers, directives, etc

This crate depends on `control-plane-api` for most low-level functionality.
