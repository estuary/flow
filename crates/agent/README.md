# Control plane agent

The control plane agent is a binary that serves:

- The HTTP (GraphQL and REST) APIs
- Controllers for live specs
- All other background `automations` executors, which today handle interactive publications, discovers, directives, etc

This crate depends on `control-plane-api` for most low-level functionality.

`--oauth-config` / `OAUTH_CONFIG` enables the OAuth authorization server. The agent
also runs expired authorization-request cleanup every five minutes. See the
[OAuth integration contract](../control-plane-api/src/server/oauth/README.md) for
configuration and migration order.
