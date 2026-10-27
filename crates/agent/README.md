# Control plane agent

The control plane agent is a binary that serves:

- The HTTP (GraphQL and REST) APIs
- Controllers for live specs
- All other background `automations` executors, which today handle interactive publications, discovers, etc.

This crate depends on `control-plane-api` for most low-level functionality.

The optional OAuth authorization service uses the flattened `oauth_server::Config`
argument group. Set `OAUTH_ISSUER`, `OAUTH_CONSENT_URL`, and
`OAUTH_ENCRYPTION_KEY` together; see [oauth-server](../oauth-server/README.md).
