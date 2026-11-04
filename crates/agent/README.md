# Control plane agent

The control plane agent is a binary that serves:

- The HTTP (GraphQL and REST) APIs
- Controllers for live specs
- All other background `automations` executors, which today handle interactive publications, discovers, etc.

This crate depends on `control-plane-api` for most low-level functionality.

The optional OAuth authorization service uses the flattened `oauth_server::Config`
argument group. Set `OAUTH_ISSUER`, `OAUTH_CONSENT_URL`, and
`OAUTH_ENCRYPTION_KEY` together; see [oauth-server](../oauth-server/README.md).

## Tracing

`src/telemetry.rs` exports traces over OTLP/HTTP when
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is set.
The SDK reads the endpoint, headers, and batching settings from the standard
`OTEL_*` variables.

`deploy-agent-api.yaml` sets the trace endpoint from the GitHub configuration
variable `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and binds authentication headers from
the Secret Manager secret `OTEL_EXPORTER_OTLP_TRACES_HEADERS:latest`. Cloud Run
keeps CPU allocated between requests so background export can run.

Export is scoped to requests by `control_plane_api::server::export_filter`. An entry-point span, which declares `otel.kind`, is exported along with every span and event at INFO or above beneath it. Work outside a request, such as controllers and automations, is not exported.
