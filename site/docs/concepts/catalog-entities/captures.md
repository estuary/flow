---
description: How Flow uses captures to pull data from external sources
---

# Captures

A **capture** is a catalog task which connects to an endpoint
and binds one or more of its resources, such as a database tables,
to Flow collections.
As documents become available for any of the bindings,
Flow validates their schema and adds them to their bound collection.

![](<captures.svg>)

## Pull Captures

Pull captures pull documents from an endpoint using a [connector](../#connectors):

```yaml
# A set of captures to include in the catalog.
# Optional, type: object
captures:
  # The name of the capture.
  acmeCo/example/source-s3:
    # Endpoint defines how to connect to the source of the capture.
    # Required, type: object
    endpoint:
      # This endpoint uses a connector provided as a Docker image.
      connector:
        # Docker image which implements the capture connector.
        image: ghcr.io/estuary/source-s3:dev
        # File which provides the connector's required configuration.
        # Configuration may also be presented inline.
        config: path/to/connector-config.yaml

    # Bindings define how collections are populated from the data source.
    # A capture may bind multiple resources to different collections.
    # Required, type: array
    bindings:
      - # The target collection to capture into.
        # This may be defined in a separate, imported catalog source file.
        # Required, type: string
        target: acmeCo/example/collection

        # The resource is additional configuration required by the endpoint
        # connector to identify and capture a specific endpoint resource.
        # The structure and meaning of this configuration is defined by
        # the specific connector.
        # Required, type: object
        resource:
          stream: a-bucket/and-prefix
          syncMode: incremental

      - target: acmeCo/example/another-collection
        resource:
          stream: a-bucket/another-prefix
          syncMode: incremental
```

### Estuary Sources

Estuary builds and maintains many real-time connectors for various technology systems,
such as database change-data-capture (CDC) connectors.

Docker images can be found [on GitHub](https://github.com/orgs/estuary/packages?repo_name=connectors).

:::note
We're working on developing reference documentation for Estuary-developed connectors.
Stay tuned!
:::

### Airbyte Sources

Flow also natively supports Airbyte source connectors.
These connectors tend to focus on SaaS APIs, and do not offer real-time streaming integrations.
Flow runs the connector at regular intervals to capture updated documents.

A list of third-party connectors can be found on the
[Airbyte docker hub](https://hub.docker.com/u/airbyte?page=1).
You can use any item whose name begins with `source-`.

### Discovery

Flow offers a CLI tool `flowctl discover --image connector/image:tag` which
provides a guided workflow for creating a correctly configured capture.

## Push Captures

Push captures expose an endpoint to which documents may be pushed using a supported ingestion protocol:

```yaml
captures:

  # The name of the capture.
  acmeCo/example/webhook-ingest:
    endpoint:
      # This endpoint is an ingestion.
      ingest: {}

    bindings:
      - # The target collection to capture into.
        target: acmeCo/example/webhooks
        # The resource configures the specific behavior of the ingestion endpoint.
        resource:
          name: webhooks
```

:::caution
Push captures are under development.
Estuary intends to offer Webhook, Websocket, and Kafka-compatible APIs for capturing into collections. Specification details are likely to exist.
:::