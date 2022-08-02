---
sidebar_position: 2
---
# Captures

A **capture** is a catalog task that connects to an external data source, or endpoint,
and binds one or more of its resources, such as database tables,
to Flow collections.
As documents become available for any of the bindings,
Flow validates their schema and adds them to their bound collection.

![](<captures.svg>)

## Pull captures

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
          # syncMode should be set to incremental for all Estuary connectors
          syncMode: incremental

      - target: acmeCo/example/another-collection
        resource:
          stream: a-bucket/another-prefix
          syncMode: incremental
```

### Estuary sources

Estuary builds and maintains many real-time connectors for various technology systems,
such as database change data capture (CDC) connectors.

See the [source connector reference documentation](../reference/Connectors/capture-connectors/README.md).

### Airbyte sources

Flow also natively supports Airbyte source connectors.
These connectors tend to focus on SaaS APIs, and do not offer real-time streaming integrations.
Flow runs the connector at regular intervals to capture updated documents.

Airbyte source connectors are independently reviewed and sometime updated for compatibility with Flow.
Estuary's [source connectors](../reference/Connectors/capture-connectors/README.md) documentation includes actively supported Airbyte connectors.
A full list of Airbyte's connectors is available at [Airbyte docker hub](https://hub.docker.com/u/airbyte?page=1).
If you see a connector you'd like to prioritize for access in the Flow web app, [contact us](mailto:support@estuary.dev).

### Discovery

To help you configure new pull captures, Flow offers the guided **discovery** workflow in the Flow web application.

To begin discovery, you tell Flow the connector you'd like to use and basic information about the endpoint.
Flow automatically stubs out the capture configuration for you. It identifies one or more
**resources** — tables, data streams, or the equivalent — and generates **bindings** so that each will be mapped to a
data collection in Flow.

You may then modify the generated configuration as needed before publishing the capture.

For detailed steps, see the [guide to create a dataflow in the web app](../guides/create-dataflow.md#create-a-capture).

## Push captures

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