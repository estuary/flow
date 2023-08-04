---
sidebar_position: 2
---
# Captures

A **capture** is how Flow ingests data from an external source.
Every Data Flow starts with a capture.

Captures are a type of Flow **task**.
They connect to an external data source, or **endpoint**,
and bind one or more of its resources, such as database tables.
Each binding adds documents to a corresponding Flow **collection**.

Captures run continuously:
as soon as new documents are made available at the endpoint resources,
Flow validates their schema and adds them to the appropriate collection.
Captures can process [documents](./collections.md#documents) up to 16 MB in size.

![](<captures-new.svg>)

You define and configure captures in **Flow specifications**.

[See the guide to create a capture](../guides/create-dataflow.md#create-a-capture)

## Connectors

Captures extract data from an endpoint using a [connector](../#connectors).
Estuary builds and maintains many real-time connectors for various technology systems,
such as database change data capture (CDC) connectors.

See the [source connector reference documentation](../reference/Connectors/capture-connectors/README.md).

### Airbyte sources

Flow supports running Airbyte source connectors (through
[airbyte-to-flow](https://github.com/estuary/airbyte/tree/master/airbyte-to-flow))
These connectors tend to focus on SaaS APIs, and do not offer real-time streaming integrations.
Flow runs the connector at regular intervals to capture updated documents.

Airbyte source connectors are independently reviewed and sometime updated for compatibility with Flow.
Estuary's [source connectors](../reference/Connectors/capture-connectors/README.md) documentation includes actively supported Airbyte connectors.
A full list of Airbyte's connectors is available at [Airbyte docker hub](https://hub.docker.com/u/airbyte?page=1).
If you see a connector you'd like to prioritize for access in the Flow web app, [contact us](mailto:support@estuary.dev).

## Discovery

To help you configure new pull captures, Flow offers the guided **discovery** workflow in the Flow web application.

To begin discovery, you tell Flow the connector you'd like to use and basic information about the endpoint.
Flow automatically generates a capture configuration for you. It identifies one or more
**resources** — tables, data streams, or the equivalent — and generates **bindings** so that each will be mapped to a
data collection in Flow.

You may then modify the generated configuration as needed before publishing the capture.

## Specification

Captures are defined in Flow specification files per the following format:

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
        # Docker image that implements the capture connector.
        image: ghcr.io/estuary/source-s3:dev
        # File that provides the connector's required configuration.
        # Configuration may also be presented inline.
        config: path/to/connector-config.yaml

    # Bindings define how collections are populated from the data source.
    # A capture may bind multiple resources to different collections.
    # Required, type: array
    bindings:
      - # The target collection to capture into.
        # This may be defined in a separate, imported specification file.
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
    
    # Interval of time between invocations of non-streaming connectors.
    # If a connector runs to completion and then exits, the capture task will
    # restart the connector after this interval of time has elapsed.
    #
    # Intervals are relative to the start of an invocation and not its completion.
    # For example, if the interval is five minutes, and an invocation of the
    # capture finishes after two minutes, then the next invocation will be started
    # after three additional minutes.
    #
    # Optional. Default: Five minutes.
    interval: 5m
```