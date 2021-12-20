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
captures:
  acmeCo/example/source-s3:
    endpoint:
      connector:
        # Docker image which implements the capture connector.
        image: ghcr.io/estuary/source-s3:dev
        config: path/to/connector-config.yaml
    bindings:
      - resource:
          stream: a-bucket/and-prefix
          syncMode: incremental
        target: acmeCo/example/collection
      - resource:
          stream: a-bucket/another-prefix
          syncMode: incremental
        target: acmeCo/example/another-collection

```

## Push Captures

Push captures expose an endpoint to which documents may be pushed using a supported ingestion protocol:

```yaml
captures:
  acmeCo/example/webhook-ingest:
    endpoint:
      ingest: {}
    bindings:
      - resource:
          name: webhooks
        target: acmeCo/example/webhooks
```

:::caution
Push captures are under development.
Estuary intends to offer Webhook, Websocket, and Kafka-compatible APIs for capturing into collections. Specification details are likely to exist.
:::