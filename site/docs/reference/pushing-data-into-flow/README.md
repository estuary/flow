---
description: Alternatives to captures for data ingestion
---

# Other data ingestion methods

The best and most common way to ingest external data into Flow is using [captures](../catalog-reference/captures/); however, you can also ingest data directly using the `flow-ingester` binary. This can be helpful in certain scenarios, such as [testing](../catalog-reference/tests.md). A full conceptual overview can be found [here](../../concepts/catalog-entities/captures.md#other-ingestion-methods).

There are several ways to use [`flow-ingester`](flow-ingester.md), divided into two broad categories:

* Create a HTTP PUT or POST requests using the [REST API](rest-api.md)
* Stream data over a Websocket in either CSV, TSV, or JSON formats using the [WebSocket API](websocket-api.md)

