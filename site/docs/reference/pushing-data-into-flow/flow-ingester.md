# Flow Ingester

When ingesting data outside of a capture, you'll work with the  `flow-ingester` binary. Flow Ingester ships with Flow and provides network service endpoints for ingesting data into Flow [collections](../../concepts/catalog-entities/collections.md).&#x20;

There are two main APIs you'll use with Flow Ingester:

* &#x20;A REST API that accepts data in HTTP PUT and POST requests,&#x20;
* &#x20;A WebSocket API that accepts data streamed over WebSocket connections. Note that only captured collections may ingest data in this way; [derived collections](../catalog-reference/derivations/) can't.

When you run `flowctl develop`, the Flow Ingester listens on the supplied `--port`.

Flow Ingester always validates all documents against the collection’s [schema](../../concepts/catalog-entities/schemas-and-data-reductions.md) before writing them, so invalid data will never be added to the collection. Note that your collection schema may be as permissive as you like, and you can always apply more restrictive schemas in derivations if you want to.

Flow Ingester also reduces all documents according to the collection key and reduction annotations on the schema, if present. This optimizes the storage space for collections that see frequent updates to the same key.
