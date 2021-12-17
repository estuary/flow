---
description: How Flow uses captures to pull data from external sources
---

# Captures

Every Flow pipeline needs to access external data. A **capture** is a binding between an external endpoint and a [collection](collections.md), which continuously pulls data from the endpoint to the collection. This means that once you initially define the capture in the catalog spec, you don't need to write any code or manage any extra tasks to continue ingesting data.

![](<captures.svg>)

Any collection that is bound in this way is referred to as a **captured collection**_,_ which means the data is pulled from some external source.&#x20;

### Endpoints

Endpoints are the systems that Flow can materialize data into or capture data from. Each capture and materialization contains information required to log in, pull from, and update the target system. You can declare all kinds of systems as endpoints, including databases, key/value stores, streaming pub/sub, Webhook APIs, and cloud storage locations.

Each capture requires an [endpoint configuration](../../reference/catalog-reference/captures/endpoint-configurations.md), which leverages a specific connector for the type of endpoint being used.&#x20;

### Other ingestion methods

Data can also be pushed into collections via [REST or Websocket endpoints](../../reference/pushing-data-into-flow/). This is useful for integrating with applications as a webhook, for ingesting large files from the command line, testing, or for any other process that you want to manage outside of Flow.

Regardless of the ingestion method, Flow adds data to collections in transactions. For example, you can use the JSON REST API to ingest multiple documents to multiple collections within a single transaction. If a fault occurs, or a document fails to validate against its collection schema, the transaction is rolled back in its entirety. This means simple retries upon error can ensure Flow has an entirely correct worldview.&#x20;
