---
description: How Flow pushes collections to your endpoints using materializations
---

# Materializations

**Materializations** are the means by which Flow pushes collections into your destination **endpoints** such as **** your databases, key/value stores, and publish/subscribe systems. It is the conceptual inverse of a capture. A materialization binds a [collection](collections.md) to an endpoint and ensures new updates are reflected in that external system with very low latency.&#x20;

Once it's defined in the catalog spec, Flow continually keeps the endpoint up to date with the most current data in a collection.

![](<materializations.svg>)

Wherever applicable, materializations are indexed by the [collection key](collections.md#collection-keys). For SQL specifically, this means components of the collection key are used as the composite primary key of the table.

Many systems are document-oriented in nature and can accept unmodified collection documents. Others are table-oriented, so in order to materialize into them, you select a subset of available projections, where each projection becomes a column in the created target table.

### Endpoints

Endpoints are the systems that Flow can materialize data into or capture data from. Each capture and materialization contains information required to log in, pull from, and update the target system. You can declare all kinds of systems as endpoints, including databases, key/value stores, streaming pub/sub, Webhook APIs, and cloud storage locations.

Each materialization requires an [endpoint configuration](../../reference/catalog-reference/materialization/endpoints.md), which leverages a specific connector for the type of endpoint being used.&#x20;

### How materializations work&#x20;

When you first declare a materialization, Flow back-fills the endpoint (say, a database table) with the historical documents of the collection. From there, Flow keeps it up to date using precise, incremental updates.\
\
Flow stores updates in transactions, as quickly as the endpoint can handle them. This might be milliseconds in the case of a fast key/value store, or many minutes in the case of an OLAP warehouse.

If the endpoint is also transactional, then these transactions are integrated for end-to-end “exactly once” semantics. At a high level, transactions:

> * **Read** current documents from the data store, stream, or other location for relevant collection keys (where applicable, and not already cached by the runtime).
> * **Reduce** one or more new collection documents into each of those read values.
> * **Write** the updated document back out to the original location.

The materialization is sensitive to back pressure from the endpoint. As a database gets busy, Flow adaptively batches and combines documents to reduce the number of database operations it must issue.&#x20;

Flow's built-in efficiencies allow it to intelligently combine documents and thus consolidate updates.&#x20;

* In a given transaction, Flow turns large volumes of incoming requests into fewer table updates by reducing like keys.
* As a target database becomes busier or slower, Flow combines more documents and issues fewer updates.

Flow issues at most one store read and one store write per collection key. It then intelligently reduces updates based on those keys. This allows you to safely materialize a collection with a high rate of changes into a small database.

#### Fully reduced vs delta updates

Reductions occur automatically during the materialization process based on the [collection key](collections.md#collection-keys) and the type of endpoint. This is driven by the [connector](../../concepts/connectors.md) in use.

When you materialize into a database-like system, Flow both inserts new documents and updates existing documents by the collection's key. For example, if you have a collection with a key of `[/userId]`, then as each document is materialized, Flow queries the system for an existing record with the same `userId` and, if found, combines both the new and existing document before updating the existing record. We say that the resulting records are _fully reduced_, which means that they represent the complete up-to-date state of the record.&#x20;

When you materialize into a streaming system such as Kafka, Flow publishes each document as it is, without further reduction. We call this a _delta update._
