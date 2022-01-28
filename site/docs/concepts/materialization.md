---
sidebar_position: 7
---
# Materializations

**Materializations** are the means by which Flow pushes collections into your destination **endpoints**:
databases, key/value stores, publish/subscribe systems, and more.
A materialization binds one or more [collections](collections.md) to specific resources
within the endpoint, such as database tables,
into which the collections are continuously materialized.
As documents are added to bound collections, the materialization ensures
that each document is reflected in the endpoint resource with very low latency.
Materializations are the conceptual inverse of [captures](captures.md).

![](<materializations.svg>)

## Specification

Materializations are expressed within a Flow catalog specification:

```yaml
# A set of materializations to include in the catalog.
# Optional, type: object
materializations:
  # The name of the materialization.
  acmeCo/example/database-views:
    # Endpoint defines how to connect to the destination of the materialization.
    # Required, type: object
    endpoint:
      # This endpoint uses a connector provided as a Docker image.
      connector:
        # Docker image which implements the materialization connector.
        image: ghcr.io/estuary/materialize-postgres:dev
        # File which provides the connector's required configuration.
        # Configuration may also be presented inline.
        config: path/to/connector-config.yaml

    # Bindings define how one or more collections map to materialized endpoint resources.
    # A single materialization may include many collections and endpoint resources,
    # each defined as a separate binding.
    # Required, type: object
    bindings:
      - # The source collection to materialize.
        # This may be defined in a separate, imported catalog source file.
        # Required, type: string
        source: acmeCo/example/collection

        # The resource is additional configuration required by the endpoint
        # connector to identify and materialize a specific endpoint resource.
        # The structure and meaning of this configuration is defined by
        # the specific connector.
        # Required, type: object
        resource:
          # The materialize-postgres connector expects a `table` key
          # which names a table to materialize into.
          table: example_table
```

## Continuous materialized views

Flow materializations are **continuous materialized views**.
They maintain a representation of the collection within the endpoint system
as a resource that is updated in near real-time. It's indexed on the
[collection key](collections.md#collection-keys).
As the materialization runs, it ensures that all collection documents
and their accumulated [reductions](../#reductions) are reflected in this
managed endpoint resource.
For example, consider a collection and its materialization:

```yaml

collections:
  acmeCo/colors:
    key: [/color]
    schema:
      type: object
      required: [color, total]
      reduce: {strategy: merge}
      properties:
        color: {enum: [red, blue, purple]}
        total:
          type: integer
          reduce: {strategy: sum}

materializations:
  acmeCo/example/database-views:
    endpoint: ...
    bindings:
      - source: acmeCo/colors
        resource: { table: colors }
```

Suppose documents are periodically added to the collection:
```json
{"color": "red", "total": 1}
{"color": "blue", "total": 2}
{"color": "blue", "total": 3}
```

Its materialization into a database table will have a single row for each unique color.
As documents arrive in the collection, the row `total` is updated within the
materialized table so that it reflects the overall count:

![](materialization.gif)

When you first declare a materialization,
Flow back-fills the endpoint resource with the historical documents of the collection.
Once caught up, Flow applies new collection documents using incremental and low-latency updates.

As collection documents arrive, Flow:

* **Reads** previously materialized documents from the endpoint for the relevant keys
* **Reduces** new documents into these read documents
* **Writes** updated documents back into the endpoint resource, indexed by their keys

Flow does _not_ keep separate internal copies of collection or reduction states,
as some other systems do. The endpoint resource is the one and only place
where state "lives" within a materialization. This makes materializations very
efficient and scalable to operate. They are able to maintain _very_ large tables
stored in highly scaled storage systems like OLAP warehouses, BigTable, or DynamoDB.

## Projected fields

Many systems are document-oriented and can directly work
with collections of JSON documents.
Others systems are table-oriented and require an up-front declaration
of columns and types to be most useful, such as a SQL `CREATE TABLE` definition.

Flow uses collection [projections](projections.md) to relate locations within
a hierarchical JSON document to equivalent named fields.
A materialization can in turn select a subset of available projected fields
where, for example, each field becomes a column in a SQL table created by
the connector.

It would be tedious to explicitly list projections for every materialization,
though you certainly can if desired.
Instead, Flow and the materialization connector _negotiate_ a recommended field selection
on your behalf, which can be fine-tuned.
For example, a SQL database connector will typically *require* that fields
comprising the primary key be included, and will *recommend* that scalar
values be included, but will by default exclude document locations that
don't have native SQL representations, such as locations which can have
multiple JSON types or are arrays or maps.

```yaml
materializations:
  acmeCo/example/database-views:
    endpoint: ...
    bindings:
      - source: acmeCo/example/collection
        resource: { table: example_table }

        # Select (or exclude) projections of the collection for materialization as fields.
        # If not provided, the recommend fields of the endpoint connector are used.
        # Optional, type: object
        fields:
          # Whether to include fields that are recommended by the endpoint connector.
          # If false, then fields can still be added using `include`.
          # Required, type: boolean
          recommended: true

          # Fields to exclude. This is useful for deselecting a subset of recommended fields.
          # Default: [], type: array
          exclude: [myField, otherField]

          # Fields to include. This can supplement recommended fields, or can
          # designate explicit fields to use if recommended fields are disabled.
          #
          # Values of this map are used to customize connector behavior on a per-field basis.
          # They are passed directly to the connector and are not interpreted by Flow.
          # Consult your connector's documentation for details of what customizations are available.
          # This is an advanced feature and is not commonly used.
          #
          # default: {}, type: object
          include:  {goodField: {}, greatField: {}}
```

## Partition selectors

Partition selectors let you materialize only a subset of a collection that has
[logical partitions](projections.md#logical-partitions).
For example, you might have a large collection that is logically partitioned
on each of your customers:

```yaml
collections:
  acmeCo/anvil/orders:
    key: [/id]
    schema: orders.schema.yaml
    projections:
      customer:
        location: /order/customer
        partition: true
```

A large customer asks if you can provide an up-to-date accounting of their orders.
This can be accomplished with a partition selector:

```yaml
materializations:
  acmeCo/example/database-views:
    endpoint: ...
    bindings:
      - source: acmeCo/anvil/orders
        resource: { table: coyote_orders }

        # Process partitions where "Coyote" is the customer.
        partitions:
          include:
            customer: [Coyote]
```

[Learn more about partition selectors](projections.md#partition-selectors).

## SQLite endpoint

In addition to materialization connectors, Flow offers a built-in SQLite endpoint
for local testing and development. SQLite is not suitable for materializations
running within a managed data plane.

```yaml
materializations:
  acmeCo/example/database-views:
    endpoint:
      # A SQLite endpoint is specified using `sqlite` instead of `connector`.
      sqlite:
        # The SQLite endpoint requires the `path` of the SQLite database to use,
        # specified as a file path. It may include URI query parameters;
        # See: https://www.sqlite.org/uri.html and https://github.com/mattn/go-sqlite3#connection-string
        path: example/database.sqlite?_journal_mode=WAL
```

## Backpressure

Flow processes updates in transactions, as quickly as the endpoint can handle them.
This might be milliseconds in the case of a fast key/value store,
or many minutes in the case of an OLAP warehouse.

If the endpoint is also transactional, Flow integrates its internal transactions
with those of the endpoint for integrated end-to-end “exactly once” semantics.

The materialization is sensitive to back pressure from the endpoint.
As a database gets busy, Flow adaptively batches and combines documents to consolidate updates:

* In a given transaction, Flow reduces all incoming documents on the collection key.
  Multiple documents combine and result in a single endpoint read and write during the transaction.
* As a target database becomes busier or slower, transactions become larger.
  Flow does more reduction work within each transaction, and each endpoint read or write
  accounts for an increasing volume of collection documents.

This allows you to safely materialize a collection with a high rate of changes into a small database,
so long as the cardinality of the materialization is of reasonable size.

## Delta updates

Not all endpoints are stateful systems, like a database.
Webhooks, APIs, and Pub/Sub systems may also be endpoints, but none of these
typically provide a state representation that Flow can query.
They are write-only in nature, and Flow cannot use their endpoint state
to help it fully reduce collection documents on their keys.

For this class of endpoint, Flow offers a **delta-updates** mode.
When using delta updates, Flow does not attempt to maintain
full reductions of each unique collection key.
Instead, Flow locally reduces documents within each transaction
(this is often called a "combine"), and then materializes one
_delta_ document per key to the endpoint.

Flow and the specific endpoint connector internally negotiate to determine
whether delta updates should be used. The specific connector may
provide configurable settings which can be used to fine-tune this behavior.