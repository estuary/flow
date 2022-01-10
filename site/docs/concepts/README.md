---
description: Understand Flow's high-level concepts
---

# Concepts

Flow helps you define data pipelines that connect your data systems, APIs, and storage, and optionally transform data along the way.
Pipelines are defined within a Flow [catalog](#catalogs), and deployed to a Flow [runtime](#runtime) using the [flowctl](#flowctl) CLI.

This page provides a high-level explanation of important concepts and terminology you'll need to understand as you begin working with Flow.

![](<architecture.png>)

## Catalogs

A **catalog** comprises all the components that describe how your data pipelines function and behave:
captures, collections, derivations, materializations, tests, and more. For example:

* How to **capture** data from **endpoints** into **collections**
* The **schemas** of those collections, which Flow enforces
* How to **derive** collections as transformations of other source collections
* **Materializations** of collections into your endpoints
* Your **tests** of schema and derivation behaviors

Together the captures, collections, derivations, and materializations of your catalog
form a graph of your data flows:

import Mermaid from '@theme/Mermaid';

<Mermaid chart={`
	graph LR;
		capture/two-->collection/D;
		capture/one-->collection/C;
		capture/one-->collection/A;
        collection/A-->derivation/B;
        collection/D-->derivation/E;
        collection/C-->derivation/E;
        derivation/B-->derivation/E;
		collection/D-->materialization/one;
		derivation/E-->materialization/two;
`}/>

### Namespace

All catalog entities, like collections, are identified by a **name**
such as `acmeCo/teams/manufacturing/anvils`. Names have directory-like
prefixes and every name within Flow is globally unique.

Thus all Catalog entities exist together in a single **namespace**,
much like how all files in S3 are uniquely identified by their bucket and file name.

:::note
Prefixes of the namespace, like `acmeCo/teams/manufacturing/`,
are the foundation for Flow's authorization model.

If you've ever used database schemas to organize your tables and authorize access,
you can think of name prefixes as being akin to database schemas with arbitrary nesting.
:::

### Builds

Catalog entities like collections are very long lived and may evolve over time.
A collection's schema might be extended with new fields,
or a transformation might be updated with a bug fix.

When one or more catalog entities are updated,
a catalog **build** validates their definitions and prepares them for execution by Flow's runtime.
Every build is assigned a unique identifier called a **build ID**,
and the build ID is used to reconcile which version of a catalog entity
is being executed by the runtime.

A catalog build is **activated** into Flow's runtime to deploy its captures, collections, and so on,
possibly replacing an older build under which they had been running.

### Specifications

A catalog build begins from a set of **catalog specifications**
which define the behavior of your catalog: its captures, derivations, tests, and more.

You define catalog specifications using either Flow's interactive UI,
or by directly creating and editing YAML or JSON files which are typically managed
in a Git repository using familiar developer workflows (often called "GitOps").
Flow integrates with VSCode for development environment support, like auto-complete,
tooltips, and inline documentation.

Whether you use the UI or Git-managed specifications is up to you,
and teams can switch back and forth depending on what's more familiar.

:::note
Flow's UI is under rapid development and expected to be generally available by end of Q1 2022.
:::

[Learn more about catalog specifications](catalog-entities/)

***

## Collections

**Collections** are a collection of documents having a common **key** and [schema](#schemas).
They are the fundamental representation for datasets within Flow, much like a database table.

They are best described as a real-time data lake:
documents are stored as an organized layout of JSON files in your cloud storage bucket.
If Flow needs to read historical data -- say, as part of creating a new materialization --
it does so by reading from your bucket.
You can use regular bucket lifecycle policies to manage the deletion of data from a collection.
However, capturing _into_ a collection or materializing _from_ a collection happens within milliseconds.

[Learn more about collections](catalog-entities/collections.md)

### Journals

**Journals** provide the low-level storage for Flow collections.
Each logical and physical partition of a collection is backed by a journal.

Task [shards](#task-shards) also use journals to provide for their durability
and fault tolerance.
Each shard has an associated **recovery log**, which is a journal into which
internal checkpoint states are written.

[Learn more about journals](journals.md)

***

## Captures

A **capture** is a catalog task which connects to an endpoint
and binds one or more of its resources to collections.
As documents become available for any of the bindings,
Flow validates their schema and adds them to their bound collection.

There are two categories of captures:
 * _Pull_ captures which pull documents from an endpoint using a [connector](#connectors).
 * _Push_ captures which expose an URL endpoint which can be directly written into, such as via a Webhook POST.

:::caution
Push captures are under development.
:::

[Learn more about captures](catalog-entities/captures.md)

***

## Materializations

A **materialization** is a catalog task which connects to an endpoint
and binds one or more collections to corresponding endpoint resources.
They are the conceptual inverse of **captures.**

As documents become available within bound collections, the materialization
keeps endpoint resources (like database tables) up to date using precise,
incremental updates.
Like captures, materializations are powered by [connectors](#connectors).

[Learn more about materializations](catalog-entities/materialization.md)

***

## Derivations

A **derivation** is a collection which continuously
derives its documents from transformations that are applied
to one or more source collections.

Derivations can be used to map, reshape, and filter documents.
They can also be used to tackle complex stateful streaming workflows,
including joins and aggregations,
and are not subject to the windowing and scaling limitations that are common to other systems.

[Learn more about derivations](catalog-entities/derivations/)

***

## Schemas

All collections in Flow have an associated
[JSON schema](https://json-schema.org/understanding-json-schema/)
against which documents are validated every time they're written or read.
Schemas are core to how Flow ensures the integrity of your data.
Flow validates your documents to ensure that
bad data doesn't make it into your collections -- or worse,
into downstream data products!

Flow pauses catalog tasks when documents don't match the collection schema,
alerting you to the mismatch and allowing you to fix it before it creates a bigger problem.

### Constraints

JSON schema is a flexible standard for representing structure, invariants,
and other constraints over your documents.

Schemas can be very permissive, or highly exacting, or somewhere in between.
JSON schema goes far beyond checking basic document structure,
to also support conditionals and invariants like
"I expect all items in this array to be unique",
or "this string must be an email",
or "this integer must be between a multiple of 10 and in the range 0-100".

### Projections

Flow leverages your JSON schemas to produce other types of schemas as-needed,
such as TypeScript types and SQL `CREATE TABLE` statements.

In many cases these projections provide comprehensive end-to-end type safety
of Flow catalogs and their TypeScript transformations, all statically verified
when the catalog is built.

### Reductions

Flow [collections](#collections) have a defined **key**, which is akin to
a database primary key declaration and determines how documents of the
collection are grouped.
When a collection is materialized into a database table, its key becomes
the SQL primary key of the materialized table.

This of course raises the question: what happens if _multiple_ documents
of a given key are added to a collection?
You might expect that the last-written document is the effective document for
that key. This "last write wins" treatment is how comparable systems behave,
and is also Flow's default.

Flow also offers schema **extensions**
that give you substantially more control over how documents are combined and reduced.
`reduce` annotations let you deeply merge documents, maintain running counts,
and achieve other complex aggregation behaviors.

### Key Strategies

Reduction annotations change the common patterns for how you think about collection keys.

Suppose you are building a reporting fact table over events of your business.
Today you would commonly consider a unique event ID to be its natural key.
You would load all events into your warehouse and perform query-time aggregation.
When that becomes too slow, you periodically refresh materialized views for fast-but-stale queries.

With Flow, you instead use a collection key of your _fact table dimensions_,
and use `reduce` annotations to define your metric aggregations.
A materialization of the collection then maintains a
database table which is keyed on your dimensions,
so that queries are both fast _and_ up to date.

[Learn more about schemas](catalog-entities/schemas-and-data-reductions.md)

***

## Tasks

Captures, derivations, and materializations are collectively referred to as catalog **tasks**.
They are the "active" components of a catalog, each running continuously and reacting to documents
as they become available.

Collections, by way of comparison, are inert. They reflect data at rest, and are acted upon by
catalog tasks:

* A capture adds documents to a collection pulled from an endpoint.
* A derivation updates a collection by applying transformations to other source collections.
* A materialization reacts to changes of a collection to update an endpoint.

### Task Shards

Task **shards** are the unit of execution for a catalog [task](#tasks).
A single task can have many shards, which allow the task to scale across
many machines to achieve more throughput and parallelism.

Shards are created and managed by the Flow runtime.
Each shard represents a slice of the overall work of the catalog task,
including its processing status and associated internal checkpoints.
Catalog tasks are created with a single shard,
which can be repeatedly subdivided at any time — with no downtime — to
increase the processing capacity of the task.

[Learn more about shards](shards.md)

***

## Endpoints

**Endpoints** are the external systems that you connect using Flow.
All kinds of systems can be endpoints: databases, key/value stores, streaming pub/sub systems, SaaS APIs, and cloud storage locations.

[Captures](#captures) pull or ingest data _from_ an endpoint, while [materializations](#materializations) push data _into_ an endpoint.
There's an essentially unbounded number of different systems and APIs to which Flow might need to capture or materialize data.
Rather than attempt to directly integrate them all, Flow's runtime communicates with endpoints through plugin [connectors](#connectors).

### Resources

An endpoint **resource** is an addressable collection of data within an endpoint.
The exact meaning of a resource is up to the endpoint and its connector. For example:

* Resources of a database endpoint might be its individual tables.
* Resources of a Kafka cluster might be its topics.
* Resources of a SaaS connector might be its various API feeds.

### Connectors

There are _lots_ of potential endpoints where you want to work with data.
Though Flow is a unified platform for data synchronization,
it's impractical for any single company — Estuary included — to provide an integration for every
possible endpoint in the growing landscape of data solutions.

**Connectors** are plugin components that bridge the gap between Flow’s runtime and
the various endpoints from which you capture or materialize data.
They're packaged as Docker images, each encapsulating the details of working with
a particular kind of endpoint.

The connector then interacts with Flow's runtime through common and open protocols
for configuration, introspection of endpoint resources, and to coordinate the
movement of data into and out of the endpoint.

Crucially, this means Flow doesn't need to know about new types of endpoint ahead of time:
so long as a connector is available Flow can work with the endpoint, and it's
relatively easy to build a connector yourself.

### Discovery

Connectors offer **discovery** APIs for understanding how a connector
should be configured, and what resources are available within an endpoint.

Flow works with connector APIs to provide a guided discovery workflow
which makes it easy to configure the connector, and select from a menu
of available endpoint resources you can capture.

[Learn more about endpoints and connectors](connectors.md)

***

## Tests

You use **tests** to verify the end-to-end behavior of your collections and derivations.
A test is a sequence of ingestion or verification steps.
Ingestion steps ingest one or more document fixtures into a collection,
and verification steps assert that the contents of another derived collection match a test expectation.

[Learn more about tests](catalog-entities/tests.md)

***

## Storage Mappings

Flow [collections](#collections) use cloud storage buckets for the durable storage of data.
Storage mappings define how Flow maps your various collections into your storage buckets and prefixes.

[Learn more about storage mappings](#undefined)

***

## flowctl

flowctl is Flow's command-line interface.
It can be used to develop and test Flow catalogs, and to deploy them into a Flow runtime.

[Learn more about flowctl](flowctl.md)