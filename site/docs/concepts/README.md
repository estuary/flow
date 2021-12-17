---
description: Understand Flow's high-level concepts
---

# High-level concepts

Flow helps you build data pipelines that connect your data systems and storage, and optionally transform data along the way. Pipelines are defined and managed through a framework known as a catalog and deployed using the flowctl CLI.&#x20;

This page provides a high-level explanation of important concepts and terminology you'll need to understand as you begin working with Flow.&#x20;

![](<architecture.png>)

### Catalogs

A **catalog** comprises all the entities that power your data pipeline: captures, collections, materializations, etc (all explained below). It is defined by a collection of YAML configurations and lambda definitions called a **catalog specification.** The catalog spec, which is usually kept in a collaborative Git repository, tells Flow how to configure catalog entities. For example:

* How to **capture** data from endpoints into **collections** with applied **schemas**
* How to **derive** collections as transformations **** of other collections
* The collections that you want Flow to **materialize** into your endpoints
* Your **tests** of documents to ingest and the outcomes that you expect

[Learn more about catalogs](catalog-entities/)

### Endpoints

**Endpoints** are the external systems that you connect using Flow. All kinds of systems can be endpoints: databases, key/value stores, streaming pub/sub systems, Webhook APIs, and cloud storage locations.&#x20;

Any catalog process that connects Flow to another system will include an endpoint in its definition. The two types of processes that do this are **captures** and **materializations**, which ingest data into Flow and push data out of Flow, respectively. In general, you'll use different types of endpoints for captures than for materializations, because data sources and data storage tend to be different types of systems.

### Collections

**Collections** are an append-only set of schematized JSON documents, which represent datasets within the Flow runtime. They are persisted in an organized cloud storage bucket, forming the intermediate stages between endpoints. Collections are a hybrid of a low-latency stream and a batch dataset, which makes them suited for just about any workflow.

Collections may be **captured**, if documents are ingested from an endpoint, or **derived** through transformations of other collections. Regardless of how data is ingested into a collection, the collection must be defined separately in the catalog spec (it's not automatically created by the capture or derivation process).

[Learn more about collections](catalog-entities/collections.md)

### Captures

**Captures** are the preferred method for ingesting data into Flow. Each capture connects an endpoint **** to a collection. Flow fetches new documents from the endpoint as they become available, validates their schema, and ingests them into the collection. Captures are generally powered by [connectors](#connectors).

[Learn more about captures](catalog-entities/captures.md)

### Materializations

**Materializations** push data from collections to destination endpoints. They are the conceptual inverse of **captures.** Through a materialization, Flow keeps the endpoint up to date using precise, incremental updates that adapt to whatever speed the endpoint can handle. Materializations are generally powered by [connectors](#connectors).

[Learn more about materializations](catalog-entities/materialization.md)

### Connectors

As discussed above, there are many places organizations work with data, which we refer to as **endpoints** when connected to Flow. Though Flow is a unified platform designed to synchronize your data wherever you need it, it's impractical for any single company — Estuary included — to provide an integration for every possible endpoint in the growing landscape of data solutions.

**Connectors** are plugin components that bridge the gap between Flow’s runtime and the various endpoints from which you capture or materialize data. Generally, each capture and materialization in your catalog is written to a connector’s configuration and will call on that connector when the data flow runs.

Crucially, connectors are built to open and compatible protocols, which allows Flow to easily integrate with new systems. Today, Flow supports open-source connectors written by Estuary and others, and it’s relatively easy to build a connector yourself.

[Learn more about connectors](connectors.md)

### Derivations

When you use Flow to transform an existing collection, the process is a **derivation**, and the result is a derived collection**.**&#x20;

Derivations are uniquely powerful transformations. You can tackle the full gamut of stateful stream workflows, including joins and aggregations, without being subject to the windowing and scaling limitations that plague other systems.&#x20;

Though simple [data reductions](#reductions) can aggregate data as part of a capture or materialization, derivations are required to significantly reshape data. Derivations are defined within the source collection's definition.

[Learn more about derivations](catalog-entities/derivations/)

### Schemas

All collections in Flow have an associated **JSON schema**, which can be as restrictive or relaxed as you'd like. Schemas ensure that bad data doesn't make its way into your collection. Flow pauses captures and derivations when documents don't match the collection schema, alerting you to the mismatch and allowing you to fix it before it creates a bigger problem.

Flow uses standard JSON schema, with one additional component: reduction annotations.&#x20;

[Learn more about schemas](catalog-entities/schemas-and-data-reductions.md)

### Storage mappings

Flow collections are a traditional cloud data lake augmented with real-time capabilities. As documents are added or updated in a collection, they are eventually stored as regular files of JSON data in your cloud storage bucket. Storage mappings define how Flow maps your various collections into appropriate user buckets and prefixes.

[Learn more about storage mappings](#undefined)

### Reductions

Data reductions are strategic aggregations based on a key. They decrease data volume prior to materialization, which improves the endpoint system performance.

Some reductions are applied automatically, such as during data capture, to improve Flow's performance. You can also add reductions to any collection's schema using a **reduction** **annotation**. This tells Flow how to combine or reduce multiple documents having the same key: for example, by summing a count, or deeply merging a map. Any data that arrives at the collection, whether through a capture or a derivation, is thus reduced.

Reduction annotations are easy to write but very powerful, and Flow leverages reductions for efficient processing and storage of your data.&#x20;

[Learn more about reductions](catalog-entities/schemas-and-data-reductions.md#reduction-annotations)

### Tests

You use **tests** to verify the integrated, end-to-end behavior of your collections and derivations. A test is a declarative sequence of ingestion or verification steps. Ingestion steps ingest one or more document fixtures into a captured collection, and verification steps assert that the contents of another derived collection match a test expectation.

Tests are written into the catalog spec, and run using flowctl.

[Learn more about tests](catalog-entities/tests.md)

### flowctl

flowctl is Flow's command-line interface. It's the only binary you need to use to test, deploy, and run Flow catalogs. flowctl is available as a Docker image. We recommend running it in a VS Code devcontainer, where you'll also be able to work with various catalog spec and schema files, and perform local tests.

[Learn more about flowctl](flowctl.md)
