---
sidebar_position: 2
description: Common pain points you might have, and how Flow addresses them.
---

# Who should use Flow?

Flow is a data movement and transformation platform designed for all members of your data team. Its powerful command-line interface gives backend engineers data integration superpowers.
At the same time, Flow allows data analysts and other user cohorts to meaningfully contribute to and manage the same data pipelines, or **data flows**, using the web application.

If you answer "yes" to any of the following questions, Flow can help:

* Do you work with multiple databases and struggle to keep them in sync with one another?
* Do you issue repeated OLAP queries to your warehouse that are expensive to execute?
  * Or do you need instant metrics for specific events like Black Friday?
* Do you operate separate batch and streaming systems, and grapple with reconciling them?
* Do you manage continuous processing workflows with tools like Spark,
  Flink, or Google Cloud Dataflow, and want a faster, easier-to-evolve alternative?
* Is your organization held back by a data engineering bottleneck,
  while less-technical stakeholders are blocked from contributing by a high barrier to entry?
* Are you implementing a new data architecture framework, like a
 [distributed data mesh](https://martinfowler.com/articles/data-monolith-to-mesh.html)
  and are seeking a tool to help with orchestration?

## How Flow can help

These unique Flow features can help you solve the problems listed above.

### Fully integrated pipelines

With Flow, you can build, test, and evolve pipelines that continuously capture, transform, and materialize data across all of your systems. With one tool, you can power workflows that have historically required you to first piece together services, then integrate and operate them in-house to meet your needs.

To achieve comparable capabilities to Flow you would need:

* A low-latency streaming system, such as AWS Kinesis
* Data lake build-out, such as Kinesis Firehose to S3
* Custom ETL application development, such as Spark, Flink, or AWS λ
* Supplemental data stores for intermediate transformation states
* ETL job management and execution, such as a self-hosting or Google Cloud Dataflow
* Custom reconciliation of historical vs streaming datasets, including onerous backfills of new streaming applications from historical data

Flow dramatically simplifies this inherent complexity. It saves you time and costs, catches mistakes before they hit production, and keeps your data fresh across all the places you use it.
With both a UI-forward web application and a powerful CLI ,
more types of professionals can contribute to what would otherwise require a
highly specialized set of technical skills.

### Efficient architecture

Flow mixes a variety of architectural techniques to deliver high throughput, avoid latency, and minimize operating costs. These include:

* Leveraging [reductions](../concepts/schemas.md#reductions) to reduce the amount of data that must be ingested, stored, and processed, often dramatically
* Executing transformations predominantly in-memory
* Optimistic pipelining and vectorization of internal remote procedure calls (RPCs) and operations
* A cloud-native design that optimizes for public cloud pricing models

Flow also makes it easy to [**materialize**](../concepts/materialization.md) focused data views directly into your warehouse, so you don't need to repeatedly query the much larger source datasets. This can dramatically lower warehouse costs.

### Powerful transformations

With Flow, you can build pipelines that join a current event with an event that happened days, weeks, even years in the past. Flow can model arbitrary stream-to-stream joins without the windowing constraints imposed by other systems, which limit how far back in time you can join.

Flow transforms data in durable micro-transactions, meaning that an outcome, once committed, won't be silently re-ordered or changed due to a crash or machine failure. This makes Flow uniquely suited for operational workflows, like assigning a dynamic amount of available inventory to a stream of requests — decisions that, once made, should not be forgotten. You can also evolve transformations as business requirements change, enriching them with new datasets or behaviors without needing to re-compute from scratch.

### Data integrity

Flow is architected to ensure that your data is accurate and that changes don't break pipelines.
It supports strong schematization, durable transactions with exactly-once semantics, and easy end-to-end testing.

* Required JSON schemas ensure that only clean, consistent data is ingested into Flow or written to external systems. If a document violates its schema, Flow pauses the pipeline, giving you a chance to fix the error.
* Schemas can encode constraints, like that a latitude value must be between +90 and -90 degrees, or that a field must be a valid email address.
* Flow can project JSON schema into other flavors, like TypeScript types or SQL tables. Strong type checking catches bugs before they're applied to production.
* Flow's declarative tests verify the integrated, end-to-end behavior of data flows.

### Dynamic scaling

The Flow runtime scales from a single process up to a large Kubernetes cluster for high-volume production deployments. Processing tasks are quickly reassigned upon any machine failure for high availability.

Each process can also be scaled independently, at any time, and without downtime. This is unique to Flow. Comparable systems require that an arbitrary data partitioning be decided upfront, a crucial performance knob that's awkward and expensive to change. Instead, Flow can repeatedly [split a running task](../concepts/advanced/shards.md) into two new tasks, each half the size, without stopping it or impacting its downstream uses.