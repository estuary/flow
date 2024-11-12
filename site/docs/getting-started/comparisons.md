---
sidebar_position: 4
description: High level explanations of Flow in terms of the systems you already know
---

# Comparisons

Because Flow combines many functionalities, it's related to many types of data systems. Choose a familiar system from the list below to jump to an explanation of how it compares with Flow (or how you can use the two together).

* [Apache Beam and Google Cloud Dataflow](comparisons.md#apache-beam-and-google-cloud-dataflow)
* [Kafka](comparisons.md#kafka)
* [Spark](comparisons.md#spark)
* [Hadoop, HDFS, and Hive](comparisons.md#hadoop-hdfs-and-hive)
* [Fivetran, Airbyte, and other ELT solutions](comparisons.md#fivetran-airbyte-and-other-elt-solutions)
* [dbt](comparisons.md#dbt)
* [Materialize, Rockset, ksqlDB, and other realtime databases](comparisons.md#materialize-rockset-ksqldb-and-other-real-time-databases)
* [Snowflake, BigQuery, and other OLAP databases](comparisons.md#snowflake-bigquery-and-other-olap-databases)

## Apache Beam and Google Cloud Dataflow

Flow’s most apt comparison is to Apache Beam. You may use a variety of runners (processing engines) for your Beam deployment. One of the most popular, Google Cloud Dataflow, is a more robust redistribution under an additional SDK. Regardless of how you use Beam, there’s a lot of conceptual overlap with Flow. This makes Beam and Flow alternatives rather than complementary technologies, but there are key differences.

Like Beam, Flow’s primary primitive is a **collection**. You build a processing graph (called a **pipeline** in Beam and a **Data Flow** in Flow) by relating multiple collections together through procedural transformations, or lambdas. As with Beam, Flow’s runtime performs automatic data shuffles and is designed to allow fully automatic scaling. Also like Beam, collections have associated schemas.

Unlike Beam, Flow doesn’t distinguish between batch and streaming contexts. Flow unifies these paradigms under a single collection concept, allowing you to seamlessly work with both data types.

Also, while Beam allows you the option to define combine operators, Flow’s runtime always applies combine operators. These are built using the declared semantics of the document’s schema, which makes it much more efficient and cost-effective to work with streaming data.

Finally, Flow allows stateful stream-to-stream joins without the windowing semantics imposed by Beam. Notably, Flow’s modeling of state – via its per-key **register** concept – is substantially more powerful than Beam's per-key-and-window model. For example, registers can trivially model the cumulative lifetime value of a customer.

## Kafka

Flow inhabits a different space than Kafka does by itself. Kafka is an infrastructure that supports streaming applications running elsewhere. Flow is an opinionated framework for working with real-time data. You might think of Flow as an analog to an opinionated bundling of several important features from the broader Kafka ecosystem.

Flow is built on [Gazette](https://gazette.readthedocs.io/en/latest/), a highly-scalable streaming broker similar to log-oriented pub/sub systems. Thus, Kafka is more directly comparable to Gazette. Flow also uses Gazette’s consumer framework, which has similarities to Kafka **consumers**. Both manage scale-out execution contexts for consumer tasks, offer durable local task stores, and provide exactly-once semantics.

[Journals](../concepts/advanced/journals.md) in Gazette and Flow are roughly analogous to Kafka **partitions**. Each journal is a single append-only log. Gazette has no native notion of a **topic**, but instead supports label-based selection of subsets of journals, which tends to be more flexible. Gazette journals store data in contiguous chunks called **fragments**, which typically live in cloud storage. Each journal can have its own separate storage configuration, which Flow leverages to allow users to bring their own cloud storage buckets. Another unique feature of Gazette is its ability to serve reads of historical data by providing clients with pre-signed cloud storage URLs, which enables it to serve many readers very efficiently.

Generally, Flow users don't need to know or care much about Gazette and its architecture, since Flow provides a higher-level interface over groups of journals, called **collections**.

Flow [collections](../concepts/collections.md) are somewhat similar to Kafka **streams**, but with some important differences. Collections always store JSON and must have an associated JSON schema. Collections also support automatic logical and physical partitioning. Each collection is backed by one or more journals, depending on the partitioning.

Flow [tasks](../concepts/README.md#tasks) are most similar to Kafka **stream processors**, but are more opinionated. Tasks fall into one of three categories: captures, derivations, and materializations. Tasks may also have more than one process, which Flow calls [**shards**](../concepts/advanced/shards.md), to allow for parallel processing. Tasks and shards are fully managed by Flow. This includes transactional state management and zero-downtime splitting of shards, which enables turnkey scaling.

See how Flow compares to popular stream processing platforms that use Kafka:

* [Flow vs Confluent feature and pricing breakdown](https://estuary.dev/vs-confluent/)
* [Flow vs Debezium feature and pricing breakdown](https://estuary.dev/vs-debezium/)

## Spark

Spark can be described as a batch engine with stream processing add-ons, where Flow is fundamentally a streaming system that is able to easily integrate with batch systems.

You can think of a Flow **collection** as a set of RDDs with common associated metadata. In Spark, you can save an RDD to a variety of external systems, like cloud storage or a database. Likewise, you can load from a variety of external systems to create an RDD. Finally, you can transform one RDD into another. You use Flow collections in a similar manner. They represent a logical dataset, which you can **materialize** to push the data into some external system like cloud storage or a database. You can also create a collection that is **derived** by applying stateful transformations to one or more source collections.

Unlike Spark RDDs, Flow collections are backed by one or more unbounded append-only logs. Therefore, you don't create a new collection each time data arrives; you simply append to the existing one. Collections can be partitioned and can support extremely large volumes of data.

Spark's processing primitives, **applications**, **jobs**, and **tasks**, don't translate perfectly to Flow, but we can make some useful analogies. This is partly because Spark is not very opinionated about what an application does. Your Spark application could read data from cloud storage, then transform it, then write the results out to a database. The closest analog to a Spark application in Flow is the **Data Flow**. A Data Flow is a composition of Flow tasks, which are quite different from tasks in Spark.

In Flow, a task is a logical unit of work that does _one_ of capture (ingest), derive (transform), or materialize (write results to an external system). What Spark calls a task is actually closer to a Flow **shard**. In Flow, a task is a logical unit of work, and [shards](../concepts/advanced/shards.md) represent the potentially numerous processes that actually carry out that work. Shards are the unit of parallelism in Flow, and you can easily split them for turnkey scaling.

Composing Flow tasks is also a little different than composing Spark jobs. Flow tasks always produce and/or consume data in collections, instead of piping data directly from one shard to another. This is because every task in Flow is transactional and, to the greatest degree possible, fault-tolerant. This design also affords painless backfills of historical data when you want to add new transformations or materializations.

## Hadoop, HDFS, and Hive

There are many different ways to use Hadoop, HDFS, and the ecosystem of related projects, several of which are useful comparisons to Flow.

To gain an understanding of Flow's processing model for derivations, see [this blog post about MapReduce in Flow](https://www.estuary.dev/why-mapreduce-is-making-a-comeback/).

HDFS is sometimes used as a system of record for analytics data, typically paired with an orchestration system for analytics jobs. If you do this, you likely export datasets from your source systems into HDFS. Then, you use some other tool to coordinate running various MapReduce jobs, often indirectly through systems like Hive.

For this use case, the best way of describing Flow is that it completely changes the paradigm. In Flow, you always append data to existing **collections**, rather than creating a new one each time a job is run. In fact, Flow has no notion of a **job** like there is in Hadoop. Flow tasks run continuously and everything stays up to date in real time, so there's never a need for outside orchestration or coordination. Put simply, Flow collections are log-like, and files in HDFS typically store table-like data. [This blog post](https://www.estuary.dev/the-power-and-implications-of-data-materialization/) explores those differences in greater depth.

To make this more concrete, imagine a hypothetical example of a workflow in the Hadoop world where you export data from a source system, perform some transformations, and then run some Hive queries.

In Flow, you instead define a **capture** of data from the source, which runs continuously and keeps a collection up to date with the latest data from the source. Then you transform the data with Flow **derivations**, which again apply the transformations incrementally and in real time. While you _could_ actually use tools like Hive to directly query data from Flow collections — the layout of collection data in cloud storage is intentionally compatible with this — you could also **materialize** a view of your transformation results to any database, which is also kept up to date in real time.

## Fivetran, Airbyte, and other ELT solutions

Tools like Fivetran and Airbyte are purpose-built to move data from one place to another. These ELT tools typically model sources and destinations, and run regularly scheduled jobs to export from the source directly to the destination. Flow models things differently. Instead of modeling the world in terms of independent scheduled jobs that copy data from source to destination, Data Flows model a directed graph of
[**captures**](../../concepts/captures) (reads from sources),
[**derivations**](../../concepts/derivations) (transforms), and
[**materializations**](../../concepts/materialization) (writes to destinations).
Collectively, these are called _tasks_.

Tasks in Flow are only indirectly linked. Captures read data from a source and output to **collections**. Flow collections store all the data in cloud storage, with configurable retention for historical data. You can then materialize each collection to any number of destination systems. Each one will be kept up to date in real time, and new materializations can automatically backfill all your historical data. Collections in Flow always have an associated JSON schema, and they use that to ensure the validity of all collection data. Tasks are also transactional and generally guarantee end-to-end exactly-once processing (so long as the endpoint system can accommodate them).

Like Airbyte, Flow uses [connectors](../concepts/connectors.md) for interacting with external systems in captures and materializations. For captures,
Flow integrates the Airbyte specification,
so all Airbyte source connectors can be used with Flow.
For materializations, Flow uses its own protocol which is not compatible with the Airbyte spec.
In either case, the usage of connectors is pretty similar.

In terms of technical capabilities, Flow can do everything that these tools can and more.
Both Fivetran and Airbyte both currently have graphical interfaces that make them much easier for
non-technical users to configure. Flow, too, is focused on empowering non-technical users through its web application.
At the same time, it Flow offers declarative YAML for configuration, which works excellently in a GitOps workflow.

[Flow vs Fivetran feature and pricing breakdown.](https://estuary.dev/vs-fivetran/)


## dbt

dbt is a tool that enables data analysts and engineers to transform data in their warehouses more effectively.

In addition to – and perhaps more important than – its transform capability, dbt brought an entirely new workflow for working with data:
one that prioritizes version control, testing, local development, documentation, composition, and re-use.

Like dbt, Flow uses a declarative model and tooling, but the similarities end there. dbt is a tool for defining transformations, which are executed within your analytics warehouse.
Flow is a tool for delivering data to that warehouse, as well as continuous operational transforms that are applied everywhere else.

These two tools can make lots of sense to use together. First, Flow brings timely, accurate data to the warehouse.
Within the warehouse, analysts can use tools like dbt to explore the data. The Flow pipeline is then ideally suited to
productionize important insights as materialized views or by pushing to another destination.

Put another way, Flow is a complete ELT platform, but you might choose to perform and manage more complex transformations in
a separate, dedicated tool like dbt. While Flow and dbt don’t interact directly, both offer easy integration through your data warehouse.

## Materialize, Rockset, ksqlDB, and other real-time databases

Modern real-time databases like Materialize, Rockset, and ksqlDB consume streams of data, oftentimes from Kafka brokers,
and can keep SQL views up to date in real time.

These real-time databases have a lot of conceptual overlap with Flow. The biggest difference is that Flow can materialize this same type of incrementally updated view into any database, regardless of whether that database has real-time capabilities or not.&#x20;

However, this doesn't mean that Flow should  _replace_ these systems in your stack. In fact, it can be optimal to use Flow to feed data into them.
Flow adds real-time data capture and materialization options that many real-time databases don't support.
Once data has arrived in the database, you have access to real-time SQL analysis and other analytical tools not native to Flow.
For further explanation, read the section below on OLAP databases.

## Snowflake, BigQuery, and other OLAP databases

Flow differs from OLAP databases mainly in that it's not a database. Flow has no query interface, and no plans to add one. Instead, Flow allows you to use the query interfaces of any database by **materializing** views into it.

Flow is similar to OLAP databases in that it can be the source of truth for all analytics data (though it's also capable enough to handle operational workloads). Instead of schemas and tables, Flow defines **collections**. These collections are conceptually similar to database tables in the sense that they are containers for data with an associated (primary) key. Under the hood, Flow collections are each backed by append-only logs, where each document in the log represents a delta update for a given key.

Collections can be easily materialized into a variety of external systems, such as Snowflake or BigQuery. This creates a table in your OLAP database that is continuously kept up to date with the collection. With Flow, there's no need to schedule exports to these systems, and thus no need to orchestrate the timing of those exports. You can also materialize a given collection into multiple destination systems, so you can always use whichever system is best for the type of queries you want to run.

Like Snowflake, Flow uses inexpensive cloud storage for all collection data. It even lets you bring your own storage bucket, so you're always in control. Unlike data warehouses, Flow is able to directly capture data from source systems, and continuously and incrementally keep everything up to date.

A common pattern is to use Flow to capture data from multiple different sources and materialize it into a data warehouse. Flow can also help you avoid expenses associated with queries you frequently pull from a data warehouse by keeping an up-to-date view of them where you want it. Because of Flow’s exactly-once processing guarantees, these materialized views are always correct, consistent, and fault-tolerant.
