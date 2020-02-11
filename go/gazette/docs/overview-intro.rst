.. image:: /_static/logo_with_text.svg
   :alt: Gazette Logo

Overview
=========

.. image:: https://circleci.com/gh/gazette/core.svg?style=svg
   :target: https://circleci.com/gh/gazette/core
   :alt: CircleCI
.. image:: https://godoc.org/go.gazette.dev/core?status.svg
   :target: https://godoc.org/go.gazette.dev/core
   :alt: GoDoc
.. image:: https://img.shields.io/badge/slack-@gazette/dev-yellow.svg?logo=slack
   :target: https://join.slack.com/t/gazette-dev/shared_invite/enQtNjQxMzgyNTEzNzk1LTU0ZjZlZmY5ODdkOTEzZDQzZWU5OTk3ZTgyNjY1ZDE1M2U1ZTViMWQxMThiMjU1N2MwOTlhMmVjYjEzMjEwMGQ
   :alt: Slack
.. image:: https://goreportcard.com/badge/github.com/gazette/core
   :target: https://goreportcard.com/report/github.com/gazette/core
   :alt: Go Report Card
.. raw:: html

   <iframe style="margin-bottom: 6px" src="https://ghbtns.com/github-btn.html?user=gazette&repo=core&type=star&count=true" frameborder="0" scrolling="0" width="170px" height="20px"></iframe>

Many organizations have adopted a streaming "spine" for event capture and movement,
but are then challenged by the realization that either they must map all of their
business processes to a streaming paradigm *or* they must own a patchwork of pipelines
and connectors that move events between the spine, data lakes, and SQL warehouses.

These patchworks often lead to non-ideal outcomes:

- Flowing events into a data lake for long-term retention and processing
  using preferred tools, but giving up capabilities for low-latency streaming.

- Streaming events into a managed, source-of-truth SQL warehouse for ease of
  ad-hoc queries and scheduled rollups, but losing a capability to stream back
  out and assuming risks from added cost and vendor lock.

- Stuffing business logic best served by a daily SQL query into a streaming
  paradigm that requires engineering resources to implement and operate.

Gazette makes it easy to build platforms that flexibly mix *SQL*, *batch*,
and *millisecond-latency streaming* processing paradigms. It enables teams,
applications, and analysts to work from a common catalog of data in the way
that's most convenient **to them**. Gazette's core abstraction is a "journal"
-- a streaming append log that's represented using regular files in a BLOB
store (i.e., S3).

The magic of this representation is that journals are simultaneously a
low-latency data stream *and* a collection of immutable, organized files
in cloud storage (aka, a data lake). Append records to a Gazette journal, and:

- Stream them in real-time to filter, forward, aggregate, index, and join.
- Consume them as a Hive-partitioned table via Spark, Flink, etc.
- Query in SQL using Snowflake, BigQuery, or Presto (with predicate push-down).

Atop the journal *broker service*, Gazette offers a powerful *consumers
framework* for building streaming applications in Go. Gazette has served
production use cases for nearly five years, with deployments scaled to
millions of streamed records per second.

Broker Features
----------------

Millisecond-latency, serializable publish/subscribe.
   Journals are a powerful primitive for building low-latency *streaming platforms*
   composed of loosely coupled services -- built and operated by distinct teams --
   that all communicate continuously through a common catalog of data streams.

Delegated storage via S3 (or other BLOB store).
   Consume journals as real-time streams *or* with any tool that understands
   files in cloud storage.

   Structure collections of journals as a partitioned data lake.
   Mount journals as external tables in a Hive metastore or in SQL warehouses
   like BigQuery, Snowflake, and Presto.

System of record.
   Represent *all* captured records as an immutable, verifiable log.
   Apply retention policies driven by your organizational requirements
   -- not your available disk space.

   Start consumers that materialize views from months or even years of historical
   data, and then seamlessly transition to real-time tailing reads.

Deploy as disposable containers.
   Clusters scale and recover from faults in seconds, with no data migrations involved.

   Brokers can stage recent writes to local_ SSDs_, providing
   substantial performance benefits and cost savings over network-attached
   persistent disks.

I/O Isolation.
   Provision only enough capacity to serve incoming appends and real-time readers.
   A small cluster can easily serve high-throughput historical reads
   by delegating IO to S3.

Durable.
   Journal replicas always span availability zones.
   Every client append requires acknowledgements from *all* replicas, full stop.

   Brokers leverage pipelining and transaction reuse to keep overhead low.

Multi-cloud, world-wide scale.
   Deploy a global broker cluster with journals homed to specific clouds,
   regions, and zones (coming soon).

   Or deploy region-specific clusters, with cross-region reads
   coordinated through nothing but an S3 bucket.

   Leverage multi-region buckets to fine tune trade-offs of durability,
   replication, and access costs.

Flexible formats.
   Produce records in Protobuf, JSONL, CSV, or (coming soon) AVRO.

   Files in cloud storage hold only raw records, with no special file format.
   Write JSONL records to journals and you'll get files of JSONL on S3.

Cost efficient.
   Cloud storage is typically multiples cheaper than persistent disks --
   and that's before accounting for costs of replication -- and provides
   vastly more aggregate read IO throughput.

   Brokers and clients are zone aware and avoid cross-zone reads where possible.
   Client reads delegated to S3 incur no inter-zone costs and can even be
   configured to offload decompression onto the storage service, saving CPU cycles.

"Batteries included" command-line tool.
   ``gazctl`` makes it easy to introspect and manage brokers and consumer
   applications, and often makes quick work of integrating existing applications.

Familiar Kubernetes primitives.
   Create and configure journals by applying declarative YAML specifications.
   Use *labels* and *selectors* to annotate and query over journals and
   other Gazette resources.

Consumer Features
------------------

Build flexible streaming applications in Go.
   Apply rich specifications to implement a wide variety of operational patterns
   simply not possible in other systems.

Stateful streaming -- on your terms.
   Transact against a remote database or key/value store.
   Or, use embedded stores like RocksDB_ for fast and tunable storage of keys & values
   or SQLite_ for full SQL support. No API wrapping required.

Exactly-once semantics by default.
   Offsets and other metadata are *always* persisted to the application's state store
   using the same transactions that capture application updates. It's straightforward
   to maintain perfect parity between materialized states and the events which
   were read to produce them.

   The framework manages commit acknowledgements for end-to-end correctness,
   with low latency and no head-of-line blocking.

Deploy consumers as disposable containers.
   Embedded RocksDB and SQLite stores are durably replicated (to journals, of course)
   and don't rely on persistence of the host disk. Use local_ SSDs_ to power ultra-fast
   APIs querying over continuously materialized views.

   The framework manages recovery of on-disk store states, provisions hot standbys,
   and performs fast fail-over so that developers can focus on their message-driven
   application behaviors.

.. _local: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ssd-instance-store.html
.. _SSDs: https://cloud.google.com/compute/docs/disks/local-ssd
.. _RocksDB: https://rocksdb.org
.. _SQLite: https://sqlite.org

