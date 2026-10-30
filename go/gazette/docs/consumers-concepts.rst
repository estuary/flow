Consumer Concepts
==================

Gazette's *consumer* framework makes it easy to build powerful streaming
applications in Go. A developer begins by adapting their event-driven
business logic to the consumer Application_  interface, most notably its
``ConsumeMessage`` callback. A complete, ready-to-run framework binary is
then built by calling `runconsumer.Main`_ from a Go ``func main()``.

.. _`runconsumer.Main`: https://godoc.org/go.gazette.dev/core/mainboilerplate/runconsumer#Main
.. _Application: https://godoc.org/go.gazette.dev/core/consumer#Application

Shards
-------

Shards are the *unit of work and scaling* for a consumer application.
A shard can usually be thought of as composing:

- An application which provides event callbacks,
- One or more source journals to be read, and
- A stateful store into which shard checkpoints and application states are captured.

A shard is a long-lived resource and may be handed off between many processes
over its lifetime. When that happens, the shard's store always travels with it.

Unlike other stream processing systems, Gazette does not prescribe how or
when shards are created, and requires that users or application-specific
tools explicitly manage shards and provide their configuration.

The trade-off is that shards are *highly* flexible, able to support
operational patterns simply not possible in other systems:

- Use multiple shards to process the same journal in varying ways,
  using custom business configuration carried by shard labels.
- Deploy a global application that homes redundant shards in multiple cloud regions,
  to provide local queryable copies of a continuous materialization.
- Configure shards to co-locate application processing to the region or
  network edges where each of a set of globally-distributed journals live.

Stores
-------

A store_ is simply a durable place to store current application states and a
shard checkpoint. Many stores support a notion of transactions, which are
leveraged to provide end-to-end "exactly once" processing semantics. Other
types of stores are possible, such as key/value databases. In many cases these
too can provide exactly-once semantics -- i.e. via check-and-set operations,
though with `significant caveats`_. Stores fall into two categories:

**Remote** stores are managed externally and accessed over the network.

  Remote stores are a good fit at smaller scales, or when store access patterns
  don't require fine-grained queries which may suffer from serialization costs
  and network latency.

**Local** stores use a process-embedded database and host disk.

  Replication and recovery of the database is provided by the framework.
  Local store applications can be incredibly performant, as they collocate
  computation with store states that are local to the machine and often
  even cached in-memory.

The choice of store is entirely up to the application. Several store
implementations are available today, with many others being easy to
integrate:

:SQLStore_: Use any remote SQL database compatible with the Go ``database/sql`` standard library.
:JSONFileStore_: Manage light-weight state using a local, replicated JSON-encoded file.
:RocksDB_: Manage high-performance key/value state using a local, replicated RocksDB.
:SQLite_: Leverage full SQL semantics using a local, replicated SQLite DB.

.. _store: https://godoc.org/go.gazette.dev/core/consumer#Store
.. _significant caveats: https://godoc.org/go.gazette.dev/core/consumer#Application
.. _SQLStore: https://godoc.org/go.gazette.dev/core/consumer#SQLStore
.. _JSONFileStore: https://godoc.org/go.gazette.dev/core/consumer#JSONFileStore
.. _RocksDB: https://godoc.org/go.gazette.dev/core/consumer/store-rocksdb
.. _SQLite: https://godoc.org/go.gazette.dev/core/consumer/store-sqlite

Clustering
-----------

Consumer applications run as scalable, ephemeral deployments with many constituent
member processes which may come and go over time. Collectively they balance and
provide fault-tolerant serving of many shards. Like Gazette brokers, an application
may deploy member processes across multiple zones, regions, and clouds.

Applications rely on Etcd for consensus over distributed state of the system (and in
fact, consumers and brokers share a common implementation_ for maintaining consensus
and member assignments).

When a consumer process starts, it "finds" its application group through a shared
Etcd prefix -- defined by flag ``--etcd.prefix`` -- under which specifications of the
application are kept.

.. note::

   Each unique deployment of a consumer application must use a different
   ``--etcd.prefix``. By convention the prefix will compose the application
   name and release name, to fully qualify the group. See the
   `consumer Kustomize manifest`_ for best-practice on deploying and configuring
   consumer applications.

.. _implementation:              https://godoc.org/go.gazette.dev/core/allocator
.. _consumer kustomize manifest: https://github.com/gazette/core/blob/master/kustomize/bases/consumer/deployment.yaml

Recovery Logs
--------------

Consumer applications have no reliance on the availability of a particular host
machine or the durability of its disks. Host disks are used only as temporary
scratch spaces for local stores. This is at odds with embedded databases like
SQLite and RocksDB that persist to a local disk and have no built-in mechanism
for replication.

To provide durability, embedded stores are replicated to a `recovery log`_:
a journal to which file operations of the store are sequenced as they're
being made. Operations are captured through low-level integrations with
store-specific OS interfaces (eg afero_, `rocksdb.Env`_, or `SQLite VFS`_).
This has the advantage of making instrumentation transparent to applications,
which use standard clients and can access the full range of store
capabilities and configuration.

Recovery logs allow a process to rebuild the on-disk state of a store by
reading the log and re-playing its file operations to the local disk. Hot standbys
live-tail the log to locally apply the operations of the current primary. To speed
up cold-start playback, the primary will periodically persist "hints" to Etcd which
inform players of how to efficiently read the log, by skipping over journal segments
known to contain only file data that's no longer needed. Hints are also used to
periodically "prune" the log by removing journal fragments which are known to not
contain live file operations.

.. _`recovery log`: https://godoc.org/go.gazette.dev/core/consumer/recoverylog
.. _afero:          https://github.com/spf13/afero
.. _`rocksdb.Env`:  https://github.com/facebook/rocksdb/blob/master/include/rocksdb/env.h#L133
.. _`SQLite VFS`:   https://www.sqlite.org/vfs.html

Transactions
-------------

Shards process messages in *consumer transactions*, with a lifecycle managed
by the framework. In general terms, a transaction:

- Begins when a source message is ready to be processed.
- Starts a corresponding transaction of the shard's store (where supported).
- Processes messages from source journals.
- Reads and/or modifies the store.
- Publishes pending (uncommitted) messages to downstream journals.

Transactions are dynamically sized: a started transaction will typically continue
so long as further messages can be immediately processed without blocking.
At low data rates transactions will quickly stall and begin to close, which
minimizes end-to-end latency. At very high rates, transactions may process
thousands of messages before closing, which can massively boost processing
efficiency.

Applications are kept informed of transaction boundaries. For some use cases
-- such as accumulating counts or other aggregates -- transactions allow
for substantial amounts of work to be done purely in-memory, with aggregate
updates only published as messages or written to the store at transaction
close.

When the transaction does close, the framework attaches a checkpoint to the
store transaction and starts a commit. The checkpoint includes metadata --
like journal offsets -- to ensure that application states in the store are
kept in lock-step with the offsets, etc which produced those states.

Upon the store's commit, pending messages which were published during the
transaction are automatically acknowledged and may now be processed by
downstream consumers. Transactions are fully pipelined: while one transaction
waits for commit acknowledgements from the store, the next has already begun
and is processing messages.

Specifications
---------------

Consumer applications manage a number of specifications stored in Etcd,
coordinated through the application's unique, shared Etcd prefix:

ShardSpecs
    Declare the existence and configuration of a shard -- its ID,
    number of hot standbys, and other user-supplied metadata and behaviors.

MemberSpecs
    Every running consumer process manages a MemberSpec which
    advertises its failure zone, endpoint, shard capacity, etc.

    MemberSpecs are *ephemeral*: they must be kept alive through an
    associated Etcd lease_, and are removed on process exit.

AssignmentSpecs
    Represent the assignment of a shard to a member, along with the assignment
    role: primary or hot-standby. Each shard may have multiple AssignmentSpecs,
    but only one will actively process the shard. Others will tail the on-disk
    state of the primary to support fast fail-over.

    AssignmentSpecs are *also* ephemeral: each is tied to its respective
    member Etcd lease, and is removed on member exit or fault.

Applications coordinate to continuously monitor specifications, solve for
(re)assignment of shards to members, and manage the set of AssignmentSpecs in Etcd.
Individual members then enact the AssignmentSpecs which they're responsible for.

.. note::

    On receiving a SIGTERM a member will announce its desire to exit
    by zeroing its declared capacity in its MemberSpec, and then wait until:

    - All shards have been handed off to a peer with up-to-date on disk state
    - All gRPCs have been drained

    In the absence of faults, shards utilizing recovery logs will always have
    at least one member with a ready on-disk representation of the store's
    state, allowing for zero-downtime rolling deployments.

    This is true even if the configured number of hot-standbys is zero:
    the cluster will temporarily over-replicate the shard to facilitate
    fast hand-off.

.. _lease: https://etcd.io/docs/v3.4.0/learning/glossary/#lease

Building APIs
---------------

Framework applications bundle gRPC and HTTP servers against which custom
APIs may be registered. Applications can use these to offer APIs which
query from local, continuously materialized states.

A service Resolver_ is provided to facilitate discovery of current shards
and endpoints, making it easy to build APIs which transparently proxy
requests, or employ scatter / gather patterns.

.. _Resolver: https://godoc.org/go.gazette.dev/core/consumer#Resolver
