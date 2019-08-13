Design Goals (and Non-Goals)
============================

Gazette has influences and shares similarities with a number of other projects.
Its architecture also reflects several departures from the solutions of those
influences.

 * Journals provide globally record ordering and publish/subscribe.

Much like Kakfa, LogDevice, Apache BookKeeper, and others. These properties are
the basic building blocks for assembling platforms composed of streaming,
decoupled, and event-sourced services.

However, where these systems are *record* oriented, journals are *byte* oriented.
They are eminently suited for streams of delimitated records, but responsibility
for representation, delimitation, packing, and parsing are responsibilities of the
client and not the broker. This simplifies broker implementation and improves
performance, as the broker can concern itself with additive byte sequences rather
than granular messages.

 * Brokers do not provide long-term storage of journal content.

This responsibility is offloaded to a "blob" object store such as S3, Google Cloud
Storage, Azure, HDFS, etc. Use of a separate storage backend stands in contrast to
Kafka, where brokers are responsible for log storage. LogDevice and Apache Pulsar
(with BookKeeper) use a similar technique of decoupling log sequencing from storage.

Separation of storage is motivated by multiple factors. Most importantly, a broker
service like Gazette typically supports diametrically opposed use-cases: capturing
critical writes of a system as they occur, and serving highly scaled reads of
historical written data. By decoupling storage, we can separately scale the write
capacity of the system from its read capacity. A second factor is that storage
separation enables taking advantage of services like S3 or GCS, which are highly
elastic and suited for scaled read IOPs, and require no explicit provisioning or
disk resizing.

 * Journals, once written, are immutable.

Gazette journals are designed to serve as the long-term system of record for data
within the platform. Journals may be trimmed by removing content from the beginning
or even the middle of the log, but an offset range can never be mutated once
written. This is similar to systems like LogDevice and BookKeeper and distinct
from Kafka, whose brokers implement "compaction" of logs based on a keyed primary
message ID.

Implementing compaction within Gazette brokers is not feasible due to its lack
of access to the structure and semantics of records stored in journals. This would
seem to make implementing a system like Apache Samza or Kafka Streaming on Gazette
impossible, as both utilize Kafka topics to replicate application key/value
state, and rely on this mechanism to compact replication logs over time.

Instead, Gazette consumers make use an insight that embedded LSM-Tree DBs such
as RocksDB *already* perform regular compaction, and structure their on-disk
state as a series of append-only and immutable files. Rather than replicate and
replay individual key/value operations, Gazette consumers instead observe and
sequence the file operations of the database itself into a "recovery log" journal
which can be pruned over time, and cheaply "tailed" by hot-standbys which replay
the file operations to local disk (and do not otherwise incur any compaction cost).

 * Brokers and Consumers are ephemeral, disposable, and quick to start up.

While they make good use of available local disk, they have no reliance on
persistence of mounted data volumes. From a cold-start, brokers are able to serve
journal read, append, and replication operations without having to first copy
any prior written data. As a trade-off, reads may block until the broker observes
that recent written content has been persisted to the backing blob store.

 * Non-goal: Topics or higher-level organizing concepts.

A common tactic to achieve horizontal scale-out of high volume message flows
is to spread a collection of like messages across a number of "partitions",
which collectively form a "topic". Many systems, like Kafka or Pulsar, provide
a formal representation of topics as an API concern. Gazette does not, and
understands only journals.

Instead, Gazette borrows Kubernetes' notion of "labels", which can be applied
to resources like journals, and "label selectors" which define queries over
declared labels. Topics can informally be implemented as a label and selector
like ``topic=my_logs`` but selectors allow for additional flexible expressions
(eg, ``topic=my_logs, region in (apac, us)``, or ``topic in (my_logs, my_new_logs)``).

 * Simple file-based integration with existing batch processing work-flows.

Spans of journal content (known as "fragments") use a content-addressed naming
convention but otherwise impose no file structure and contain only raw journal
content, optionally compressed. Fragments are also written under predictable
prefixes to the backing blob storage service, which means existing batch
processing work-flows can "integrate" with Gazette by directly reading and
watching for files within the blob store, using a service Amazon SNS to
receive file notifications, or using a library which implements such polling
already (such as Spark DStreams).

 * Fast, zone/rack aware balancing and fail-over.

Gazette brokers and consumers dynamically balance work items (eg, journals)
across the current cohort of application instances deployed by the operator.
Those instances may come and go, or even fail, at any time.

Failure of a broker or consumer process should be detected and fail-over quickly,
and should be tolerant to rack or whole availability zone failures. Such failures
should never result in data-loss, or interrupt broker or consumer services for more
than the seconds it takes to detect failure and remove affected members from the
topology, appropriately re-balancing their load.

Brokers are able to immediately serve a newly assigned journal without any
replication delay. Gazette consumers may optionally have a number of "hot
standbys" which replicate database file state and can immediately take over
for a failed peer.

 * Non-goals: distributed state & consensus.

Gazette uses Etcd v3 as the single source-of-truth for distributed state (eg
current membership, journal specifications, and process assignments). Etcd v3
leases are used to detect process failures and gate distributed topology changes.
Gazette employs an "allocator", running atop Etcd API primitives, which solves
for distributed zone-aware assignment and horizontal rebalancing.

 * Non-goals: resource management and job scheduling.

Gazette does not manage workloads or services, such as the provisioning or
scaling of brokers or consumers, and relies on an external orchestration framework
to perform these tasks. The authors use and enthusiastically recommend Kubernetes.
