Broker Concepts
================

Journals
---------

Brokers serve **journals**, a resource resembling a file. Journals are append-only:
bytes that have previously been written to a journal are immutable and
cannot be changed. Like files, journals may be read from any byte offset. Readers
that have caught up to the next offset to be written (called the *write head*) may
optionally block and have future content streamed to them. In this way, reads
over journals resemble ``tail -c ${my_offset} -f`` operations over Linux files.

*Unlike* files, journals are frequently written to by distributed systems having many
concurrent writers, and a key function of the broker is to provide global ordering
of how raced append requests are sequenced into a journal. Crucially, writers are
assured that their entire append lands together (commits) in the journal or that
none of it does. Formally journal appends are *serializable* -- appended spans have
a total ordering and will never interleave. Readers are similarly assured they'll
read only committed journal content and will never see a partial append that is
later rolled back (i.e., because a broker or client faulted partway through).

Clustering
-----------

A broker **cluster** is a set of ``gazette`` broker processes which collectively
balance and provide fault-tolerant serving of many journals. Clusters may be
deployed across multiple zones, global regions, and even clouds.

An individual broker process is stateless, having no expectation or reliance
on continuity of the host machine or its disks. Brokers use host disks only as
temporary scratch spaces for capturing and serving very recent journal content.

Durability is provided in the short term by replicating all
journal appends across multiple brokers, assigned to span at least two
availability zones. BLOB stores provide long term durability.

.. tip::

    It's recommended to operate Gazette as a Kubernetes Deployment_,
    with local SSDs mounted to the container as temporary scratch space.
    The repository provides related Kustomize manifests.

.. _Deployment: https://kubernetes.io/docs/concepts/workloads/controllers/deployment/

Journals are the *unit of scale* for brokers: every journal append RPC must pass
through a designated "primary" broker which coordinates the append transaction,
and be synchronously acknowledged by other replicating brokers.

Collectively a distributed system cannot append to a journal any faster than
those brokers can handle, no matter how many other brokers may exist in the
cluster. Scaling further requires partitioning across multiple journals.

.. note::

    Gazette employs pipelining and transaction reuse to maximize journal
    throughput with minimal latency (typically one round-trip for ACKs).
    The practical bottleneck is the compression of journal content.

Specifications
---------------

Gazette relies on **Etcd** for consensus over distributed configuration of the
cluster. Broker processes manage a number of *specifications* which are stored
in Etcd, coordinated through a shared Etcd prefix:

JournalSpecs
    Declare the existence and configuration of a journal -- its name,
    required replication factor, and other user-supplied metadata and behaviors.

BrokerSpecs
    Every running ``gazette`` process manages a BrokerSpec which
    advertises its failure zone, endpoint, journal capacity, etc.

    BrokerSpecs are *ephemeral*: they must be kept alive through an
    associated Etcd lease_, and are removed on process exit.

AssignmentSpecs
    Represent the assignment of a journal to a broker, along with the assignment
    role: primary or replica. Each journal may have multiple AssignmentSpecs.

    AssignmentSpecs are *also* ephemeral: each is tied to its respective
    broker Etcd lease, and is removed on broker exit or fault.

Brokers coordinate to continuously monitor specifications, solve for
(re)assignment of journals to brokers, and manage the set of AssignmentSpecs in Etcd.
Individual brokers then enact the AssignmentSpecs which they're responsible for.

Leases are the *only* mechanism by which cluster faults are detected and mitigated.
Gazette does not have a concept of an in-sync replica set, as re-assignments are
very fast and do not require data migrations.

.. note::

    On receiving a SIGTERM a broker will announce its desire to exit
    by zeroing its declared capacity in its BrokerSpec, and then wait until:

    - All assigned journals are gracefully handed off,
    - All local journal content has been persisted, and
    - All gRPCs have been drained

    In the absence of faults journals will **never** deviate under the desired
    number of replicas or availability zones during a rolling deployment.

.. _lease: https://etcd.io/docs/v3.4.0/learning/glossary/#lease

Fragments
----------

Fragments define ranges of a journal which have been written and are now immutable.
They're formally defined by:

:journal-name:
    Each journal is a distinct stream.
:begin-offset:
    The first byte offset of the stream which is captured by the fragment, inclusive.
:end-offset:
    The last byte offset captured by the fragment, exclusive.
:SHA-sum:
    A verifiable SHA sum of the stream byte content from [begin-offset, end-offset).

A constraint of fragment definitions is that they may never subdivide a client's
write. Put differently, fragments contain only whole client appends -- if those appends
each consist of properly delimited messages then so does the fragment.

This property lets fragments directly support a wide range of text and binary
record formats -- like CSV, JSON lines, Protobuf -- using their natural delimiters
(i.e, newlines) and without resorting to a custom file format or record framing.

Fragment Files
---------------

Fragment files hold journal content, using a content-addressed naming scheme
that incorporates the fragment definition. Once a fragment is *closed* by brokers,
it's immediately offloaded to a configured BLOB store for long-term storage
as a fragment file.

Users have fine-grained control over exactly how this happens for each journal:
the BLOB bucket and prefix to use, compression to apply, and further tools to
adapt files to preferred partitioning schemes.

.. admonition:: Example

   Given a fragment store of ``s3://my-bucket/prefix``, a journal
   ``some/events`` and Snappy compression, a persisted fragment file might be
   ``s3://my-bucket/prefix/some/events/00000003b4b-0000014455f-49b43a0783974daee3ff4265b1e.sz``

Fragment Index
---------------

Fragment files are named so that a BLOB store listing is a complete description
of available content for a journal, including the relative placement of files
within the continuous journal stream.

The "source of truth" representation of available content is therefore the BLOB
store itself. Brokers perform periodic listings to maintain an in-memory index of
a journal's stored fragments, which is combined with local fragments still being
built or persisted.

New fragments are discovered and locally indexed from BLOB listings, and fragments
removed from the store (i.e., due to a bucket lifecycle policy or
``gazctl journals prune``) are also purged from the local index.

The fragment index is used to serve all reads by first locating a suitable fragment
for the given journal offset. Then:

- A local fragment is directly served to the client, or
- The broker *proxies* to the BLOB store on the client's behalf, or
- The broker *delegates* by giving the client a signed store URL for direct file access.

Labels and Selectors
---------------------

Journals are the *unit of scale* for brokers. They're roughly equivalent to
*partitions* of a "topic" entity in other systems like Kafka and Pulsar.
It's common to spread a collection of like events across a number of journals,
each acting as a partition of the corpus.

However Gazette has no formal notion of a topic. While journals often have path
components that express a hierarchy of some kind, like ``org-name/group-name/event-type``,
this is purely convention. Journal names are treated as a flat key-space.

Instead Gazette adopts the Kubernetes notion of `Labels and Selectors`_.
Journals are annotated with *labels* which describe their content, such as a
message type, serving region, or anything else. The choice of labels and values
is arbitrary and teams can evolve their own meanings over time, but Gazette does
`provide some conventions`_.

*Selectors* are used to query over journals in terms of their labels, and can
be thought of as a means to define "topics" on an application specific,
ex post facto basis. Each use case can define for itself what dimensions are desired --
like message type, geographic region, or staging environment -- and, by crafting an
appropriate selector, then be assured of observing the set of partitions that
exist now or in the future.

.. _Labels and Selectors: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/
.. _provide some conventions: https://godoc.org/go.gazette.dev/core/labels

gRPC and HTTP
--------------

Brokers present a `gRPC Journal service`_ for interacting with journals.
They also offer an HTTP *gateway* for reading from and appending to journals
using familiar ``GET`` and ``PUT`` verbs. The gateway maps operations to
equivalent gRPC service requests.

The HTTP gateway is handy for building simple clients or reading journals from a
web browser, but at high volumes in production a native gRPC client should be
used instead (such as the `Go client`_).

Other gateway APIs may be offered in the future to ease integration
with common messaging systems.

.. _gRPC Journal service: https://godoc.org/go.gazette.dev/core/broker/protocol#JournalServer
.. _Go client: https://godoc.org/github.com/gazette/core/broker/client

Gazctl
-------

``gazctl`` is a powerful command-line interface for working with Gazette
broker clusters and consumer applications. Use it to:

- Query, add, remove, and update journals served by the cluster.
- Inspect and administer brokers or consumer applications.
- Integrate non-native applications or batch processing pipelines.
