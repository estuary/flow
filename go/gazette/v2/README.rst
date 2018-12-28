|circleci status|

.. |circleci status| image:: https://circleci.com/gh/LiveRamp/gazette.svg?style=svg
   :target: https://circleci.com/gh/LiveRamp/gazette

Overview
========

Gazette is infrastructure for building streaming platforms, consisting of a
*broker service* for durable logging and publish/subscribe, and a *consumer
framework* for long-lived, scaled, stateful, and highly-available streaming
applications written in Go.

Gazette has been continuously operated in production since early 2015,
and powers a number of critical systems and use-cases at LiveRamp.

Cluster QuickStart
~~~~~~~~~~~~~~~~~~

.. code-block:: console

    # Perform a containerized build and test of the project and examples (requires Docker).
    $ v2/build/all .

    # (If required) bootstrap a Kubernetes cluster with Helm (https://helm.sh).
    $ v2/test/bootstrap_local_kubernetes.sh

    # Deploy Gazette brokers and interactive examples to a Kubernetes cluster.
    # Context defaults to "docker-for-desktop" (if installed) or "minikube".
    # Namespace defaults to "default".
    $ v2/test/deploy_test_environment.sh -c ${MY_CONTEXT} -n ${MY_NAMESPACE}
    Using context "minikube" & namespace "default"
    ... trimmed output ...
    NOTES:
    The Gazette broker cluster is now running.
    1. Get the application URL by running these commands:
      export POD_NAME=$(kubectl get pods --namespace default -l "app.kubernetes.io/name=gazette,app.kubernetes.io/instance=virtuous-owl" -o jsonpath="{.items[0].metadata.name}")
      echo "Visit http://127.0.0.1:8080 to use your application"
      kubectl port-forward $POD_NAME 8080:80


- `build/ <build/>`_ provides an overview of Gazette's build infrastructure.

- `test/ <test/>`_ has details on deployment, example applications,
  and means of provisioning a local Kubernetes cluster (eg, Minikube) with a
  complete Gazette environment, including interactive examples.

Broker Service
==============

Brokers serve "Journals", a byte-oriented resource resembling a file. Journals
may be read from an arbitrary offset, they may be appended to (only), and they
may grow to an unbounded length far exceeding disk capacity.

Append operations are atomic: a writer is assured that either its entire write
is contiguously sequenced into the journal, or that none of it is. No reader
of a journal will observe a write until it has been fully committed.

The brokers provide global sequencing of client writes to journals, and replicate
those writes to ensure durability. They also serve streamed journal reads, which may
begin at any offset and will optionally block upon reaching an offset which has not
yet been written (the "write head"). In this way, read operations very much resemble
``tail -c ${my_offset} -f`` operations over files.

Interacting with Brokers
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    # Port-forward to a running Gazette Pod (or, use a service IP).
    $ kubectl port-forward $A_GAZETTE_POD_NAME 8080

    # Write data to a journal using the HTTP gateway API:
    $ curl -X PUT --data-binary @- http://localhost:8080/examples/foobar << EOF
    > Hello, Gazette!
    > EOF

    $ curl http://localhost:8080/examples/foobar
    Hello, Gazette!

    # Read beginning at an arbitrary offset:
    $ curl "http://localhost:8080/examples/foobar?offset=7"
    Gazette!

    # Perform ongoing, background writes to the journal:
    $ while true; do curl -X PUT --data-binary @- http://localhost:8080/examples/foobar << EOF
    > ping
    > EOF
    > sleep 1
    > done

    # Read from an offset, continuing to stream new writes as they arrive:
    $ curl -N "http://localhost:8080/examples/foobar?offset=7&block=true"
    Gazette!
    ping
    ping
    ping
    ... etc

    # Use the gazctl CLI tool to interact with Gazette:
    $ gazctl journals list --primary
    +----------------------------------------------------+--------------------------------------+
    |                        NAME                        |               PRIMARY                |
    +----------------------------------------------------+--------------------------------------+
    | examples/foobar                                    | virtuous-owl-gazette-bc5d97fbd-8xw8s |
    | examples/stream-sum/chunks/part-000                | virtuous-owl-gazette-bc5d97fbd-8xw8s |
    | examples/stream-sum/chunks/part-001                | virtuous-owl-gazette-bc5d97fbd-8xw8s |
    | examples/stream-sum/chunks/part-002                | virtuous-owl-gazette-bc5d97fbd-8xw8s |
    |                   ... etc ...                      |                                      |
    +----------------------------------------------------+--------------------------------------+

Consumers Framework
===================

The consumers framework simplifies the development of user applications which
"consume" messages streamed from journals. Applications are empowered to keep
substantial amounts of application-defined state in an embedded database
(typically a RocksDB), and the framework manages concerns such as database
replication and recovery, distributed routing, failure recovery, and high-
availability. Applications may be very long lived and scale horizontally.

Design Goals (and Non-Goals)
============================

Gazette has influences and shares similarities with a number of other projects.
Its architecture also reflects several departures from the solutions of those
influences.

 * Journals provide globally record ordering, durable storage, and publish/subscribe.

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
elastic and suited for scaled read IOPs, and require no explicit provisioning.

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
standbys" which replicate database file state and can immediately take over.

 * Non-goals: distributed state & consensus.

Gazette uses Etcd v3 as the single source-of-truth for distributed state (eg
current members, journals, and current assignments). Etcd v3 leases are used
to detect process failures, and Gazette employs an "allocator" which solves
for and applies assignment updates via checked Etcd transactions.

 * Non-goals: resource management and job scheduling.

Gazette does not manage workloads or services, such as the provisioning or
scaling of brokers or consumers, and relies on an external orchestration framework
to perform these tasks. The authors use and enthusiastically recommend Kubernetes.

