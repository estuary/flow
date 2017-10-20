========
Gazette
========

Architecture Overview & Design Documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Overview
---------

Gazette is, at its core, *a highly distributed (though not highly available)
byte-stream transaction engine*. It provides useful guarantees about reads and
writes and stores all data in cloud storage. [*]_ This final property makes it
powerful in the sense that *reading data out of it does not necessarily require
a connection to a broker* (server)
if you have access to cloud storage.

.. [*] While the current implementation uses cloud storage, Gazette could also
       be used with non-cloud storage systems such as HDFS.

It consists of several pieces:

(1) A pool of broker-nodes, which are responsible for deciding what data should
    be written to persistent storage and then writing it to an ongoing
    byte-stream called the journal
(2) The representation of the journal on disk, which serves as a transition
    point between parts (1) and (3)
(3) A set of consumer processes, which are responsible for reading from the
    on-disk representation of the journal and converting it into a form that can
    be sent to a client.

Architecture
~~~~~~~~~~~~~

From the perspective of a writer to the Gazette system, Gazette provides an
append-only journal which accepts byte-oriented chunks of data. Writes are
atomic -- either they are written in their entirety or not at all.

Broker-Nodes & Writing the Journals.
------------------------------------

After a writer has sent a write request to Gazette, it falls to the broker-nodes
to decide where and whether the content will be written. A broker-node is a
server process, and we maintain a pool of broker-nodes to be used for writing to
the journals.

Assigning Brokers to a Journal
```````````````````````````````
Gazette uses `etcd <http://github.com/coreos/etcd>`_ to track broker membership,
ie. the complete set of brokers which are live. The etcd server ensures that
each broker is aware of updates to the topology, but is not aware of the
journals or other concerns.

Gazette uses `highest random weight hashing
<http://en.wikipedia.org/wiki/Rendezvous_hashing>`_ to assign a responsible
broker and set of replicas for each journal, which allows each broker to
independently determine which set of brokers is responsible for a specific
journal. The journal name is used as a routing key. Note that a single broker
may be a member of more than one replica set, and thus responsible for more than
one journal.

The assignment of brokers to journals is only changed when the topology of the
larger broker pool changes, for example when a broker is added to or removed
from the pool. Gazette strives to maintain existing topology, and it is
preferable that no more than two broker-nodes are inaccessible or down at any
one time since the replication factor is three.

The three broker-nodes chosen for writes to a specific journal are responsible
for determining the current write offset in the journal, and for writing the
data to each of three copies of the journal. Through the highest random weight
hashing and the topology view provided by etcd, the brokers achieve consensus
and elect a master broker. This master is responsible for coordination and
communication with the client writers. Writes will be replicated to the other
brokers in the replica set.

Consider the following example, where we have a pool of four brokers with the
given HRW values for a given journal *J*. Broker-2, having the highest HRW value
for *J*, will be elected master for that journal’s broker replica set. Broker-1
and Broker-0, having the second-highest and third-highest HRW values
respectively, will be the other two members of the replica set.

 .. image:: img/assigning_brokers_example.jpg

In this example, when Broker-2 fails and is removed from the topology of the
broker pool, the next-highest-ranked broker (Broker-1) will become master. The
new third-highest-ranked broker (Broker-3) will join the replica set for writes
to journal *J*.

Write Transactions
```````````````````

The steps of a successful write transaction are as follows:

1) A client writer wants to append to a journal, and sends a write request to
   the master broker-node. If the broker does not believe itself master, the
   write request is rejected.
2) The master looks at the write and determines that it is writing to the
   current journal, as well as what master believes is the current write-offset.
   The master will queue up any other competing writes.
3) The master begins a `two-phase commit <http://en.wikipedia.org/wiki/Two-phase_commit_protocol>`_.
   It sends (its idea of) the current write-offset and the topology of the
   replica set to the other two broker-nodes in the replica set.
4) If a non-master broker-node agrees with master, it sends back a `100 continue
   response <http://httpstatusdogs.com/100-continue>`_ to the master.
5) If master receives the continue response from all replica set members, it
   enters the second phase of the commit. It will send the write to its own log,
   and to the other members to be replicated.
6) The master node can add on more of the queued write requests onto the same
   transaction.
7) Once master drains the existing write requests, it closes the request body
   (replication stream.)
8) Assuming success, each of the other two members of the replica set sends back
   an event signifying that they have synced their data to disk.
9) If all syncs are successful, master sends back a success message to the
   writer. Otherwise, if anything has gone wrong, it sends back a failure.

On the other hand, due to partitions or other issues, the transaction may be
unsuccessful. For example, one of the broker-nodes may believe the write offset
has moved forwards while the others do not. If this is the case, all of the
brokers will use the highest write offset of all the brokers in the replica set.
[*]_ It is always safe to skip forward to a higher
write offset.

.. [*] This happens either because master passes its higher write offset to the
       other brokers in step 3, or because one of the other brokers passes back
       its offset in step 4 above and the write fails.

Due to this property, the guarantee that Gazette makes is that writes will be
written at least once. There is *no* guarantee that writes will be written
exactly once.

With regards to the CAP theorem, Gazette provides C&P. It is not a highly
available system: in the case of a partition in the replica set, writes are not
allowed to continue. Writes must be buffered by the client until all three
brokers come back up and are available.

Representing the Journal On-disk.
------------------------------------

Conceptually, Gazette presents an infinite journal abstraction to clients.
On-disk, the journal is implemented via fragments, files which capture part of
the journal’s stream of bytes. The fragment files are identified by their start
and stop offsets in the journal, and the SHA-1 hash of their contents.

 .. image:: img/rep_journal_ondisk_diagram.jpg

We refer to a fragment that is being actively written as a spool. After a
certain period of time, each broker-node will close off its spool and upload the
fragment to cloud storage. The master broker will send hints of when to start a
new spool and to close the last one, such that brokers usually roll spools in
tandem and thus produce identical fragments. In a set-up where three brokers are
responsible for a series of write operations, we would ideally expect to end up
with three copies of a fragment containing those writes. In actuality, due to
failed writes, the three copies may not be identical and one or more copies may
be missing some of the writes.

When the three sets of fragments are uploaded to cloud storage, the client must
decide which of the fragments to pick. It makes the decision based off of a
heuristic, choosing the fragment that covers the requested offset and contains
the most content after the requested offset.

How the Journal Maps to Messages
---------------------------------

Let’s pause to discuss how these on-disk fragments can be mapped back to the
original messages written by our writers, and how we organize them to enable
horizontal scaling of the overall system.

We can think of topics as a group of messages, which are captured via one or
more journal streams and written into fragments into cloud storage. Topics also
encompass a set of rules for how messages are distributed on-disk and how they
are marshalled to or unmarshalled from bytes.

However, the broker-nodes do not concern themselves with anything but bytes and
the journal. They are unaware of how those bytes map to messages, and do not
care about the overarching topics into which those messages fit.

Given this separation of concerns, one could expect that reading from a Gazette
journal might return an incomplete hunk of a message. This can happen, but only
in the case of partial HTTP reads. In terms of fully flushed writes, only
complete messages end up being written to Gazette. When a partial HTTP read does
occur, a `client abstraction
<https://git.liveramp.net/arbortech/workspace/blob/master/src/github.com/pippio/gazette/journal/io.go>`_
exists to tell us how many more bytes to wait for to read a complete message. At
any rate, ensuring that writes are of entire messages and nothing smaller makes
Gazette capable of acting as a message-stream rather than just a byte-stream.

Often, we do not want to record all messages corresponding to a particular topic
in one journal, as this limits horizontal scaling with regards to reads from the
journal. Instead, we want to partition the topic into several separate journal
byte streams. Different brokers can handle each partition, and topics are hashed
to determine which partition they are written to. On-disk, each topic has an
independent directory, and each partition has an independent directory inside
the relevant topic. The fragment files live inside the relevant partition
directory.

 .. image:: img/maps_to_messages.jpg

A journal is equivalent to a partition\: the journal is the abstraction used by
the writers, whereas the partitions are the abstraction used by the readers. For
the writers, the journal is a raw byte-stream. For the readers, the partitions
contain messages and are oriented around topics. Thus, when we discuss the
brokers, we refer to a journal; when we discuss reading from the file system, we
refer to partitions.

A good rule of thumb is to aim for as many partitions as there are concurrent
readers, henceforth called consumer shards, of the topic’s journals. Note that
while we aim for the number of consumer shards to be upper-bounded by the number
of partitions, this limit is not imposed by Gazette. However, we want to process
each message only once and to not handle coordinating which consumer shard is
responsible for which messages in the partition. Thus, ensuring that at most one
consumer shard acts on each partition is a reasonable design decision. (One
consumer is free to read from more than one partition.)

Consumers.
-----------

A consumer is a group of reader processes, or consumer shards. From a client
perspective, a consumer is responsible for serving reads. Internally, a consumer
relies on its consumer shards to read from each partition in a topic, and each
consumer shard may be responsible for one or more partitions.

The process by which consumer shards read from partitions is as follows:

1) A consumer shard attempts to take an exclusive lock on a partition. If it
   fails, end.
2) If lock is successful, the consumer shard pulls current read offset from
   etcd.
3) The consumer shard begins to read and produce messages.
4) Every [unit of time], the consumer shard persists its current read offset to
   etcd.
5) When read is complete, consumer shard releases the lock.

We use highest random weight hashing to associate consumer shards with
partitions, which is an imperfect system as some consumer shards may be
unutilized. There is room for improvement in the matching process.

A consumer is further responsible for sending notices to the consumer shards
that they should ‘check point’, ie. write their current offset back to local
storage. This local storage is likely to be a database, where local state
(including the read offsets) is represented and written to.

We use `RocksDB <http://github.com/facebook/rocksdb>`_ as a local
storage engine for the consumers, due to the use of `LSM trees
<https://www.cs.umb.edu/~poneil/lsmtree.pdf>`_. We plan to write custom hooks
for RocksDB to make use of the output of their compaction. We can transform the
compacted records to write updated values to wherever we desire. This is
discussed more in the document linked in the (Joins) section below.

So far, we have covered the core components of Gazette. Below, we will discuss
future, planned additions to the Gazette system, and their effects on Gazette’s
basic architecture.

Tracking Local State.
``````````````````````

When performing a join, consumers need a way of tracking local state over time.
This local state includes the read offset discussed above, but must also
encompass the current state of the join operation. Since a consumer may fail
mid-join, we want to ensure that a new consumer can pick up where the failed
consumer left off, to avoid redoing work or duplicating results. Thus, the
storage of local state must be resistant to failure.

The current idea is to use a local database which supports transactions. Each
consumer will be responsible for committing local state to a distinct local
database storage.

Glossary.
~~~~~~~~~~

Broker-node
  An etcd server that handles write requests to the journal. Responsible for
  tracking the write offset and opening and closing spools. Sometimes
  abbreviated to *broker*.
Consumer
  A conceptual ‘reader’ of one or more processes, responsible (from the client
  perspective) for serving reads. Consumers typically implement continuous
  map/reduce operations, often maintain state, and are scaled & formed of a
  grouping of multiple consumer shard processes.
Consumer shard
  An individual reader process, responsible for consuming one partition of a
  topic. Together with its peers, it implements a consumer. Sometimes
  abbreviated to *shard*.
Dataflow
  A directed acyclic graph composed of one or more chained consumers, which
  describes a continuous computation.
Event
  Something that happened, usually captured in a message and written as bytes to
  the journal.
Fragment
  A file containing part of the journal’s stream of bytes.
Journal
  A conceptual, infinite-length byte stream abstraction, which supports reads
  from arbitrary offsets (including blocking reads from its “head”), and
  transactional appends to the head coordinated by Gazette broker-nodes.
  Equivalent to a *partition*.
Messsage
  An event, represented in a specific format for Gazette.
Broker replica set
  A selection of three broker-nodes, which are responsible for writing to a
  journal. The members of the replica set are selected from a larger pool of
  broker-nodes.
Routing Key
  The key that is used as input to the hashing algorithm that decides which
  journal is written to. Also called the *partition key*.
Spool
  A fragment that is actively being written by a broker-node.
Topic
  Abstraction: a conceptual stream of messages; a collection of related journals
  that capture the same type of events, as well as a specification for how
  messages are distributed. A topic is mapped across one or more Gazette
  journals (“partitions”) by a routing key.
Topic Partition
 An individual journal which, in concert with its peers, implements the
 underlying storage for topic messages. A partition is a Gazette journal, when
 referred to in the context of a topic. Sometimes abbreviated to *partition*.
