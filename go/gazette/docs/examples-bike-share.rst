Finding Cycles in Bike-Share Streams
====================================

In this walk through, we'll be building a continuous data pipeline for processing
Citi Bike `system data`_ using the Gazette consumer_ framework.

The implementation can be found in package bike-share_.

.. _system data: https://www.citibikenyc.com/system-data
.. _consumer:    https://godoc.org/go.gazette.dev/core/consumer
.. _bike-share:  https://godoc.org/go.gazette.dev/core/examples/bike-share

Objectives
----------

We've been asked to help with an anomaly detection task: we want to find cases
where a Citi Bike completes a graph cycle starting and ending at a station **T**,
without re-visiting **T** in between. We assume shorter graph cycles are common
but want to find longer instances (having a path length of at least 10).

Bikes will sometimes be relocated from one station to another in the Citi Bike
system, which appears in our ride data as having magically moved between stations.
We want to filter these cases out to retain only "true" cycles of our... cycles.

Finally, we'll offer a "history" API which serves the most recent rides of
a given bike ID.

Deploy the Example to Kubernetes
--------------------------------

Deploy the example to the ``bike-share`` namespace of a test Kubernetes cluster,
such as Minikube or Docker for Desktop:

.. code-block:: console

   $ kubectl apply --kustomize ./kustomize/test/deploy-bike-share/
   namespace/bike-share created
   serviceaccount/minio created
   configmap/etcd-scripts-24t872gm88 created
   configmap/example-journal-specs created
   configmap/gazette-zonemap created
   configmap/minio-create-bucket-mm6f469cbt created
   configmap/postgres-init created
   configmap/shard-specs-bike-share-7b52ht4t87 created
   configmap/stream-rides-bike-share-thbh946769 created
   secret/gazette-aws-credentials created
   secret/minio-credentials-fgdm8fkm5m created
   service/consumer-bike-share created
   service/etcd created
   service/gazette created
   service/minio created
   service/postgres created
   deployment.apps/consumer-bike-share created
   deployment.apps/gazette created
   deployment.apps/minio created
   statefulset.apps/etcd created
   statefulset.apps/postgres created
   job.batch/apply-journal-specs created
   job.batch/apply-shard-specs-bike-share created
   job.batch/minio-create-bucket created
   job.batch/stream-rides-bike-share created

After giving pods a moment to start:

.. code-block:: console
   
   $ kubectl -nbike-share get pod
   NAME                                   READY   STATUS      RESTARTS   AGE
   apply-journal-specs-xr4vw              0/1     Completed   0          112s
   apply-shard-specs-bike-share-5g8cb     0/1     Completed   0          112s
   consumer-bike-share-78bcdbcf8b-q9vxl   1/1     Running     0          112s
   consumer-bike-share-78bcdbcf8b-t85tb   1/1     Running     0          112s
   etcd-0                                 1/1     Running     0          112s
   etcd-1                                 1/1     Running     0          102s
   etcd-2                                 1/1     Running     0          98s
   gazette-5d87c4fdb-7fm9b                1/1     Running     0          112s
   gazette-5d87c4fdb-7fzcj                1/1     Running     0          112s
   gazette-5d87c4fdb-djzfq                1/1     Running     0          112s
   minio-7695c66fd8-p87fj                 1/1     Running     0          112s
   minio-create-bucket-4bdcb              0/1     Completed   0          112s
   postgres-0                             1/1     Running     0          112s
   stream-rides-bike-share-p5b8k          1/1     Running     0          112s

Along with the application itself, the Kustomize manifest has deployed a few
dependencies:

 - A gazette broker deployment, of course.
 - Minio is a self-contained, S3-compatible BLOB store to which fragments are persisted.
 - A StatefulSet of Etcd pods are used by the broker and consumer-bike-share
   application pods for process grouping, storage of specifications, and managing
   process work assignments.
 - One-time jobs to apply-journal-specs and apply-shard-specs.
 - A Postgres database for our application to talk to.
 - A stream-rides job, which appends Citi Bike system records at a rate of ~3k QPS.

Configure Gazctl for Cluster Access
-----------------------------------

Gazctl supports a global configuration file at ``~/.config/gazette/gazctl.ini`` (see ``gazctl --help``).
Use it in combination with a couple of ``port-forwards`` to access cluster services.
This works even for deployments scaled to many machines & pods, because brokers
and consumers will proxy requests on our behalf.

.. code-block:: console

   $ GO111MODULE=on go install go.gazette.dev/core/cmd/gazctl

   $ mkdir -p ~/.config/gazette
   $ cat > ~/.config/gazette/gazctl.ini <<EOF
   [journals.Broker]
   Address = http://localhost:32180

   [shards.Broker]
   Address = http://localhost:32180

   [shards.Consumer]
   Address = http://localhost:32190
   EOF


Start long-lived port-forwards to a broker and consumer pod, in their own terminal tabs.
Also port-forward for Postgres access:

.. code-block:: console

   $ kubectl -nbike-share port-forward svc/gazette             32180:8080
   $ kubectl -nbike-share port-forward svc/consumer-bike-share 32190:8080
   $ kubectl -nbike-share port-forward svc/postgres            32432:5432

Examining Journals
-------------------

Several JournalSpecs have been applied, to which ride records and found cycles
are written. These specs use Gazette's recommended_ label names and values, which are
modeled after and extend those of Kubernetes_.
Like ``kubectl``, ``gazctl`` supports familiar ``-l`` and ``-L`` flags to
select over and list labels attached to resources (many other flags are also
supported; check ``--help``). Let's use them to inspect example journals in the cluster:

.. _recommended: https://godoc.org/go.gazette.dev/core/labels
.. _Kubernetes:  https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/#labels

.. code-block:: console

   $ gazctl journals list -l example-name=bike-share -L app.gazette.dev/message-type -L content-type
   +---------------------------------------------------+------------------------------+-----------------------------------+
   |                       NAME                        | APP GAZETTE DEV/MESSAGE-TYPE |           CONTENT-TYPE            |
   +---------------------------------------------------+------------------------------+-----------------------------------+
   | examples/bike-share/cycles/part-000               | bike_share.Cycle             | application/x-ndjson              |
   | examples/bike-share/cycles/part-001               | bike_share.Cycle             | application/x-ndjson              |
   | examples/bike-share/recovery-logs/cycles-part-002 | <none>                       | application/x-gazette-recoverylog |
   | examples/bike-share/recovery-logs/cycles-part-003 | <none>                       | application/x-gazette-recoverylog |
   | examples/bike-share/rides/part-000                | bike_share.Ride              | text/csv                          |
   | examples/bike-share/rides/part-001                | bike_share.Ride              | text/csv                          |
   | examples/bike-share/rides/part-002                | bike_share.Ride              | text/csv                          |
   | examples/bike-share/rides/part-003                | bike_share.Ride              | text/csv                          |
   +---------------------------------------------------+------------------------------+-----------------------------------+

There are four partitions of ``-l app.gazette.dev/message-type=bike_share.Ride``
with MIME content-type ``text/csv``, matching our data source. These are journals
to which Citi Bike system records are written. Two partitions of
``-l app.gazette.dev/message-type=bike_share.Cycle`` have also been created with
content-type ``application/x-ndjson`` (`newline-delimited JSON`_).
They'll hold completed Cycles found by our consumer. Finally, there are two
journals used for recovery logs. We'll talk about these more later.

.. _`newline-delimited JSON`: https://github.com/ndjson/ndjson-spec

.. tip::

   While Gazette has no formalized notion of a "topic", the *message-type* label
   is often a good first approximation for what we mean when thinking in terms
   of topics.

Preparing the Dataset
---------------------

The stream-rides job is running a script_ which pulls down, unpacks, and streams a portion
of the Ciki Bike system data. In this section we'll unpack the pieces of the processing pipeline
that it's running.

It begins by fetching a portion of the dataset:

.. code-block:: console

   $ curl -o 201909-citibike-tripdata.csv.zip https://s3.amazonaws.com/tripdata/201909-citibike-tripdata.csv.zip
   $ unzip 201909-citibike-tripdata.csv.zip

It then runs records through a ``gazctl attach-uuids`` Unix pipeline.
The Gazette broker service provides an at-least-once guarantee: it's possible
that an Append RPC is reported to the client as failed, requiring that the client
retry, even though the append was actually applied.

To support exactly-once message semantics atop an at-least-once broker service,
Gazette asks that messages take and carry a v1 UUID which it provides. The UUID composes
the pieces required for exactly-once processing, such as a unique ProducerID_
and a monotonic Clock_ -- which, together, act as a `Lamport timestamp`_.

.. _ProducerID:        https://godoc.org/go.gazette.dev/core/message#ProducerID
.. _Clock:             https://godoc.org/go.gazette.dev/core/message#Clock
.. _Lamport timestamp: https://en.wikipedia.org/wiki/Lamport_timestamps

Use the ``attach-uuids`` tool to simplify generating and attaching UUIDs.
It generates a UUID for each read input line and runs a configurable Go
text/template to combine into a final output. See its ``--help`` for more discussion.

.. note::

   When processing files in preparation for append to Gazette, it's best practice
   to attach UUIDs into new temporary file(s), and *then* append the temporary
   files to journals. This ensures messages are processed only once even if the
   preparation or append steps fail partway through and are restarted.

   Avoid appending large numbers of small files in this way, as each unique ProducerID
   and Clock must be tracked by read-committed readers. Instead, first combine many
   small files into few large ones before attaching UUIDs.

   For streaming data sources, ``attach-uuids`` can be composed into a Unix pipeline
   which processes and appends each record as it arrives.

After attaching UUIDs, the script_ runs records through an Awk pipeline.
When appending a given CSV row, we have to choose among multiple partitioned journals.
A sensible first strategy would be to select a journal at random for each record. Random
routing provides a strong guarantee that our data will distribute evenly across all
journal partitions, and is incredibly easy to scale. If we instead partition on a key
derived from the message, consideration must always be payed to how write volume
will distribute across journals: is our choice of partition key reasonably uniform?
Or does it exhibit heavy skew?

.. note::

   Many real-world partition schemes have lots of skew, aka "hot keys". A powerful
   pattern to help mitigate this is to introduce a second processing stage:

   - First append high-volume messages randomly across partitions, which ensures they're well-balanced.
   - Then introduce a consumer which performs limited reduction, typically just in the
     context of a current consumer transaction, and which then emits lower-volume message
     aggregates which are partitioned on the desired key.

For this use case we partition on "Bike ID", so that all rides of a given bike are
routed to the same journal partition. ``gazctl journals append`` implements several mapping
functions controlled via ``--mapping``, such as *random* and *modulo*. The latter
requires that the partition key appear alone on a line preceding its value. The
script_ uses an Awk pipeline to do so:

.. code-block:: console

   # Use awk to pluck the bike ID onto its own line, followed by the full record.
   $ awk -F "," '{print $13}{print}' 201909-citibike-tripdata.csv.with_uuids | head

We can put these steps together and start a streaming load of bike-share data
points into our journals. The script_ uses the ``pv`` tool to rate-limit
appends, turning the dataset into a streaming source:

.. code-block:: console

   $ awk -F "," '{print $13}{print}' 201909-citibike-tripdata.csv.with_uuids \
       | pv --line-mode --quiet --rate-limit 10 \
       | gazctl journals append -l app.gazette.dev/message-type=bikeshare.Ride --framing=lines --mapping=modulo --log.level=debug

.. _script: https://github.com/gazette/core/blob/master/kustomize/bases/example-bike-share/stream_rides.sh

Initializing a Store
--------------------

Next we need a place to keep some state. PostgreSQL is running in the cluster
with some initialized tables_, which we can inspect over our forwarded port:

.. _tables: https://github.com/gazette/core/blob/master/kustomize/test/bases/environment/postgres_tables.sql

.. code-block:: console

   $ psql postgres://test:test@localhost:32432 -c '\d'
                 List of relations
    Schema |        Name         | Type  | Owner
   --------+---------------------+-------+-------
    public | gazette_checkpoints | table | test
    public | rides               | table | test
   (2 rows)

The ``rides`` relation models rides from our dataset, while the
``gazette_checkpoints`` table will be used to persist checkpoints.

Examining Shards
-----------------

Shards are the unit-of-work for a Gazette consumer deployment. A shard can be thought
of as the composition of an Application_, one or more source journals to be read,
and a stateful store.

ShardSpecs_ for this example include labels that dynamically configure the
backing store for each shard: either a "remote" PostgreSQL database,
or an embedded SQLite database. As with ``gazctl journals``, the ``gazctl shards``
command can be used to inspect, select over, apply, and edit ShardSpecs.

.. _Application: https://godoc.org/go.gazette.dev/core/consumer#Application
.. _ShardSpecs: https://github.com/gazette/core/blob/master/kustomize/bases/example-bike-share/shard_specs.yaml

.. code-block:: console

   $ gazctl shards list -p -L store
   +-----------------+---------+----------------------------------------------+----------+
   |       ID        | STATUS  |                   PRIMARY                    |  STORE   |
   +-----------------+---------+----------------------------------------------+----------+
   | cycles-part-000 | PRIMARY | consumer-bike-share-78bcdbcf8b-q9vxl:PRIMARY | postgres |
   | cycles-part-001 | PRIMARY | consumer-bike-share-78bcdbcf8b-t85tb:PRIMARY | postgres |
   | cycles-part-002 | PRIMARY | consumer-bike-share-78bcdbcf8b-t85tb:PRIMARY | sqlite   |
   | cycles-part-003 | PRIMARY | consumer-bike-share-78bcdbcf8b-q9vxl:PRIMARY | sqlite   |
   +-----------------+---------+----------------------------------------------+----------+

   # List ShardSpecs using a PostgreSQL store, in YAML format.
   $ gazctl shards list -p -l store=postgres -o yaml
   common:
     max_txn_duration: 1s
     labels:
     - name: store
       value: postgres
   shards:
   - id: cycles-part-000
     sources:
     - journal: examples/bike-share/rides/part-000
     revision: 77
   - id: cycles-part-001
     sources:
     - journal: examples/bike-share/rides/part-001
     revision: 77

   # Inspect the processing "lag" of each shard (ie, an upper-bound estimate
   # of the number of bytes behind the current journal head).
   $ gazctl shards list --lag
   +-----------------+---------+------------------------------------------+
   |       ID        | STATUS  |                   LAG                    |
   +-----------------+---------+------------------------------------------+
   | cycles-part-000 | PRIMARY | examples/bike-share/rides/part-000:0     |
   | cycles-part-001 | PRIMARY | examples/bike-share/rides/part-001:10930 |
   | cycles-part-002 | PRIMARY | examples/bike-share/rides/part-002:7496  |
   | cycles-part-003 | PRIMARY | examples/bike-share/rides/part-003:7774  |
   +-----------------+---------+------------------------------------------+

.. note::

   Having multiple store types in use with a single consumer is pretty a-typical,
   and it's downright silly in this case. The bike-share example does so only
   to demonstrate the possibility, and to cover more ground.

Poking at PostgreSQL
--------------------

Run a query a few times to see that ride data-points are being loaded into the database:

.. code-block:: console

   $ psql postgres://test:test@localhost:32432 -x -c 'SELECT uuid,bike_id, start_time, start_station_name FROM rides ORDER BY start_time DESC LIMIT 3;'
   -[ RECORD 1 ]------+-------------------------------------
   uuid               | 032aa58b-f9b7-11e9-b400-0d04970419de
   bike_id            | 29568
   start_time         | 2019-09-30 17:53:32.845
   start_station_name | E 2 St & Avenue A
   -[ RECORD 2 ]------+-------------------------------------
   uuid               | 9a619832-f9b6-11e9-ac00-0d04970419de
   bike_id            | 32057
   start_time         | 2019-09-27 13:55:42.457
   start_station_name | Lafayette St & E 8 St
   -[ RECORD 3 ]------+-------------------------------------
   uuid               | bdd88fb7-f9b6-11e9-8800-0d04970419de
   bike_id            | 15307
   start_time         | 2019-09-28 14:09:05.298
   start_station_name | Grand Army Plaza & Plaza St West

We also see that a checkpoint row is being regularly updated for shards
``cycles-part-000`` and ``cycles-part-001`` (but not the other two shards).

.. code-block:: console

   $ psql postgres://test:test@localhost:32432 -x -c 'SELECT * FROM gazette_checkpoints;'
   -[ RECORD 1 ]------------------------------------------------------------------------------------------------------------------------------------------------------------
   shard_fqn  | /gazette/consumers/bike-share-bike-share/items/cycles-part-001
   fence      | 2
   checkpoint | \x0a4b0a226578616d706c65732f62696b652d73686172652f72696465732f706172742d303031122508c4e09642121e0a060d04970419de121409b5cf95c4709b9f1e10ffffffffffffffffff01
   -[ RECORD 2 ]------------------------------------------------------------------------------------------------------------------------------------------------------------
   shard_fqn  | /gazette/consumers/bike-share-bike-share/items/cycles-part-000
   fence      | 1
   checkpoint | \x0a4b0a226578616d706c65732f62696b652d73686172652f72696465732f706172742d303030122508a6ced542121e0a060d04970419de121409b1cf95c4709b9f1e10ffffffffffffffffff01

.. note::

   The ``fence`` column is used to implement a transactional write fence,
   as required by the consumer Store_ interface. ``fence`` is increased with
   each re-assignment of the shard to a new process.

.. _Store: https://godoc.org/go.gazette.dev/core/consumer#Store

Tailing Found Cycles
--------------------

The bike-share application processes records
`using a few SQL queries`__:

- It loads the unmodified record into the ``rides`` table.
- It windows ``rides`` rows of the record's bike ID to the N most-recent rides.
- It uses a recursive common table expression to search for a graph cycle
  of length >= 10 which was just completed by the bike. If found, it's
  written out as a Cycle message.

We can follow along with cycles as they're found by tailing their partitions,
and running through jq_ to pretty-print. It turns out they're not all that anomalous!

__ https://github.com/gazette/core/blob/master/examples/bike-share/sql_statements.go
.. _jq: https://stedolan.github.io/jq/

.. code-block:: console

   $ gazctl journals read -l app.gazette.dev/message-type=bike_share.Cycle --block | jq '.'
   {
     "UUID": "0d2b9119-f9b7-11e9-8c01-6fb7f64cdd31",
     "BikeID": 18871,
     "Steps": [
       {
         "Time": "2019-09-25T12:57:15.265Z",
         "Station": "Park Pl & Vanderbilt Ave"
       },
       {
         "Time": "2019-09-25T13:05:15.297Z",
         "Station": "Berkeley Pl & 7 Ave"
       },
       {
         "Time": "2019-09-25T13:34:49.286Z",
         "Station": "Schermerhorn St & Bond St"
       },
       {
         "Time": "2019-09-28T15:45:50.255Z",
         "Station": "Wyckoff St & Bond St"
       },
       ... etc ...
     ]
   }

.. note::

   In the output, you'll also see messages like
   ``{"UUID":"0d2b9119-f9b7-11e9-9002-6fb7f64cdd31","BikeID":0,"Steps":null}``,
   which were automatically generated and written by the framework in order to
   provide exactly-once semantics. These messages were initialized by a call to
   NewAcknowledgement_, and bear UUIDs which *acknowledge* a set of pending
   transaction messages that should now be considered as committed.

.. _NewAcknowledgement: https://godoc.org/go.gazette.dev/core/message#Message

Embedded SQLite and Recovery Logs
---------------------------------

Using a remote database as a shard's store can sometimes not be ideal: our bike-share
application is issuing an expensive query to the database and waiting for its response
with every message processed. That introduces two fundamental problems:

 * The query puts significant CPU pressure on the database. We can scale up a
   consumer by adding processes (up to the number of ShardSpecs), but there's
   only one database, and eventually it can become a bottleneck.

 * The database is accessed over a network, which means our consumer can never
   process a message any faster than the network round-trip time to the DB. At
   scale, even sub-millisecond RTTs can be a substantial throughput bottleneck.

The usage pattern and database driver implementation matter quite a bit: if the
application is only loading into the database, and those loads are asynchronous,
then the network RTT can often be amortized away. Or the application may be
able to cache and aggregate in-memory, turning many source events into a handful
of queries & table updates. And of course, at smaller scales using a RDBMS is
often an easy and convenient choice.

For uses cases which can benefit, Gazette offers local store implementations.
Shards ``cycles-part-002`` and ``cycles-part-003`` each use an embedded SQLite
instance instead of the remote PostgreSQL database. The framework automatically
records file states of these DBs to the recovery log journals we examined
earlier. When shard assignments change, newly assigned processes will tail and
recover the on-disk DB states from this log.

Other than the choice of store, the message processing flow and particular SQL
statements used by these shards are identical.

.. note::

   The ``gazctl shards prune -l my-selector`` command prunes recovery log fragments
   which are no longer referenced by recovery hints of the selected shards.

Querying Bike History
---------------------

The consumers framework makes it easy to offer APIs over gRPC, HTTP, and other
protocols. APIs are often a great way of "activating" data that's continuously
indexed and distributed across the embedded stores of a scaled consumer
application deployment. They're typically blazing fast since the API processing
logic is already co-resident with the data being served.

The bike-share example offers a simple HTTP API for fetching the most recent
rides of a given Bike ID.

.. code-block:: console

   $ curl "http://localhost:32190/api/bikes?id=38536"
   {"UUID":"afd344a9-f9e9-11e9-8000-573b3770b247","StartTime":"2019-09-07T00:04:31.064Z","EndTime":"2019-09-07T00:12:31.21Z","StartStation":"W 87 St \u0026 Amsterdam Ave","EndStation":"E 84 St \u0026 3 Ave"}
   {"UUID":"b0d6a8e2-f9e9-11e9-b000-573b3770b247","StartTime":"2019-09-07T07:14:37.698Z","EndTime":"2019-09-07T07:20:45.978Z","StartStation":"E 84 St \u0026 3 Ave","EndStation":"E 72 St \u0026 York Ave"}
   {"UUID":"b1caccfd-f9e9-11e9-b800-573b3770b247","StartTime":"2019-09-07T08:43:39.806Z","EndTime":"2019-09-07T08:51:35.018Z","StartStation":"E 72 St \u0026 York Ave","EndStation":"E 67 St \u0026 Park Ave"}
   {"UUID":"b5e7a842-f9e9-11e9-ac00-573b3770b247","StartTime":"2019-09-07T11:14:52.864Z","EndTime":"2019-09-07T12:02:37.312Z","StartStation":"E 67 St \u0026 Park Ave","EndStation":"Liberty St \u0026 Broadway"}
   ... etc ...

Consumers run as distributed applications, and in many cases a particular API
request may be served only from a specific ShardSpec (as is the case here,
since shards are partitioned on bike ID). For these cases it's best practice
to offer appropriate server-side proxying of API requests, by mapping requests
to corresponding journal partitions, and resolving_ to the local or remote shard
primary. See bike-share's `api.go`_ for a complete example of how this may be done.

.. _resolving: https://godoc.org/go.gazette.dev/core/consumer#Resolver
.. _api.go:    https://github.com/gazette/core/blob/master/examples/bike-share/api.go