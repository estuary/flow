---
description: How Flow stores your data in cloud storage and accesses it at run-time
---

# Collection storage

Collections are stored as JSON files in cloud storage and further broken down into journals. Journals are moved in and out of cloud storage for task processing via brokers. This optimizes both storage and compute resources, allowing data flows to easily scale.

### How collections are stored

Each data collection in your Flow catalog is stored as regular JSON with a unique prefix in a cloud storage bucket. Each collection is made up of one or more **journals**. A journal is a durable and low-latency data stream that persists its content as files in cloud storage.

This means that the Flow catalog creates a data lake that's directly accessible to other tools. This is an important aspect of Flow's design, and it has some major implications for data storage.

* You can use tools including Snowflake, Spark, Hive, Pandas, and many others to read and process the data in your Flow collections.
* Flow can streamline data workflows simply by ingesting data and organizing it in cloud storage. Transformations may be helpful, but are not required.
* Flow coexists with your existing tools and workflows. If you ever decide to stop using Flow, there's no need to migrate your data out of it.
* You can configure data retention simply by using bucket policies to automatically delete unwanted
[fragment files](README.md#how-brokers-connect-collections-to-the-runtime).

Each collection has all of its data stored under a single prefix in a cloud storage bucket. No other collection's data can use that same prefix (this is one reason why collection names cannot be prefixes of one another). When collections have
[partitions](README.md#storage-of-partitioned-collections), the prefixes are further divided.&#x20;

Not every collection in your catalog needs to follow the same storage mapping. The bucket and prefix used for each collection are entirely up to you, and can be [configured in your catalog spec](../concepts/catalog-entities/storage-mappings.md).

### How brokers connect collections to the runtime

The Flow runtime reads your catalog spec and enacts captures, materializations, and derivations as needed. These **tasks** require access to the collections in the cloud storage bucket.&#x20;

To facilitate this access, the runtime evokes streaming data **brokers.** Brokers move collection data in and out of cloud storage by streaming the individual **journals** that comprise it.&#x20;

The number of broker processes can easily be scaled up or down without downtime, and completely independently. The number of broker processes is not coupled to anything in your Flow catalog; rather, it represents a global compute resource that will be shared among all the tasks managed by Flow. Data from the brokers is written to cloud storage as **fragment** files, which each store a contiguous sequence of documents of the collection.

The scaling of brokers is quite simple, but this is not something that is managed in your catalog spec or directly by the Flow user. It is instead managed by the operator of the Flow deployment. If you're an Estuary customer, it's handled for you.

### Journals

As mentioned above, journals are durable, low-latency data streams that comprise collections. They can also be thought of as append-only logs.

Journals are also what Flow uses under the hood to map logical and physical partitions of a collection. They can adapt to any scale and power Flow's low-latency architecture. More specifically, the advantages of journals include:

* Journal readers can fetch any amount of historical content directly from cloud storage, and then seamlessly switch to low-latency tailing reads from brokers upon catching up to the present.
* Brokers locally manage only very recent data; everything else is in cloud storage. This makes Flow cheap to operate and scale, since it doesn't require expensive and slow data migrations or large network-attached disks.
* Cloud-stored files directly hold journal content; if you write JSON to a journal, you get files of JSON in cloud storage. This makes it easy to integrate other systems or workflows that understand JSON files in S3.

### Broker guarantees

Using brokers to move data into and out of your cloud storage bucket provides strong durability and atomicity guarantees.&#x20;

* Once you get a successful response from ingesting data, that data is guaranteed to be replicated to the minimum number of brokers. Thus it is guaranteed to survive a loss or outage of all but one of those brokers, which is extremely durable if configured across regions or availability zones.
* Each collection has a configurable **flush interval**, which is the maximum amount of time before data from the brokers will be written to cloud storage. Values between five minutes and an hour often provide a good balance between throughput performance and durability in the face of catastrophic failures (those that result in the termination of all replicas).
* Data added as part of a single transaction is guaranteed to be written contiguously within a single fragment file per collection partition. In other words, if you add multiple JSON documents to a single collection partition as part of a single transaction, Flow will keep them all together within a single fragment file.
* Derivations also produce data in transactional commits, and thus have the same guarantees.

### Storage of partitioned collections

[Logically partitioned](../concepts/catalog-entities/other-entities.md#logical-partitions) collections share a single storage mapping prefix, but they add additional, separate prefixes for each distinct partition. Flow translates the partition keys and values into a storage location based on the broadly adopted convention established by Hive. This allows Hive and other tools that use the same convention to "just work" when you want to import or query data from your collection's cloud storage. For example, in Hive this convention is used to support predicate pushdowns, which makes queries against specific partitions much faster.

To illustrate this, let's look at an example collection of animal sightings that's partitioned on `genus` and`species`.

```yaml
collections:
  animals/sightings:
    schema:
      type: object
      properties:
        sightingId: {type: integer}
        genus: {type: string}
        species: {type: string}
        time: {type: string, format: date-time}
        location: { $ref: https://geojson.org/schema/Point.json}
      required: [sightingId, genus, species, time, location]
    key: [/sightingId]
    projections:
      genus:
        location: /genus
        partition: true
      species:
        location: /species
        partition: true
```

Given sightings of `Elephas maximus`,  `Felis catus`, and `Homo sapiens`,  you would end up with the following fragment files.

```
animals/sightings/genus=Felis/species=catus/pivot=00/utc_date=2021-03-22/utc_hour=14/0000000000000000-00000000000000c5-9cd2514f76b6bdb7e45a8ee9d3adb8f03fcabd39.gz
animals/sightings/genus=Homo/species=sapiens/pivot=00/utc_date=2021-03-22/utc_hour=14/0000000000000000-00000000000000c6-409a86cd6ad4e2f564432ee0e4bd2cfdb369163a.gz
animals/sightings/genus=Elephas/species=maximus/pivot=00/utc_date=2021-03-22/utc_hour=14/00000000000000c9-000000000000010e-8e3811a21945a747c82197d1eaa18424b73518a9.gz
```

#### Automatic storage partitions

Fragments will automatically be stored under separate prefixes for `pivot`, `utc_date`, and `utc_hour`. The `utc_date|hour` prefixes separate documents by the time that they were written. The `pivot` prefix separates fragments that were written by different [shards](scaling.md#processing-with-shards).

In addition to optimizing storage, partitions can also help optimize processing resources. See [Catalog tasks and shards](scaling.md#logical-partitions) to learn more.

### Collection storage for local development

When you test locally using `flowctl develop`, Flow never uses cloud storage. Data from all collections is instead stored under the `flowctl develop` directory on the local machine. Thus you can always run `flowctl develop` without fear of modifying production data in your cloud storage. Note that materializations can still reference production systems, so you still need to take care when running `flowctl develop` with a `--source` that may include materializations.

