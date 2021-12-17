---
description: How Flow adapts to process data at any scale.
---

# Task processing

The Flow runtime uses a stateful _consumer framework,_ which divides tasks across shards to manage resources. This allows it to scale from a single process all the way to large distributed deployments that can handle massive amounts of data. You can start small and scale up gradually, without complicated migrations.&#x20;

### Processing with shards

Each task within the runtime — captures, derivations, and materializations — is handled by one or more **shards**. A shard is a fault-tolerant and stateful task that the runtime assigns and runs on a scalable pool of compute resources.

The number of shards corresponds to the amount of parallelism for each task. Each shard has its own recovery log and register storage and acts more or less independently of all the others. Flow uses consistent hashing to ensure that documents with the same key will always be processed by the same shard. It also hashes the keys to ensure consistent performance in face of highly skewed key ranges.

In future releases of Flow, you'll be able to directly control the number of shards for each task. By splitting the keyspace each shard handles, you will be able to increase the number of shards for a given task. This will allow processes to quickly and easily scale large enough to handle any data volume.

#### Flow ingester

If you're using [`flow-ingester`](../reference/pushing-data-into-flow/) instead of captures to ingest data into Flow, it won't use shards, but it is still a scalable service. This binary handles the ingestion of collection data via REST or Websockets. It's a small stateless service that can also be independently scaled without downtime and without any sort of coupling to your catalogs or tasks. `flow-ingester` is shared amongst all non-capture ingestion processes. Unlike with shards, scaling with `flow-ingester` can be an infrastructure concern. For this and other reasons, captures are the preferred method for data ingestion in Flow.

### Optimizing processing with partitions

Logical [partitioning](../concepts/catalog-entities/other-entities.md#logical-partitions) can optimize catalog tasks by only reading from the desired partitions and filtering out unneeded data. Leveraging partitions is an easy way to improve performance.

Let's say you have a collection of animal sightings partitioned on `family`, `genus`, and `species`. Now you want to add a derivation that only wants to process sightings of `Felis catus`. The derivation _could_ read every single document in the collection and simply filter out those that it doesn't care about. But doing so means that you use a lot of compute and network capacity reading all the documents that you end up filtering out. Derivations may define a partition selector instead to only read from the partitions they actually care about. For example, you might use the following selector in a derivation if you only wanted to process sightings of house cats.

```yaml
# within a derivation
    source: 
      name: animals/sightings
      partitions:
        include:
          family: [Felidae]
          genus: [Felis]
          species: [catus]
```

