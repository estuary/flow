---
description: How Flow persists collections in cloud storage
---

# Storage mappings

A storage mapping defines how Flow should persist the documents of collections into cloud storage locations, such as a user’s S3 bucket. When you first set up Flow, a default storage mapping is created for you, in which all collections will be stored by default. However, you can also override this default for one or more collections by specifying a storage mapping in your catalog specification.

Each storage mapping consists of a **catalog prefix** and a mapped storage location. For example:

```yaml
storageMappings:
  acmeCo/:
    stores:
      - provider: S3
        bucket: acmeco-bucket
        prefix: my-prefix/
```

This mapping causes Flow to store the data of any collection having prefix `acmeCo/` into `s3://acmeco-bucket/my-prefix/`. A collection like:

```yaml
collections:
  acmeCo/anvils:
    key: [/id]
    schema: anvil-schema.yaml
    projections:
      owner:
        location: /anvil/owner
        partition: true
```

...would store all of its data files under path `s3://acmeco-bucket/my-prefix/acmeCo/anvils/`.

Every Flow collection must have an associated storage mapping, and a catalog build will fail if multiple storage mappings have overlapping prefixes.

### Logical partitions

This collection also uses a logical partition, `owner`, which causes data to be further organized under sub-prefixes like `.../anvils/owner=bugs-bunny/` or `.../anvils/owner=Elmer/`. Those familiar with Apache Hive will notice that the layout is compatible with Hive’s partitioned layout. This enables Flow collections to integrate with systems that understand Hive partitioning, such as a [BigQuery external table](https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs)

### Recovery logs

Flow tasks — captures, derivations, and materializations — use recovery logs to durably store their processing context. Recovery logs are an opaque binary log, but may contain user data and are stored within the user’s buckets, and must have a defined storage mapping.\
\
The recovery logs of a task are always prefixed by `recovery/`, and a task named `acmeCo/produce-TNT` would require a storage mapping like:

```yaml
storageMappings:
  recovery/acmeCo/:
    stores:
      - provider: S3
        bucket: acmeco-recovery
```

You may wish to use a separate bucket for recovery logs, distinct from the bucket where collection data is stored. Buckets holding collection data are free to use a bucket lifecycle policy to manage data retention; for example, to remove data after six months.\
\
This is not true of buckets holding recovery logs. Flow prunes data from recovery logs once it is no longer required. Deleting data from recovery logs while it is still in use can cause Flow processing tasks to fail permanently.
