# Journals

:::tip
Journals are an advanced concept of Flow.
You can use Flow without knowing the details of journals,
but this section may help you better understand how Flow works.
:::

Flow collections store their data in one or more **journals**,
resources resembling files.
Journals are part of the Gazette project.
[See Gazette's Journal concepts page for details](
https://gazette.readthedocs.io/en/latest/brokers-concepts.html#journals).
The number of journals that comprise a given collection depends
on how the collection is partitioned.

Journals are a real-time data lake.
Historical journal data is stored as an organized layout of
[fragment files](#fragment-files) in cloud storage.
Fragment files are regular files that collectively hold the journal's content.
Just-written data is held in a replicated buffer,
where it is immediately available to readers.
From there, buffers are regularly persisted
to your bucket for long-term storage.

Journals may be read from any offset.
Readers of historical data,
such as a new materialization or derivation task,
fetch files directly from your bucket for efficiency and throughput.
Then, as they reach the present, they automatically switch to
streaming new documents within milliseconds of their being written.

![](<journals.svg>)

All data of a collection is stored as regular JSON files
under a common and unique prefix within your cloud storage bucket.
For example, all fragment files of collection `acmeCo/orders`
would live under the storage prefix
`s3://acmeCo-bucket/acmeCo/orders`.

Files are **directly accessible** by other tools.
This is an important aspect of Flow's design,
and it has some major implications:

* You can use tools including Snowflake, Spark, Hive, Pandas,
  and many others to read and process the data in your Flow collections.
* You can capture and organize data into Flow collections
  without knowing how it will be used quite yet.
  Perform ad-hoc analysis using the collection data lake,
  and layer in [derivations](../derivations.md)
  or [materializations](/concepts/materialization) later,
  or not at all.
* If you ever decide to stop using Flow,
  your data is still yours.
  There's no lock-in or need to migrate data out.
* Removing files from your bucket also removes them from your collection.
  Apply bucket lifecycle policies or directly delete files to permanently
  drop their contents from the collection.

Flow collections have one or more
[logical partitions](./projections.md#logical-partitions),
and each logical partition has one or more
[physical partitions](#physical-partitions).
Every physical partition is implemented as a **journal**,
and a journal may have many [fragment files](#fragment-files).

| Entity | Example |
| --- | --- |
| Collection | **`acmeCo/orders`** |
| Logical Partition | `acmeCo/orders/`**`category=Anvils`** |
| Physical Partition / Journal | `acmeCo/orders/category=Anvils/`**`pivot=00`** |
| Journal Storage | **`s3://acmeCo-bucket/`**`acmeCo/orders/category=Anvils/pivot=00` |
| Fragment File | `s3://acmeCo-bucket/acmeCo/orders/category=Anvils/pivot=00/`**`utc_date=2022-01-07/utc_hour=19/0000000000000000-00000000201a3f27-1ec69e2de187b7720fb864a8cd6d50bb69cc7f26.gz`** |

## Specification

Flow [collections](../collections.md) can control some aspects of how
their contents are mapped into journals through the `journals` stanza:

```yaml
collections:
  acmeCo/orders:
    schema: orders.schema.yaml
    key: [/id]

    journals:
      # Configuration for journal fragments.
      # Required, type: object.
      fragments:
        # Codec used to compress fragment files.
        # One of ZSTANDARD, SNAPPY, GZIP, or NONE.
        # Optional. Default is GZIP.
        compressionCodec: GZIP
        # Maximum flush delay before in-progress fragment buffers are closed
        # and persisted. Default uses no flush interval.
        # Optional. Given as a time duration.
        flushInterval: 15m
        # Desired content length of each fragment, in megabytes before compression.
        # Default is 512MB.
        # Optional, type: integer.
        length: 512
        # Duration for which historical files of the collection should be kept.
        # Default is forever.
        # Optional. Given as a time duration.
        retention: 720h
```

Your [storage mappings](../storage-mappings.md) determine
which of your cloud storage buckets is used
for storage of collection fragment files.

## Physical partitions

Every logical partition of a Flow collection
is created with a single physical partition.
Later and as required, new physical partitions are added
in order to increase the write throughput of the collection.

Each physical partition is responsible for all new writes
covering a range of keys occurring in collection documents.
Conceptually, if keys range from [A-Z] then one partition
might cover [A-F] while another covers [G-Z].
The `pivot` of a partition reflects the first key
in its covered range.
One physical partition is turned into more partitions
by subdividing its range of key ownership.
For instance, a partition covering [A-F]
is split into partitions [A-C] and [D-F].

Physical partitions are journals.
The relationship between the journal and
its specific collection and logical partition are
encoded within
[its journal specification](
  https://gazette.readthedocs.io/en/latest/brokers-concepts.html#specifications
).

## Fragment files

Journal fragment files each hold a slice of your collection's content,
stored as a compressed file of newline-delimited JSON documents
in your cloud storage bucket.

Files are flushed to cloud storage periodically,
typically after they reach a desired size threshold.
They use a content-addressed naming scheme
which allows Flow to understand
how each file stitches into the overall journal.
Consider a fragment file path like:

`
s3://acmeCo-bucket/acmeCo/orders/category=Anvils/pivot=00/utc_date=2022-01-07/utc_hour=19/0000000000000000-00000000201a3f27-1ec69e2de187b7720fb864a8cd6d50bb69cc7f26.gz
`

This path has the following components:

| Component | Example |
| --- | --- |
| Storage prefix of physical partition | `s3://acmeCo-bucket/acmeCo/orders/category=Anvils/pivot=00/` |
| Supplemental time pseudo-partitions | `utc_date=2022-01-07/utc_hour=19/` |
| Beginning content offset | `0000000000000000` |
| Ending content offset | `00000000201a3f27` |
| SHA content checksum | `1ec69e2de187b7720fb864a8cd6d50bb69cc7f26` |
| Compression codec | `.gz` |

The supplemental **time pseudo-partitions** are not logical partitions,
but are added to each fragment file path to facilitate
integration with external tools that understand **Hive layouts**.

## Hive layouts

As we've seen,
collection fragment files are written to cloud storage
with path components like
`/category=Anvils/pivot=00/utc_date=2022-01-07/utc_hour=19/`.

If you've used tools within the Apache Hive ecosystem, this layout should feel familiar.
Flow organizes files in this way to make them directly usable
by tools that understand Hive partitioning, like Spark and Hive itself.
Collections can also be integrated as Hive-compatible external tables
in tools like
[Snowflake](https://docs.snowflake.com/en/user-guide/tables-external-intro.html#partitioned-external-tables)
and
[BigQuery](https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs)
for ad-hoc analysis.

SQL queries against these tables can even utilize **predicate push-down**,
taking query predicates over `category`, `utc_date`, and `utc_hour`
and pushing them down into the selection of files that must be read to answer
the query — often offering much faster and more efficient query execution because
far less data must be read.