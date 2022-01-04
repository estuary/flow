---
description: How Flow translates between JSON-based collections
---

# Projections

Flow documents are arbitrary JSON, and may contain multiple levels of hierarchy and nesting.
However systems that Flow integrates with often model flat tables of rows and columns, without hierarchy.
Others are somewhere in between.

**Projections** are the means by which Flow translates between the documents
of a collection and a table representation.
A projection defines a mapping between a structured document location,
given as a [JSON-Pointer](https://tools.ietf.org/html/rfc6901),
and a corresponding **field** name used as, for example, a CSV file header or SQL table column.

Many projections are inferred automatically from a collection’s JSON schema,
using a field that is simply the JSON Pointer with its leading slash removed.
For example a schema scalar with pointer `/myScalar` will generate a projection with field `myScalar`.

You can supplement by providing additional collection projections,
and a document location can have more than one projection field that references it.

Some examples:

```yaml
collections:
  acmeCo/user-sessions:
    schema: session.schema.yaml
    key: [/user/id, /timestamp]
    projections:
      # A "user/id" projection field is automatically inferred.
      # Add an supplemental field that doesn't have a slash.
      user_id: /user/id
      # Partly decompose a nested array of requests into a handful of named projections.
      "first request": /requests/0
      "second request": /requests/1
      "third request": /requests/2
```

## Logical Partitions

Projections can also be used to logically partition a collection,
specified as a longer-form variant of a projection definition:

```yaml
collections:
  acmeCo/user-sessions:
    schema: session.schema.yaml
    key: [/user/id, /timestamp]
    projections:
      # Define logical partitions over country and device type.
      country:
        location_ptr: /country
        partition: true
      device:
        location_ptr: /agent/type
        partition: true
      robot:
        location_ptr: /agent/robot
        partition: true
```

Logical partitions isolate the storage of documents by their differing
values for partitioned fields.
Under the hood, the partitioned fields of a document are applied to name and identify the
[journal](../../architecture/README.md#how-brokers-connect-collections-to-the-runtime)
into which the document is written, which in turn prescribes how journal fragment files are arranged within cloud storage.

For example, a document of "acmeCo/user-sessions" like:

```json
{"country": "CA", "agent": {"type": "iPhone"}, ...}
```

Would map to a journal prefix of
`example/sessions/country=CA/device=iPhone/`,
which in turn would produce fragment files in cloud storage like:

```
s3://bucket/example/sessions/country=CA/device=iPhone/pivot=00/utc_date=2020-11-04/utc_hour=16/<name>.gz
```

:::info
`pivot` identifies a _physical partition_,
while `utc_date` and `utc_hour` reflect the time at which the journal fragment was created.

Within a logical partition, there are one or more physical partitions, each a journal, into which documents are actually written. The logical partition prefix is extended with a `pivot` suffix to arrive at a concrete journal name.

Flow is designed so that physical partitions can be dynamically added at any time,
to scale the write and read throughput capacity of a collection.
:::

### Partition Selectors

When reading from a collection, Flow catalog entities like derivations, materializations,
and tests can provide a **partition selector** which identifies the subset
of partitions that should be read from a source collection:

```yaml
# Partition selectors are included as part of a larger entity,
# such as a derivation or materialization.
partitions:
  # `include` selects partitioned fields and corresponding values which
  # must be matched in order for a partition to be processed.
  # All of the included fields must be matched.
  # Default: All partitions are included. type: object
  include:
    # Include partitions from North America.
    country: [US, CA]
    # AND where the device is a mobile phone.
    device: [iPhone, Android]

  # `exclude` selects partitioned fields and corresponding values which,
  # if matched, exclude the partition from being processed.
  # A match of any of the excluded fields will exclude the partition.
  # Default: No partitions are excluded. type: object
  exclude:
    # Don't process sessions from robots.
    robot: [true]
```

Partition selectors are efficient as they allow Flow to altogether
avoid reading documents that aren’t needed.

### Hive Layouts

As discussed in [Logical Partitions](#logical-partitions),
Flow ultimately produces fragment files in cloud storage with names like:

```
s3://bucket/example/sessions/country=CA/device=iPhone/pivot=00/utc_date=2020-11-04/utc_hour=16/<name>.gz
```

If you're familiar with Apache Hive, this layout should feel familiar.
Flow names and organizes collection fragment files to make them directly usable
by tools that understand Hive partitioning, like Spark and Hive itself.
Collections can also be integrated as Hive-compatible external tables
in tools like
[Snowflake](https://docs.snowflake.com/en/user-guide/tables-external-intro.html#partitioned-external-tables)
and
[BigQuery](https://cloud.google.com/bigquery/docs/hive-partitioned-queries-gcs).


SQL queries against these tables can even utilize **predicate push-down**,
taking query predicates over `country`, `device`, or `utc_date` and `utc_hour`
and pushing them down into the selection of files that must be read to answer
the query — often offering much faster and more efficient query execution because
far less data must be read.