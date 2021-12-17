---
description: How Flow translates between JSON-based collections
---

# Projections and partitions

Flow documents are arbitrary JSON, and may contain multiple levels of hierarchy and nesting. However, systems that Flow integrates with often model flattened tables with rows and columns, but no nesting. Others are somewhere in between.

**Projections** are the means by which Flow translates between the JSON documents of a collection and a table representation. A projection defines a mapping between a structured document location (as a [JSON-Pointer](https://tools.ietf.org/html/rfc6901)) and a corresponding column name (a “field”) in, for example, a CSV file or SQL table.

Many projections are inferred automatically from a collection’s JSON schema, using a field that is simply the JSON-Pointer with its leading slash removed. For example, a schema scalar with pointer `/myScalar` will generate a projection with field `myScalar`.

You can supplement by providing additional collection projections, and a document location can have more than one projection field that references it. Projections are also how logical partitions of a collection are declared.

Some examples:

```yaml
collections:
- name: example/sessions
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
        # Define logical partitions over country and device type.
        country:
            location_ptr: /country
            partition: true
        device:
            location_ptr: /agent/type
            partition: true
```

### Logical partitions

A logical partition of a collection is a projection that physically segregates the storage of documents by the partitioned value. Derived collections can in turn provide a **partition selector** that identifies the subset of partitions that should be read from the source collection:

```yaml
collections:
- name: example/derived
  derivation:
    transform:
        fromSessions:
            source:
                name: example/sessions
                partitions:
                    include:
                        country: [US, CA]
                    exclude:
                        device: [Desktop]
```

Partition selectors are very efficient, as they allow Flow to altogether avoid reading documents that aren’t needed by the derived collection, and thus [improve performance](../../architecture/scaling.md#optimizing-processing-with-partitions).

#### Partitions make data warehousing more efficient

Partitions also enable **predicate push-down** when directly processing collection files using tools that understand Hive partitioning, like Snowflake, BigQuery, and Spark. Under the hood, the partitioned fields of a document are applied to name and identify the [journal](../../architecture/README.md#how-brokers-connect-collections-to-the-runtime) into which the document is written, which in turn prescribes how journal fragment files are arranged within cloud storage.

For example, a document of "example/sessions" like `{"country": "CA", "agent": {"type": "iPhone"}, ...}` would map to a journal prefix of `example/sessions/country=CA/device=iPhone/`, which in turn would produce fragment files in cloud storage like: `s3://bucket/example/sessions/country=CA/device=iPhone/pivot=00/utc_date=2020-11-04/utc_hour=16/<name>.gz`.

Tools that understand Hive partitioning are able to take query predicates over `“country”`, `“device”`, or `“utc_date/hour”` and push them down into the selection of files that must be read to answer the query — often offering much faster and more efficient query execution because far less data must be read.

{% hint style="info" %}
`“pivot”` identifies a _physical partition_, while `“utc_date”` and `“utc_hour”` reflect the time at which the journal fragment was created.

Within a logical partition, there are one or more physical partitions, each a journal, into which documents are actually written. The logical partition prefix is extended with a `“pivot”` suffix to arrive at a concrete journal name.

Flow is designed so that physical partitions can be dynamically added at any time, to scale the write and read throughput capacity of a collection.
{% endhint %}



###
