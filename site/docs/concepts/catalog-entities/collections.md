---
description: How Flow stores data
---

# Collections

Flow stores data in **collections**: append-only sets of immutable JSON documents.
Collections may be appended to by **captures**, or be _derived_ from transformed source collections.

Every collection has a key and an associated JSON schema that documents must validate against.
JSON schema is flexible and the schema could be exactingly strict,
extremely permissive, or somewhere in between.

## Storage

Collections are hybrids of low-latency streams and batch datasets.
Stream readers receive an added document within milliseconds of its being committed.
Once written, documents group into regular JSON files and persist into an organized layout in cloud storage (a "data lake").
Your [storage mappings](storage-mappings.md) determine the cloud storage location of each collection.
Flow understands how persisted files stitch back together with the stream,
and readers of collections transparently switch between direct read from cloud storage and low-latency streaming.

Persisted files integrate with your existing batch tools, like Spark and MapReduce.
They use Hive-compatible partitioning, which systems like BigQuery and Snowflake use
to read only the subset of files that match the `WHERE` clause of your ad-hoc query (known as _predicate push-down_).

Learn more about how Flow stores data in the [architecture documentation](../../architecture/storage.md).

## Schemas

Every Flow collection must declare a JSON schema.
Flow will never allow data to be added to a collection that does not validate against a schema.
This helps ensure the quality of your data products and the reliability of your derivations and materializations.

Schemas may either be declared inline, or provided as a reference to a file.
References can also include JSON pointers as a URL fragment to name a specific schema of a larger schema document:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="Inline" default>

```yaml
collections:
  acmeCo/collection:
    schema:
      type: object
      required: [id]
      properties:
        id: string
    key: [/id]
```

</TabItem>
<TabItem value="File Reference">

```yaml
collections:
  acmeCo/collection:
    schema: ../path/to/collection.schema.yaml
    key: [/id]
```

</TabItem>
<TabItem value="Reference with Pointer">

```yaml
collections:
  acmeCo/collection:
    schema: ../path/to/collection.schema.yaml#/definitions/mySchema
    key: [/id]
```

</TabItem>
</Tabs>

[Learn more about schemas](schemas-and-data-reductions.md)

## Keys

Every Flow collection must declare a `key` which is used to group its documents.
Keys are specified as an array of JSON pointers to document locations. For example:

<Tabs>
<TabItem value="flow.yaml" default>

```yaml
collections:
  acmeCo/users:
    schema: schema.yaml
    key: [/userId]
```

</TabItem>
<TabItem value="schema.yaml" default>

```yaml
type: object
  properties:
    userId: {type: integer}
    name: {type: string}
  required: [userId, name]
```

</TabItem>
</Tabs>

Suppose the following JSON documents are captured into `acmeCo/users`:

```json
{"userId": 1, "name": "Will"}
{"userId": 1, "name": "William"}
{"userId": 1, "name": "Will"}
```

As its key is `[/userId]`, a materialization of the collection into a database table will reduce to a single row:

```
userId | name
1      | Will
```

If its key were instead `[/name]`, there would be two rows in the table:

```
userId | name
1      | Will
1      | William
```

### Schema Restrictions

Keyed document locations may be of a limited set of allowed types:

* `boolean`
* `integer`
* `string`

Excluded types are:

* `array`
* `null`
* `object`
* Fractional `number`

Keyed fields also must always exist in collection documents.
Flow performs static inference of the collection schema to verify the existence
and types of all keyed document locations, and will report an error if the
location could not exist, or could exist with the wrong type.

Flow itself doesn't mind if a keyed location could have multiple types,
so long as they're each of the allowed types: an `integer` or `string` for example.
Some materialization [connectors](../connectors.md) however may impose further type
restrictions as required by the endpoint.
For example, SQL databases do not support multiple types for a primary key.

### Composite Keys

A collection may have multiple locations which collectively form a composite key.
This can include locations within nested objects and arrays:

<Tabs>
<TabItem value="flow.yaml" default>

```yaml
collections:
  acmeCo/compound-key:
    schema: schema.yaml
    key: [/foo/a, /foo/b, /foo/c/0, /foo/c/1]
```

</TabItem>
<TabItem value="schema.yaml" default>

```yaml
type: object
required: [foo]
properties:
  foo:
    type: object
    required: [a, b, c]
    properties:
      a: {type: integer}
      b: {type: string}
      c:
        type: array
        items: {type: boolean}
        minItems: 2
```

</TabItem>
</Tabs>

### Key Behaviors

A collection key instructs Flow how documents of a collection are to be
reduced, such as while being materialized to an endpoint.
Flow also performs opportunistic local reductions over windows of documents
(often called a "combine") to improve its performance and reduce the volumes
of data at each processing stage.

An important subtlety is that the underlying storage of a collection
will potentially retain _many_ documents of a given key.

In the [acmeCo/users example](#keys), each of the "Will" vs "William" variants
is likely represented in the collection's storage -- so long as they didn't
arrive so closely together that they were locally combined by Flow.
If desired a derivation could re-key the collection
on `[/userId, /name]` to materialize the various `/name`s seen for a `/userId`.

This property makes keys less "lossy" than they might otherwise appear,
and it is generally good practice to chose a key that reflects how
you wish to _query_ a collection, rather than an exhaustive key
that's certain to be unique for every document.

## Characteristics of Collections

When working with collections, it's important to know how they're implemented to understand what they are and what they aren't.

### Optimized for Low-Latency Processing

As documents are added to a collection, materializations and derivations that use that collection are immediately notified (within milliseconds). This allows Flow to minimize end-to-end processing latency.

### As Data Lakes

Collections organize, index, and durably store documents within a hierarchy of files implemented atop cloud storage. These files are Flow’s native, source-of-truth representation for the contents of the collection, and can be managed and deleted using regular bucket life-cycle policies.

Files hold collection documents with no special formatting (for example, as JSON lines), and can be directly processed using Spark and other preferred tools.

### Unbounded Size

The Flow runtime persists data to cloud storage as soon as possible, and uses machine disks only for temporary data and indexes. Collection retention policies can be driven only by your organizational requirements – not your available disk space.

A new derivation or materialization will efficiently back-fill over all collection documents – even where they span months or even years of data – by reading directly out of cloud storage.

Crucially, a high scale back-fill that sources from a collection doesn’t compete with and cannot harm the collection’s ability to accept new writes, as reads depend _only_ on cloud storage for serving up historical data. This is a guarantee that’s unique to [Flow's architecture.](../../architecture/)

### Logical Partitions

Logical partitions are defined in terms of a [JSON-Pointer](https://tools.ietf.org/html/rfc6901): the pointer `/region` would extract a partition value of “EU” from collection document `{"region": "EU", ...}`.

Documents are segregated by partition values, and are organized within cloud storage using a Hive-compatible layout. Partitioned collections are directly interpretable as external tables by tools that understand Hive partitioning and predicate push-down, like Snowflake, BigQuery, and Hive itself.

Each logical partition will have one or more _physical_ partitions, backed by a corresponding Gazette journal.
Physical partitions are largely transparent to users, but enable Flow to scale out processing as the data rate increases, and may be added at any time.

Learn more about [logical partitions](projections.md#logical-partitions).