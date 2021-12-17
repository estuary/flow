---
description: How Flow stores data
---

# Collections

Flow stores data in **collections**: append-only sets of immutable JSON documents. Collections are either _captured_, meaning documents come directly from an external source, or they’re _derived_ by applying transformations to other collections, which may themselves be either captured or derived.&#x20;

Every collection has an associated schema that documents must validate against, and the schema can be a strict or lenient as you'd like, as long as it has a key.&#x20;

### How collections work

Collections are hybrids of low-latency streams and batch datasets. Stream readers receive an added document within milliseconds of its being committed. Once written, documents group into regular JSON files and persist into an organized layout in cloud storage (a "data lake"). Your default or customized [storage mapping](storage-mappings.md) determines the exact cloud storage location of each collection. Flow understands how persisted files stitch back together with the stream, and readers of collections transparently switch between direct file reads and low-latency streaming.\
\
Persisted files integrate with your existing batch tools, like Spark and MapReduce. They use Hive-compatible partitioning, which systems like BigQuery and Snowflake use to read only the subset of files that match the `WHERE` clause of your ad-hoc query (known as _predicate push-down)_.

### Collection schemas

Every Flow collection must declare a JSON schema. Flow will never allow data to be added to a collection that does not validate against a schema. This helps ensure the quality of your data products and the reliability of your derivations and materializations.&#x20;

Schemas must be referenced within the collection definition, but should be stored in a separate file rather than being defined inline.

[Learn more about schemas](schemas-and-data-reductions.md)

### Collection keys

Every Flow collection must declare a `key`. This key represents a set of one or more fields within each document that uniquely define a record within that collection. Let's clarify that with an example.

{% tabs %}
{% tab title="flow.yaml" %}
```yaml
  collections:
    users:
      schema: schemas.yaml#/$defs/UserSchema
      key: [/userId]
```
{% endtab %}

{% tab title="schemas.yaml" %}
```yaml
$defs:
  UserSchema:
   type: object
      properties:
        userId: {type: integer}
        name: {type: string}
      required: [userId, name]
```
{% endtab %}
{% endtabs %}

Say our `users` collection has the following JSON documents:

```
{"userId": 1, "name": "Billy"}
{"userId": 1, "name": "William"}
{"userId": 1, "name": "Will"}
```

Since we've declared the `key` as being the `userId`, Flow knows to interpret the subsequent documents with the same `userId` value as updates to the same entity. So if you materialized this collection to a database, you'd end up with a single row:

```
userId | name
1      | Will
```

If you had instead declared the key as `[/name]`, you would end up with 3 rows in your database, one for each unique `name`.

#### Allowed key field types

Not all JSON types are appropriate for use as a key. Flow is opinionated about collection keys so that it can help you avoid common pitfalls. For example, floating-point numbers notoriously make terrible keys. So do objects, because of the potential ambiguity of field order, and the fact that each object may have different fields.&#x20;

The following are the only allowed field types in keyed locations:

* `integer`
* `string`
* `boolean`

The Flow runtime allows each keyed location to have multiple types.
However, this may be prohibited by certain materialization [connectors](../connectors.md). This is to prevent errors in the endpoint system.

The below example would be allowed by Flow, but prohibited by a connector to a SQL database, because such a database can't represent a primary key that could be either `integer` or `string`.

{% tabs %}
{% tab title="flow.yaml" %}
```yaml
collections:
  invalid/key-multiple-types:
    schema: schemas.yaml#/$defs/KeySchema
    key: [/id]
```
{% endtab %}

{% tab title="schemas.yaml" %}
```yaml
$defs:
  KeySchema:
    type: object
      properties:
        id: {type: [integer, string]}
        value: {type: string}
      required: [value]

```
{% endtab %}
{% endtabs %}

#### Keys can be compound

While you're not allowed to use object and array fields as keys, you _can_ use multiple fields _within_ objects and arrays as the key. Take the following example:

{% tabs %}
{% tab title="flow.yaml" %}
```yaml
compound-key:
  schema: schemas.yaml#/$defs/compKeySchema
  key: [/foo/a, /foo/b, /foo/c]
```
{% endtab %}

{% tab title="schemas.yaml" %}
```yaml
$defs:
  compKeySchema:
    type: object
    properties:
      foo:
        type: object
        properties:
          a: {type: integer}
          b: {type: string}
          c: {type: boolean}
        required: [a, b, c]
    required: [foo]
```
{% endtab %}
{% endtabs %}

While you may not use `/foo` as a key, you _may_ use a compound key, like `[/foo/a, /foo/b, /foo/c]`.&#x20;

#### Keys may be unique to each document

Oftentimes, keys correspond to some domain entity (for example, a user). But it's perfectly fine to have keys that are unique to each _document._ For example, you could use a UUID as a key, or have a compound key that includes a timestamp field.

### Other characteristics of collections

When working with collections, it's important to know how they're implemented to understand what they are and what they aren't.

#### **Collections are optimized for low-latency processing**.

> As documents are added to a collection, materializations and derivations that use that collection are immediately notified (within milliseconds). This allows Flow to minimize end-to-end processing latency.

#### **Collections are “data lakes”**.

> Collections organize, index, and durably store documents within a hierarchy of files implemented atop cloud storage. These files are Flow’s native, source-of-truth representation for the contents of the collection, and can be managed and deleted using regular bucket life-cycle policies.
>
> Files hold collection documents with no special formatting (for example, as JSON lines), and can be directly processed using Spark and other preferred tools.

#### **Collections can be of unbounded size**.

> The Flow runtime persists data to cloud storage as soon as possible, and uses machine disks only for temporary data and indexes. Collection retention policies can be driven only by your organizational requirements – not your available disk space.
>
> A new derivation or materialization will efficiently back-fill over all collection documents – even where they span months or even years of data – by reading directly out of cloud storage.
>
> Crucially, a high scale back-fill that sources from a collection doesn’t compete with and cannot harm the collection’s ability to accept new writes, as reads depend _only_ on cloud storage for serving up historical data. This is a guarantee that’s unique to [Flow's architecture.](../../architecture/)

#### **Collections may have logical partitions**.

> Logical partitions are defined in terms of a [JSON-Pointer](https://tools.ietf.org/html/rfc6901): the pointer `/region` would extract a partition value of “EU” from collection document `{"region": "EU", ...}`.
>
> Documents are segregated by partition values, and are organized within cloud storage using a Hive-compatible layout. Partitioned collections are directly interpretable as external tables by tools that understand Hive partitioning and predicate push-down, like Snowflake, BigQuery, and Hive itself.
>
> Each logical partition will have one or more _physical_ partitions, backed by a corresponding Gazette journal. Physical partitions are largely transparent to users, but enable Flow to scale out processing as the data rate increases, and may be added at any time.  More information on logical partitions can be found [here](other-entities.md#logical-partitions).

To learn more about collection options and programming elements, see the [collections reference documentation](../../reference/catalog-reference/collections.md).
