---
sidebar_position: 3
---
# Collections

The documents of your Data Flows are stored in **collections**:
real-time data lakes of JSON documents in cloud storage.

The data in a collection may be [captured](./captures.md) from an external system,
or [derived](./derivations.md) as a transformation of one or more other collections.
When you [create a new capture in a typical workflow](../guides/create-dataflow.md#create-a-capture),
you define one or more new collections as part of that process.
[Materializations](./materialization.md) then read data from collections.

Every collection has a key and an associated [schema](#schemas)
that its documents must validate against.

## Viewing collection data

In many cases, it's not necessary to view your collection data — you're able to materialize it directly to a destination in the correct shape using a [connector](../concepts/README.md#connectors).

However, it can be helpful to view collection data to confirm the source data was captured as expected, or verify a schema change.

#### In the web application

Sign into the Flow web application and click the **Collections** tab. The collections to which you have access are listed.
Click the **Details** drop down to show a sample of collection data as well as the collection [specification](#specification).

The collection documents are displayed by key. Click the desired key to preview it in its native JSON format.

#### Using the flowctl CLI

In your [authenticated flowctl session](../reference/authentication.md#authenticating-flow-using-the-cli), issue the command `flowctl collections read --collection <full/collection-name> --uncommitted`. For example, `flowctl collections read --collection acmeCo/inventory/anvils --uncommitted`.

Options are available to read a subset of data from collections.
For example, `--since` allows you to specify an approximate start time from which to read data, and
`--include-partition` allows you to read only data from a specified [logical partition](../concepts/advanced/projections.md#logical-partitions).
Use `flowctl collections read --help` to see documentation for all options.

:::info Beta
While in beta, this command currently has the following limitations. They will be removed in a later release:

* The `--uncommitted` flag is required. This means that all collection documents are read, regardless of whether they were successfully committed or not.
In the future, reads of committed documents will be the default.

* Only reads of a single [partition](../concepts/advanced/projections.md#logical-partitions) are supported. If you need to read from a partitioned collection, use `--include-partition` or `--exclude-partition` to narrow down to a single partition.

* The `--output` flag is not usable for this command. Only JSON data can be read from collections.
:::

## Specification

Collections are defined in Flow specification files per the following format:

```yaml
# A set of collections to include in the catalog.
# Optional, type: object
collections:
  # The unique name of the collection.
  acmeCo/products/anvils:

    # The schema of the collection, against which collection documents
    # are validated. This may be an inline definition or a relative URI
    # reference.
    # Required, type: string (relative URI form) or object (inline form)
    schema: anvils.schema.yaml

    # The key of the collection, specified as JSON pointers of one or more
    # locations within collection documents. If multiple fields are given,
    # they act as a composite key, equivalent to a SQL table PRIMARY KEY
    # with multiple table columns.
    # Required, type: array
    key: [/product/id]

    # Projections and logical partitions for this collection.
    # Optional, type: object
    projections:

    # Derivation that builds this collection from others through transformations.
    # See the "Derivations" concept page to learn more.
    # Optional, type: object
    derivation:
```

## Schemas

Every Flow collection must declare a schema,
and will never accept documents
that do not validate against the schema.
This helps ensure the quality of your data products
and the reliability of your derivations and materializations.
Schema specifications are flexible:
yours could be exactingly strict, extremely permissive, or somewhere in between.
For many source types, Flow is able to generate a basic schema during [discovery](./captures.md#discovery).

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
<TabItem value="File reference">

```yaml
collections:
  acmeCo/collection:
    schema: ../path/to/collection.schema.yaml
    key: [/id]
```

</TabItem>
<TabItem value="Reference with pointer">

```yaml
collections:
  acmeCo/collection:
    schema: ../path/to/collection.schema.yaml#/definitions/mySchema
    key: [/id]
```

</TabItem>
</Tabs>

[Learn more about schemas](schemas.md)

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

### Schema restrictions

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

Flow itself doesn't mind if a keyed location has multiple types,
so long as they're each of the allowed types: an `integer` or `string` for example.
Some materialization [connectors](connectors.md), however, may impose further type
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

### Key behaviors

A collection key instructs Flow how documents of a collection are to be
reduced, such as while being materialized to an endpoint.
Flow also performs opportunistic local reductions over windows of documents
to improve its performance and reduce the volumes
of data at each processing stage.

An important subtlety is that the underlying storage of a collection
will potentially retain _many_ documents of a given key.

In the [acmeCo/users example](#keys), each of the "Will" or "William" variants
is likely represented in the collection's storage — so long as they didn't
arrive so closely together that they were locally combined by Flow.
If desired, a derivation could re-key the collection
on `[/userId, /name]` to materialize the various `/name`s seen for a `/userId`.

This property makes keys less lossy than they might otherwise appear,
and it is generally good practice to chose a key that reflects how
you wish to _query_ a collection, rather than an exhaustive key
that's certain to be unique for every document.

### Empty keys

When a specification is automatically generated, there may not be an unambiguously correct key for all collections. This could occur, for example, when a SQL database doesn't have a primary key defined for some table.

In cases like this, the generated specification will contain an empty collection key. However, every collection must have a non-empty key, so you'll need to manually edit the generated specification and specify keys for those collections before publishing to the catalog.

## Projections

Projections are named locations within a collection document that may be used for
logical partitioning or directly exposed to databases into which collections are
materialized.

Many projections are automatically inferred from the collection schema.
The `projections` stanza can be used to provide additional projections,
and to declare logical partitions:

```yaml
collections:
  acmeCo/products/anvils:
    schema: anvils.schema.yaml
    key: [/product/id]

    # Projections and logical partitions for this collection.
    # Keys name the unique projection field, and values are its JSON Pointer
    # location within the document and configure logical partitioning.
    # Optional, type: object
    projections:
      # Short form: define a field "product_id" with document pointer /product/id.
      product_id: "/product/id"

      # Long form: define a field "metal" with document pointer /metal_type
      # which is a logical partition of the collection.
      metal:
        location: "/metal_type"
        partition: true

```

[Learn more about projections](./advanced/projections.md).

## Storage

Collections are real-time data lakes.
Historical documents of the collection
are stored as an organized layout of
regular JSON files in your cloud storage bucket.
Reads of that history are served by
directly reading files from your bucket.

Your [storage mappings](storage-mappings.md)
determine how Flow collections are mapped into
your cloud storage buckets.

Unlike a traditional data lake, however,
it's very efficient to read collection documents as they are written.
Derivations and materializations that source from a collection
are notified of its new documents within milliseconds of their being published.

[Learn more about journals, which provide storage for collections](./advanced/journals.md)