---
sidebar_position: 7
---
# Schemas

Flow documents and [collections](collections.md) always have an associated schema
that defines the structure, representation, and constraints
of your documents.
Collections must have one schema, but [may have two](#write-and-read-schemas) for different types of Flow tasks.

Schemas are a powerful tool for data quality.
Flow verifies every document against its schema whenever it's read or written,
which provides a strong guarantee that your collections hold only "clean" data,
and that bugs and invalid documents are caught before they can impact downstream data products.

In most cases, Flow generates a functioning schema on your behalf during the [discovery](./captures.md#discovery)
phase of capture.
In advanced use cases, however, customizing your schema becomes more important.

## JSON Schema

[JSON Schema](https://json-schema.org/understanding-json-schema/)
is an expressive open standard for defining the schema and structure of documents.
Flow uses it for all schemas defined in Flow specifications.

JSON Schema goes well beyond basic type information and can model
[tagged unions](https://en.wikipedia.org/wiki/Tagged\_union),
recursion, and other complex, real-world composite types.
Schemas can also define rich data validations like minimum and maximum values,
regular expressions, dates, timestamps, email addresses, and other formats.

Together, these features let schemas represent structure _as well as_
expectations and constraints that are evaluated and must hold true
for every collection document _before_ it’s added to the collection.
They’re a powerful tool for ensuring end-to-end data quality:
for catching data errors and mistakes early,
before they can impact your production data products.

### Generation

When capturing data from an external system,
Flow can usually generate suitable JSON schemas on your behalf.

[Learn more about using connectors](connectors.md#using-connectors)

### Translations

You must only provide Flow
a model of a given dataset _one time_, as a JSON schema.
Having done that, Flow leverages static inference over your schemas
to perform many build-time validations of your catalog entities,
helping you catch potential problems early.

Schema inference is also used to provide translations into other schema flavors:

* Most [projections](./advanced/projections.md) of a collection
  are automatically inferred from its schema.
  Materializations use your projections to create appropriate representations
  in your endpoint system.
  A SQL connector will create table definitions with appropriate
  columns, types, and constraints.
* Flow generates TypeScript definitions from schemas to provide
  compile-time type checks of user lambda functions.
  These checks are immensely helpful for surfacing mismatched expectations around,
  for example, whether a field could ever be null or is misspelt —
  which, if not caught, might otherwise fail at runtime.

### Annotations

The JSON Schema standard introduces the concept of
[annotations](http://json-schema.org/understanding-json-schema/reference/generic.html#annotations),
which are keywords that attach metadata to a location within a validated JSON document.
For example, `title` and `description` can be used to annotate a schema with its meaning:

```yaml
properties:
  myField:
    title: My Field
    description: A description of myField
```

Flow extends JSON Schema with additional annotation keywords,
which provide Flow with further instruction for how documents should be processed.
In particular, the [`reduce`](#reduce-annotations) and [`default`](#default-annotations) keywords
help you define merge behaviors and avoid null values at your destination systems, respectively.

What’s especially powerful about annotations is that they respond to
**conditionals** within the schema.
Consider a schema validating a positive or negative number:

```yaml
type: number
oneOf:
  - exclusiveMinimum: 0
    description: A positive number.
  - exclusiveMaximum: 0
    description: A negative number.
  - const: 0
    description: Zero.
```

Here, the activated `description` of this schema location depends
on whether the integer is positive, negative, or zero.

## Writing schemas

Your schema can be quite permissive or as strict as you wish.
There are a few things to know, however.

* The top-level type must be `object`.
  Flow adds a bit of metadata to each of your documents under the `_meta` property,
  which can only be done with a top-level object.

* Any fields that are part of the collection's `key` must provably exist
  in any document that validates against the schema.
  Put another way, every document within a collection must include all of the fields
  of the collection's key, and the schema must guarantee that.

For example, the following collection schema would be invalid because
the `id` field, which is used as its key, is not `required`,
so it might not actually exist in all documents:

```yaml
collections:
  acmeCo/whoops:
    schema:
      type: object
      required: [value]
      properties:
        id: {type: integer}
        value: {type: string}
    key: [/id]
```

To fix the above schema, change `required` to `[id, value]`.

[Learn more of how schemas can be expressed within collections](collections.md#Schemas).

### Organization

JSON schema has a `$ref` keyword which is used to reference a schema stored elsewhere.
Flow resolves `$ref` as a relative URL of the current file,
and also supports
[JSON fragment pointers](https://datatracker.ietf.org/doc/html/rfc6901#section-6)
for referencing a specific schema within a larger schema document,
such as `../my/widget.schema.yaml#/path/to/schema`.
It's recommended to use references in order to organize your schemas for reuse.

`$ref` can also be used in combination with other schema keywords
to further refine a base schema.
Here's an example that uses references to organize and
further tighten the constraints of a reused base schema:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="flow.yaml" default>

```yaml
collections:
  acmeCo/coordinates:
    key: [/id]
    schema: schemas.yaml#/definitions/coordinate

  acmeCo/integer-coordinates:
    key: [/id]
    schema: schemas.yaml#/definitions/integer-coordinate

  acmeCo/positive-coordinates:
    key: [/id]
    schema:
      # Compose a restriction that `x` & `y` must be positive.
      $ref: schemas.yaml#/definitions/coordinate
      properties:
        x: {exclusiveMinimum: 0}
        y: {exclusiveMinimum: 0}
```

</TabItem>
<TabItem value="schemas.yaml">

```yaml
definitions:
  coordinate:
    type: object
    required: [id, x, y]
    properties:
      id: {type: string}
      x:
        description: The X axis value of the coordinate.
        type: number
      y:
        description: The Y axis value of the coordinate.
        type: number

  integer-coordinate:
    $ref: "#/definitions/coordinate"
    # Compose a restriction that `x` & `y` cannot be fractional.
    properties:
      x: {type: integer}
      y: {type: integer}
```

</TabItem>
</Tabs>

:::tip
You can write your JSON schemas as either YAML or JSON across any number
of files, all referenced from Flow catalog files or other schemas.

Schema references are always resolved as URLs relative to the current file,
but you can also use absolute URLs to a third-party schema like
[schemastore.org](https://www.schemastore.org).
:::

## Write and read schemas

In some cases, you may want to impose different constraints to data that is entering (_written to_) the collection
and data that is exiting (_read from_) the collection.

To achieve this, you can replace the collection's standard `schema` with a `writeSchema` and `readSchema`.

MORE INFO AND EXAMPLE HERE

## Reductions

Flow collections have keys, and multiple documents
may be added to collections that share a common key.
When this happens, Flow will opportunistically merge all such documents
into a single representative document for that key through a process
known as _reduction_.

Flow's default is simply to retain the most recent document of a given key,
which is often the behavior that you're after.
Schema `reduce` annotations allow for far more powerful behaviors.

The Flow runtime performs reductions frequently and continuously
to reduce the overall movement and cost of data transfer and storage.
A torrent of input collection documents can often become a trickle
of reduced updates that must be stored or materialized into your
endpoints.

:::info
Flow never delays processing in order to batch or combine more documents,
as some systems do (commonly known as _micro-batches_, or time-based _polling_).
Every document is processed as quickly as possible, from end to end.

Instead Flow uses optimistic transaction pipelining to do as much useful work as possible,
while it awaits the commit of a previous transaction.
This natural back-pressure affords plenty of opportunity for
data reductions while minimizing latency.
:::

### `reduce` annotations

Reduction behaviors are defined by `reduce`
[JSON schema annotations](#annotations)
within your document schemas.
These annotations provide Flow with the specific reduction strategies
to use at your various document locations.

If you're familiar with the _map_ and _reduce_ primitives present in
Python, Javascript, and many other languages, this should feel familiar.
When multiple documents map into a collection with a common key,
Flow reduces them on your behalf by using your `reduce` annotations.

Here's an example that sums an integer:

```yaml
type: integer
reduce: { strategy: sum }

# 1, 2, -1 => 2
```

Or deeply merges a map:

```yaml
type: object
reduce: { strategy: merge }

# {"a": "b"}, {"c": "d"} => {"a": "b", "c": "d"}
```

Learn more in the
[reductions strategies](../../../reference/reduction-strategies/)
reference documentation.

#### Reductions and collection keys

Reduction annotations change the common patterns for how you think about collection keys.

Suppose you are building a reporting fact table over events of your business.
Today you would commonly consider a unique event ID to be its natural key.
You would load all events into your warehouse and perform query-time aggregation.
When that becomes too slow, you periodically refresh materialized views for fast-but-stale queries.

With Flow, you instead use a collection key of your _fact table dimensions_,
and use `reduce` annotations to define your metric aggregations.
A materialization of the collection then maintains a
database table which is keyed on your dimensions,
so that queries are both fast _and_ up to date.

#### Composition with conditionals

Like any other JSON Schema annotation,
`reduce` annotations respond to schema conditionals.
Here we compose `append` and `lastWriteWins` strategies to
reduce an appended array which can also be cleared:

```yaml
type: array
oneOf:
  # If the array is non-empty, reduce by appending its items.
  - minItems: 1
    reduce: { strategy: append }
  # Otherwise, if the array is empty, reset the reduced array to be empty.
  - maxItems: 0
    reduce: { strategy: lastWriteWins }

# [1, 2], [3, 4, 5] => [1, 2, 3, 4, 5]
# [1, 2], [], [3, 4, 5] => [3, 4, 5]
# [1, 2], [3, 4, 5], [] => []
```

Combining schema conditionals with annotations can be used to build
[rich behaviors](../reference/reduction-strategies/composing-with-conditionals.md).

## `default` annotations

You can use `default` annotations to prevent null values from being materialized to your endpoint system.

When this annotation is absent for a non-required field, missing values in that field are materialized as `null`.
When the annotation is present, missing values are materialized with the field's `default` value:

```yaml
collections:
  acmeCo/coyotes:
    schema:
      type: object
      required: [id]
      properties:
        id: {type: integer}
        anvils_dropped: {type: integer}
          reduce: {strategy: sum }
          default: 0
    key: [/id]
```

`default` annotations are only used for materializations; they're ignored by captures in derivations.
If your collection has both a [write and read schema](#write-and-read-schemas), make sure you add this annotation to the read schema.
