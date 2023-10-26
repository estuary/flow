# Schema evolution

:::info
Evolutions are a relatively advanced concept in Flow.
Before continuing, you should have a basic understanding of [Flow captures](/concepts/captures.md), [collections](/concepts/collections.md), [schemas](/concepts/schemas.md), and [materializations](/concepts/materialization.md).
:::

## Background

To review, Flow collections are the realtime containers for your data, which sit in between captures and materializations.

import Mermaid from '@theme/Mermaid';
<Mermaid chart={`
  graph LR;
    Source[Source System]-->Capture;
		Capture-->Collection;
    Collection-->Materialization;
    Materialization-->Dest[Destination System];
`}/>

Collection specs serve as a formal contract between producers (captures) and consumers (derivations and materializations) of data. 
This contract encompasses the `id`, `schema` (or `readSchema` and `writeSchema` if defined separately), and logical partitioning of the collection. When any part of the contract changes, both the producers and consumers must approve of the change. If any party rejects the proposed change, Flow will fail the publication with an error. Evolutions are a feature that updates your draft to allow such a publication to proceed.

Collection specs may change for a huge variety of reasons, such as:

- The source system is a database, and someone ran an `ALTER TABLE` statement on a captured table.
- The source system contains unstructured data, and some data with a different shape was just captured.
- Someone manually published a change to the collection's logical partitions.

Regardless of why or how a spec change is introduced, the effect is the same. Flow will never knowingly permit you to publish changes that break the contract between producers and consumers of a collection.

## What do evolutions do?

Evolutions operations that update a draft. Evolutions don't do anything that you couldn't do yourself by editing specs directly. They just make it easier to handle common and repetitive scenarios. They can update drafts in two ways:

Update materialization bindings to materialize a collection into a new resource (database table, for example): Any materializations of the evolving collection will be updated to materialize it into a new resource (database table, for example). For example, if the collection was previously materialized into a table called `my_table`, the evolution would update it to instead materialize into `my_table_v2`. The Flow collection itself remains unchanged.

Re-create the Flow collection with a new name: This creates a completely new collection with a `_v2` (`_v3`, etc) suffix, which will start out empty and will need to backfill from the source. All Captures and materializations that reference the old collection will be updated to instead reference the new collection. This will also update any materializations to materialize the new collection into a new resource.

:::info
Evolutions will soon support additional operations to re-create materialization resources (e.g. tables) while keeping the same names.
:::

In most common cases, evolutions will only need to update materialization bindings. Evolutions will always try to avoid re-creating the collection if at all possible. Collections will only be re-created in cases where the key or logical partitioning have changed. The remaining cases are due to changes to the collection schema. For example, perhaps a field has changed from `type: string` to `type: integer`, which results in a database materialization rejecting the change. In those cases, it's usually sufficient to materialize the collection into a new table.

## Evolutions in the UI

When you attempt to publish a breaking change to a collection via the UI, you'll get an error message that looks similar to this one:

![](<./evolutions-images/ui-evolution-re-create.png>)

If you click the "Apply" button, then it will trigger an evolution, which updates your draft. You'll then be able to review and publish your draft, which should succeed now that you've handled all the breaking changes.

## Breaking schema changes

Changes to the collection `key` or logical partition can happen, but in practice they tend to not be as common. The most common cause of a breaking change is just a change to the collection schema itself. Generally, it's the Materializations, not the Captures, that will complain about breaking schema changes. This is because the new collection specs are discovered by introspecting the data source, or else the collection uses schema inference and you just have to accept whatever came from the source. (TODO: awkward wording here) In any case, changes typically flow from the data sources toward the destinations.

Consider an example materialization of a collection that looks like this:

```yaml
schema:
  type: object
  properties:
    id: { type: integer }
    foo: { type: string, format: date-time }
  required: [id]
key: [/id]
```

If you materialized that collection into a relational database table, the table would look something like `my_table (id integer primary key, foo timestamptz)`.

Now say you create a draft where you update the collection spec to remove `format: date-time` from `bar`. You'd expect the resulting table to then look like `(id integer primary key, foo text)`. But since the column type of `foo` has changed, this will fail when you try to publish your draft. An easy solution in this case would be to change the name of the table that the collection is materialized into. A common convention, which is used by evolutions, is to suffix the table name with `_v2`, or increment the suffix if one is already present. Thus you'd end up with `my_table_v2 (id integer primary key, foo text)`.