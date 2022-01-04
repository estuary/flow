---
description: Flow's default reduction behaviors and available strategies to customize them
---

# Reduction strategies

Flow uses [reductions](../../concepts/catalog-entities/schemas-and-data-reductions.md#reductions)
to aggregate data in the runtime in order to improve endpoint performance.
Reductions tell Flow how two versions of a document can be meaningfully combined together. Guarantees that underlie all Flow reduction behavior are explained in depth [below](./#reduction-guarantees).&#x20;

Some reductions [occur automatically](../../concepts/catalog-entities/materialization.md#how-materializations-work) during captures and materializations to optimize performance, but you can define more advanced behavior using reduction annotations in collection schemas.

The available strategies are:&#x20;

* [append](append.md)
* [firstWriteWins and lastWriteWins](firstwritewins-and-lastwritewins.md)
* [merge](merge.md)
* [minimize and maximize](minimize-and-maximize.md)
* [set](set.md)
* [sum](sum.md)

When no other strategy is specified in a schema, Flow defaults to `lastWriteWins`.  For even more customization, you can use [conditional statements](composing-with-conditionals.md).&#x20;

:::info

Estuary has many future plans for reduction annotations:

> * More strategies, including data sketches like HyperLogLogs, T-Digests, and others.
> * Eviction policies and constraints, for bounding the sizes of objects and arrays with fine-grained removal ordering.

What’s here today can be considered a minimal, useful proof-of-concept.
:::

### Reduction guarantees

In Flow, documents having the same collection key and written to the same logical partition have a **total order,** meaning that one document is universally understood to have been written before the other.

This doesn’t hold for documents of the same key written to different logical partitions. These documents can be considered “mostly” ordered: Flow uses timestamps to understand the relative ordering of these documents, and while this largely does the “right thing," small amounts of re-ordering are possible and even likely.

Flow guarantees **exactly-once** semantics within derived collections and materializations (so long as the target system supports transactions), and a document reduction will be applied exactly one time.

Flow does _not_ guarantee that documents are reduced in sequential order, directly into a “base” document. For example, documents of a single Flow ingest transaction are combined together into one document per collection key at ingestion time – and that document may be again combined with still others, and so on until a final reduction into the base document occurs.

Taken together, these total-order and exactly-once guarantees mean that reduction strategies must be _associative_ \[as in (2 + 3) + 4 = 2 + (3 + 4) ], but need not be commutative \[ 2 + 3 = 3 + 2 ] or idempotent \[ S u S = S ]. They expand the palette of strategies that can be implemented, and allow for more efficient implementations as compared to, for example [CRDTs](https://en.wikipedia.org/wiki/Conflict-free\_replicated\_data\_type).

In this documentation, we’ll refer to the “left-hand side” (LHS) as the preceding document and the “right-hand side” (RHS) as the following one. Keep in mind that both the LHS and RHS may themselves represent a combination of still more ordered documents (for example, reductions are applied _associatively_).

