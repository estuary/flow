# Reduction Types

Estuary implements a number of reduction strategies for use within schemas,
which tell Estuary how two instances of a document can be meaningfully
combined together.

## Guarantees

In Estuary, documents having the same collection key and written to the same
logical partition have a "total order", meaning that one document is
universally understood to have been written _before_ the other.

This doesn't hold for documents of the same key written to _different_
logical partitions. These documents can be considered "mostly" ordered:
Estuary uses timestamps to understand the relative ordering of these documents,
and while this largely does the "Right Thing", small amounts of re-ordering
are possible and even likely.

Estuary guarantees exactly-once semantics within derived collections and
materializations (so long as the target system supports transactions),
and a document reduction will be applied exactly one time.

Estuary does _not_ guarantee that documents are reduced in sequential order,
directly into a "base" document. For example, documents of a single Data Flow
ingest transaction are combined together into one document per collection key
at ingestion time -- and that document may be again combined with still others,
and so on until a final reduction into the base document occurs.

Taken together, these "total order" and "exactly-once" guarantees mean that
reduction strategies must be _associative_ [e.g. (2 + 3) + 4 = 2 + (3 + 4) ],
but need not be commutative [ 2 + 3 = 3 + 2 ] or idempotent [ S u S = S ].
They expand the palette of strategies which can be implemented,
and allow for more efficient implementations as compared to, e.g.,
[CRDTs](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type).

In documentation, we'll refer to the "left-hand side" (LHS) as the preceding
document, and the "right-hand side" (RHS) as the following one. Keep in mind
that both the LHS and RHS may themselves represent a combination of still
more ordered documents (e.g, reductions are applied _associatively_).

### `append`

See [append.flow.yaml](append.flow.yaml).

`append` works with arrays, and extends the left-hand array with items of the right-hand side.
The right-hand side _must_ always be an array. The left-hand side _may_ be null, in which case
the reduction is treated as a no-op and its result remains null. This can be combined
with schema conditionals to "toggle" whether reduction reduction should be done or not.

### `firstWriteWins` / `lastWriteWins`

See [fww_lww.flow.yaml](fww_lww.flow.yaml).

`firstWriteWins` always takes the first value seen at the annotated location.
Likewise `lastWriteWins` always takes the last. Schemas which don't have
an explicit reduce annotation default to lastWriteWins behavior.

### `merge`

See [merge.flow.yaml](merge.flow.yaml).

`merge` reduces the LHS and RHS by recursively reducing shared document
locations. The LHS and RHS must either both be objects, or both be arrays.

If both sides are objects then it performs a deep merge of each property.
If LHS and RHS are both arrays then items at each index of both sides
are merged together, extending the shorter of the two sides by taking items
of the longer.

### `merge` With Key

See [merge_key.flow.yaml](merge_key.flow.yaml).

Merge may also take a `key`, which is one or more JSON pointers that are
relative to the reduced location. If both sides are arrays and a merge
key is present, then a deep sorted merge of the respective items is
done, as ordered by the key. Arrays must be pre-sorted and de-duplicated
by the key, and merge itself always maintains this invariant.

Note that a key of [""] can be used for natural item ordering, e.g. when
merging sorted arrays of scalars.

As with `append`, the left-hand side of `merge` _may_ be null, in which case
the reduction is treated as a no-op and its result remains null.

### `minimize` / `maximize`

See [min_max.flow.yaml](min_max.flow.yaml).

`minimize` and `maximize` reduce by taking the smallest (or largest) seen value.

### `minimize` / `maximize` With Key.

See [min_max_key.flow.yaml](min_max_key.flow.yaml).

Minimize and maximize can also take a `key`, which is one or more JSON pointers
that are relative to the reduced location. Keys make it possible to min/max over
complex types, by ordering over an extracted composite key.

In the event that a RHS document key equals the current LHS minimum (or maximum),
then documents are deeply merged. This can be used to, for example, track not
just the minimum value but also the number of times it's been seen.

### `set` With Object.

See [set.flow.yaml](set.flow.yaml).

`set` interprets the document location as an update to a set.
The location must be an object having only "add", "intersect",
and "remove" properties. Any single "add", "intersect", or "remove"
is always allowed.

A document with "intersect" and "add" is allowed, and is interpreted
as applying the intersection to the LHS set, followed by a union with
the additions.

A document with "remove" and "add" is also allowed, and is interpreted
as applying the removals to the base set, followed by a union with
the additions.

"remove" and "intersect" within the same document is prohibited.

Set additions are deeply merged. This makes sets behave like associative
maps, where the "value" of a set member can be updated by adding it to
set again, with a reducible update.

### `set` With Array

See [set_array.flow.yaml](set_array.flow.yaml).

Sets can also be sorted arrays, which are ordered using a provide `key`
extractor. Keys are given as one or more JSON pointers, each relative to
the item. As with "merge", arrays must be pre-sorted and de-duplicated
by the key, and set reductions always maintain this invariant

Use a key extractor of [""] to apply the natural ordering of scalar values.

Whether array or object types are used, the type must always be
consistent across the "add" / "intersect" / "remove" terms of both
sides of the reduction.

### `sum`

See [sum.flow.yaml](sum.flow.yaml).
`sum` reduces two numbers or integers by adding their values.

## Composing with Conditionals

Reduction strategies are [JSON Schema annotations](https://json-schema.org/draft/2019-09/json-schema-core.html#rfc.section.7.7),
and as such their
applicability at a given document location can be controlled through the use
of [conditional](https://json-schema.org/understanding-json-schema/reference/conditionals.html)
keywords within the schema like `oneOf` or `if/then/else`.
This means Estuary's built-in strategies below can be combined with schema
conditionals to construct a wider variety of custom reduction behaviors.

For example, see [reset_counter.flow.yaml](reset_counter.flow.yaml) for a reset-able counter.
