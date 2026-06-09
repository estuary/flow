---
description: Using the firstWriteWins and lastWriteWins reduction strategies
slug: /reference/reduction-strategies/firstwritewins-and-lastwritewins/
sidebar_position: 2
---

# firstWriteWins and lastWriteWins

`firstWriteWins` always takes the first value seen at the annotated location. Likewise, `lastWriteWins` always takes the last. Schemas that don’t have an explicit reduce annotation default to `lastWriteWins` behavior.

```yaml
collections:
  - name: example/reductions/fww-lww
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        fww: { reduce: { strategy: firstWriteWins } }
        lww: { reduce: { strategy: lastWriteWins } }
      required: [key]
    key: [/key]

tests:
  "Expect we can track first- and list-written values":
    - ingest:
        collection: example/reductions/fww-lww
        documents:
          - { key: "key", fww: "one", lww: "one" }
          - { key: "key", fww: "two", lww: "two" }
    - verify:
        collection: example/reductions/fww-lww
        documents:
          - { key: "key", fww: "one", lww: "two" }
```

## Disabling associative reduction

By default, Estuary assumes `lastWriteWins` reductions are **associative** — that
documents sharing a collection key can be combined incrementally, in any grouping,
without changing the final result (see [Reduction guarantees](./#reduction-guarantees)).
This is what produces one row per key: same-key documents are progressively combined
down to a single value.

Setting `associative: false` tells the runtime that same-key documents **cannot** be
safely combined incrementally. Instead of collapsing them, the runtime holds each
document back until a full reduction against the base document can be performed — or,
if the materialization uses [delta updates](/concepts/materialization/#delta-updates),
combines them never.

```yaml
collections:
  - name: example/reductions/no-dedup
    schema:
      type: object
      # Applied at the document root, this disables incremental combining
      # on the collection key.
      reduce:
        strategy: lastWriteWins
        associative: false
      properties:
        id: { type: integer }
      required: [id]
    key: [/id]
```

To preserve **every** document for a key end-to-end (no deduplication at any stage),
pair `associative: false` on the collection with a **delta-updates** materialization:

* `associative: false` stops same-key documents from being combined as they're
  written to and read from the collection.
* Delta updates skips the destination's load-reduce-store cycle, so the
  materialization never merges a new document into the existing row for its key.

Delta updates alone is not sufficient: without `associative: false`, same-key
documents that land in the same materialization transaction can still be combined
before they're written. Both knobs are needed for a fully un-reduced stream.

This is the mechanism CDC **history mode** uses to retain the full change history of
a source row, rather than just its latest state.

:::note
`associative: false` is an advanced control. The default associative behavior is
correct for the large majority of use cases — reach out to Estuary support before
using it if you're unsure whether it fits yours.
:::
