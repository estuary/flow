---
description: Learn about Estuary's firstWriteWins and lastWriteWins reduction strategies to reduce document fields to their first or last values.
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
      reduce:
        strategy: lastWriteWins
        associative: false
      properties:
        id: { type: integer }
      required: [id]
    key: [/id]
```

`associative: false` can be set on any reduce annotation, not just the document root.
A non-associative reduction at any location holds back incremental combining of the
**entire** document, so the example above — annotating the root — is the simplest way
to stop same-key documents from being combined on the collection key.

To preserve **every** document for a key end-to-end (no deduplication at any stage),
you need both `associative: false` on the collection **and** a **delta-updates**
materialization, because reductions happen at more than one place:

* **At capture or derivation time**, documents sharing a key are combined with a
  *partial* (associative) reduction within each transaction before they're written to
  the collection. `associative: false` is what stops unequal same-key documents from
  being merged here.
* **A standard materialization** loads the existing destination row and performs a
  *full* reduction of new documents into it — always one row per key. The
  `associative` flag has no effect on a full reduction, so this step must be removed
  with [delta updates](/concepts/materialization/#delta-updates).
* **A delta-updates materialization** still combines documents with a *partial*
  reduction within each transaction, so `associative: false` is needed here too —
  otherwise same-key documents that land in one transaction are merged before they're
  written to the destination.

In short: delta updates removes the full reduction at the destination, and
`associative: false` prevents merging at every remaining (partial) reduction stage.
Either knob alone still deduplicates.

This is the mechanism CDC **history mode** uses to retain the full change history of
a source row, rather than just its latest state.

:::note
`associative: false` is an advanced control. The default associative behavior is
correct for the large majority of use cases — reach out to Estuary support before
using it if you're unsure whether it fits yours.
:::
