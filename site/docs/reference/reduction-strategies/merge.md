---
description: Using the merge reduction strategy
---

# merge

`merge` reduces the LHS and RHS by recursively reducing shared document locations. The LHS and RHS must either both be objects, or both be arrays.

If both sides are objects, `merge` performs a deep merge of each property. If LHS and RHS are both arrays, items at each index of both sides are merged together, extending the shorter of the two sides by taking items off the longer:

```yaml
collections:
  - name: example/reductions/merge
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        value:
          # Merge only works with types "array" or "object".
          # Others will throw an error at build time.
          type: [array, object]
          reduce: { strategy: merge }
          # Deeply merge sub-locations (items or properties) by summing them.
          items:
            type: number
            reduce: { strategy: sum }
          additionalProperties:
            type: number
            reduce: { strategy: sum }
      required: [key]
    key: [/key]

tests:
  "Expect we can merge arrays by index":
    - ingest:
        collection: example/reductions/merge
        documents:
          - { key: "key", value: [1, 1] }
          - { key: "key", value: [2, 2, 2] }
    - verify:
        collection: example/reductions/merge
        documents:
          - { key: "key", value: [3, 3, 2] }

  "Expect we can merge objects by property":
    - ingest:
        collection: example/reductions/merge
        documents:
          - { key: "key", value: { "a": 1, "b": 1 } }
          - { key: "key", value: { "a": 1, "c": 1 } }
    - verify:
        collection: example/reductions/merge
        documents:
          - { key: "key", value: { "a": 2, "b": 1, "c": 1 } }
```

Merge may also take a `key`, which is one or more JSON pointers that are relative to the reduced location. If both sides are arrays and a merge key is present, then a deep sorted merge of the respective items is done, as ordered by the key. Arrays must be pre-sorted and de-duplicated by the key, and `merge` itself always maintains this invariant.

Note that you can use a key of \[“”] for natural item ordering, such as merging sorted arrays of scalars.

```yaml
collections:
  - name: example/reductions/merge-key
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        value:
          type: array
          reduce:
            strategy: merge
            key: [/k]
          items: { reduce: { strategy: firstWriteWins } }
      required: [key]
    key: [/key]

tests:
  "Expect we can merge sorted arrays":
    - ingest:
        collection: example/reductions/merge-key
        documents:
          - { key: "key", value: [{ k: "a", v: 1 }, { k: "b", v: 1 }] }
          - { key: "key", value: [{ k: "a", v: 2 }, { k: "c", v: 2 }] }
    - verify:
        collection: example/reductions/merge-key
        documents:
          - {
              key: "key",
              value: [{ k: "a", v: 1 }, { k: "b", v: 1 }, { k: "c", v: 2 }],
            }
```

As with `append`, the left-hand side of `merge` _may_ be null, in which case the reduction is treated as a no-op and its result remains null.
