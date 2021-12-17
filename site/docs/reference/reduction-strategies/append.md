---
description: Using the append reduction strategy
---

# append

`append` works with arrays, and extends the left-hand array with items from the right-hand side.

```yaml
collections:
  - name: example/reductions/append
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        value:
          # Append only works with type "array".
          # Others will throw an error at build time.
          type: array
          reduce: { strategy: append }
      required: [key]
    key: [/key]

tests:
  "Expect we can append arrays":
    - ingest:
        collection: example/reductions/append
        documents:
          - { key: "key", value: [1, 2] }
          - { key: "key", value: [3, null, "abc"] }
    - verify:
        collection: example/reductions/append
        documents:
          - { key: "key", value: [1, 2, 3, null, "abc"] }
```

The right-hand side _must_ always be an array. The left-hand side _may_ be null, in which case the reduction is treated as a no-op and its result remains null. This can be combined with schema conditionals to toggle whether reduction-reduction should be done or not.
