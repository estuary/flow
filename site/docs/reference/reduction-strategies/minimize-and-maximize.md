---
description: Using the minimize and maximize reduction strategies
---

# minimize and maximize

`minimize` and `maximize` reduce by taking the smallest or largest seen value, respectively.

```yaml
collections:
  - name: example/reductions/min-max
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        min: { reduce: { strategy: minimize } }
        max: { reduce: { strategy: maximize } }
      required: [key]
    key: [/key]

tests:
  "Expect we can min/max values":
    - ingest:
        collection: example/reductions/min-max
        documents:
          - { key: "key", min: 32, max: "abc" }
          - { key: "key", min: 42, max: "def" }
    - verify:
        collection: example/reductions/min-max
        documents:
          - { key: "key", min: 32, max: "def" }
```

`minimize` and `maximize` can also take a `key`, which is one or more JSON pointers that are relative to the reduced location. Keys make it possible to minimize and maximize over complex types, by ordering over an extracted composite key.

In the event that a RHS document key equals the current LHS minimum or maximum, the documents are deeply merged. This can be used to, for example, track not just the minimum value but also the number of times it’s been seen:

```yaml
collections:
  - name: example/reductions/min-max-key
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        min:
          $anchor: min-max-value
          type: array
          items:
            - type: string
            - type: number
              reduce: { strategy: sum }
          reduce:
            strategy: minimize
            key: [/0]
        max:
          $ref: "#min-max-value"
          reduce:
            strategy: maximize
            key: [/0]
      required: [key]
    key: [/key]

tests:
  "Expect we can min/max values using a key extractor":
    - ingest:
        collection: example/reductions/min-max-key
        documents:
          - { key: "key", min: ["a", 1], max: ["a", 1] }
          - { key: "key", min: ["c", 2], max: ["c", 2] }
          - { key: "key", min: ["b", 3], max: ["b", 3] }
          - { key: "key", min: ["a", 4], max: ["a", 4] }
    - verify:
        collection: example/reductions/min-max-key
        documents:
          # Min of equal keys ["a", 1] and ["a", 4] => ["a", 5].
          - { key: "key", min: ["a", 5], max: ["c", 2] }
```
