---
description: Using the set reduction strategy
---

# set

`set` interprets the document location as an update to a set.

The location must be an object having only “add”, “intersect”, and “remove” properties. Any single “add”, “intersect”, or “remove” is always allowed.

A document with “intersect” and “add” is allowed, and is interpreted as applying the intersection to the LHS set, followed by a union with the additions.

A document with “remove” and “add” is also allowed, and is interpreted as applying the removals to the base set, followed by a union with the additions.

“remove” and “intersect” within the same document are prohibited.

Set additions are deeply merged. This makes sets behave like associative maps, where the “value” of a set member can be updated by adding it to set again, with a reducible update.

Sets may be objects, in which case the object property serves as the set item key:

```yaml
collections:
  - name: example/reductions/set
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        value:
          # Sets are always represented as an object.
          type: object
          reduce: { strategy: set }
          # Schema for "add", "intersect", and "remove" properties
          # (each a map of keys and their associated sums):
          additionalProperties:
            type: object
            additionalProperties:
              type: number
              reduce: { strategy: sum }
            # Flow requires that all parents of locations with a reduce
            # annotation also have one themselves.
            # This strategy therefore must (currently) be here, but is ignored.
            reduce: { strategy: lastWriteWins }

      required: [key]
    key: [/key]

tests:
  "Expect we can apply set operations to incrementally build associative maps":
    - ingest:
        collection: example/reductions/set
        documents:
          - { key: "key", value: { "add": { "a": 1, "b": 1, "c": 1 } } }
          - { key: "key", value: { "remove": { "b": 0 } } }
          - { key: "key", value: { "add": { "a": 1, "d": 1 } } }
    - verify:
        collection: example/reductions/set
        documents:
          - { key: "key", value: { "add": { "a": 2, "c": 1, "d": 1 } } }
    - ingest:
        collection: example/reductions/set
        documents:
          - { key: "key", value: { "intersect": { "a": 0, "d": 0 } } }
          - { key: "key", value: { "add": { "a": 1, "e": 1 } } }
    - verify:
        collection: example/reductions/set
        documents:
          - { key: "key", value: { "add": { "a": 3, "d": 1, "e": 1 } } }
```

Sets can also be sorted arrays, which are ordered using a provide `key` extractor. Keys are given as one or more JSON pointers, each relative to the item. As with `merge`, arrays must be pre-sorted and de-duplicated by the key, and set reductions always maintain this invariant.

Use a key extractor of `[“”]` to apply the natural ordering of scalar values.

Whether array or object types are used, the type must always be consistent across the “add” / “intersect” / “remove” terms of both sides of the reduction.

```yaml
collections:
  - name: example/reductions/set-array
    schema:
      type: object
      reduce: { strategy: merge }
      properties:
        key: { type: string }
        value:
          # Sets are always represented as an object.
          type: object
          reduce:
            strategy: set
            key: [/0]
          # Schema for "add", "intersect", & "remove" properties
          # (each a sorted array of [key, sum] 2-tuples):
          additionalProperties:
            type: array
            # Flow requires that all parents of locations with a reduce
            # annotation also have one themselves.
            # This strategy therefore must (currently) be here, but is ignored.
            reduce: { strategy: lastWriteWins }
            # Schema for contained [key, sum] 2-tuples:
            items:
              type: array
              items:
                - type: string
                - type: number
                  reduce: { strategy: sum }
              reduce: { strategy: merge }

      required: [key]
    key: [/key]

tests:
  ? "Expect we can apply operations of sorted-array sets to incrementally build associative maps"
  : - ingest:
        collection: example/reductions/set-array
        documents:
          - { key: "key", value: { "add": [["a", 1], ["b", 1], ["c", 1]] } }
          - { key: "key", value: { "remove": [["b", 0]] } }
          - { key: "key", value: { "add": [["a", 1], ["d", 1]] } }
    - verify:
        collection: example/reductions/set-array
        documents:
          - { key: "key", value: { "add": [["a", 2], ["c", 1], ["d", 1]] } }
    - ingest:
        collection: example/reductions/set-array
        documents:
          - { key: "key", value: { "intersect": [["a", 0], ["d", 0]] } }
          - { key: "key", value: { "add": [["a", 1], ["e", 1]] } }
    - verify:
        collection: example/reductions/set-array
        documents:
          - { key: "key", value: { "add": [["a", 3], ["d", 1], ["e", 1]] } }
```
