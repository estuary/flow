---
description: Using conditionals statements to fine-tune reductions
---

# Composing with conditionals

Reduction strategies are JSON Schema [annotations](https://json-schema.org/draft/2019-09/json-schema-core.html#rfc.section.7.7), and as such their applicability at a given document location can be controlled through the use of [conditional](https://json-schema.org/understanding-json-schema/reference/conditionals.html) keywords within the schema like `oneOf` or `if/then/else`. This means Flow’s built-in strategies below can be combined with schema conditionals to construct a wider variety of custom reduction behaviors.

For example, here’s a reset-able counter:

```yaml
collections:
  - name: example/reductions/sum-reset
    schema:
      type: object
      properties:
        key: { type: string }
        value: { type: number }
      required: [key]
      # Use oneOf to express a tagged union over "action".
      oneOf:
        # When action = reset, reduce by taking this document.
        - properties: { action: { const: reset } }
          reduce: { strategy: lastWriteWins }
        # When action = sum, reduce by summing "value". Keep the LHS "action",
        # preserving a LHS "reset", so that resets are properly associative.
        - properties:
            action:
              const: sum
              reduce: { strategy: firstWriteWins }
            value: { reduce: { strategy: sum } }
          reduce: { strategy: merge }
    key: [/key]

tests:
  "Expect we can sum or reset numbers":
    - ingest:
        collection: example/reductions/sum-reset
        documents:
          - { key: "key", action: sum, value: 5 }
          - { key: "key", action: sum, value: -1.2 }
    - verify:
        collection: example/reductions/sum-reset
        documents:
          - { key: "key", value: 3.8 }
    - ingest:
        collection: example/reductions/sum-reset
        documents:
          - { key: "key", action: reset, value: 0 }
          - { key: "key", action: sum, value: 1.3 }
    - verify:
        collection: example/reductions/sum-reset
        documents:
          - { key: "key", value: 1.3 }
```

