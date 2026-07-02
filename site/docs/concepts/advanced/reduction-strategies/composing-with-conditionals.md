---
description: Fine-tune reduction strategies with conditional statements. Use specific reduction strategies based on document shape or field values to customize behavior.
slug: /reference/reduction-strategies/composing-with-conditionals/
sidebar_position: 30
---

# Composing Reductions with Conditionals

Reduction strategies are JSON Schema [annotations](https://json-schema.org/draft/2019-09/json-schema-core.html#rfc.section.7.7). As such, their applicability at a given document location can be controlled through the use of [conditional](https://json-schema.org/understanding-json-schema/reference/conditionals.html) keywords within the schema, like `oneOf` or `if/then/else`. This means Estuary’s built-in strategies can be combined with schema conditionals to construct a wider variety of custom reduction behaviors.

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
      # Use oneOf to express a tagged union over "action". Each branch
      # declares `required: [action]` — see the pitfall section below for why.
      oneOf:
        # When action = reset, reduce by taking this document.
        - required: [action]
          properties: { action: { const: reset } }
          reduce: { strategy: lastWriteWins }
        # When action = sum, reduce by summing "value". Keep the LHS "action",
        # preserving a LHS "reset", so that resets are properly associative.
        - required: [action]
          properties:
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

## Pitfall: pin `required` on the discriminator

JSON Schema's `properties` keyword only constrains a property *when it is present* — a document missing the property still validates. So `properties: { action: { const: "reset" } }` matches both documents where `action == "reset"` *and* documents where `action` is absent.

In a tagged-union reduction this silently routes every document missing the discriminator down the wrong branch. A delete-by-`_meta/op` reduction is one case to watch for:

```yaml
# WRONG — `if` matches when _meta/op = "d" *or* when _meta/op is absent.
if:
  properties:
    _meta:
      properties:
        op: { const: "d" }
then:
  reduce: { strategy: merge, delete: true }
else:
  reduce: { strategy: merge }
```

Any document without `_meta/op` satisfies the `if` and takes the `then` branch with `delete: true`. Fix it by declaring the discriminator path as `required` at every level:

```yaml
if:
  required: ["_meta"]
  properties:
    _meta:
      required: ["op"]
      properties:
        op: { const: "d" }
then:
  reduce: { strategy: merge, delete: true }
else:
  reduce: { strategy: merge }
```

The same rule applies to every branch of `oneOf` and `anyOf`: any property a branch matches on must also appear in the branch's `required` list.
