---
description: Fine-tuning accumulations, joins, and comparisons in derivations
---

# Derivation patterns

Derivations allow a high degree of control over how and where data is transformed. This page details some important considerations and options:

* [Accumulation](derivation-patterns.md#where-to-accumulate) of intermediate results
* Types of [joins](derivation-patterns.md#types-of-joins)
* [Comparing](derivation-patterns.md#comparing-registers) register states

### Where to accumulate?

When you [build a derived collection](./), you must choose where **accumulation** will happen, or where you'll store the intermediate results of the derivation steps before the final output is published. You can accumulate within a materialized database or within derivation registers. These two approaches can produce equivalent results, but they do so in very different ways.

#### Accumulate in the database

To accumulate in the database, you define a collection with a reducible schema and a derivation that uses only `publish` lambdas and no registers. The Flow runtime uses the reduction annotations to reduce new documents into the collection, and ultimately keep the materialized table up to date.

A key insight is that the database is the _only_ stateful system in this scenario, and Flow uses reductions in two steps:

1. To combine many published documents into intermediate **delta states**, which are the literal documents written to the collection
2. To reduce delta states into the final database-stored value

For example, consider a collection that’s summing a value:

| Time | DB    | Lambdas           | Derived Document |
| ---- | ----- | ----------------- | ---------------- |
| T0   | **0** | publish(2, 1, 2)  | **5**            |
| T1   | **5** | publish(-2, 1)    | **-1**           |
| T2   | **4** | publish(3, -2, 1) | **2**            |
| T3   | **6** | publish()         |                  |

This works especially well when materializing into a transactional database. Flow couples its processing transactions with corresponding database transactions, ensuring end-to-end “exactly once” semantics.

When materializing into a non-transactional store, Flow is only able to provide weaker “at least once” semantics; it’s possible that a document may be combined into a database value more than once. Whether that’s a concern depends a bit on the task at hand. Some reductions can be applied repeatedly without changing the result (idempotently), and in some use-cases, approximations are acceptable. For the summing example above, "at-least-once" semantics could give an incorrect result.

When materializing into a pub/sub topic, there _is_ no store to hold final values, and Flow publishes delta states: each a partial update of the (unknown) final value.

#### Accumulate in registers

**Registers** are arbitrary documents that can be shared and updated by the various transformations of a derivation. To accumulate in registers, you use a derivation that defines a reducible register schema and uses `update` lambdas. The Flow runtime allocates, manages, and scales durable storage for registers; you don’t have to.

When you use registers, accumulations typically follow the same basic pattern. They use reduction annotations to perform each update to the register. Then, they publish the final, fully-reduced value following the last-write-wins policy.

Returning to our summing example:

| Time | Register | Lambdas                             | Derived Document |
| ---- | -------- | ----------------------------------- | ---------------- |
| T0   | **0**    | update(2, 1, 2), publish(register)  | **5**            |
| T1   | **5**    | update(-2, 1), publish(register)    | **4**            |
| T2   | **4**    | update(3, -2, 1), publish(register) | **6**            |
| T3   | **6**    | update()                            |                  |

Register derivations are a great solution for materializations into non-transactional stores because the documents they produce can be applied multiple times without breaking correctness.

They’re also well-suited for materializations that publish into pub/sub because they can produce fully reduced values as stand-alone updates.

#### &#x20;Example: Comparing database and register accumulation

This example demonstrates the different processes used to sum counts in the database versus in registers.

```yaml
import:
  - inputs.flow.yaml

collections:
  - name: patterns/sums-db
    schema: &schema
      type: object
      properties:
        Key: { type: string }
        Sum:
          type: integer
          reduce: { strategy: sum }
      required: [Key]
      reduce: { strategy: merge }
    key: [/Key]

    derivation:
      transform:
        fromInts:
          source: { name: patterns/ints }
          shuffle: [/Key]
          publish:
            nodeJS: |
              return [{Key: source.Key, Sum: source.Int}];

  - name: patterns/sums-register
    schema:
      # Re-use the schema defined above.
      <<: *schema
      reduce: { strategy: lastWriteWins }
    key: [/Key]

    derivation:
      register:
        schema:
          type: integer
          reduce: { strategy: sum }
        initial: 0

      transform:
        fromInts:
          source: { name: patterns/ints }
          shuffle: [/Key]
          update:
            nodeJS: |
              return [source.Int];
          publish:
            nodeJS: |
              return [{Key: source.Key, Sum: register}];

tests:
  "Expect we can do sums during materialization or within registers":
    - ingest:
        collection: patterns/ints
        documents:
          - { Key: key, Int: -3 }
          - { Key: key, Int: 5 }
    - ingest:
        collection: patterns/ints
        documents: [{ Key: key, Int: 10 }]
    - verify:
        # "verify" steps fully reduce documents of the collection.
        # Under the hood, these are multiple delta updates.
        collection: patterns/sums-db
        documents: [{ Key: key, Sum: 12 }]
    - verify:
        # These are multiple snapshots, of which "verify" takes the last.
        collection: patterns/sums-register
        documents: [{ Key: key, Sum: 12 }]
```

### Types of joins

When you design a derivation, you have several methods available to join data. To pick the best option, consider not only your data management goals, but also your accumulation strategy, as discussed above. Use the following examples as a guide.

{% hint style="info" %}
Some of the schema is omitted for brevity in the examples below but can be found [here](https://github.com/estuary/docs/blob/developer-docs/derive-patterns/schema.yaml).
{% endhint %}

#### Outer join accumulated in the database

This example shows an outer join between two source collections that is reduced within a target database table. This join is **fully reactive**: it updates with either source collection, and reflects the complete accumulation of their documents on both sides.

The literal documents written to the collection are combined delta states, reflecting changes on one or both sides of the join. These delta states are then fully reduced into the database table. No other storage _but_ the table is required by this example.

```yaml
import:
  - inputs.flow.yaml

collections:
  - name: patterns/outer-join
    schema:
      $ref: schema.yaml#Join
      reduce: { strategy: merge }
      required: [Key]
    key: [/Key]

    derivation:
      transform:
        fromInts:
          source: { name: patterns/ints }
          shuffle: [/Key]
          publish:
            nodeJS: |
              return [{Key: source.Key, LHS: source.Int}];

        fromStrings:
          source: { name: patterns/strings }
          shuffle: [/Key]
          publish:
            nodeJS: |
              return [{Key: source.Key, RHS: [source.String]}];

tests:
  "Expect a fully reactive outer join":
    - ingest:
        collection: patterns/ints
        documents: [{ Key: key, Int: 5 }]
    - verify:
        collection: patterns/outer-join
        documents: [{ Key: key, LHS: 5 }]
    - ingest:
        collection: patterns/strings
        documents: [{ Key: key, String: hello }]
    - verify:
        collection: patterns/outer-join
        documents: [{ Key: key, LHS: 5, RHS: [hello] }]
    - ingest:
        collection: patterns/ints
        documents: [{ Key: key, Int: 7 }]
    - ingest:
        collection: patterns/strings
        documents: [{ Key: key, String: goodbye }]
    - verify:
        collection: patterns/outer-join
        documents: [{ Key: key, LHS: 12, RHS: [hello, goodbye] }]
```

#### Inner join accumulated in registers

This example shows an inner join between two source collections that is reduced within the derivation’s registers. This join is also fully reactive, updating with either source collection, and reflects the complete accumulation of their documents on both sides.

By definition, inner joins prevent you from publishing anything to the database until _both_ sides of the join are matched. Therefore, you are _required_ to use registers to store the incomplete accumulation data. The literal documents written to the collection are fully reduced snapshots of the current join state.

```yaml
import:
  - inputs.flow.yaml

collections:
  - name: patterns/inner-join
    schema:
      $ref: schema.yaml#Join
      reduce: { strategy: lastWriteWins }
      required: [Key]
    key: [/Key]

    derivation:
      register:
        schema:
          $ref: schema.yaml#Join
          reduce: { strategy: merge }
        initial: {}

      transform:
        fromInts:
          source: { name: patterns/ints }
          shuffle: [/Key]
          update:
            nodeJS: |
              return [{LHS: source.Int}];
          publish:
            nodeJS: &innerJoinLambda |
              // Inner join requires that both sides be matched.
              if (register.LHS && register.RHS) {
                return [{Key: source.Key, ...register}]
              }
              return [];

        fromStrings:
          source: { name: patterns/strings }
          shuffle: [/Key]
          update:
            nodeJS: |
              return [{RHS: [source.String]}];
          publish:
            nodeJS: *innerJoinLambda

tests:
  "Expect a fully reactive inner-join":
    - ingest:
        collection: patterns/ints
        documents: [{ Key: key, Int: 5 }]
    - verify:
        # Both sides must be matched before a document is published.
        collection: patterns/inner-join
        documents: []
    - ingest:
        collection: patterns/strings
        documents: [{ Key: key, String: hello }]
    - verify:
        collection: patterns/inner-join
        documents: [{ Key: key, LHS: 5, RHS: [hello] }]
    - ingest:
        collection: patterns/ints
        documents: [{ Key: key, Int: 7 }]
    - ingest:
        collection: patterns/strings
        documents: [{ Key: key, String: goodbye }]
    - verify:
        # Reacts to accumulated updates of both sides.
        collection: patterns/inner-join
        documents: [{ Key: key, LHS: 12, RHS: [hello, goodbye] }]
```

#### One-sided join accumulated in registers

This example shows a one-sided join that publishes a current left-hand side (LHS) joined with an accumulated right-hand side (RHS).

This example is _not_ fully reactive. It publishes only on a LHS document paired with a reduced snapshot of the RHS accumulator at that time.

```yaml
import:
  - inputs.flow.yaml

collections:
  - name: patterns/inner-join
    schema:
      $ref: schema.yaml#Join
      reduce: { strategy: lastWriteWins }
      required: [Key]
    key: [/Key]

    derivation:
      register:
        schema:
          $ref: schema.yaml#Join
          reduce: { strategy: merge }
        initial: {}

      transform:
        fromInts:
          source: { name: patterns/ints }
          shuffle: [/Key]
          update:
            nodeJS: |
              return [{LHS: source.Int}];
          publish:
            nodeJS: &innerJoinLambda |
              // Inner join requires that both sides be matched.
              if (register.LHS && register.RHS) {
                return [{Key: source.Key, ...register}]
              }
              return [];

        fromStrings:
          source: { name: patterns/strings }
          shuffle: [/Key]
          update:
            nodeJS: |
              return [{RHS: [source.String]}];
          publish:
            nodeJS: *innerJoinLambda

tests:
  "Expect a fully reactive inner-join":
    - ingest:
        collection: patterns/ints
        documents: [{ Key: key, Int: 5 }]
    - verify:
        # Both sides must be matched before a document is published.
        collection: patterns/inner-join
        documents: []
    - ingest:
        collection: patterns/strings
        documents: [{ Key: key, String: hello }]
    - verify:
        collection: patterns/inner-join
        documents: [{ Key: key, LHS: 5, RHS: [hello] }]
    - ingest:
        collection: patterns/ints
        documents: [{ Key: key, Int: 7 }]
    - ingest:
        collection: patterns/strings
        documents: [{ Key: key, String: goodbye }]
    - verify:
        # Reacts to accumulated updates of both sides.
        collection: patterns/inner-join
        documents: [{ Key: key, LHS: 12, RHS: [hello, goodbye] }]
```

### Comparing registers

In some situations, it's beneficial to take action based on how a register is changing.

For example, suppose you want to detect zero-crossings for a running sum, and then filter the source collection to isolate the documents that caused the sum to cross from positive to negative, or vice versa.

You can use the `previous` register value to do so, as shown below:

```yaml
import:
  - inputs.flow.yaml

collections:
  - name: patterns/zero-crossing
    schema: schema.yaml#Int
    key: [/Key]

    derivation:
      register:
        schema:
          type: integer
          reduce: { strategy: sum }
        initial: 0

      transform:
        fromInts:
          source: { name: patterns/ints }
          shuffle: [/Key]
          update:
            nodeJS: return [source.Int];
          publish:
            nodeJS: |
              if (register > 0 != previous > 0) {
                return [source];
              }
              return [];

tests:
  "Expect we can filter to zero-crossing documents":
    - ingest:
        collection: patterns/ints
        documents:
          - { Key: key, Int: -5 } # => -5
          - { Key: key, Int: -10 } # => -10
    - ingest:
        collection: patterns/ints
        documents: [{ Key: key, Int: 13 }] # => -2
    - verify:
        collection: patterns/zero-crossing
        documents: []
    - ingest:
        collection: patterns/ints
        documents:
          - { Key: key, Int: 4 } # => 2 (zero crossing)
          - { Key: key, Int: 10 } # => 12
    - verify:
        collection: patterns/zero-crossing
        documents: [{ Key: key, Int: 4 }]
    - ingest:
        collection: patterns/ints
        documents:
          - { Key: key, Int: -13 } # => -1 (zero crossing)
          - { Key: key, Int: -5 } # => -6
    - verify:
        collection: patterns/zero-crossing
        documents: [{ Key: key, Int: -13 }]
```
