---
description: How Flow leverages JSON to aggregate the data in the runtime
---

# Reductions

**Data reductions** aggregate data in the Flow runtime so that the data that is eventually materialized to your endpoints is more manageable. This improves those systems' query time, lowers cost, and ensures that they're easily kept up-to-date.&#x20;

Each reduction uses a defined strategy to combine multiple JSON documents based on a collection key. Data reductions occur automatically in captures and materializations, depending on the collection and endpoint type, as described [here](materialization.md#how-materializations-work). In derivations, they can also be combined with lambda functions to re-shape data more powerfully.&#x20;

:::info
We created the [Transformation basics](../../getting-started/flow-tutorials/transformation-basics.md) tutorial to show reductions in action.
If you haven't already, we recommend you try that quick workflow to develop a deeper understanding.
:::

### JSON reduction annotations

Reductions are defined by reduction annotations. These are the only component Flow adds to standard JSON Schema.&#x20;

JSON Schema introduces the concept of [annotations](https://json-schema.org/draft/2019-09/json-schema-core.html#rfc.section.7.7), which allow schemas to attach metadata at locations within a validated JSON document. For example, `description` can be used to describe the meaning of a particular property:

```
properties:
    myField:
        description: "A description of myField"
```

Collection schemas in Flow use reduction annotations to define how one document is to be combined into another. They can use various strategies to do so; here's an example that sums an integer:

```
type: integer
reduce: { strategy: sum }

# [ 1, 2, -1 ] => 2
```

What’s especially powerful about annotations is that they respond to **conditionals** within the schema. A tagged union type might alter the `description` of a property depending on which variant of the union type was matched. This also applies to reduction annotations, which can use conditionals to [compose richer behaviors](../../reference/reduction-strategies/composing-with-conditionals.md).

Reduction annotations are a Flow super-power. They make it easy to define **combiners** over arbitrary JSON documents, and they allow Flow to employ those combiners early and often within the runtime – regularly collapsing a torrent of ingested documents into a trickle.

{% hint style="info" %}
Flow never delays processing in order to batch or combine more documents, as some systems do (commonly known as _micro-batches_, or time-based _polling_). Every document is processed as quickly as possible, from end to end.

Instead, Flow uses optimistic transaction pipelining to do as much useful work as possible, while it awaits the commit of a previous transaction. This natural back-pressure affords _plenty_ of opportunity for data reductions while minimizing latency.
{% endhint %}

To learn more about schema and reduction options and programming elements, see the [schemas ](../../reference/catalog-reference/schemas-and-data-reductions.md)and [reductions strategies](../../reference/reduction-strategies/) reference documentation.
