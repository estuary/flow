---
description: >-
  Transformations combine sources, shuffles, registers, and lambdas and make up
  the core of derivations.
---

# Transformations

Transformations put sources, shuffles, registers, and lambdas all together. We'll continue using our simple transformation example as a reference:

```yaml
derivation:
      transform:
        transformOnesName:
          source:
            name: sourceOneName
            schema: "remote.schema.yaml#/$defs/withRequired"
          publish: { lambda: typescript }
```

Transformations of a derivation specify a source and (optional) shuffle key, and may invoke either an update lambda_,_ a publish lambda, or both.

* **Update** lambdas update the value of a derivation register. These lambdas are invoked with a source document as their only argument and return zero, one, or more documents, which Flow then reduces into the current register value. &#x20;
* **Publish** lambdas publish new documents into a derived collection. Publish lambdas run _after_ an update lambda, if the transformation has one. They’re invoked with a source document, its current register value, and its previous register value (if applicable). In turn, publish lambdas return zero, one, or more documents which are then incorporated into the derived collection.

Flow employs powerful data reductions with the strategy of combining early and often. Documents returned by publish lambdas are not _directly_ added to collections. They’re first reduced by Flow into a single document update for each unique key encountered in the derivation.

To accomplish a stateful processing task, generally, an update lambda first updates the register to reflect one or more encountered documents of interest. A publish lambda then examines a source document, its current register, and prior register. It can compare the prior and current registers to identify meaningful inflections, such as when a sum transitions between negative and positive. Whatever its semantics, it takes action by returning documents that are combined into the derived collection.

{% hint style="info" %}
While Flow is an event-driven system, the update/publish formulation has a direct translation into a traditional batch MapReduce paradigm, which Flow may offer in the future for even faster back-fills over massive datasets.
{% endhint %}
